#!/usr/bin/env python3

# This script is currently only used by Jörg, in case someone else is also interested in using it,
# we can make it more flexible.

import argparse
import csv
import datetime
import json
import os
import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Any, NoReturn

from sevdesk import Client
from sevdesk.client.api.check_account import create_check_account, get_check_accounts
from sevdesk.client.api.check_account_transaction import create_transaction
from sevdesk.client.models.check_account_model import (
    CheckAccountModel,
    CheckAccountModelImportType,
    CheckAccountModelStatus,
    CheckAccountModelType,
)
from sevdesk.client.models.check_account_response_model import (
    CheckAccountResponseModelType,
)
from sevdesk.client.models.check_account_transaction_model import (
    CheckAccountTransactionModel,
)
from sevdesk.client.models.check_account_transaction_model_check_account import (
    CheckAccountTransactionModelCheckAccount,
)
from sevdesk.client.models.check_account_transaction_model_status import (
    CheckAccountTransactionModelStatus,
)
from sevdesk.client.types import UNSET, Unset


def die(msg: str) -> NoReturn:
    print(msg, file=sys.stderr)
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    api_token = os.environ.get("SEVDESK_API_TOKEN")
    parser.add_argument(
        "--sevdesk-api-token",
        default=api_token,
        required=api_token is None,
        help="Get one from https://my.sevdesk.de/#/admin/userManagement",
    )
    parser.add_argument(
        "--import-state-file",
        default="import-state.json",
        type=Path,
        help="Used to memorize already imported transactions",
    )
    parser.add_argument(
        "--add-account",
        metavar=("account_number", "currency"),
        nargs=2,
        action="append",
        default=[],
        help='Add a currency to the bank account number mapping (IBAN or account number) i.e. --add-account "BE00 0000 0000 0000" EUR',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually import anything, just print what would be done",
    )
    parser.add_argument(
        "csv_file",
        help="CSV file containing wise bank statements (as opposed to stdin)",
    )
    return parser.parse_args()


class Accounts:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.accounts: dict[str, str] = {}
        self.cache: dict[str, int] = {}

    def add_account(self, account_id: str, currency: str) -> None:
        if currency in self.accounts:
            die(f"Duplicate currency {currency}")
        self.accounts[currency] = account_id

    def get_or_create_account(self, currency: str) -> int:
        account_id = self.accounts.get(currency)
        if account_id is None:
            die(f"Missing account id for currency {currency}")
        if currency in self.cache:
            return self.cache[currency]
        name = f"Wise ({currency}, {account_id})"
        res = get_check_accounts.sync(client=self.client)
        if res is not None and res.objects is not Unset:
            for obj in res.objects:
                # We only want to return accounts that are not registers (German KASSE)
                if (
                    obj.name == name
                    and obj.type != CheckAccountResponseModelType.REGISTER
                ):
                    return obj.id
        account = CheckAccountModel(
            name=name,
            type=CheckAccountModelType.ONLINE,
            currency=currency,
            status=CheckAccountModelStatus.VALUE_100,
            id=UNSET,
            object_name=UNSET,
            create=UNSET,
            update=UNSET,
            sev_client=UNSET,
            import_type=CheckAccountModelImportType.CSV,
            bank_server=UNSET,
            auto_map_transactions=1,
        )

        res = create_check_account.sync(client=self.client, json_body=account)

        if res is None or res.objects is Unset:
            die(f"Failed to create account {name}")
        self.cache[currency] = res.objects.id
        return res.objects.id


# These had to be introduced when switching from the wise API to the CSV export
ALIASES = {
    "CARD_TRANSACTION": "CARD",
    "DIRECT_DEBIT_TRANSACTION": "DIRECT_DEBIT",
}


def import_record(
    client: Client,
    accounts: Accounts,
    record: dict[str, Any],
    import_state: set[str],
    dry_run: bool = False,
) -> None:
    if record["Status"] == "REFUNDED":
        print(f"Skipping refunded transaction {record['ID']}")
        return
    direction = record["Direction"]
    if direction == "IN":
        currency = record["Target currency"]
        payee_payer_name = record["Source name"]
        amount = float(record["Target amount (after fees)"])
    elif direction == "OUT":
        currency = record["Source currency"]
        payee_payer_name = record["Target name"]
        source_fee_str = record["Source fee amount"]
        source_fee = float(source_fee_str) if source_fee_str else 0.0
        amount = -float(record["Source amount (after fees)"]) - source_fee
    else:
        assert (
            direction == "NEUTRAL"
        ), f"Unknown direction {direction} for {record['ID']}"
        print(f"Skipping internal transfer {record['ID']}")
        return

    reference = record["Reference"]

    account_number = accounts.get_or_create_account(currency)
    record_id = record["ID"]
    if "CARD_TRANSACTION" in record_id and reference == "" and direction == "OUT":
        target_currency = record["Target currency"]
        target_amount = record["Target amount (after fees)"]
        reference = f"Card transaction of {target_amount} ({target_currency})"
    for original_name, replacement in ALIASES.items():
        record_id = record_id.replace(original_name, replacement)

    transaction_id = f"{currency}-{account_number}-{record_id}"

    if transaction_id in import_state:
        print(f"Skipping already imported transaction {transaction_id}")
        return

    import_state.add(transaction_id)
    # What timezone is this?
    created_on = datetime.datetime.strptime(record["Created on"], "%Y-%m-%d %H:%M:%S")
    finished_on = datetime.datetime.strptime(record["Finished on"], "%Y-%m-%d %H:%M:%S")
    if created_on > finished_on:
        print(
            f"WARNING: Transaction {transaction_id} has created_on > finished_on, skipping",
            file=sys.stderr,
        )
    transaction = CheckAccountTransactionModel(
        check_account=CheckAccountTransactionModelCheckAccount(
            id=account_number, object_name="CheckAccount"
        ),
        status=CheckAccountTransactionModelStatus.VALUE_100,
        entry_date=created_on,
        value_date=finished_on,
        amount=amount,
        payee_payer_name=payee_payer_name,
        paymt_purpose=reference,
    )
    if dry_run:
        print(
            f"id={record_id} currency={currency} entry_date={record['Created on']}, value_date={record['Finished on']}, amount={amount}, payee_payer_name={payee_payer_name}, paymt_purpose={reference}"
        )
    else:
        create_transaction.sync(client=client, json_body=transaction)


def main() -> None:
    args = parse_args()
    if len(args.add_account) == 0:
        die("No accounts specifed, use --add-account")
    with ExitStack() as exit_stack:
        if args.csv_file:
            csv_file = exit_stack.enter_context(Path(args.csv_file).open(newline=""))
            records = csv.DictReader(csv_file)
        else:
            records = csv.DictReader(sys.stdin)
        client = Client(
            base_url="https://my.sevdesk.de/api/v1", token=args.sevdesk_api_token
        )
        accounts = Accounts(
            client=client,
        )

        for account_number, currency in args.add_account:
            accounts.add_account(account_number, currency)

        client = Client(
            base_url="https://my.sevdesk.de/api/v1", token=args.sevdesk_api_token
        )
        if args.import_state_file.exists():
            imported_transactions = set(json.loads(args.import_state_file.read_text()))
        else:
            imported_transactions = set()

        for record in records:
            import_record(
                client, accounts, record, imported_transactions, dry_run=args.dry_run
            )
            if not args.dry_run:
                args.import_state_file.write_text(
                    json.dumps(sorted(imported_transactions), indent=2)
                )


if __name__ == "__main__":
    main()
