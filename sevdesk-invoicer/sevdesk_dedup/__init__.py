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
from typing import Any, NoReturn, Dict, List, Tuple
from collections import defaultdict
import re
from sevdesk import Client
from sevdesk.client.api.check_account import create_check_account, get_check_accounts
from sevdesk.client.api.check_account_transaction import (
    get_transactions, # <-- Import function to get transactions
    delete_check_account_transaction,
    update_check_account_transaction
)
from sevdesk.client.models.check_account_transaction_update_model import (
    CheckAccountTransactionUpdateModel,
)
from sevdesk.client.models.check_account_model import (
    CheckAccountModel,
    CheckAccountModelImportType,
    CheckAccountModelStatus,
    CheckAccountModelType,
)
from sevdesk.client.models.check_account_response_model import (
    CheckAccountResponseModelType,
    CheckAccountResponseModel, # <-- Import for type hinting
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

# --- Constants ---
API_BASE_URL = "https://my.sevdesk.de/api/v1"
TRANSACTION_FETCH_LIMIT = 100 # How many transactions to fetch per API call

# --- Helper Functions ---
def die(msg: str) -> NoReturn:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage sevDesk check accounts and find duplicate transactions."
    )
    api_token = os.environ.get("SEVDESK_API_TOKEN")
    parser.add_argument(
        "--sevdesk-api-token",
        default=api_token,
        required=api_token is None,
        help="Get one from https://my.sevdesk.de/#/admin/userManagement. Can also be set via SEVDESK_API_TOKEN environment variable.",
    )
    parser.add_argument(
        "--account",
        metavar=("ACCOUNT_IDENTIFIER", "CURRENCY"),
        nargs=2,
        required=True, # Made required as the script needs an account to work on
        help='Specify the account identifier (e.g., IBAN, account number, or a custom name used for matching) and its currency. Example: --account "BE00 0000 0000 0000" EUR',
    )
    # --dry-run argument is kept but doesn't affect transaction fetching/analysis
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually create accounts (if they dont exist). Transaction analysis will still run.",
    )
    return parser.parse_args()

# --- Classes ---
class Accounts:
    def __init__(self, client: Client, dry_run: bool = False) -> None:
        self.client = client
        self.dry_run = dry_run
        # Stores mapping from (identifier, currency) tuple to sevDesk account ID
        self.account_map: dict[tuple[str, str], int] = {}
        # Cache for fetched sevDesk accounts by ID
        self.sevdesk_accounts_cache: dict[int, CheckAccountResponseModel] = {}
        # Cache for sevDesk accounts by name (for faster lookup during creation check)
        self.sevdesk_accounts_by_name: dict[str, CheckAccountResponseModel] = {}
        self._load_existing_accounts()

    def _load_existing_accounts(self) -> None:
        """Loads existing non-register accounts from sevDesk."""
        print("Fetching existing sevDesk accounts...")
        try:
            res = get_check_accounts.sync(client=self.client) # Assuming max 1000 accounts
            if res and res.objects is not Unset:
                for acc in res.objects:
                    # We only care about non-register (non-Kasse) accounts for this script's purpose
                    if acc.type != CheckAccountResponseModelType.REGISTER and acc.id and acc.name:
                         # Check if acc.id and acc.name are not None before using them
                        self.sevdesk_accounts_cache[acc.id] = acc
                        self.sevdesk_accounts_by_name[acc.name] = acc
                print(f"Found {len(self.sevdesk_accounts_cache)} existing non-register accounts.")
            else:
                print("No existing accounts found or API error.")
        except Exception as e:
            die(f"Failed to fetch existing accounts: {e}")

    def _generate_account_name(self, identifier: str, currency: str) -> str:
        """Generates a consistent name for the sevDesk account."""
        # Use a clear naming convention. Adjust if needed.
        return f"Wise ({currency}, {identifier})"

    def get_or_create_account_id(self, identifier: str, currency: str) -> int:
        """
        Finds an existing sevDesk account matching the identifier and currency
        or creates a new one. Returns the sevDesk internal account ID.
        """
        account_key = (identifier, currency)
        if account_key in self.account_map:
            return self.account_map[account_key]

        target_name = self._generate_account_name(identifier, currency)
        print(f"Looking for sevDesk account named '{target_name}'...")

        # Check cache first
        existing_account = self.sevdesk_accounts_by_name.get(target_name)
        if existing_account and existing_account.id:
             # Check if existing_account.id is not None before using it
            print(f"Found existing account with ID: {existing_account.id}")
            self.account_map[account_key] = existing_account.id
            return existing_account.id

        # If not found in initial load (e.g., race condition or cache issue), try fetching again just in case
        # This part might be redundant if _load_existing_accounts is reliable.
        res = get_check_accounts.sync(client=self.client, name=target_name)
        if res and res.objects is not Unset:
            for obj in res.objects:
                if (
                    obj.name == target_name
                    and obj.type != CheckAccountResponseModelType.REGISTER
                    and obj.id # Check if obj.id is not None
                ):
                    print(f"Found existing account with ID: {obj.id} (double check)")
                    self.account_map[account_key] = obj.id
                    # Update caches
                    self.sevdesk_accounts_cache[obj.id] = obj
                    self.sevdesk_accounts_by_name[target_name] = obj
                    return obj.id

        # Account does not exist, create it (unless dry run)
        print(f"Account '{target_name}' not found.")
        if self.dry_run:
            print("DRY RUN: Would create account. Exiting.")
            # In dry run, we cannot proceed without an account ID
            die("Cannot proceed in dry run without an existing account.")

        print(f"Creating new account '{target_name}'...")
        account_model = CheckAccountModel(
            name=target_name,
            type=CheckAccountModelType.ONLINE, # Assuming 'Wise' is an online account
            currency=currency.upper(),
            status=CheckAccountModelStatus.VALUE_100, # Active
            import_type=CheckAccountModelImportType.CSV, # Default, adjust if using other import methods later
            auto_map_transactions=Unset, # Let sevDesk handle mapping? Or set to 1? Defaulting to Unset
            # --- Fields that are usually Unset on creation ---
            id=UNSET,
            object_name=UNSET,
            create=UNSET,
            update=UNSET,
            sev_client=UNSET,
            bank_server=UNSET,
            # Set account number if relevant, otherwise UNSET. Using 'identifier' here.
            # Might need adjustment based on whether 'identifier' is IBAN, number etc.
            account_number=identifier,
            # iban=identifier if it looks like an IBAN else UNSET,
            # bic=UNSET, # Add BIC if known/needed
        )

        try:
            res = create_check_account.sync(client=self.client, json_body=account_model)
            if res and res.objects and res.objects.id: # Check if res.objects and res.objects.id are not None
                created_account = res.objects
                print(f"Successfully created account with ID: {created_account.id}")
                self.account_map[account_key] = created_account.id
                # Update caches
                self.sevdesk_accounts_cache[created_account.id] = created_account # Store the response model
                self.sevdesk_accounts_by_name[target_name] = created_account
                return created_account.id
            else:
                 # Check if res is None before accessing res.to_dict()
                details = res.to_dict() if res else "No response details"
                die(f"Failed to create account '{target_name}'. API response: {details}")
        except Exception as e:
            die(f"Exception during account creation for '{target_name}': {e}")


# --- Transaction Functions ---

def get_all_account_transactions(
    client: Client, account_id: int
) -> List[CheckAccountTransactionModel]:
    """Fetches all transactions for a given sevDesk check account ID, handling pagination."""
    all_transactions: List[CheckAccountTransactionModel] = []
    offset = 0
    print(f"Fetching transactions for account ID {account_id}...")
    while True:

        # Use the correct function 'get_transactions' and its expected parameters
        response = get_transactions.sync( # <--- Corrected function call
            client=client,
            check_accountid=account_id,    # Assuming this parameter filters by account
            check_accountobject_name="CheckAccount",
            offset=offset,
        )

        # The rest of the logic should remain the same
        if response and response.objects is not Unset and response.objects:
            fetched_count = len(response.objects)
            all_transactions.extend(response.objects)
            offset += fetched_count
            # Check if we received fewer transactions than the limit, meaning we reached the end
            if fetched_count < TRANSACTION_FETCH_LIMIT:
                break
        else:
            # No more transactions found or an issue occurred
            print("  No more transactions found or API error on fetch.")
            break

    print(f"Total transactions fetched for account {account_id}: {len(all_transactions)}")
    return all_transactions

def find_duplicate_transactions(
    transactions: List[CheckAccountTransactionModel],
) -> Dict[Tuple[datetime.date, float], List[CheckAccountTransactionModel]]:
    """
    Groups transactions by (date, amount) and returns groups with more than one transaction.
    """
    grouped_transactions: Dict[Tuple[datetime.date, float], List[CheckAccountTransactionModel]] = defaultdict(list)
    potential_duplicates: Dict[Tuple[datetime.date, float], List[CheckAccountTransactionModel]] = {}

    print("\nAnalyzing transactions for potential duplicates (same date and amount)...")
    skipped_count = 0
    for tx in transactions:
        try:
            # Ensure entry_date and amount are present and valid
            if tx.entry_date is Unset or tx.amount is Unset or tx.entry_date is None or tx.amount is None:
                print(f"  Skipping transaction ID {tx.id or 'N/A'} due to missing date or amount.", file=sys.stderr)
                skipped_count += 1
                continue

            # Parse the date string (assuming ISO format like YYYY-MM-DD or with time)
            entry_date_str = str(tx.entry_date)
            # Use datetime.fromisoformat which handles standard ISO formats like
            # YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...
            # This will raise ValueError if the format is not recognized.
            tx_datetime_obj = datetime.datetime.fromisoformat(entry_date_str)
            tx_date = tx_datetime_obj.date()


            # Convert amount string to float
            tx_amount = float(str(tx.amount)) # Amount might be negative or positive

            key = (tx_date, tx_amount)
            grouped_transactions[key].append(tx)

        except (ValueError, TypeError) as e:
            print(f"  Skipping transaction ID {tx.id or 'N/A'} due to parsing error (date='{tx.entry_date}', amount='{tx.amount}'): {e}", file=sys.stderr)
            skipped_count += 1
            continue
        except Exception as e: # Catch other potential errors
             print(f"  Skipping transaction ID {tx.id or 'N/A'} due to unexpected error: {e}", file=sys.stderr)
             skipped_count += 1
             continue

    if skipped_count > 0:
        print(f"Skipped {skipped_count} transactions due to missing data or errors.")

    # Filter for groups with more than one transaction
    for key, tx_list in grouped_transactions.items():
        if len(tx_list) > 1:
            potential_duplicates[key] = tx_list

    print(f"Found {len(potential_duplicates)} groups of transactions with the same date and amount.")
    return potential_duplicates


def ask_for_confirmation(prompt: str) -> bool:
    """Asks the user for confirmation (y/N)."""
    while True:
        response = input(f"{prompt} (y/N): ").lower().strip()
        if response.startswith('y'):
            return True
        elif response.startswith('n') or response == "": # Default to No
            return False
        else:
            print("Please answer 'y' or 'n'.")

# --- Main Execution ---
def main() -> None:
    args = parse_args()

    # Create SevDesk client
    client = Client(base_url=API_BASE_URL, token=args.sevdesk_api_token)

    # Initialize Accounts helper
    accounts = Accounts(client=client, dry_run=args.dry_run)

    # Get the sevDesk internal ID for the specified account
    account_identifier, currency = args.account
    try:
        sevdesk_account_id = accounts.get_or_create_account_id(account_identifier, currency)
    except SystemExit as e:
        # Let die() handle the exit, but catch it here to avoid traceback for expected exits
        sys.exit(e.code) # Propagate the exit code from die()

    print(f"\nWorking with sevDesk account ID: {sevdesk_account_id}")

    # Fetch all transactions for this account
    transactions = get_all_account_transactions(client, sevdesk_account_id)

    if not transactions:
        print("No transactions found for this account.")
        return # Exit cleanly if no transactions

    # Find transactions with the same date and amount
    duplicate_groups = find_duplicate_transactions(transactions)
    transactions_to_delete_ids: list[int] = []
    specific_duplicates_found = 0
    # Print the results
    if not duplicate_groups:
        print("\nNo transactions found with the same date and amount.")
    else:
        print("\n--- Potential Duplicate Transactions ---")
        for (tx_date, tx_amount), tx_list in duplicate_groups.items():
            earliest_tx = None
            for tx in tx_list:
                if earliest_tx is None or tx.create < earliest_tx.create:
                    earliest_tx = tx

            for tx in tx_list:
               if tx.create > earliest_tx.create:
                    pattern = r"Card transaction of \d+\.\d+ \([A-Z]+\)"

                    # Ensure paymt_purpose is available and is a string before matching
                    purpose_str = str(tx.paymt_purpose)

                    # Check if the pattern is found within the payment purpose string
                    if re.search(pattern, purpose_str):
                        print("\n-----------------------------------------------")
                        print(f"  [ORIGINAL] ID: {earliest_tx.id}, Created: {earliest_tx.create}, Purpose: '{earliest_tx.paymt_purpose}'")
                        print(f"  [DUPLICATE] ID: {tx.id}, Created: {tx.create}, Purpose: '{purpose_str}' <-- Targeted for Deletion")
                        transactions_to_delete_ids.append(tx.id) # Add ID to the list for deletion
                        specific_duplicates_found += 1
                    elif tx.paymt_purpose == earliest_tx.paymt_purpose:
                        print("\n-----------------------------------------------")
                        print(f"  [ORIGINAL] ID: {earliest_tx.id} Cost: {earliest_tx.amount}, Created: {earliest_tx.create}, Purpose: '{earliest_tx.paymt_purpose}'")
                        print(f"  [DUPLICATE] ID: {tx.id} Cost: {tx.amount}, Created: {tx.create}, Purpose: '{purpose_str}' <-- Targeted for Deletion")
                        transactions_to_delete_ids.append(tx.id) # Add ID to the list for deletion
                        specific_duplicates_found += 1


     # --- Confirmation and Deletion Phase ---
    if not transactions_to_delete_ids:
        print("\nNo transactions marked for deletion.")
        return

    print("\n--- Deletion Confirmation ---")
    if args.dry_run:
        print(f"DRY RUN: No actual deletions will occur. Found {len(transactions_to_delete_ids)} transactions marked for deletion.")
        return # Exit after listing in dry run

    # Ask for confirmation for the whole batch
    if ask_for_confirmation(f"Proceed with deleting {len(transactions_to_delete_ids)} identified duplicate transaction(s)?"):
        print("\nUser confirmed. Proceeding with deletions...")
        deleted_count = 0
        failed_count = 0
        for tx_id in transactions_to_delete_ids:
            print(f"  Attempting to delete transaction ID: {tx_id}...")
            try:
                # # 1. Mark transaction as open (status 100) before deleting
                # print(f"    Marking transaction {tx_id} as open (status 100)...")
                # update_payload = CheckAccountTransactionUpdateModel(
                #     status=CheckAccountTransactionModelStatus.VALUE_100 # Assuming 100 means 'open'/'created'
                # )

                # # Ensure the necessary import is present at the top:
                # # from sevdesk.client.api.check_account_transaction import update_check_account_transaction
                # # from sevdesk.client.models.update_check_account_transaction_model import UpdateCheckAccountTransactionModel
                # update_response = update_check_account_transaction.sync(
                #     client=client,
                #     check_account_transaction_id=tx_id,
                #     json_body=update_payload
                # )
                # # Optional: Check update_response if needed, sync often returns the updated object or raises on error
                # print(f"      Transaction {tx_id} marked as open.")

                # 2. Delete the transaction
                print(f"    Deleting transaction {tx_id}...")
                # Call the delete API function
                response = delete_check_account_transaction.sync(
                    client=client,
                    check_account_transaction_id=tx_id
                )
                # Check response - sync delete often returns None on success (204 No Content)
                # or raises an exception on failure.
                print(f"    Successfully deleted transaction ID: {tx_id}")
                deleted_count += 1
            except Exception as e:
                print(f"    FAILED to delete transaction ID: {tx_id}. Error: {e}", file=sys.stderr)
                failed_count += 1

        print(f"\nDeletion Summary:")
        print(f"  Successfully deleted: {deleted_count}")
        print(f"  Failed to delete: {failed_count}")
    else:
        print("\nUser declined. No transactions were deleted.")


if __name__ == "__main__":
    main()