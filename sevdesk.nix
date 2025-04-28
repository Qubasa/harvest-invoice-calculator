{ buildPythonPackage
, fetchFromGitHub
, requests
, attrs
, cattrs
, httpx
, python-dateutil
, poetry-core
, exceptiongroup
}:

buildPythonPackage {
  pname = "SevDesk-Python-Client";
  version = "2024-06-01";
  src = fetchFromGitHub {
    owner = "Qubasa";
    repo = "SevDesk-Python-Client";
    rev = "0f83b4fbf6c940bbb88652da33538a14c35b37ef";
    sha256 = "sha256-GSYrG+Jk1UJIZOnoUiHhjAMQA+/WsznGS4czlz2YQU4=";
  };
  postPatch = ''
    sed -i -e 's/"^.*"/"*"/' pyproject.toml
    sed -i -e '/openapi-python-client/d' pyproject.toml
    cat pyproject.toml
  '';
  propagatedBuildInputs = [
    requests
    attrs
    cattrs
    httpx
    python-dateutil
    exceptiongroup
  ];
  nativeBuildInputs = [
    poetry-core
  ];
  pyproject = true;
}
