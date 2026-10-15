"""lakebase_endpoint: caminho do recurso ou host do Postgres (erro comum no deploy do cliente)."""
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nzexport.db import resolve_endpoint  # noqa: E402

EP = "projects/nzx/branches/production/endpoints/primary"
HOST = "ep-young-recipe-d2bcewpk.database.us-east-1.cloud.databricks.com"


class FakePostgres:
    def __init__(self):
        self.calls = 0

    def list_projects(self):
        self.calls += 1
        return [NS(name="projects/outro"), NS(name="projects/nzx")]

    def list_branches(self, parent):
        return [NS(name=f"{parent}/branches/production")]

    def list_endpoints(self, parent):
        host = HOST if parent.startswith("projects/nzx") else "ep-other.database.us-east-1.cloud.databricks.com"
        return [NS(name=f"{parent}/endpoints/primary", status=NS(hosts=NS(host=host)))]


def w():
    return NS(postgres=FakePostgres())


def test_resource_path_is_used_as_is_without_api_calls():
    c = w()
    assert resolve_endpoint(c, EP) == (EP, None, None)
    assert c.postgres.calls == 0


@pytest.mark.parametrize("value, db", [
    (f"{HOST}/databricks_postgres", "databricks_postgres"),   # o valor do erro do cliente
    (HOST, None),
    (f"postgresql://usr@{HOST}:5432/meu_banco?sslmode=require", "meu_banco"),
    (f" {HOST.upper()}/ ", None),
])
def test_host_forms_resolve_to_the_endpoint(value, db):
    assert resolve_endpoint(w(), value) == (EP, HOST, db)


def test_unknown_host_and_incomplete_path_explain_the_expected_format():
    with pytest.raises(ValueError, match="projects/<projeto>/branches/production/endpoints/primary"):
        resolve_endpoint(w(), "ep-nao-existe.database.us-east-1.cloud.databricks.com")
    with pytest.raises(ValueError, match="incompleto"):
        resolve_endpoint(w(), "projects/nzx/branches/production")
    with pytest.raises(ValueError, match="incompleto"):
        resolve_endpoint(w(), "")
