"""Pós-deploy: orientação para falhas ao conectar no Lakebase (antes, um PoolTimeout escondia a causa)."""
import sys
from pathlib import Path
from types import SimpleNamespace as NS

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pos_deploy import dica_banco  # noqa: E402

CFG = NS(endpoint="projects/nzx/branches/production/endpoints/primary", database="databricks_postgres",
         host="ep-x.database.us-east-1.cloud.databricks.com")
SP = {"principal": "50608dd3-aa99-4e4e-9479-522491bb7780", "sp": True, "nome": "50608dd3-aa99-4e4e-9479-522491bb7780"}


def test_missing_role_points_to_create_role_for_the_jobs_identity():
    d = dica_banco(Exception('FATAL: password authentication failed for user "50608dd3-..."'), CFG, SP)
    assert "create-role projects/nzx/branches/production --role-id 50608dd3-aa99-4e4e-9479-522491bb7780" in d
    assert "SERVICE_PRINCIPAL" in d and "GRANT CREATE ON DATABASE databricks_postgres" in d


def test_schema_owned_by_someone_else_and_network():
    assert "OWNER TO" in dica_banco(Exception("permission denied for schema nzexport"), CFG, SP)
    assert "rede" in dica_banco(Exception("connection timeout expired"), CFG, SP)
