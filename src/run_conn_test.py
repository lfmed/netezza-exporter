"""Entrypoint do Job de teste de conexão Netezza (mesmo caminho de rede/NCC do dispatcher)."""
import argparse
import base64
import inspect
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))

from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402
from nzexport.procedure import ConnectError, build_client  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--database", default="databricks_postgres")
    p.add_argument("--source-id", required=True)
    p.add_argument("--test-id", required=True)
    p.add_argument("--run-id", type=int)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    w = WorkspaceClient()
    db = Database(LakebaseConfig(endpoint=a.endpoint, database=a.database, w=w), min_size=1, max_size=2)
    try:
        repo.update_connection_test(db, a.test_id, "RUNNING", job_run_id=a.run_id)
        source = repo.get_source(db, a.source_id)
        try:
            client = build_client(source, secrets_reader=lambda scope, key: base64.b64decode(
                w.secrets.get_secret(scope=scope, key=key).value).decode())
            if hasattr(client, "conn_params"):
                results = client.test(repo.procedure_checks(db, a.source_id))
            else:
                results = client.test()
        except ConnectError as e:
            results = [{"check": "conexão", "ok": False, "detail": str(e)}]
        ok = bool(results) and results[0]["ok"] and all(r["ok"] for r in results)
        repo.update_connection_test(db, a.test_id, "OK" if ok else "FAILED", details=results)
        logging.info("teste de conexão %s: %s", a.source_id, results)
    except Exception as e:  # noqa: BLE001 - registra qualquer erro no teste para a tela
        repo.update_connection_test(db, a.test_id, "FAILED",
                                    details=[{"check": "execução do teste", "ok": False, "detail": f"{type(e).__name__}: {e}"}])
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
