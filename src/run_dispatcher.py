"""Entrypoint do Job dispatcher (spark_python_task em serverless)."""
import argparse
import inspect
import logging
import os
import sys

# spark_python_task executa via exec() sem __file__; co_filename traz o caminho real do script
sys.path.insert(0, os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))

from databricks.sdk import WorkspaceClient  # noqa: E402

from nzexport import migrate  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402
from nzexport.dispatcher import Dispatcher  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--database", default="databricks_postgres")
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--job-id", type=int)
    p.add_argument("--idle-exit-s", type=int, default=120)
    p.add_argument("--max-runtime-min", type=int, default=720)
    p.add_argument("--grant-role", default="", help="roles (vírgula) que recebem acesso ao schema")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    w = WorkspaceClient()
    cfg = LakebaseConfig(endpoint=a.endpoint, database=a.database, w=w)
    migrate.run(cfg, grant_to=[r.strip() for r in a.grant_role.split(",") if r.strip()])
    db = Database(cfg, min_size=2, max_size=16)
    try:
        Dispatcher(db, run_id=a.run_id, w=w, job_id=a.job_id, idle_exit_s=a.idle_exit_s,
                   max_runtime_s=a.max_runtime_min * 60).run()
    finally:
        db.close()


if __name__ == "__main__":
    main()
