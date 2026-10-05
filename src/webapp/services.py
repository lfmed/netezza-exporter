"""Recursos compartilhados do app: pool do Lakebase e integração com o Job dispatcher."""
import logging
import os
import threading

from databricks.sdk import WorkspaceClient

from nzexport import repo
from nzexport.db import Database, LakebaseConfig

log = logging.getLogger("nzexport.app")
_lock = threading.Lock()
_db: Database | None = None
_w: WorkspaceClient | None = None


def workspace() -> WorkspaceClient:
    global _w
    if _w is None:
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
        _w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    return _w


def db() -> Database:
    global _db
    with _lock:
        if _db is None:
            _db = Database(LakebaseConfig(w=workspace()), min_size=1, max_size=8)
        return _db


def _job_id() -> int | None:
    v = os.environ.get("DISPATCHER_JOB_ID")
    return int(v) if v else None


def _active_runs(job_id: int) -> list:
    return list(workspace().jobs.list_runs(job_id=job_id, active_only=True))


def ensure_dispatcher(force: bool = False) -> int | None:
    """Dispara o Job dispatcher se não houver um ativo. Retorna o run_id disparado."""
    job_id = _job_id()
    if not job_id:
        log.warning("DISPATCHER_JOB_ID não configurado; dispatcher não disparado")
        return None
    try:
        if repo.live_dispatcher(db()) is not None and not force:
            return None
        if _active_runs(job_id):
            return None
        return workspace().jobs.run_now(job_id=job_id).run_id
    except Exception:  # noqa: BLE001 - não bloquear a UI; o usuário pode disparar manualmente
        log.exception("falha ao disparar o dispatcher")
        return None


def cancel_dispatcher(actor: str) -> int:
    job_id = _job_id()
    if not job_id:
        return 0
    runs = _active_runs(job_id)
    for r in runs:
        workspace().jobs.cancel_run(run_id=r.run_id)
    with db().tx() as conn:
        repo.log_event(conn, None, "DISPATCHER_CANCEL_REQUESTED", actor, {"runs": [r.run_id for r in runs]})
    return len(runs)
