"""Recursos compartilhados do app: pool do Lakebase e integração com o Job dispatcher."""
import logging
import os
import threading
import time

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


SECRET_SCOPE = os.environ.get("NETEZZA_SECRET_SCOPE", "nzexport-netezza")


def put_credentials(source_id: str, user: str | None, password: str | None):
    """Grava usuário/senha no secret scope (o app nunca guarda nem exibe as credenciais)."""
    w = workspace()
    if user:
        w.secrets.put_secret(scope=SECRET_SCOPE, key=f"{source_id}-user", string_value=user)
    if password:
        w.secrets.put_secret(scope=SECRET_SCOPE, key=f"{source_id}-password", string_value=password)


def credentials_present(source_id: str) -> dict:
    try:
        keys = {s.key for s in workspace().secrets.list_secrets(scope=SECRET_SCOPE)}
    except Exception:  # noqa: BLE001
        return {"user": False, "password": False, "error": True}
    return {"user": f"{source_id}-user" in keys, "password": f"{source_id}-password" in keys, "error": False}


def start_connection_test(source_id: str, actor: str) -> str:
    """Registra o teste e dispara o Job que testa a conexão pela mesma rede do dispatcher."""
    test_id = repo.create_connection_test(db(), source_id, actor)
    job = os.environ.get("CONN_TEST_JOB_ID")
    if not job:
        repo.update_connection_test(db(), test_id, "FAILED", details=[
            {"check": "execução do teste", "ok": False, "detail": "CONN_TEST_JOB_ID não configurado no app"}])
        return test_id
    try:
        run = workspace().jobs.run_now(job_id=int(job), job_parameters={"source_id": source_id, "test_id": test_id})
        repo.update_connection_test(db(), test_id, "PENDING", job_run_id=run.run_id)
    except Exception as e:  # noqa: BLE001
        repo.update_connection_test(db(), test_id, "FAILED", details=[
            {"check": "execução do teste", "ok": False, "detail": f"falha ao disparar o job: {e}"}])
    return test_id


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


_SCHEDULER_LOCK_KEY = 4207311  # advisory lock: só um processo do app dispara por vez
_scheduler_started = False


def _scheduler_tick():
    with db().tx() as conn:
        if not conn.execute("SELECT pg_try_advisory_xact_lock(%s) AS ok", (_SCHEDULER_LOCK_KEY,)).fetchone()["ok"]:
            return
        if repo.work_due(conn) and repo.live_dispatcher(db()) is None:
            run_id = ensure_dispatcher()
            if run_id:
                repo.log_event(conn, None, "DISPATCHER_AUTO_TRIGGERED", "agendador-app", {"run_id": run_id})


def start_scheduler(interval_s: int = 60):
    """Dispara o dispatcher quando chega a hora de pedidos agendados (ou de retries com espera)."""
    global _scheduler_started
    if _scheduler_started or not _job_id():
        return
    _scheduler_started = True

    def loop():
        while True:
            time.sleep(interval_s)
            try:
                _scheduler_tick()
            except Exception:  # noqa: BLE001 - o agendador nunca deve morrer
                log.exception("falha no agendador")

    threading.Thread(target=loop, name="nzexport-scheduler", daemon=True).start()
