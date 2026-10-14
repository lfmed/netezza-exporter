"""Dispatcher: drena a fila de subblocos de todos os pedidos respeitando os limites de concorrência.

Limites aplicados a cada claim (o menor vence):
  * global por origem Netezza  -> source_connection.max_sessions
  * por tabela                 -> table_config_version.max_parallel
  * por pedido (definido no app) -> export_request.max_parallel
"""
import json
import logging
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date

from databricks.sdk import WorkspaceClient

from .db import Database
from .procedure import (CallSpec, ConfigError, ConnectError, OutcomeUnknown, ProcedureError, build_call,
                        build_client)
from . import lifecycle
from .repo import log_event

log = logging.getLogger("nzexport.dispatcher")

ACTOR = "dispatcher"
MAX_WORKERS = 64
LEASE_S = 180
RENEW_EVERY_S = 30


@dataclass
class Work:
    subblock_id: str
    request_id: str
    request_table_id: str
    table_config_id: str
    source_id: str
    attempt_no: int
    attempt_id: str
    spec: CallSpec
    data_ini: date
    data_fim: date
    max_retries: int
    retry_unknown: bool
    file_name: str | None = None     # .dat que a procedure gera (None = sem importação)


def _backoff_sql(attempts: int) -> str:
    minutes = min(30, 2 ** max(0, attempts - 1))
    return f"now() + interval '{minutes} minutes'"


class Dispatcher:
    def __init__(self, db: Database, run_id: int, w: WorkspaceClient | None = None, job_id: int | None = None,
                 idle_exit_s: int = 120, max_runtime_s: int = 12 * 3600, poll_s: float = 5.0,
                 importer_job_id: int | None = None):
        self.db = db
        self.run_id = run_id
        self.owner = f"run-{run_id}"
        self.w = w
        self.job_id = job_id
        self.idle_exit_s = idle_exit_s
        self.max_runtime_s = max_runtime_s
        self.poll_s = poll_s
        self.pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="nzcall")
        self.in_flight: dict[str, Work] = {}
        self.lock = threading.Lock()
        self._clients: dict[str, tuple[float, object]] = {}
        self.importer_job_id = importer_job_id
        self._import_due = False
        self._last_import_trigger = 0.0

    # ------------------------------------------------------------ ciclo principal
    def run(self) -> dict:
        started = time.time()
        idle_since = None
        last_renew = 0.0
        stats = Counter()
        self._register()
        try:
            while True:
                self._reap_expired(stats)
                self._finalize_requests()
                self._fire_hooks()
                self._trigger_importer()
                draining = time.time() - started > self.max_runtime_s
                if not draining:
                    free = MAX_WORKERS - len(self.in_flight)
                    for work in self._claim(free) if free > 0 else []:
                        with self.lock:
                            self.in_flight[work.subblock_id] = work
                        stats["claimed"] += 1
                        self.pool.submit(self._execute, work, stats)
                if time.time() - last_renew > RENEW_EVERY_S:
                    self._renew()
                    last_renew = time.time()

                busy = bool(self.in_flight)
                if busy or (not draining and self._has_future_work()):
                    idle_since = None
                else:
                    idle_since = idle_since or time.time()
                    if draining or time.time() - idle_since >= self.idle_exit_s:
                        break
                time.sleep(self.poll_s)
        finally:
            self.pool.shutdown(wait=True)
            self._finalize_requests()
            self._fire_hooks()
            self._trigger_importer(force=True)
            self._finish()
        if self._has_future_work() and self.w and self.job_id:
            # tempo máximo atingido com trabalho restante: agenda a continuação (fila do job)
            self.w.jobs.run_now(job_id=self.job_id)
            stats["retriggered"] += 1
        log.info("dispatcher finalizado: %s", dict(stats))
        return dict(stats)

    # ------------------------------------------------------------ registro do run
    def _register(self):
        with self.db.tx() as conn:
            conn.execute("""INSERT INTO dispatcher_run (run_id, kind) VALUES (%s, 'dispatcher')
                            ON CONFLICT (run_id) DO UPDATE SET heartbeat_at = now(), finished_at = NULL,
                            status = 'RUNNING'""", (self.run_id,))

    def _finish(self):
        with self.db.tx() as conn:
            conn.execute("""UPDATE dispatcher_run SET finished_at = now(), status = 'FINISHED', in_flight = 0
                            WHERE run_id = %s""", (self.run_id,))

    def _renew(self):
        with self.lock:
            ids = list(self.in_flight)
        with self.db.tx() as conn:
            conn.execute("UPDATE dispatcher_run SET heartbeat_at = now(), in_flight = %s WHERE run_id = %s",
                         (len(ids), self.run_id))
            if ids:
                conn.execute(f"""UPDATE export_subblock SET lease_expires_at = now() + interval '{LEASE_S} seconds'
                                 WHERE subblock_id = ANY(%s) AND status = 'RUNNING' AND lease_owner = %s""",
                             (ids, self.owner))

    # ------------------------------------------------------------ claim
    def _claim(self, free_slots: int) -> list[Work]:
        with self.db.tx() as conn:
            # trava as origens: serializa claims concorrentes e garante o limite global
            sources = {s["source_id"]: s for s in
                       conn.execute("SELECT * FROM source_connection FOR UPDATE").fetchall()}
            running = conn.execute("""
                SELECT s.request_id, s.table_config_id, tc.source_id, count(*) AS n
                FROM export_subblock s JOIN table_config tc USING (table_config_id)
                WHERE s.status = 'RUNNING' GROUP BY 1, 2, 3""").fetchall()
            by_source, by_table, by_request = Counter(), Counter(), Counter()
            for r in running:
                by_source[r["source_id"]] += r["n"]
                by_table[r["table_config_id"]] += r["n"]
                by_request[r["request_id"]] += r["n"]

            candidates = conn.execute("""
                SELECT s.subblock_id, s.request_id, s.request_table_id, s.table_config_id, s.attempts,
                       lower(s.data_range) AS data_ini, upper(s.data_range) - 1 AS data_fim,
                       r.max_parallel AS req_max, tc.source_id, v.max_parallel AS tbl_max,
                       v.call_profile, v.procedure_name, v.source_database, v.source_table, v.ext_database,
                       v.ext_schema, v.time_column, v.date_format, v.dest_catalog, v.dest_schema, v.dest_table,
                       v.call_timeout_min, v.max_retries, v.retry_unknown
                FROM export_subblock s
                JOIN export_request r USING (request_id)
                JOIN export_request_table rt USING (request_table_id)
                JOIN table_config tc ON tc.table_config_id = s.table_config_id
                JOIN table_config_version v ON v.table_config_id = rt.table_config_id
                                           AND v.config_version = rt.config_version
                WHERE s.status = 'PENDING' AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= now())
                  AND r.status IN ('SUBMITTED', 'RUNNING')
                  AND (r.scheduled_start_at IS NULL OR r.scheduled_start_at <= now())
                  AND (r.deadline_at IS NULL OR r.deadline_at > now())
                  AND tc.active
                -- pedidos em ordem de chegada; dentro de cada tabela, do período mais recente para o mais antigo
                ORDER BY r.submitted_at, rt.ordinal, lower(s.data_range) DESC
                LIMIT 2000
                FOR UPDATE OF s SKIP LOCKED""").fetchall()

            picked = []
            for c in candidates:
                if len(picked) >= free_slots:
                    break
                src = sources.get(c["source_id"])
                if (src is None or by_source[c["source_id"]] >= src["max_sessions"]
                        or by_table[c["table_config_id"]] >= c["tbl_max"]
                        or by_request[c["request_id"]] >= c["req_max"]):
                    continue
                by_source[c["source_id"]] += 1
                by_table[c["table_config_id"]] += 1
                by_request[c["request_id"]] += 1
                picked.append(c)

            works = []
            for c in picked:
                attempt_no = c["attempts"] + 1
                try:
                    spec = build_call(c, sources[c["source_id"]], c["data_ini"], c["data_fim"])
                    config_error = None
                except ConfigError as e:
                    spec, config_error = None, str(e)
                conn.execute(f"""
                    UPDATE export_subblock SET status = 'RUNNING', attempts = %s, lease_owner = %s,
                           lease_expires_at = now() + interval '{LEASE_S} seconds', started_at = now(),
                           finished_at = NULL, next_attempt_at = NULL, updated_at = now()
                    WHERE subblock_id = %s""", (attempt_no, self.owner, c["subblock_id"]))
                att = conn.execute("""
                    INSERT INTO export_attempt (subblock_id, attempt_no, databricks_run_id, worker, status,
                                                call_statement, output_object)
                    VALUES (%s,%s,%s,%s,'RUNNING',%s,%s) RETURNING attempt_id""",
                    (c["subblock_id"], attempt_no, self.run_id, self.owner,
                     spec.describe() if spec else None, spec.output_object if spec else None)).fetchone()
                has_dest = bool(c["dest_catalog"] and c["dest_schema"] and c["dest_table"])
                file_name = (spec.extra.get("data_object", "").rsplit("/", 1)[-1] or None) if spec and has_dest else None
                work = Work(str(c["subblock_id"]), str(c["request_id"]), str(c["request_table_id"]),
                            c["table_config_id"], c["source_id"], attempt_no, str(att["attempt_id"]),
                            spec, c["data_ini"], c["data_fim"], c["max_retries"], c["retry_unknown"], file_name)
                if config_error:
                    # cadastro inválido: falha definitiva sem chamar o Netezza
                    self._complete(work, "FAILED", "CONFIG", config_error, conn=conn)
                    continue
                works.append(work)
            if works:
                conn.execute("""UPDATE export_request SET status = 'RUNNING', started_at = coalesce(started_at, now()),
                                updated_at = now() WHERE request_id = ANY(%s) AND status = 'SUBMITTED'""",
                             (list({w.request_id for w in works}),))
            return works

    def _has_future_work(self) -> bool:
        row = self.db.one("""
            SELECT EXISTS (SELECT 1 FROM export_subblock s JOIN export_request r USING (request_id)
                           JOIN table_config tc ON tc.table_config_id = s.table_config_id
                           WHERE s.status = 'PENDING' AND r.status IN ('SUBMITTED','RUNNING')
                             -- pedidos agendados para depois não seguram o dispatcher: o app o dispara na hora
                             AND (r.scheduled_start_at IS NULL OR r.scheduled_start_at <= now())
                             AND (r.deadline_at IS NULL OR r.deadline_at > now()) AND tc.active) AS x""")
        return row["x"]

    # ------------------------------------------------------------ execução
    def _client(self, source_id: str):
        cached = self._clients.get(source_id)
        if cached and time.time() - cached[0] < 300:
            return cached[1]
        src = self.db.one("SELECT * FROM source_connection WHERE source_id = %s", (source_id,))
        client = build_client(src, secrets_reader=self._read_secret)
        self._clients[source_id] = (time.time(), client)
        return client

    def _read_secret(self, scope: str, key: str) -> str:
        import base64
        return base64.b64decode(self.w.secrets.get_secret(scope=scope, key=key).value).decode()

    def _execute(self, work: Work, stats: Counter):
        outcome, err_class, err, result = "SUCCEEDED", None, None, None
        try:
            result = self._client(work.source_id).call(work.spec)
        except OutcomeUnknown as e:
            outcome, err_class, err = "UNKNOWN", "OUTCOME_UNKNOWN", str(e)
        except ConnectError as e:
            outcome, err_class, err = "FAILED", "CONNECT", str(e)
        except ConfigError as e:
            outcome, err_class, err = "FAILED", "CONFIG", str(e)
        except ProcedureError as e:
            outcome, err_class, err = "FAILED", "PROCEDURE", str(e)
        except Exception as e:  # noqa: BLE001
            outcome, err_class, err = "FAILED", "INTERNAL", f"{type(e).__name__}: {e}"
        try:
            self._complete(work, outcome, err_class, err, result=result)
            stats[outcome.lower()] += 1
        except Exception:  # noqa: BLE001
            log.exception("falha ao gravar resultado do subbloco %s", work.subblock_id)
        finally:
            with self.lock:
                self.in_flight.pop(work.subblock_id, None)

    def _complete(self, work: Work, outcome: str, err_class: str | None, err: str | None,
                  result: str | None = None, conn=None):
        if conn is None:
            with self.db.tx() as c:
                return self._complete(work, outcome, err_class, err, result=result, conn=c)
        conn.execute("""UPDATE export_attempt SET status = %s, finished_at = now(), error_class = %s,
                        error_message = %s, call_result = %s WHERE attempt_id = %s""",
                     (outcome, err_class, err, result, work.attempt_id))
        # condicional ao número da tentativa: se o reaper marcou UNKNOWN, o resultado real prevalece
        guard = "WHERE subblock_id = %s AND attempts = %s AND status IN ('RUNNING','UNKNOWN')"
        if outcome == "SUCCEEDED":
            # com destino no Databricks, o subbloco aguarda o importador; sem destino, termina aqui
            status = "EXPORTED" if work.file_name else "DONE"
            conn.execute(f"""UPDATE export_subblock SET status = %s, file_name = %s, exported_at = now(),
                             finished_at = now(), last_error = NULL, import_attempts = 0,
                             lease_owner = NULL, lease_expires_at = NULL, updated_at = now() {guard}""",
                         (status, work.file_name, work.subblock_id, work.attempt_no))
            if work.file_name:
                self._import_due = True
            return
        retry = (work.attempt_no <= work.max_retries and err_class != "CONFIG"
                 and (outcome == "FAILED" or work.retry_unknown))
        new_status = "PENDING" if retry else outcome
        next_at = _backoff_sql(work.attempt_no) if retry else "NULL"
        conn.execute(f"""UPDATE export_subblock SET status = %s, next_attempt_at = {next_at},
                         finished_at = now(), last_error = %s, lease_owner = NULL, lease_expires_at = NULL,
                         updated_at = now() {guard}""",
                     (new_status, f"[{err_class}] {err}", work.subblock_id, work.attempt_no))
        log_event(conn, work.request_id, f"SUBBLOCK_{outcome}", ACTOR,
                  {"attempt": work.attempt_no, "error_class": err_class, "error": (err or "")[:500],
                   "next_status": new_status, "data_ini": work.data_ini, "data_fim": work.data_fim},
                  work.subblock_id)

    # ------------------------------------------------------------ reconciliação
    def _reap_expired(self, stats: Counter):
        """Subblocos RUNNING com lease vencido (worker morreu): resultado desconhecido."""
        with self.db.tx() as conn:
            rows = conn.execute("""
                SELECT s.subblock_id, s.request_id, s.attempts, v.max_retries, v.retry_unknown
                FROM export_subblock s
                JOIN export_request_table rt USING (request_table_id)
                JOIN table_config_version v ON v.table_config_id = rt.table_config_id
                                           AND v.config_version = rt.config_version
                WHERE s.status = 'RUNNING' AND s.lease_expires_at < now()
                  AND NOT (s.lease_owner = %s AND s.subblock_id = ANY(%s))
                FOR UPDATE OF s SKIP LOCKED""", (self.owner, list(self.in_flight))).fetchall()
            for r in rows:
                retry = r["retry_unknown"] and r["attempts"] <= r["max_retries"]
                new_status = "PENDING" if retry else "UNKNOWN"
                conn.execute(f"""UPDATE export_subblock SET status = %s,
                                 next_attempt_at = {_backoff_sql(r['attempts']) if retry else 'NULL'},
                                 last_error = '[LEASE_EXPIRED] worker perdeu o heartbeat; resultado desconhecido',
                                 lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                                 WHERE subblock_id = %s""", (new_status, r["subblock_id"]))
                conn.execute("""UPDATE export_attempt SET status = 'ABANDONED', finished_at = now(),
                                error_class = 'LEASE_EXPIRED'
                                WHERE subblock_id = %s AND attempt_no = %s AND status = 'RUNNING'""",
                             (r["subblock_id"], r["attempts"]))
                log_event(conn, r["request_id"], "SUBBLOCK_LEASE_EXPIRED", ACTOR, {"next_status": new_status},
                          r["subblock_id"])
                stats["reaped"] += 1

    def _trigger_importer(self, force: bool = False):
        """Arquivos exportados: garante o importador rodando (no máximo uma tentativa a cada 30 s)."""
        if not (self._import_due and self.w and self.importer_job_id):
            return
        if not force and time.time() - self._last_import_trigger < 30:
            return
        self._last_import_trigger = time.time()
        try:
            if lifecycle.ensure_importer(self.db, self.w, self.importer_job_id):
                log.info("importador disparado")
            self._import_due = False
        except Exception:  # noqa: BLE001 - o agendador do app também dispara o importador
            log.exception("falha ao disparar o importador")

    def _finalize_requests(self):
        lifecycle.finalize_requests(self.db)

    def _fire_hooks(self):
        lifecycle.fire_hooks(self.db, self.w)


def to_json(stats: dict) -> str:
    return json.dumps(stats)
