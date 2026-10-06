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
from .procedure import CallSpec, ConnectError, OutcomeUnknown, ProcedureError, build_client
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
    procedure_name: str
    source_table: str
    data_ini: date
    data_fim: date
    timeout_s: int
    max_retries: int
    retry_unknown: bool


def _backoff_sql(attempts: int) -> str:
    minutes = min(30, 2 ** max(0, attempts - 1))
    return f"now() + interval '{minutes} minutes'"


class Dispatcher:
    def __init__(self, db: Database, run_id: int, w: WorkspaceClient | None = None, job_id: int | None = None,
                 idle_exit_s: int = 120, max_runtime_s: int = 12 * 3600, poll_s: float = 5.0):
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
            conn.execute("""INSERT INTO dispatcher_run (run_id) VALUES (%s)
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
                       conn.execute("SELECT source_id, max_sessions FROM source_connection FOR UPDATE").fetchall()}
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
                       v.procedure_name, v.source_table, v.call_timeout_min, v.max_retries, v.retry_unknown
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
                conn.execute(f"""
                    UPDATE export_subblock SET status = 'RUNNING', attempts = %s, lease_owner = %s,
                           lease_expires_at = now() + interval '{LEASE_S} seconds', started_at = now(),
                           finished_at = NULL, next_attempt_at = NULL, updated_at = now()
                    WHERE subblock_id = %s""", (attempt_no, self.owner, c["subblock_id"]))
                spec = CallSpec(c["procedure_name"], c["source_table"], c["data_ini"], c["data_fim"],
                                c["call_timeout_min"] * 60)
                att = conn.execute("""
                    INSERT INTO export_attempt (subblock_id, attempt_no, databricks_run_id, worker, status,
                                                call_statement)
                    VALUES (%s,%s,%s,%s,'RUNNING',%s) RETURNING attempt_id""",
                    (c["subblock_id"], attempt_no, self.run_id, self.owner, spec.describe())).fetchone()
                works.append(Work(str(c["subblock_id"]), str(c["request_id"]), str(c["request_table_id"]),
                                  c["table_config_id"], c["source_id"], attempt_no, str(att["attempt_id"]),
                                  c["procedure_name"], c["source_table"], c["data_ini"], c["data_fim"],
                                  c["call_timeout_min"] * 60, c["max_retries"], c["retry_unknown"]))
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
        spec = CallSpec(work.procedure_name, work.source_table, work.data_ini, work.data_fim, work.timeout_s)
        outcome, err_class, err = "SUCCEEDED", None, None
        try:
            self._client(work.source_id).call(spec)
        except OutcomeUnknown as e:
            outcome, err_class, err = "UNKNOWN", "OUTCOME_UNKNOWN", str(e)
        except ConnectError as e:
            outcome, err_class, err = "FAILED", "CONNECT", str(e)
        except ProcedureError as e:
            outcome, err_class, err = "FAILED", "PROCEDURE", str(e)
        except Exception as e:  # noqa: BLE001
            outcome, err_class, err = "FAILED", "INTERNAL", f"{type(e).__name__}: {e}"
        try:
            self._complete(work, outcome, err_class, err)
            stats[outcome.lower()] += 1
        except Exception:  # noqa: BLE001
            log.exception("falha ao gravar resultado do subbloco %s", work.subblock_id)
        finally:
            with self.lock:
                self.in_flight.pop(work.subblock_id, None)

    def _complete(self, work: Work, outcome: str, err_class: str | None, err: str | None):
        with self.db.tx() as conn:
            conn.execute("""UPDATE export_attempt SET status = %s, finished_at = now(), error_class = %s,
                            error_message = %s WHERE attempt_id = %s""",
                         (outcome, err_class, err, work.attempt_id))
            # condicional ao número da tentativa: se o reaper marcou UNKNOWN, o resultado real prevalece
            guard = "WHERE subblock_id = %s AND attempts = %s AND status IN ('RUNNING','UNKNOWN')"
            if outcome == "SUCCEEDED":
                conn.execute(f"""UPDATE export_subblock SET status = 'DONE', finished_at = now(), last_error = NULL,
                                 lease_owner = NULL, lease_expires_at = NULL, updated_at = now() {guard}""",
                             (work.subblock_id, work.attempt_no))
                return
            retry = work.attempt_no <= work.max_retries and (outcome == "FAILED" or work.retry_unknown)
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

    def _finalize_requests(self):
        with self.db.tx() as conn:
            rows = conn.execute("""
                SELECT r.request_id, r.status, (r.deadline_at IS NOT NULL AND r.deadline_at <= now()) AS expired,
                       c.running, c.pending, c.bad, c.done, c.canceled
                FROM export_request r
                CROSS JOIN LATERAL (
                  SELECT count(*) FILTER (WHERE s.status = 'RUNNING') AS running,
                         count(*) FILTER (WHERE s.status = 'PENDING') AS pending,
                         count(*) FILTER (WHERE s.status IN ('FAILED','UNKNOWN')) AS bad,
                         count(*) FILTER (WHERE s.status = 'DONE') AS done,
                         count(*) FILTER (WHERE s.status = 'CANCELED') AS canceled
                  FROM export_subblock s WHERE s.request_id = r.request_id) c
                WHERE r.status IN ('SUBMITTED','RUNNING','STOPPING')
                FOR UPDATE OF r SKIP LOCKED""").fetchall()
            for r in rows:
                new, reason = None, None
                if r["running"] > 0:
                    continue
                if r["status"] == "STOPPING":
                    new, reason = "STOPPED", "parado pelo usuário"
                elif r["pending"] == 0:
                    new = "COMPLETED" if r["bad"] == 0 else "FAILED"
                    reason = (f"{r['bad']} subbloco(s) com falha ou estado desconhecido" if new == "FAILED" else
                              f"{r['canceled']} subbloco(s) cancelado(s)" if r["canceled"] else None)
                elif r["expired"]:
                    new, reason = "STOPPED", "janela operacional encerrada"
                if new:
                    conn.execute("""UPDATE export_request SET status = %s, status_reason = %s, finished_at = now(),
                                    updated_at = now() WHERE request_id = %s""", (new, reason, r["request_id"]))
                    log_event(conn, r["request_id"], f"REQUEST_{new}", ACTOR,
                              {"done": r["done"], "bad": r["bad"], "pending": r["pending"], "reason": reason})

    # ------------------------------------------------------------ hooks
    def _fire_hooks(self):
        due = self.db.query("""
            WITH rt_done AS (
              SELECT p.request_id, p.request_table_id, p.table_config_id, p.period_start, p.period_end,
                     p.last_finished_at
              FROM v_request_table_progress p
              -- só dispara com a tabela inteira concluída (subbloco cancelado = período não exportado)
              WHERE p.total > p.canceled AND p.done = p.total - p.canceled AND p.canceled_only = 0)
            SELECT h.hook_id, h.job_id, h.trigger_level, d.request_id, d.request_table_id, d.table_config_id,
                   d.period_start, d.period_end
            FROM post_export_hook h JOIN rt_done d
              ON h.trigger_level = 'REQUEST_TABLE' AND (h.table_config_id IS NULL OR h.table_config_id = d.table_config_id)
            -- só cargas concluídas depois da criação do hook (não dispara retroativamente)
            WHERE h.enabled AND d.last_finished_at >= h.created_at
            UNION ALL
            SELECT h.hook_id, h.job_id, h.trigger_level, r.request_id, NULL, NULL, NULL, NULL
            FROM post_export_hook h JOIN export_request r ON r.status = 'COMPLETED'
            WHERE h.enabled AND h.trigger_level = 'REQUEST' AND r.finished_at >= h.created_at
              AND (h.table_config_id IS NULL OR EXISTS (SELECT 1 FROM export_request_table rt
                   WHERE rt.request_id = r.request_id AND rt.table_config_id = h.table_config_id))
        """)
        for d in due:
            with self.db.tx() as conn:
                inv = conn.execute("""
                    INSERT INTO hook_invocation (hook_id, request_id, request_table_id, status)
                    VALUES (%s,%s,%s,'TRIGGERED') ON CONFLICT DO NOTHING RETURNING invocation_id""",
                    (d["hook_id"], d["request_id"], d["request_table_id"])).fetchone()
            if inv is None:
                continue
            params = {"request_id": str(d["request_id"]), "trigger_level": d["trigger_level"],
                      "request_table_id": str(d["request_table_id"] or ""),
                      "table_config_id": d["table_config_id"] or "",
                      "data_ini": str(d["period_start"] or ""), "data_fim": str(d["period_end"] or "")}
            try:
                run = self.w.jobs.run_now(job_id=d["job_id"], job_parameters=params)
                with self.db.tx() as conn:
                    conn.execute("UPDATE hook_invocation SET job_run_id = %s WHERE invocation_id = %s",
                                 (run.run_id, inv["invocation_id"]))
                    log_event(conn, d["request_id"], "HOOK_TRIGGERED", ACTOR,
                              {"hook_id": d["hook_id"], "job_id": d["job_id"], "run_id": run.run_id, **params})
            except Exception as e:  # noqa: BLE001 - falha no hook não afeta a exportação
                with self.db.tx() as conn:
                    conn.execute("UPDATE hook_invocation SET status = 'FAILED', error_message = %s "
                                 "WHERE invocation_id = %s", (str(e)[:2000], inv["invocation_id"]))
                    log_event(conn, d["request_id"], "HOOK_FAILED", ACTOR, {"hook_id": d["hook_id"], "error": str(e)[:500]})


def to_json(stats: dict) -> str:
    return json.dumps(stats)
