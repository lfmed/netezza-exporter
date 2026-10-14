"""App Flask: plano de controle do exportador Netezza."""
import logging
import os
import re
from pathlib import Path
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from flask import Flask, Response, abort, flash, jsonify, redirect, render_template, request, url_for

from nzexport import repo, table_csv
from nzexport.planner import UNITS
from nzexport.procedure import (DATE_FORMATS, DEFAULT_PROFILE, PROFILES, build_call, validate_destination,
                                validate_table_config)

from . import catalog, services

log = logging.getLogger("nzexport.app")
APP_VERSION = (Path(__file__).resolve().parents[1] / "VERSION").read_text().strip()
APP_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Sao_Paulo"))  # datas/horas digitadas e exibidas

STATUS_LABEL = {
    "SUBMITTED": "Na fila", "RUNNING": "Executando", "STOPPING": "Parando", "STOPPED": "Parado",
    "FAILED": "Falhou", "COMPLETED": "Concluído", "CANCELED": "Encerrado",
    "PENDING": "Pendente", "DONE": "Concluído", "UNKNOWN": "Desconhecido", "SUPERSEDED": "Substituído",
    "SUCCEEDED": "Sucesso", "ABANDONED": "Abandonado",
    "SCHEDULED": "Agendado", "COMPLETA": "Completa", "EM_ANDAMENTO": "Em andamento", "INCOMPLETA": "Incompleta (lacunas)",
    "NAO_INICIADA": "Não iniciada", "COM_FALHAS": "Com falhas", "SEM_ESCOPO": "Sem escopo",
    "ESTADO_DESCONHECIDO": "Estado desconhecido", "INATIVA": "Inativa", "THIS_REQUEST": "Neste pedido",
}
UNIT_LABEL = {"DAY": "dia(s)", "WEEK": "semana(s)", "MONTH": "mês(es)", "YEAR": "ano(s)"}


def _user() -> str:
    return (request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-Preferred-Username")
            or os.environ.get("LOCAL_USER", "local"))


def _local_dt(v: str | None) -> datetime | None:
    """datetime-local do formulário (sem fuso) interpretado no fuso do app."""
    return datetime.fromisoformat(v).replace(tzinfo=APP_TZ) if v else None


def _date(v: str) -> date:
    return datetime.strptime(v, "%Y-%m-%d").date()


def _int(v, default=None, lo=None, hi=None):
    try:
        n = int(v)
    except (TypeError, ValueError):
        if default is None:
            raise ValueError(f"número inválido: {v!r}")
        return default
    if (lo is not None and n < lo) or (hi is not None and n > hi):
        raise ValueError(f"valor fora do intervalo [{lo}, {hi}]: {n}")
    return n


def _parse_table_rows(form) -> list[repo.TableRequest]:
    rows = []
    tables = form.getlist("table")
    for i, tid in enumerate(tables):
        if not tid:
            continue
        reprocess = form.getlist("reprocess")[i] == "1"
        rows.append(repo.TableRequest(
            table_config_id=tid,
            period_start=_date(form.getlist("start")[i]),
            period_end=_date(form.getlist("end")[i]),
            subblock_unit=form.getlist("unit")[i] if form.getlist("unit")[i] in UNITS else "MONTH",
            subblock_size=_int(form.getlist("size")[i], lo=1, hi=1000),
            calendar_aligned=form.getlist("aligned")[i] == "1",
            is_reprocess=reprocess,
            reprocess_reason=(form.getlist("reason")[i] or "").strip() or None,
        ))
    if not rows:
        raise ValueError("selecione pelo menos uma tabela")
    for r in rows:
        if r.period_start > r.period_end:
            raise ValueError(f"{r.table_config_id}: data inicial maior que a final")
        if r.is_reprocess and not r.reprocess_reason:
            raise ValueError(f"{r.table_config_id}: reprocessamento exige justificativa")
    return rows


FORM_ROW_FIELDS = ("table", "start", "end", "size", "unit", "aligned", "reprocess", "reason")


def _form_rows(form) -> list[dict]:
    """Linhas do formulário para repopular a tela após erro."""
    if not form:
        return []
    cols = {k: form.getlist(k) for k in FORM_ROW_FIELDS}
    return [{k: (cols[k][i] if i < len(cols[k]) else "") for k in FORM_ROW_FIELDS}
            for i in range(len(cols["table"]))]


def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(24)
    app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # upload de CSV
    app.jinja_env.globals.update(STATUS_LABEL=STATUS_LABEL, UNIT_LABEL=UNIT_LABEL, UNITS=UNITS,
                                  APP_VERSION=APP_VERSION, PROFILES=PROFILES, DEFAULT_PROFILE=DEFAULT_PROFILE,
                                  DATE_FORMATS=list(DATE_FORMATS), DEMO_INI=date(2016, 1, 1),
                                  DEMO_FIM=date(2016, 1, 31))

    def call_preview(cfg, source, data_ini, data_fim) -> str:
        try:
            return build_call(cfg, source, data_ini, data_fim).statement()
        except Exception as e:  # noqa: BLE001 - prévia: mostra o problema de cadastro
            return f"⚠ cadastro incompleto: {e}"
    app.jinja_env.globals["call_preview"] = call_preview

    @app.template_filter("dt")
    def _fmt_dt(v):
        if not v:
            return "—"
        if isinstance(v, datetime):
            return v.astimezone(APP_TZ).strftime("%d/%m/%Y %H:%M:%S")
        return v.strftime("%d/%m/%Y") if isinstance(v, date) else str(v)

    @app.template_filter("pct")
    def _pct(done, total):
        return 0 if not total else int(round(100 * (done or 0) / total))

    @app.template_filter("dur")
    def _dur(start, end=None):
        if not start:
            return "—"
        end = end or datetime.now(start.tzinfo)
        s = int((end - start).total_seconds())
        return f"{s // 3600}h{(s % 3600) // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"

    @app.template_filter("dtlocal")
    def _fmt_dtlocal(v):
        """Valor para <input type=datetime-local>."""
        return v.astimezone(APP_TZ).strftime("%Y-%m-%dT%H:%M") if isinstance(v, datetime) else ""

    @app.context_processor
    def _ctx():
        ctx = {"current_user": _user(), "nav_dispatcher": None, "nav_attention": 0, "nav_source_id": None,
               "now_utc": datetime.now(timezone.utc), "APP_TZ_NAME": str(APP_TZ)}
        if request.endpoint and request.endpoint != "static" and not request.path.endswith("/partial"):
            try:
                db = services.db()
                ctx["nav_dispatcher"] = repo.live_dispatcher(db)
                first = db.one("SELECT source_id FROM source_connection ORDER BY source_id LIMIT 1")
                ctx["nav_source_id"] = first["source_id"] if first else None
                ctx["nav_attention"] = db.one("""
                    SELECT count(DISTINCT r.request_id) AS n FROM export_request r
                    LEFT JOIN export_subblock s ON s.request_id = r.request_id AND s.status = 'UNKNOWN'
                    WHERE r.status = 'FAILED' OR s.subblock_id IS NOT NULL""")["n"]
            except Exception:  # noqa: BLE001 - o menu não deve derrubar a página
                log.exception("falha ao montar o menu")
        return ctx

    @app.errorhandler(repo.StateError)
    def _state_error(e):
        flash(str(e), "error")
        return redirect(request.referrer or url_for("dashboard"))

    # ------------------------------------------------------------ dashboard
    @app.get("/")
    def dashboard():
        db = services.db()
        tables = repo.migration_coverage(db)
        scoped = [t for t in tables if t["scope_days"]]
        overall = {k: sum(t[k] for t in scoped) for k in ("scope_days", "done_days", "inflight_days",
                                                          "failed_days", "unknown_days", "missing_days")}
        overall["complete_tables"] = sum(1 for t in tables if t["coverage_status"] == "COMPLETA")
        overall["tables"] = len(tables)
        return render_template("dashboard.html", tables=tables, overall=overall,
                               active=repo.list_requests(db, limit=20),
                               dispatcher=repo.live_dispatcher(db),
                               runs=repo.recent_dispatcher_runs(db, 5))

    @app.post("/dispatcher/start")
    def dispatcher_start():
        run_id = services.ensure_dispatcher(force=True)
        flash(f"Dispatcher disparado (run {run_id})." if run_id else "Dispatcher já está em execução.", "info")
        return redirect(url_for("dashboard"))

    @app.post("/dispatcher/cancel")
    def dispatcher_cancel():
        n = services.cancel_dispatcher(_user())
        flash(f"Cancelamento solicitado para {n} run(s). Chamadas em curso no Netezza não são interrompidas; "
              "os subblocos ficarão como 'Desconhecido' quando o lease expirar.", "warn")
        return redirect(url_for("dashboard"))

    # ------------------------------------------------------------ pedidos
    @app.get("/requests")
    def requests_list():
        status = request.args.get("status") or None
        return render_template("requests.html", requests=repo.list_requests(services.db(), status=status),
                               status=status)

    @app.get("/requests/new")
    def request_new():
        tables = repo.list_tables(services.db(), only_active=True)
        return render_template("request_new.html", tables=tables, form=None, rows=[])

    @app.post("/requests/gaps")
    def request_from_gaps():
        """Pré-preenche um pedido com as lacunas de migração das tabelas selecionadas."""
        db = services.db()
        ids = request.form.getlist("table_ids")
        rows = repo.gap_rows(db, ids)
        if not rows:
            flash("As tabelas selecionadas não têm lacunas no escopo de migração.", "info")
            return redirect(url_for("dashboard"))
        form = {"name": "Lacunas de migração — " + ", ".join(ids), "max_parallel": "2", "deadline_at": "", "notes":
                "Pedido gerado a partir das lacunas do painel de completude."}
        return render_template("request_new.html", tables=repo.list_tables(db, only_active=True),
                               form=form, rows=rows)

    @app.get("/coverage/<table_config_id>")
    def coverage_detail(table_config_id):
        db = services.db()
        cov = next((t for t in repo.migration_coverage(db) if t["table_config_id"] == table_config_id), None)
        if not cov:
            abort(404)
        reqs = db.query("""SELECT DISTINCT r.request_id, r.request_name, r.status, r.submitted_at, rt.period_start,
                                  rt.period_end, rt.is_reprocess
                           FROM export_request_table rt JOIN export_request r USING (request_id)
                           WHERE rt.table_config_id = %s ORDER BY r.submitted_at DESC""", (table_config_id,))
        return render_template("coverage.html", t=cov, ranges=repo.table_ranges(db, table_config_id), reqs=reqs)

    @app.post("/requests/review")
    def request_review():
        db = services.db()
        try:
            rows = _parse_table_rows(request.form)
            max_parallel = _int(request.form.get("max_parallel"), lo=1, hi=200)
            reviewed = repo.review(db, rows)
        except ValueError as e:
            flash(str(e), "error")
            return render_template("request_new.html", tables=repo.list_tables(db, only_active=True),
                                   form=request.form, rows=_form_rows(request.form)), 400
        sources = {s["source_id"]: s for s in repo.list_sources(db)}
        cfg = {t["table_config_id"]: t for t in repo.list_tables(db)}
        return render_template("request_review.html", rows=reviewed, form=request.form,
                               max_parallel=max_parallel, cfg=cfg, sources=sources,
                               has_conflicts=any(r.conflicts for r in reviewed),
                               nothing_to_run=not any(r.blocks for r in reviewed))

    @app.post("/requests")
    def request_create():
        db = services.db()
        try:
            rows = _parse_table_rows(request.form)
            scheduled = _local_dt(request.form.get("scheduled_start_at"))
            if scheduled and scheduled <= datetime.now(timezone.utc):
                raise ValueError("o início agendado precisa ser no futuro (deixe em branco para iniciar já)")
            rid = repo.create_request(
                db, _user(), request.form.get("name", "").strip() or "pedido sem nome",
                _int(request.form.get("max_parallel"), lo=1, hi=200),
                _local_dt(request.form.get("deadline_at")),
                request.form.get("notes") or None, rows, scheduled_start_at=scheduled)
        except repo.ConflictError as e:
            flash(f"Pedido não criado: {len(e.conflicts)} conflito(s) de período. Revise.", "error")
            return request_review()
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("request_new"))
        if scheduled:
            flash(f"Pedido criado e agendado para {scheduled.strftime('%d/%m/%Y %H:%M')} ({APP_TZ}).", "info")
        else:
            services.ensure_dispatcher()
            flash("Pedido criado e enviado para execução.", "info")
        return redirect(url_for("request_detail", request_id=rid))

    def _load_request(request_id):
        req = repo.get_request(services.db(), request_id)
        if not req:
            abort(404)
        return req

    @app.get("/requests/<request_id>")
    def request_detail(request_id):
        db = services.db()
        req = _load_request(request_id)
        return render_template("request_detail.html", req=req, **_detail_ctx(db, request_id),
                               events=repo.request_events(db, request_id, 100))

    @app.get("/requests/<request_id>/partial")
    def request_partial(request_id):
        db = services.db()
        req = _load_request(request_id)
        return render_template("_request_progress.html", req=req, **_detail_ctx(db, request_id))

    def _detail_ctx(db, request_id):
        status = request.args.get("sb_status") or None
        tables = repo.request_tables(db, request_id)
        totals = {k: sum(t[k] or 0 for t in tables) for k in ("total", "done", "running", "pending", "failed",
                                                             "unknown", "canceled")}
        return {"tables": tables, "totals": totals, "sb_status": status,
                "subblocks": repo.request_subblocks(db, request_id, status=status, limit=1000),
                "dispatcher": repo.live_dispatcher(db)}

    @app.post("/requests/<request_id>/stop")
    def request_stop(request_id):
        repo.stop_request(services.db(), request_id, _user())
        flash("Parada solicitada: nenhum subbloco novo será iniciado; os que estão em execução vão terminar.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/resume")
    def request_resume(request_id):
        repo.resume_request(services.db(), request_id, _user(),
                            retry_failed=request.form.get("retry_failed") == "1",
                            retry_unknown=request.form.get("retry_unknown") == "1")
        services.ensure_dispatcher()
        flash("Pedido retomado.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/cancel")
    def request_cancel(request_id):
        repo.cancel_request(services.db(), request_id, _user(), request.form.get("reason") or None)
        flash("Pedido encerrado. Subblocos pendentes foram cancelados e os períodos liberados.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/schedule")
    def request_schedule(request_id):
        action = request.form.get("action")
        try:
            if action == "now":
                when = datetime.now(timezone.utc)
            elif action == "clear":
                when = None
            else:
                when = _local_dt(request.form.get("scheduled_start_at"))
                if not when or when <= datetime.now(timezone.utc):
                    raise ValueError("informe uma data/hora futura")
            repo.reschedule_request(services.db(), request_id, _user(), when)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("request_detail", request_id=request_id))
        if action in ("now", "clear"):
            services.ensure_dispatcher()
            flash("Execução iniciada.", "info")
        else:
            flash(f"Início reagendado para {when.strftime('%d/%m/%Y %H:%M')} ({APP_TZ}).", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/parallelism")
    def request_parallelism(request_id):
        try:
            n = _int(request.form.get("max_parallel"), lo=1, hi=200)
        except ValueError as e:
            flash(str(e), "error")
        else:
            repo.set_parallelism(services.db(), request_id, _user(), n)
            flash(f"Paralelismo do pedido alterado para {n}. Vale a partir do próximo ciclo do dispatcher.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/failed/retry")
    def request_failed_retry(request_id):
        n = repo.retry_failed(services.db(), request_id, _user())
        if n:
            services.ensure_dispatcher()
        flash(f"{n} subbloco(s) com falha recolocado(s) na fila." if n else "Nenhum subbloco com falha.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/requests/<request_id>/failed/cancel")
    def request_failed_cancel(request_id):
        reason = (request.form.get("reason") or "").strip()
        if not reason:
            flash("Informe o motivo do cancelamento.", "error")
        else:
            n = repo.cancel_failed(services.db(), request_id, _user(), reason)
            flash(f"{n} subbloco(s) com falha cancelado(s). Os períodos agora aparecem como lacunas e podem entrar "
                  "em um novo pedido.", "info")
        return redirect(url_for("request_detail", request_id=request_id))

    @app.post("/subblocks/<subblock_id>/cancel")
    def subblock_cancel(subblock_id):
        db = services.db()
        sb = db.one("SELECT request_id FROM export_subblock WHERE subblock_id = %s", (subblock_id,))
        if not sb:
            abort(404)
        reason = (request.form.get("reason") or "").strip()
        if not reason:
            flash("Informe o motivo do cancelamento.", "error")
        else:
            repo.cancel_failed(db, sb["request_id"], _user(), reason, subblock_id=subblock_id)
            flash("Subbloco cancelado. O período ficou livre para um novo pedido.", "info")
        return redirect(request.referrer or url_for("request_detail", request_id=sb["request_id"]))

    @app.get("/subblocks/<subblock_id>")
    def subblock_detail(subblock_id):
        db = services.db()
        sb = db.one("""SELECT s.*, lower(s.data_range) data_ini, upper(s.data_range) - 1 data_fim, r.request_name
                       FROM export_subblock s JOIN export_request r USING (request_id)
                       WHERE subblock_id = %s""", (subblock_id,))
        if not sb:
            abort(404)
        return render_template("subblock.html", sb=sb, attempts=repo.subblock_attempts(db, subblock_id))

    @app.post("/subblocks/<subblock_id>/retry")
    def subblock_retry(subblock_id):
        repo.retry_subblock(services.db(), subblock_id, _user())
        services.ensure_dispatcher()
        flash("Subbloco recolocado na fila.", "info")
        return redirect(request.referrer or url_for("dashboard"))

    @app.post("/subblocks/<subblock_id>/resolve")
    def subblock_resolve(subblock_id):
        reason = (request.form.get("reason") or "").strip()
        if not reason:
            flash("Informe a evidência/justificativa da reconciliação.", "error")
        else:
            repo.resolve_unknown(services.db(), subblock_id, _user(), request.form.get("done") == "1", reason)
            flash("Subbloco reconciliado.", "info")
        return redirect(request.referrer or url_for("dashboard"))

    # ------------------------------------------------------------ administração
    @app.get("/admin")
    def admin():
        db = services.db()
        last_tests = {r["source_id"]: r for r in db.query(
            "SELECT DISTINCT ON (source_id) * FROM connection_test ORDER BY source_id, requested_at DESC")}
        return render_template("admin.html", sources=repo.list_sources(db), tables=repo.list_tables(db),
                               hooks=repo.list_hooks(db), last_tests=last_tests)

    # ------------------------------------------------------------ conexão Netezza
    SECURITY_LEVELS = {0: "0 — preferir sem TLS", 1: "1 — preferir TLS (padrão)", 2: "2 — somente sem TLS",
                       3: "3 — somente TLS"}

    @app.get("/admin/sources/new")
    @app.get("/admin/sources/<source_id>")
    def admin_source_form(source_id=None):
        db = services.db()
        src = repo.get_source(db, source_id) if source_id else None
        if source_id and not src:
            abort(404)
        tests = repo.connection_tests(db, source_id) if src else []
        return render_template("admin_source_form.html", s=src, tests=tests, SECURITY_LEVELS=SECURITY_LEVELS,
                               creds=services.credentials_present(source_id) if src else {},
                               checks=repo.procedure_checks(db, source_id) if src else [],
                               live_test=any(t["status"] in ("PENDING", "RUNNING") for t in tests))

    @app.post("/admin/sources/save")
    def admin_source_save_full():
        f = request.form
        sid = (f.get("source_id") or "").strip().lower()
        try:
            if not re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", sid):
                raise ValueError("identificador: letras minúsculas, números, _ ou - (ex.: netezza-prod)")
            driver = "nzpy"
            ident = lambda k: (f.get(k) or "").strip().upper() or None  # noqa: E731
            for k in ("database", "default_source_database", "default_ext_database", "default_ext_schema"):
                if ident(k) and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]{0,127}", ident(k)):
                    raise ValueError(f"{k}: nome inválido ({ident(k)!r})")
            host = (f.get("host") or "").strip()
            if host and not re.fullmatch(r"[A-Za-z0-9.\-]{1,253}", host):
                raise ValueError("host inválido")
            if not host or not ident("database"):
                raise ValueError("informe host e banco da sessão")
            fields = dict(display_name=(f.get("display_name") or sid).strip(), driver=driver, host=host or None,
                          port=_int(f.get("port"), 5480, 1, 65535), database=ident("database"),
                          security_level=_int(f.get("security_level"), 1, 0, 3),
                          connect_timeout_s=_int(f.get("connect_timeout_s"), 30, 1, 600),
                          max_sessions=_int(f.get("max_sessions"), lo=1, hi=200),
                          default_source_database=ident("default_source_database"),
                          default_ext_database=ident("default_ext_database"),
                          default_ext_schema=ident("default_ext_schema"))
            user, password = (f.get("nz_user") or "").strip(), f.get("nz_password") or ""
            if user or password:
                services.put_credentials(sid, user or None, password or None)
            repo.save_connection(services.db(), _user(), sid, fields, secret_scope=services.SECRET_SCOPE,
                                 credentials_changed=bool(user or password))
            flash("Conexão salva." + (" Credenciais gravadas no secret scope." if user or password else ""), "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
            return redirect(url_for("admin_source_form", source_id=sid) if sid and repo.get_source(services.db(), sid)
                            else url_for("admin_source_form"))
        except Exception as e:  # noqa: BLE001 - ex.: sem permissão no secret scope
            log.exception("falha ao salvar conexão")
            flash(f"Erro ao gravar credenciais: {e}", "error")
        return redirect(url_for("admin_source_form", source_id=sid))

    @app.post("/admin/sources/<source_id>/test")
    def admin_source_test(source_id):
        if not repo.get_source(services.db(), source_id):
            abort(404)
        services.start_connection_test(source_id, _user())
        flash("Teste de conexão iniciado: ele roda como Job, pela mesma rede do dispatcher (leva ~1 min).", "info")
        return redirect(url_for("admin_source_form", source_id=source_id))

    @app.get("/admin/tables/new")
    @app.get("/admin/tables/<table_config_id>")
    def admin_table_form(table_config_id=None):
        db = services.db()
        t = repo.get_table(db, table_config_id) if table_config_id else None
        versions = db.query("SELECT * FROM table_config_version WHERE table_config_id = %s ORDER BY config_version DESC",
                            (table_config_id,)) if table_config_id else []
        return render_template("admin_table_form.html", t=t, is_new=t is None, sources=repo.list_sources(db),
                               versions=versions)

    @app.post("/admin/tables")
    def admin_table_save():
        f = request.form
        fields, tid = {}, (f.get("table_config_id") or "").strip().upper()
        try:
            tid = f["table_config_id"].strip().upper()
            if not tid:
                raise ValueError("identificador obrigatório")
            mig_start = _date(f["migration_start"]) if f.get("migration_start") else None
            mig_end = _date(f["migration_end"]) if f.get("migration_end") else None
            if mig_start and mig_end and mig_start > mig_end:
                raise ValueError("início do escopo maior que o fim")
            fields = dict(
                call_profile=f.get("call_profile") or DEFAULT_PROFILE,
                source_database=(f.get("source_database") or "").strip().upper() or None,
                ext_database=(f.get("ext_database") or "").strip().upper() or None,
                ext_schema=(f.get("ext_schema") or "").strip().upper() or None,
                time_column=(f.get("time_column") or "").strip().upper() or None,
                dest_catalog=(f.get("dest_catalog") or "").strip() or None,
                dest_schema=(f.get("dest_schema") or "").strip() or None,
                dest_table=(f.get("dest_table") or "").strip() or None,
                date_format=f.get("date_format") or "YYYYMMDD",
                source_table=f["source_table"].strip().upper(), procedure_name=f["procedure_name"].strip().upper(),
                default_subblock_unit=f["default_subblock_unit"],
                default_subblock_size=_int(f["default_subblock_size"], lo=1),
                max_parallel=_int(f["max_parallel"], lo=1, hi=200),
                call_timeout_min=_int(f["call_timeout_min"], lo=1),
                max_retries=_int(f["max_retries"], lo=0, hi=10),
                retry_unknown=f.get("retry_unknown") == "1",
                migration_start=mig_start, migration_end=mig_end,
                description=f.get("description") or None)
            db = services.db()
            source = next((x for x in repo.list_sources(db) if x["source_id"] == f["source_id"]), None)
            errors = validate_table_config(fields, source) + validate_destination(fields)
            if errors:
                raise ValueError("cadastro da chamada incompleto: " + "; ".join(errors))
            v = repo.save_table(db, _user(), tid, f["source_id"], f.get("active") == "1", fields)
            flash(f"Tabela {tid} salva (versão {v}).", "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
            # reabre o formulário com o que foi digitado
            db = services.db()
            existing = repo.get_table(db, tid) if tid else None
            t = {**(existing or {}), **{k: v for k, v in f.items()}, **fields, "table_config_id": tid,
                 "active": f.get("active") == "1", "retry_unknown": f.get("retry_unknown") == "1"}
            for k in ("default_subblock_size", "max_parallel", "call_timeout_min", "max_retries"):
                t[k] = f.get(k)
            return render_template("admin_table_form.html", t=t, is_new=existing is None,
                                   sources=repo.list_sources(db), versions=[]), 400
        return redirect(url_for("admin"))

    # ------------------------------------------------------------ carga de tabelas via CSV
    def _csv_response(text: str, filename: str) -> Response:
        return Response(text.encode("utf-8"), mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{filename}"'})

    @app.get("/admin/tables/template.csv")
    def admin_tables_template():
        return _csv_response(table_csv.template_csv(), "modelo_tabelas_exportaveis.csv")

    @app.get("/admin/tables/export.csv")
    def admin_tables_export():
        return _csv_response(table_csv.export_csv(repo.list_tables(services.db())),
                             f"tabelas_exportaveis_{date.today().isoformat()}.csv")

    def _import_preview(text: str, filename: str):
        db = services.db()
        sources = {x["source_id"]: x for x in repo.list_sources(db)}
        rows = table_csv.parse(text, sources)
        current = {t["table_config_id"]: t for t in repo.list_tables(db)}
        return table_csv.classify(rows, current)

    @app.post("/admin/tables/import")
    def admin_tables_import():
        f = request.files.get("csv_file")
        if not f or not f.filename:
            flash("Selecione um arquivo CSV.", "error")
            return redirect(url_for("admin"))
        try:
            text = table_csv.decode(f.read())
            rows = _import_preview(text, f.filename)
        except ValueError as e:
            flash(f"Arquivo não importado: {e}", "error")
            return redirect(url_for("admin"))
        return render_template("admin_import.html", rows=rows, csv_text=text, filename=f.filename,
                               columns=table_csv.COLUMNS)

    @app.post("/admin/tables/import/confirm")
    def admin_tables_import_confirm():
        text, filename = request.form.get("csv_text", ""), request.form.get("filename", "arquivo.csv")
        try:
            rows = _import_preview(text, filename)
        except ValueError as e:
            flash(f"Arquivo não importado: {e}", "error")
            return redirect(url_for("admin"))
        if any(r.action == "ERRO" for r in rows):
            flash("Corrija as linhas com erro antes de importar.", "error")
            return render_template("admin_import.html", rows=rows, csv_text=text, filename=filename,
                                   columns=table_csv.COLUMNS)
        c = repo.import_tables(services.db(), _user(), rows, filename)
        flash(f"Importação concluída: {c['NOVA']} tabela(s) nova(s), {c['NOVA_VERSAO']} nova(s) versão(ões), "
              f"{c['SEM_ALTERACAO']} sem alteração.", "info")
        return redirect(url_for("admin"))

    @app.post("/admin/hooks")
    def admin_hook_save():
        f = request.form
        try:
            repo.save_hook(services.db(), _user(), f["hook_id"].strip(), f.get("description") or "",
                           f.get("table_config_id") or None, f["trigger_level"], _int(f["job_id"], lo=1),
                           f.get("enabled") == "1")
            flash("Hook salvo. Ele só dispara para cargas concluídas a partir de agora.", "info")
        except (ValueError, KeyError) as e:
            flash(f"Erro: {e}", "error")
        return redirect(url_for("admin"))

    # ------------------------------------------------------------ navegador do Unity Catalog
    def _uc(fn, *args):
        try:
            return jsonify({"ok": True, "items": fn(*args)})
        except Exception as e:  # noqa: BLE001 - sem permissão / catálogo inexistente
            return jsonify({"ok": False, "error": str(e)[:400]}), 200

    @app.get("/api/uc/catalogs")
    def api_uc_catalogs():
        return _uc(catalog.catalogs)

    @app.get("/api/uc/schemas")
    def api_uc_schemas():
        return _uc(catalog.schemas, request.args.get("catalog", ""))

    @app.get("/api/uc/tables")
    def api_uc_tables():
        return _uc(catalog.tables, request.args.get("catalog", ""), request.args.get("schema", ""))

    @app.get("/api/uc/table")
    def api_uc_table():
        name = request.args.get("name", "")
        if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", name):
            return jsonify({"exists": False, "error": "use catálogo.schema.tabela"})
        return jsonify(catalog.table_info(name))

    # ------------------------------------------------------------ API mínima
    @app.get("/api/requests/<request_id>")
    def api_request(request_id):
        db = services.db()
        req = _load_request(request_id)
        return jsonify({"request": req, "tables": repo.request_tables(db, request_id)})

    @app.get("/api/health")
    def healthz():
        services.db().one("SELECT 1 AS ok")
        return {"ok": True, "version": APP_VERSION}

    if not app.testing and os.environ.get("DISABLE_SCHEDULER") != "1":
        services.start_scheduler()
    return app
