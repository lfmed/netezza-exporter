"""Pós-deploy idempotente do nzexport: rode depois de TODO deploy (databricks bundle run nzexport_pos_deploy -t <alvo>).

1. migra o banco do app (Lakebase) com a identidade dos jobs (dona do schema);
2. descobre o service principal do app e dá a ele acesso ao schema;
3. ACLs do secret scope do Netezza: WRITE para o app (grava as credenciais) e READ para a identidade dos jobs;
4. BROWSE nos catálogos de destino para o app (navegador do catálogo);
5. confere os pré-requisitos externos e diz o que falta e quem resolve: credenciais, landing (external location),
   tabelas destino (existem, Delta, MODIFY para os jobs) e conexão com o Netezza.
Só ADICIONA permissões; nunca remove. Termina com erro apenas se uma etapa obrigatória (1-3) falhar.
"""
import argparse
import base64
import inspect
import json
import logging
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.catalog import PermissionsChange, Privilege  # noqa: E402
from databricks.sdk.service.workspace import AclPermission  # noqa: E402

from nzexport import migrate, repo  # noqa: E402
from nzexport.db import Database, LakebaseConfig  # noqa: E402
from nzexport.procedure import build_client  # noqa: E402

OK, AVISO, ERRO = "OK", "AVISO", "ERRO"


class Relatorio:
    def __init__(self):
        self.itens = []

    def add(self, nivel, etapa, detalhe, quem=None):
        self.itens.append({"nivel": nivel, "etapa": etapa, "detalhe": str(detalhe)[:1500], "quem": quem})
        print(f"[{nivel:5}] {etapa}: {detalhe}" + (f"  (responsável: {quem})" if quem else ""))

    @property
    def erros(self):
        return [i for i in self.itens if i["nivel"] == ERRO]


def identidade(w: WorkspaceClient) -> dict:
    me = w.current_user.me()
    # service principal: user_name é o application id (sem @); usuário: e-mail
    nome = me.user_name or ""
    sp = getattr(me, "application_id", None) or (nome if "@" not in nome else None)
    return {"nome": nome, "principal": sp or nome, "sp": bool(sp)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--database", default="databricks_postgres")
    p.add_argument("--app-name", required=True)
    p.add_argument("--secret-scope", required=True)
    p.add_argument("--dest-catalogs", default="")
    p.add_argument("--run-id", type=int, default=0)
    a = p.parse_args()
    logging.basicConfig(level=logging.WARNING)
    w = WorkspaceClient()
    rel = Relatorio()
    eu = identidade(w)
    rel.add(OK, "identidade dos jobs", f"{eu['principal']} ({'service principal' if eu['sp'] else 'usuário'})")

    # 1-2. app, banco e grants -------------------------------------------------------------------------------
    app_sp = None
    try:
        app = w.apps.get(name=a.app_name)
        app_sp = app.service_principal_client_id
        rel.add(OK, "service principal do app", f"{app.service_principal_name} ({app_sp})")
    except Exception as e:  # noqa: BLE001
        rel.add(ERRO, "service principal do app", f"app {a.app_name} não encontrado: {e}", "deploy do bundle")
    cfg = LakebaseConfig(endpoint=a.endpoint, database=a.database, w=w)
    try:
        migrate.run(cfg, grant_to=[app_sp] if app_sp else [])
        rel.add(OK, "banco do app (Lakebase)", f"schema nzexport migrado em {a.endpoint}"
                + (f"; acesso concedido a {app_sp}" if app_sp else ""))
        with cfg.connect() as c:
            tem_role = app_sp and c.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (app_sp,)).fetchone()
        if app_sp and not tem_role:
            rel.add(ERRO, "acesso do app ao banco", f"role {app_sp} ainda não existe no Lakebase: o recurso postgres do "
                    "app não foi aplicado. Rode 'bundle deploy' e depois este job de novo.", "deploy do bundle")
    except Exception as e:  # noqa: BLE001
        rel.add(ERRO, "banco do app (Lakebase)", e, "deploy do bundle / permissão no projeto Lakebase")

    # 3. secret scope -----------------------------------------------------------------------------------------
    try:
        acls = {x.principal: x.permission for x in w.secrets.list_acls(scope=a.secret_scope)}
        ordem = {AclPermission.READ: 1, AclPermission.WRITE: 2, AclPermission.MANAGE: 3}
        alvo = ([(app_sp, AclPermission.WRITE)] if app_sp else []) + [(eu["principal"], AclPermission.READ)]
        for principal, nivel in alvo:
            if ordem.get(acls.get(principal), 0) >= ordem[nivel]:
                continue  # já tem igual ou maior: nunca rebaixa
            w.secrets.put_acl(scope=a.secret_scope, principal=principal, permission=nivel)
        rel.add(OK, "secret scope", f"{a.secret_scope}: app com WRITE, jobs com READ (ACLs só adicionadas)")
    except Exception as e:  # noqa: BLE001
        rel.add(ERRO, "secret scope", f"{a.secret_scope}: {e}", "deploy do bundle (recurso secret_scopes)")

    # 4. BROWSE nos catálogos de destino ----------------------------------------------------------------------
    for cat in [c.strip() for c in a.dest_catalogs.split(",") if c.strip() and not c.strip().startswith("<")]:
        if not app_sp:
            break
        try:
            w.grants.update(securable_type="catalog", full_name=cat,
                            changes=[PermissionsChange(principal=app_sp, add=[Privilege.BROWSE])])
            rel.add(OK, f"BROWSE em {cat}", "app enxerga o catálogo no navegador")
        except Exception as e:  # noqa: BLE001
            rel.add(AVISO, f"BROWSE em {cat}", f"não concedido ({str(e)[:200]})",
                    f"dono do catálogo: GRANT BROWSE ON CATALOG {cat} TO `{app_sp}`")

    # 5. pré-requisitos externos --------------------------------------------------------------------------------
    db = Database(cfg, min_size=1, max_size=2)
    try:
        conferir_externos(w, db, rel, eu, a.secret_scope)
    finally:
        guardar(db, rel, a.run_id)
        db.close()

    print(f"\nResumo: {sum(i['nivel'] == OK for i in rel.itens)} OK · "
          f"{sum(i['nivel'] == AVISO for i in rel.itens)} aviso(s) · {len(rel.erros)} erro(s)")
    if rel.erros:
        raise SystemExit("pós-deploy com erro: " + "; ".join(f"{i['etapa']}: {i['detalhe']}" for i in rel.erros))


def conferir_externos(w, db, rel, eu, scope):
    from databricks.sdk.runtime import dbutils

    for src in repo.list_sources(db):
        sid = src["source_id"]
        # credenciais
        try:
            keys = {s.key for s in w.secrets.list_secrets(scope=scope)}
        except Exception:  # noqa: BLE001
            keys = set()
        if f"{sid}-host" in keys:
            rel.add(OK, f"host {sid}", "host do Netezza presente no secret scope"
                    + ("" if f"{sid}-port" in keys else " (porta padrão 5480)"))
        else:
            rel.add(AVISO, f"host {sid}", "host do Netezza não definido no secret scope",
                    f"CI/CD ou administrador: databricks secrets put-secret {scope} {sid}-host (e {sid}-port)")
        if {f"{sid}-user", f"{sid}-password"} <= keys:
            rel.add(OK, f"credenciais {sid}", "usuário e senha presentes no secret scope")
        else:
            rel.add(AVISO, f"credenciais {sid}", "usuário/senha do Netezza não definidos",
                    "app (Conexão Netezza) ou CI/CD: databricks secrets put-secret")
        # landing
        if not src.get("landing_root"):
            rel.add(AVISO, f"landing {sid}", "caminho da landing não configurado", "app (Conexão Netezza)")
        else:
            base = src["landing_root"].rstrip("/")
            teste = f"{base}/{src['landing_work_folder'].strip('/')}/_nzexport_check_{uuid.uuid4().hex[:8]}"
            try:
                dbutils.fs.put(teste, "ok", True)   # escrita (o importador apaga e move arquivos)
                dbutils.fs.ls(teste)                # leitura
                dbutils.fs.rm(teste)
                rel.add(OK, f"landing {sid}", f"{base}: leitura e escrita OK")
            except Exception as e:  # noqa: BLE001
                rel.add(AVISO, f"landing {sid}", f"{base}: sem acesso de leitura/escrita ({str(e)[:200]})",
                        f"administrador do Unity Catalog: external location cobrindo o caminho com READ FILES e "
                        f"WRITE FILES para {eu['principal']}")
        # conexão Netezza (só se configurada)
        if {f"{sid}-host", f"{sid}-user", f"{sid}-password"} <= keys:
            try:
                client = build_client(src, secrets_reader=lambda s, k: base64.b64decode(
                    w.secrets.get_secret(scope=s, key=k).value).decode())
                res = client.test(repo.procedure_checks(db, sid))
                ok = all(r["ok"] for r in res)
                rel.add(OK if ok else AVISO, f"conexão Netezza {sid}",
                        "; ".join(f"{'✔' if r['ok'] else '✘'} {r['check']}: {r['detail']}" for r in res),
                        None if ok else "rede (NCC do serverless até o Netezza) / DBA do Netezza")
            except Exception as e:  # noqa: BLE001
                rel.add(AVISO, f"conexão Netezza {sid}", str(e)[:300], "rede (NCC) / credenciais")
    # tabelas destino
    for t in repo.list_tables(db, only_active=True):
        if not t.get("dest_table"):
            rel.add(AVISO, f"destino {t['table_config_id']}", "tabela sem destino no Databricks (só exporta)", "app")
            continue
        full = f"{t['dest_catalog']}.{t['dest_schema']}.{t['dest_table']}"
        try:
            info = w.tables.get(full_name=full)
            fmt = str(info.data_source_format.value) if info.data_source_format else "?"
            if fmt.upper() != "DELTA":
                rel.add(AVISO, f"destino {t['table_config_id']}", f"{full} não é Delta ({fmt})", "dono da tabela")
                continue
            dono = info.owner == eu["principal"] or info.owner == eu["nome"]
            privs = set()
            if not dono:
                eff = w.grants.get_effective(securable_type="table", full_name=full, principal=eu["principal"])
                for pa in eff.privilege_assignments or []:
                    privs |= {str(x.privilege.value) for x in (pa.privileges or [])}
            if dono or {"MODIFY", "SELECT"} <= privs or "ALL_PRIVILEGES" in privs:
                rel.add(OK, f"destino {t['table_config_id']}", f"{full}: Delta, jobs com SELECT/MODIFY")
            else:
                rel.add(AVISO, f"destino {t['table_config_id']}", f"{full}: jobs sem MODIFY/SELECT ({sorted(privs)})",
                        f"dono da tabela: GRANT SELECT, MODIFY ON TABLE {full} TO `{eu['principal']}`")
        except Exception as e:  # noqa: BLE001
            rel.add(AVISO, f"destino {t['table_config_id']}", f"{full}: não encontrada ou sem acesso ({str(e)[:150]})",
                    "dono do schema/tabela")


def guardar(db, rel, run_id):
    try:
        with db.tx() as c:
            c.execute("INSERT INTO deploy_check (run_id, ok, warnings, errors, items) VALUES (%s,%s,%s,%s,%s)",
                      (run_id, sum(i["nivel"] == OK for i in rel.itens), sum(i["nivel"] == AVISO for i in rel.itens),
                       len(rel.erros), json.dumps(rel.itens)))
    except Exception as e:  # noqa: BLE001
        print(f"não consegui guardar o relatório: {e}")


if __name__ == "__main__":
    main()
