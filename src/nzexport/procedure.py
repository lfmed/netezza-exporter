"""Chamada da procedure Netezza que gera os arquivos de um subbloco.

Perfis de chamada (definidos no cadastro da tabela):
  * SP_CARGA_EVENTUAL: SP_CARGA_EVENTUAL(p_banco, p_tabela, p_banco_ext_table, p_esquema_ext_table,
                                         p_dtini, p_dtfim, p_NomeColunaTempo)  -- datas AAAAMMDD, fim inclusivo
  * TABELA_INI_FIM:    procedure(tabela, data_ini, data_fim)

Tipo de filtro da tabela (faixa.py), só no perfil SP_CARGA_EVENTUAL para Número e Sem filtro:
  * Data:       p_dtini/p_dtfim em AAAAMMDD
  * Número:     p_dtini/p_dtfim = valores inteiros mínimo e máximo (a SP concatena no BETWEEN)
  * Sem filtro: p_NomeColunaTempo, p_dtini e p_dtfim = '1' -> WHERE 1 BETWEEN 1 AND 1 (a tabela toda)

A procedure grava os arquivos por conta própria; o retorno sem exceção significa concluído.
Todos os valores são validados (identificadores e datas) antes de montar o comando, porque o nzpy
substitui parâmetros sem escapar e a procedure concatena os valores em SQL dinâmico.
"""
import re
import socket
from dataclasses import dataclass, field
from datetime import date

from . import faixa

_PROC_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,127}$")
_DOTTED = _PROC_NAME
_INT = re.compile(r"^-?\d{1,19}$")

DATE_FORMATS = {"YYYYMMDD": "%Y%m%d", "YYYY-MM-DD": "%Y-%m-%d"}

# argumentos de cada perfil, na ordem da assinatura da procedure: (campo resolvido, nome do parâmetro)
PROFILES = {
    "SP_CARGA_EVENTUAL": {
        "label": "SP_CARGA_EVENTUAL — 7 parâmetros (banco, tabela, banco ext., esquema ext., dtini, dtfim, coluna de tempo)",
        "default_procedure": "SP_CARGA_EVENTUAL",
        "args": [("source_database", "p_banco"), ("source_table", "p_tabela"),
                 ("ext_database", "p_banco_ext_table"), ("ext_schema", "p_esquema_ext_table"),
                 ("data_ini", "p_dtini"), ("data_fim", "p_dtfim"), ("time_column", "p_NomeColunaTempo")],
    },
    "TABELA_INI_FIM": {
        "label": "procedure(tabela, data_ini, data_fim) — 3 parâmetros",
        "default_procedure": "",
        "args": [("source_table", "tabela"), ("data_ini", "data_ini"), ("data_fim", "data_fim")],
    },
}
DEFAULT_PROFILE = "SP_CARGA_EVENTUAL"


class ProcedureError(Exception):
    """A procedure executou e retornou erro: falha desta tentativa."""


class ConfigError(ProcedureError):
    """Cadastro incompleto/inválido: não adianta repetir sem corrigir a configuração."""


class ConnectError(Exception):
    """Não foi possível conectar; a procedure não foi chamada (retry seguro)."""


class OutcomeUnknown(Exception):
    """A chamada foi enviada, mas a conexão caiu/expirou: não se sabe se terminou."""


@dataclass
class CallSpec:
    procedure_name: str
    args: list[str]
    arg_names: list[str]
    timeout_s: int
    session_database: str | None = None   # banco da sessão nzpy
    output_object: str | None = None      # external table / arquivo esperado (rastreabilidade)
    profile: str = DEFAULT_PROFILE
    extra: dict = field(default_factory=dict)

    def statement(self) -> str:
        quoted = ", ".join("'" + a.replace("'", "''") + "'" for a in self.args)
        return f"CALL {self.procedure_name}({quoted})"

    def describe(self) -> str:
        return self.statement()


def _resolve(cfg: dict, source: dict | None) -> dict:
    """Valores efetivos: cadastro da tabela, com padrões da conexão para banco/esquema."""
    src = source or {}
    return {
        "source_database": (cfg.get("source_database") or src.get("default_source_database") or "").strip(),
        "source_table": (cfg.get("source_table") or "").strip(),
        "ext_database": (cfg.get("ext_database") or src.get("default_ext_database") or "").strip(),
        "ext_schema": (cfg.get("ext_schema") or src.get("default_ext_schema") or "").strip(),
        "time_column": (cfg.get("time_column") or "").strip(),
    }


def build_call(cfg: dict, source: dict | None, data_ini, data_fim) -> CallSpec:
    """Monta a chamada de um subbloco a partir do cadastro da tabela e da conexão. Levanta ConfigError.

    data_ini/data_fim são os valores nativos do tipo de filtro: date (Data), int (Número) ou 0 (Sem filtro).
    """
    profile = cfg.get("call_profile") or DEFAULT_PROFILE
    if profile not in PROFILES:
        raise ConfigError(f"perfil de chamada desconhecido: {profile!r}")
    try:
        ft = faixa.ftype(cfg)
    except ValueError as e:
        raise ConfigError(str(e)) from None
    if ft != "DATE" and profile != "SP_CARGA_EVENTUAL":
        raise ConfigError(f"o tipo de filtro {faixa.FILTER_TYPES[ft]} só existe no perfil SP_CARGA_EVENTUAL")
    proc = (cfg.get("procedure_name") or PROFILES[profile]["default_procedure"]).strip()
    if not _PROC_NAME.match(proc):
        raise ConfigError(f"nome de procedure inválido: {proc!r}")
    v = _resolve(cfg, source)
    if ft == "DATE":
        fmt = DATE_FORMATS.get(cfg.get("date_format") or "YYYYMMDD")
        if not fmt:
            raise ConfigError(f"formato de data inválido: {cfg.get('date_format')!r}")
        v["data_ini"], v["data_fim"] = data_ini.strftime(fmt), data_fim.strftime(fmt)
    elif ft == "NUMBER":
        v["data_ini"], v["data_fim"] = str(int(data_ini)), str(int(data_fim))
        if not (_INT.match(v["data_ini"]) and _INT.match(v["data_fim"])):
            raise ConfigError(f"faixa numérica inválida: {v['data_ini']}..{v['data_fim']}")
    else:  # sem filtro: filtro sempre verdadeiro, montado pelo app (não vem do cadastro)
        v["time_column"] = v["data_ini"] = v["data_fim"] = faixa.ALWAYS_TRUE

    errors = []
    for key, pname in PROFILES[profile]["args"]:
        if key in ("data_ini", "data_fim") or (ft == "NONE" and key == "time_column"):
            continue
        val = v[key]
        pattern = _DOTTED if (profile == "TABELA_INI_FIM" and key == "source_table") else _IDENT
        if not val:
            errors.append(f"{pname} não informado")
        elif not pattern.match(val):
            errors.append(f"{pname} inválido: {val!r} (use só letras, números e _)")
    if errors:
        raise ConfigError("; ".join(errors))

    args = [v[k] for k, _ in PROFILES[profile]["args"]]
    names = [n for _, n in PROFILES[profile]["args"]]
    timeout_s = int(cfg.get("call_timeout_min") or 240) * 60
    if profile == "SP_CARGA_EVENTUAL":
        ext_name = f"{v['source_table']}_{v['data_ini']}_{v['data_fim']}"
        # a procedure faz INSERT INTO <esquema>.<tabela> sem o banco: a sessão precisa estar no banco da external table
        return CallSpec(proc, args, names, timeout_s, session_database=v["ext_database"], profile=profile,
                        output_object=f"{v['ext_database']}.{v['ext_schema']}.{ext_name} -> /tmp/{ext_name}.dat",
                        extra={"external_table": f"{v['ext_database']}.{v['ext_schema']}.{ext_name}",
                               "data_object": f"/tmp/{ext_name}.dat"})
    return CallSpec(proc, args, names, timeout_s, session_database=None, profile=profile)


_UC_NAME = re.compile(r"^[A-Za-z0-9_]{1,255}$")


def validate_destination(cfg: dict) -> list[str]:
    """Catálogo, schema e tabela destino no Unity Catalog: os três juntos (ou nenhum), nomes simples."""
    parts = {k: (cfg.get(k) or "").strip() for k in ("dest_catalog", "dest_schema", "dest_table")}
    if not any(parts.values()):
        return []
    errors = [f"{k}: não informado" for k, v in parts.items() if not v]
    errors += [f"{k}: nome inválido {v!r} (use letras, números e _)" for k, v in parts.items() if v and not _UC_NAME.match(v)]
    return errors


def validate_table_config(cfg: dict, source: dict | None) -> list[str]:
    """Erros de cadastro (para a tela e o CSV), usando um período de exemplo."""
    try:
        build_call(cfg, source, *faixa.example(faixa.ftype(cfg)))
        return []
    except ValueError as e:  # tipo de filtro inválido
        return [str(e)]
    except ConfigError as e:
        return str(e).split("; ")


class NzpyProcedureClient:
    def __init__(self, conn_params: dict, connect_timeout_s: int = 30, **_):
        self.conn_params = conn_params
        self.connect_timeout_s = connect_timeout_s

    def _connect(self, database: str | None):
        import nzpy

        p = self.conn_params
        try:
            return nzpy.connect(
                user=p["user"], password=p["password"], host=p["host"], port=int(p.get("port") or 5480),
                database=database or p["database"], securityLevel=int(p.get("security_level") or 1),
                timeout=self.connect_timeout_s, application_name="nzexport", logLevel=0,
            )
        except Exception as e:  # noqa: BLE001 - qualquer falha aqui = não enviou
            raise ConnectError(_mask(str(e), p)) from None

    def call(self, spec: CallSpec, should_abort=lambda: False) -> str | None:
        import nzpy

        stmt = spec.statement()
        conn = self._connect(spec.session_database)
        try:
            # timeout de socket passa a valer para a execução da procedure
            sock = getattr(conn, "_usock", None)
            if isinstance(sock, socket.socket):
                sock.settimeout(spec.timeout_s)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(stmt)  # valores já validados e literais montados por nós (sem substituição do nzpy)
            result = None
            try:
                row = cur.fetchone()
                result = str(row[0])[:4000] if row else None
            except Exception:  # noqa: BLE001 - procedure sem resultset
                pass
            return result
        except (socket.timeout, TimeoutError, OSError, ConnectionError, nzpy.InterfaceError) as e:
            # chamada já enviada e a conexão caiu/expirou: resultado desconhecido
            raise OutcomeUnknown(f"conexão perdida/expirada durante a chamada: {type(e).__name__}: "
                                 f"{_mask(str(e), self.conn_params)}") from None
        except Exception as e:  # noqa: BLE001 - erro retornado pelo Netezza (DatabaseError etc.)
            raise ProcedureError(_mask(str(e), self.conn_params)) from None
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def test(self, procedure_checks: list[tuple[str, str]] | None = None) -> list[dict]:
        """Teste de conexão: autentica, identifica a sessão e verifica se as procedures existem."""
        results = []
        try:
            conn = self._connect(None)
        except ConnectError as e:
            return [{"check": "conexão", "ok": False, "detail": str(e)}]
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_USER, CURRENT_CATALOG")
            u, db = cur.fetchone()
            results.append({"check": "conexão", "ok": True, "detail": f"usuário {u}, banco {db}"})
            try:
                cur.execute("SELECT VERSION()")
                results.append({"check": "versão", "ok": True, "detail": str(cur.fetchone()[0])[:300]})
            except Exception as e:  # noqa: BLE001
                results.append({"check": "versão", "ok": False, "detail": _mask(str(e), self.conn_params)})
            for database, proc in procedure_checks or []:
                if not (_IDENT.match(database or "") and _PROC_NAME.match(proc or "")):
                    continue
                name = proc.split(".")[-1].upper()
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {database}.._V_PROCEDURE WHERE UPPER(PROCEDURE) = '{name}'")
                    n = cur.fetchone()[0]
                    results.append({"check": f"procedure {name} em {database}", "ok": n > 0,
                                    "detail": "encontrada" if n else "não encontrada"})
                except Exception as e:  # noqa: BLE001
                    results.append({"check": f"procedure {name} em {database}", "ok": False,
                                    "detail": _mask(str(e), self.conn_params)})
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        return results


def _mask(msg: str, params: dict) -> str:
    for k in ("password", "user", "host"):
        v = params.get(k)
        if v:
            msg = msg.replace(str(v), "***")
    return msg[:2000]


def build_client(source: dict, scope: str | None, secrets_reader=None):
    """Cliente nzpy com host, porta, usuário e senha lidos do secret scope. O nome do scope vem da configuração
    do deploy (variável secret_scope do bundle), nunca do banco do app."""
    if source.get("driver", "nzpy") == "nzpy":
        if not scope or secrets_reader is None:
            raise ConnectError("secret scope do Netezza não informado (variável secret_scope do bundle)")
        sid = source["source_id"]
        try:
            host = secrets_reader(scope, f"{sid}-host").strip()
        except Exception:  # noqa: BLE001
            raise ConnectError(f"host do Netezza não encontrado no secret scope {scope} "
                               f"(chave {sid}-host: databricks secrets put-secret)") from None
        try:
            port = secrets_reader(scope, f"{sid}-port").strip() or "5480"
        except Exception:  # noqa: BLE001 - porta é opcional
            port = "5480"
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ConnectError(f"porta inválida na chave {sid}-port do secret scope")
        try:
            user = secrets_reader(scope, f"{sid}-user")
            password = secrets_reader(scope, f"{sid}-password")
        except Exception:  # noqa: BLE001
            raise ConnectError(f"credenciais da conexão não encontradas no secret scope {scope} "
                               f"(chaves {sid}-user e {sid}-password)") from None
        if not source.get("database"):
            raise ConnectError("conexão sem banco da sessão configurado (Administração → Conexão)")
        params = {"host": host, "port": int(port), "database": source["database"],
                  "user": user, "password": password, "security_level": source.get("security_level", 1)}
        return NzpyProcedureClient(params, connect_timeout_s=source.get("connect_timeout_s", 30))
    raise ValueError(f"driver desconhecido: {source['driver']}")
