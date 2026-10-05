"""Chamada da procedure Netezza: procedure(tabela, data_ini, data_fim), datas inclusivas.

A procedure grava os arquivos por conta própria; o retorno sem exceção significa concluído.
"""
import random
import re
import socket
import time
from dataclasses import dataclass
from datetime import date

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")


class ProcedureError(Exception):
    """A procedure executou e retornou erro: falha definitiva desta tentativa."""


class ConnectError(Exception):
    """Não foi possível conectar; a procedure não foi chamada (retry seguro)."""


class OutcomeUnknown(Exception):
    """A chamada foi enviada, mas a conexão caiu/expirou: não se sabe se terminou."""


@dataclass
class CallSpec:
    procedure_name: str
    source_table: str
    data_ini: date
    data_fim: date
    timeout_s: int

    def statement(self) -> str:
        if not _IDENT.match(self.procedure_name):
            raise ProcedureError(f"nome de procedure inválido: {self.procedure_name!r}")
        return f"CALL {self.procedure_name}(?, ?, ?)"

    def params(self) -> tuple:
        return (self.source_table, self.data_ini.isoformat(), self.data_fim.isoformat())

    def describe(self) -> str:
        p = self.params()
        return f"CALL {self.procedure_name}('{p[0]}', '{p[1]}', '{p[2]}')"


class FakeProcedureClient:
    """Simula a procedure para desenvolvimento e testes sem Netezza."""

    def __init__(self, options: dict | None = None, **_):
        o = options or {}
        self.min_s = float(o.get("min_s", 2))
        self.max_s = float(o.get("max_s", 8))
        self.fail_pct = float(o.get("fail_pct", 0))
        self.unknown_pct = float(o.get("unknown_pct", 0))

    def call(self, spec: CallSpec, should_abort=lambda: False) -> None:
        spec.statement()
        duration = random.uniform(self.min_s, self.max_s)
        deadline = time.time() + duration
        while time.time() < deadline:
            time.sleep(min(0.5, max(0, deadline - time.time())))
        roll = random.uniform(0, 100)
        if roll < self.fail_pct:
            raise ProcedureError(f"[fake] erro simulado na procedure para {spec.source_table}")
        if roll < self.fail_pct + self.unknown_pct:
            raise OutcomeUnknown("[fake] conexão perdida após o envio")


class NzpyProcedureClient:
    def __init__(self, conn_params: dict, connect_timeout_s: int = 30, **_):
        self.conn_params = conn_params
        self.connect_timeout_s = connect_timeout_s

    def call(self, spec: CallSpec, should_abort=lambda: False) -> None:
        import nzpy

        stmt = spec.statement()
        try:
            conn = nzpy.connect(
                user=self.conn_params["user"], password=self.conn_params["password"],
                host=self.conn_params["host"], port=int(self.conn_params.get("port", 5480)),
                database=self.conn_params["database"], securityLevel=int(self.conn_params.get("security_level", 1)),
                timeout=self.connect_timeout_s, application_name="nzexport", logLevel=0,
            )
        except Exception as e:  # noqa: BLE001 - qualquer falha aqui = não enviou
            raise ConnectError(_mask(str(e), self.conn_params)) from None
        try:
            # timeout de socket passa a valer para a execução da procedure
            sock = getattr(conn, "_usock", None)
            if isinstance(sock, socket.socket):
                sock.settimeout(spec.timeout_s)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(stmt, spec.params())
            try:
                cur.fetchall()
            except Exception:  # noqa: BLE001 - procedure sem resultset
                pass
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


def _mask(msg: str, params: dict) -> str:
    for k in ("password", "user", "host"):
        v = params.get(k)
        if v:
            msg = msg.replace(str(v), "***")
    return msg[:2000]


def build_client(source: dict, secrets_reader=None):
    if source["driver"] == "fake":
        return FakeProcedureClient(options=source.get("driver_options") or {})
    if source["driver"] == "nzpy":
        scope = source.get("secret_scope")
        if not scope or secrets_reader is None:
            raise ConnectError("origem nzpy sem secret_scope configurado")
        params = {k: secrets_reader(scope, k) for k in ("host", "port", "database", "user", "password")}
        params["security_level"] = (source.get("driver_options") or {}).get("security_level", 1)
        return NzpyProcedureClient(params, connect_timeout_s=source.get("connect_timeout_s", 30))
    raise ValueError(f"driver desconhecido: {source['driver']}")
