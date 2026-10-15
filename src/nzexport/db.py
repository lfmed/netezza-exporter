"""Conexão com o Lakebase (Postgres) usando token OAuth renovado automaticamente."""
import os
import re
import threading
import time
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from databricks.sdk import WorkspaceClient

SCHEMA = "nzexport"
_TOKEN_TTL_S = 40 * 60
_ENDPOINT_PATH = re.compile(r"^projects/[^/]+/branches/[^/]+/endpoints/[^/]+$")
_FORMATO = "projects/<projeto>/branches/production/endpoints/primary"


def resolve_endpoint(w: WorkspaceClient, value: str) -> tuple[str, str | None, str | None]:
    """Endpoint do Lakebase a partir de `lakebase_endpoint`.

    Aceita o caminho do recurso (projects/<projeto>/branches/<branch>/endpoints/<endpoint>) e, como alternativa,
    o host do Postgres (ep-....database.<região>.cloud.databricks.com), com ou sem /<banco> ou como URL
    postgresql://: nesse caso procura o endpoint com esse host. Retorna (caminho do endpoint, host, banco).
    """
    v = (value or "").strip().rstrip("/")
    if _ENDPOINT_PATH.match(v):
        return v, None, None
    if not v or v.startswith("projects/"):
        raise ValueError(f"lakebase_endpoint {value!r} incompleto: use {_FORMATO}")
    rest = re.sub(r"^[a-z]+://", "", v).split("@")[-1].split("?")[0]
    host, _, database = rest.partition("/")
    host = host.split(":")[0].lower()
    for p in w.postgres.list_projects():
        for b in w.postgres.list_branches(parent=p.name):
            for e in w.postgres.list_endpoints(parent=b.name):
                h = e.status.hosts.host if e.status and e.status.hosts else None
                if h and h.lower() == host:
                    return e.name, h, database or None
    raise ValueError(f"lakebase_endpoint {value!r} não é o caminho de um endpoint e nenhum endpoint do Lakebase "
                     f"visível para esta identidade tem o host {host!r}. Use {_FORMATO} "
                     f"(databricks postgres list-endpoints projects/<projeto>/branches/production)")


class _TokenProvider:
    def __init__(self, w: WorkspaceClient, endpoint: str):
        self._w = w
        self._endpoint = endpoint
        self._lock = threading.Lock()
        self._token = None
        self._expires = 0.0

    def get(self) -> str:
        with self._lock:
            if self._token is None or time.time() >= self._expires:
                cred = self._w.postgres.generate_database_credential(endpoint=self._endpoint)
                self._token = cred.token
                self._expires = time.time() + _TOKEN_TTL_S
            return self._token


class LakebaseConfig:
    """Resolve host/usuário/banco a partir do ambiente (App) ou do SDK (Job/local)."""

    def __init__(self, endpoint: str | None = None, database: str | None = None,
                 w: WorkspaceClient | None = None):
        self.w = w or WorkspaceClient()
        self.endpoint, host, db = resolve_endpoint(self.w, endpoint or os.environ["LAKEBASE_ENDPOINT"])
        self.database = database or os.environ.get("PGDATABASE") or db or "databricks_postgres"
        self.host = os.environ.get("PGHOST") or host or self.w.postgres.get_endpoint(name=self.endpoint).status.hosts.host
        self.user = os.environ.get("PGUSER") or self._default_user()
        self.port = int(os.environ.get("PGPORT", "5432"))
        self.tokens = _TokenProvider(self.w, self.endpoint)

    def _default_user(self) -> str:
        me = self.w.current_user.me()
        # usuário: e-mail; service principal: o user_name já é o application id (role no Postgres)
        return getattr(me, "application_id", None) or me.user_name

    def conninfo(self) -> dict:
        info = dict(host=self.host, port=self.port, dbname=self.database, user=self.user,
                    sslmode="require", application_name="nzexport",
                    options=f"-c search_path={SCHEMA},public")
        hostaddr = os.environ.get("PGHOSTADDR")  # contorno p/ DNS de hostnames longos no macOS
        if hostaddr:
            info["hostaddr"] = hostaddr
        return info

    def connect(self, **kwargs) -> psycopg.Connection:
        return psycopg.connect(**self.conninfo(), password=self.tokens.get(),
                               row_factory=dict_row, **kwargs)


class Database:
    def __init__(self, cfg: LakebaseConfig, min_size: int = 1, max_size: int = 10):
        self.cfg = cfg
        self.last_error: str | None = None   # o pool repete em silêncio: guarda o motivo para o PoolTimeout
        tokens, owner = cfg.tokens, self

        class _OAuthConnection(psycopg.Connection):
            @classmethod
            def connect(cls, conninfo="", **kw):
                try:
                    kw["password"] = tokens.get()
                    return super().connect(conninfo, **kw)
                except Exception as e:
                    owner.last_error = f"{type(e).__name__}: {str(e).strip()[:500]}"
                    raise

        self.pool = ConnectionPool(
            kwargs={**cfg.conninfo(), "row_factory": dict_row},
            connection_class=_OAuthConnection,
            min_size=min_size, max_size=max_size,
            max_lifetime=_TOKEN_TTL_S, max_idle=300,
            check=ConnectionPool.check_connection, open=True,
        )

    @contextmanager
    def connection(self):
        """Conexão do pool; num PoolTimeout, mostra o último erro real ao conectar (rede, role, token...)."""
        try:
            conn = self.pool.getconn()
        except PoolTimeout as e:
            raise PoolTimeout(f"{e}. Último erro ao conectar em {self.cfg.host} como {self.cfg.user}: "
                              f"{self.last_error or 'desconhecido'}") from None
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def tx(self):
        """Transação: commit ao sair sem erro, rollback em exceção."""
        with self.connection() as conn:
            with conn.transaction():
                yield conn

    def query(self, sql: str, params=None) -> list[dict]:
        with self.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def one(self, sql: str, params=None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self):
        self.pool.close()
