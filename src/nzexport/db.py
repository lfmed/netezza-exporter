"""Conexão com o Lakebase (Postgres) usando token OAuth renovado automaticamente."""
import os
import threading
import time
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

SCHEMA = "nzexport"
_TOKEN_TTL_S = 40 * 60


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
        self.endpoint = endpoint or os.environ["LAKEBASE_ENDPOINT"]
        self.database = database or os.environ.get("PGDATABASE", "databricks_postgres")
        self.host = os.environ.get("PGHOST") or self.w.postgres.get_endpoint(name=self.endpoint).status.hosts.host
        self.user = os.environ.get("PGUSER") or self._default_user()
        self.port = int(os.environ.get("PGPORT", "5432"))
        self.tokens = _TokenProvider(self.w, self.endpoint)

    def _default_user(self) -> str:
        me = self.w.current_user.me()
        # Service principals se autenticam com o application id
        return me.user_name if me.user_name and "@" in me.user_name else (me.application_id or me.user_name)

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
        tokens = cfg.tokens

        class _OAuthConnection(psycopg.Connection):
            @classmethod
            def connect(cls, conninfo="", **kw):
                kw["password"] = tokens.get()
                return super().connect(conninfo, **kw)

        self.pool = ConnectionPool(
            kwargs={**cfg.conninfo(), "row_factory": dict_row},
            connection_class=_OAuthConnection,
            min_size=min_size, max_size=max_size,
            max_lifetime=_TOKEN_TTL_S, max_idle=300,
            check=ConnectionPool.check_connection, open=True,
        )

    @contextmanager
    def tx(self):
        """Transação: commit ao sair sem erro, rollback em exceção."""
        with self.pool.connection() as conn:
            with conn.transaction():
                yield conn

    def query(self, sql: str, params=None) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def one(self, sql: str, params=None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self):
        self.pool.close()
