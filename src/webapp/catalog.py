"""Navegação no Unity Catalog para escolher a tabela destino (identidade do app: precisa de BROWSE/USE)."""
import time

from . import services

_CACHE: dict[tuple, tuple[float, list]] = {}
_TTL_S = 60


def _cached(key, fn):
    hit = _CACHE.get(key)
    if hit and time.time() - hit[0] < _TTL_S:
        return hit[1]
    val = fn()
    _CACHE[key] = (time.time(), val)
    return val


def catalogs() -> list[dict]:
    def load():
        out = []
        for c in services.workspace().catalogs.list():
            if c.name in ("system", "__databricks_internal", "samples"):
                continue
            out.append({"name": c.name, "comment": c.comment or "", "type": str(c.catalog_type.value) if c.catalog_type else ""})
        return sorted(out, key=lambda x: x["name"])
    return _cached(("cat",), load)


def schemas(catalog: str) -> list[dict]:
    def load():
        return sorted(({"name": s.name, "comment": s.comment or ""}
                       for s in services.workspace().schemas.list(catalog_name=catalog)
                       if s.name != "information_schema"), key=lambda x: x["name"])
    return _cached(("sch", catalog), load)


def tables(catalog: str, schema: str) -> list[dict]:
    def load():
        return sorted(({"name": t.name, "type": str(t.table_type.value) if t.table_type else "",
                        "comment": t.comment or ""}
                       for t in services.workspace().tables.list(catalog_name=catalog, schema_name=schema,
                                                                 omit_columns=True)), key=lambda x: x["name"])
    return _cached(("tab", catalog, schema), load)


def table_info(full_name: str) -> dict:
    """Existe? Tipo, formato e colunas (para conferir a coluna de tempo)."""
    try:
        t = services.workspace().tables.get(full_name=full_name)
    except Exception as e:  # noqa: BLE001 - inexistente ou sem permissão
        msg = str(e)
        return {"exists": False, "error": "não encontrada ou sem permissão de leitura" if "not" in msg.lower()
                or "permission" in msg.lower() or "does not exist" in msg.lower() else msg[:300]}
    return {"exists": True, "type": str(t.table_type.value) if t.table_type else "",
            "format": str(t.data_source_format.value) if t.data_source_format else "",
            "columns": [{"name": c.name, "type": c.type_text} for c in (t.columns or [])]}
