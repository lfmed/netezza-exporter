"""Tipo de filtro da tabela e faixa genérica dos subblocos.

Todo subbloco guarda uma faixa inteira [lo, hi], inclusiva (no banco, int8range semiaberto [lo, hi+1)):
  * DATE:   dias desde 1970-01-01 (contíguo: 31/01 -> 01/02 = +1); a SP recebe AAAAMMDD
  * NUMBER: o próprio valor inteiro; a SP recebe o número
  * NONE:   faixa fixa [0, 0] = a tabela inteira numa única chamada; a SP recebe um filtro sempre verdadeiro

No app, os valores "nativos" de cada tipo são: date (DATE), int (NUMBER) e 0 (NONE).
"""
from datetime import date, timedelta

FILTER_TYPES = {"DATE": "Data (AAAAMMDD)", "NUMBER": "Número inteiro", "NONE": "Sem filtro (tabela inteira)"}
DEFAULT_FILTER = "DATE"
EPOCH = date(1970, 1, 1)
NUM_MIN, NUM_MAX = -(2 ** 63), 2 ** 63 - 2   # hi + 1 precisa caber em bigint
ALWAYS_TRUE = "1"   # coluna, ini e fim no sem filtro: WHERE 1 BETWEEN 1 AND 1 (a tabela toda)
MAX_COUNT = 5000
UNIT_NAME = {"DATE": "dias", "NUMBER": "valores", "NONE": "tabela"}


def ftype(cfg: dict | None) -> str:
    ft = (cfg or {}).get("filter_type") or DEFAULT_FILTER
    if ft not in FILTER_TYPES:
        raise ValueError(f"tipo de filtro inválido: {ft!r}")
    return ft


def to_key(ft: str, v) -> int:
    if ft == "DATE":
        return (v - EPOCH).days
    if ft == "NUMBER":
        return int(v)
    return 0


def from_key(ft: str, k: int):
    if ft == "DATE":
        return EPOCH + timedelta(days=int(k))
    if ft == "NUMBER":
        return int(k)
    return 0


def units(ft: str, a, b) -> int:
    """Tamanho da faixa inclusiva [a, b] em unidades do tipo (dias, valores ou 1 tabela)."""
    if ft == "DATE":
        return (b - a).days + 1
    if ft == "NUMBER":
        return b - a + 1
    return 1


def parse_int(v, campo: str = "valor") -> int:
    """Inteiro digitado pelo usuário (aceita separador de milhar com ponto ou espaço)."""
    txt = str(v if v is not None else "").strip().replace(".", "").replace(" ", "").replace("_", "")
    try:
        n = int(txt)
    except ValueError:
        raise ValueError(f"{campo}: número inteiro inválido ({v!r})") from None
    if not NUM_MIN <= n <= NUM_MAX:
        raise ValueError(f"{campo}: fora do intervalo do bigint ({n})")
    return n


def scope(cfg: dict) -> tuple | None:
    """Escopo da migração em valores nativos (inclusivo) ou None se não definido."""
    ft = ftype(cfg)
    if ft == "NONE":
        return (0, 0)
    a, b = (cfg.get("scope_num_min"), cfg.get("scope_num_max")) if ft == "NUMBER" else \
        (cfg.get("migration_start"), cfg.get("migration_end"))
    return (a, b) if a is not None and b is not None else None


def fmt_value(ft: str, v) -> str:
    if v is None or v == "":
        return "—"
    if ft == "DATE":
        return v.strftime("%d/%m/%Y") if isinstance(v, date) else str(v)
    if ft == "NUMBER":
        return f"{int(v):,}".replace(",", ".")
    return "tabela inteira"


def fmt_range(ft: str, a, b) -> str:
    if ft == "NONE":
        return "tabela inteira"
    return f"{fmt_value(ft, a)} → {fmt_value(ft, b)}"


def example(ft: str) -> tuple:
    """Subbloco de exemplo para validar o cadastro e para a prévia da chamada."""
    if ft == "DATE":
        return date(2016, 1, 1), date(2016, 1, 31)
    if ft == "NUMBER":
        return 1, 1_000_000
    return 0, 0
