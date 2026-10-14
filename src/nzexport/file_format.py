"""Formato do .dat por tabela. Vazio = padrão do motor PLHDWA (mesmo formato do Netezza do cliente)."""
import re

# chave: (padrão, rótulo, ajuda)
FILE_OPTIONS = {
    "delimitador":      ("|", "Delimitador", "separador de colunas"),
    "encoding":         ("ISO-8859-15", "Encoding", "LATIN9 do Netezza (padrão do PLHDWA)"),
    "header":           ("true", "Primeira linha é o header", "true/false (a SP grava com IncludeHeader)"),
    "quote":            ("", "Aspas (sem limpeza)", "aspas do texto quando a limpeza está desligada"),
    "escape":           ("", "Escape (sem limpeza)", "caractere de escape quando a limpeza está desligada"),
    "null_value":       ("", "Valor nulo", "texto que representa NULL"),
    "date_format":      ("yyyy-MM-dd", "Formato de data", "padrão Spark"),
    "timestamp_format": ("yyyy-MM-dd HH:mm[:ss]", "Formato de data/hora", "padrão Spark"),
    "limpeza":          ("true", "Limpeza do DAT (regex do cliente)", "true/false"),
    "limpeza_modo":     ("CLIENTE", "Modo da limpeza", "CLIENTE, AUTO, PARALELA, DIRETA ou MULTILINHA"),
    "limpeza_quote":    ("'", "Aspas do texto (com limpeza)", "a SP grava os textos com quote_literal (aspas simples)"),
}
CLEAN_MODES = ("CLIENTE", "AUTO", "PARALELA", "DIRETA", "MULTILINHA")


def effective(options: dict | None) -> dict:
    """Opções efetivas: as do cadastro sobre os padrões."""
    o = options or {}
    return {k: (o.get(k) if o.get(k) not in (None, "") else d[0]) for k, d in FILE_OPTIONS.items()}


def clean(form_values: dict) -> dict:
    """Guarda só o que difere do padrão (cadastro enxuto); valida os valores fechados."""
    out = {}
    for k, (default, _label, _help) in FILE_OPTIONS.items():
        v = form_values.get(k)
        if v is None or str(v) == "":
            continue  # em branco = padrão
        v = str(v)
        if k in ("header", "limpeza"):
            v = "true" if v.lower() in ("true", "1", "sim", "s", "on") else "false"
        if k == "limpeza_modo":
            v = v.upper()
            if v not in CLEAN_MODES:
                raise ValueError(f"modo de limpeza inválido: {v}")
        if k == "delimitador" and len(v) != 1:
            raise ValueError("delimitador deve ter 1 caractere")
        if v != default:
            out[k] = v
    return out


_NOME = re.compile(r"[A-Za-z0-9_]+")


def predicado_periodo(coluna: str, tipo: str, ini: str, fim: str) -> str:
    """Filtro SQL do período do subbloco (ini/fim AAAAMMDD, inclusivos) conforme o tipo da coluna de tempo,
    como no PLHDWA: DATE, texto ou numérico (AAAAMMDD); timestamp cobre o dia final inteiro."""
    if not (_NOME.fullmatch(coluna or "") and re.fullmatch(r"\d{8}", ini) and re.fullmatch(r"\d{8}", fim)):
        raise ValueError(f"período/coluna inválidos: {coluna!r} {ini}..{fim}")
    c = f"`{coluna}`"
    d = lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}"  # noqa: E731
    t = (tipo or "").lower()
    if t == "date":
        return f"{c} BETWEEN DATE'{d(ini)}' AND DATE'{d(fim)}'"
    if t.startswith("timestamp"):
        return f"{c} >= TIMESTAMP'{d(ini)} 00:00:00' AND {c} < TIMESTAMP'{d(fim)} 00:00:00' + INTERVAL 1 DAY"
    if t in ("string", "varchar", "char") or t.startswith(("varchar", "char")):
        return f"{c} BETWEEN '{ini}' AND '{fim}'"
    return f"{c} BETWEEN {int(ini)} AND {int(fim)}"
