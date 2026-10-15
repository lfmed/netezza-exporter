"""Carga de tabelas exportáveis via CSV: modelo, exportação, leitura e validação."""
import csv
import io
import re
from dataclasses import dataclass, field
from datetime import date, datetime

from . import faixa
from .procedure import DEFAULT_PROFILE, PROFILES, validate_destination, validate_table_config

# coluna, obrigatória, descrição (vai para o modelo e para a tela)
COLUMNS = [
    ("table_config_id", True, "Identificador lógico da tabela no app (maiúsculas, sem espaços). Ex.: FATO_VENDAS"),
    ("source_id", False, "Origem Netezza cadastrada em Administração (padrão: a primeira origem)"),
    ("active", False, "sim/não: tabela disponível para novos pedidos (padrão: sim)"),
    ("call_profile", False, "Perfil de chamada: SP_CARGA_EVENTUAL (7 parâmetros) ou TABELA_INI_FIM (padrão: SP_CARGA_EVENTUAL)"),
    ("procedure_name", False, "Procedure chamada (padrão do perfil: SP_CARGA_EVENTUAL)"),
    ("source_database", False, "p_banco: banco de origem dos metadados (vazio = padrão da conexão)"),
    ("source_table", True, "p_tabela: nome da tabela no Netezza. Ex.: FATO_VENDAS"),
    ("ext_database", False, "p_banco_ext_table: banco da external table (vazio = padrão da conexão)"),
    ("ext_schema", False, "p_esquema_ext_table: esquema da external table (vazio = padrão da conexão)"),
    ("filter_type", False, "Tipo de filtro: DATA (datas AAAAMMDD), NUMERO (faixa de inteiros mínimo-máximo) ou SEM_FILTRO (tabela inteira numa chamada) (padrão: DATA)"),
    ("time_column", False, "p_NomeColunaTempo: coluna do filtro (obrigatória em DATA e NUMERO no perfil SP_CARGA_EVENTUAL; vazia em SEM_FILTRO)"),
    ("dest_catalog", False, "Destino no Databricks: catálogo do Unity Catalog"),
    ("dest_schema", False, "Destino no Databricks: schema"),
    ("dest_table", False, "Destino no Databricks: tabela Delta que recebe o .dat"),
    ("default_subblock_unit", False, "Unidade do subbloco padrão: DIA, SEMANA, MES ou ANO (padrão: MES)"),
    ("default_subblock_size", False, "Quantidade de unidades por subbloco (padrão: 1)"),
    ("default_subblock_count", False, "NUMERO: em quantos subblocos dividir a faixa do escopo (padrão: 10, máx. 5000)"),
    ("max_parallel", False, "Máximo de chamadas simultâneas desta tabela (padrão: 2)"),
    ("call_timeout_min", False, "Timeout de cada chamada, em minutos (padrão: 240)"),
    ("max_retries", False, "Retries automáticos após falha (padrão: 2)"),
    ("retry_unknown", False, "sim/não: repetir resultado desconhecido automaticamente; só se a procedure for idempotente (padrão: não)"),
    ("migration_start", False, "Primeiro dia de dados a migrar (AAAA-MM-DD ou DD/MM/AAAA)"),
    ("migration_end", False, "Último dia de dados a migrar, inclusivo (AAAA-MM-DD ou DD/MM/AAAA)"),
    ("scope_min", False, "NUMERO: menor valor da coluna a migrar (inteiro)"),
    ("scope_max", False, "NUMERO: maior valor da coluna a migrar, inclusivo (inteiro)"),
    ("description", False, "Descrição livre"),
]
FIELDS = [c[0] for c in COLUMNS]
LEGACY_FIELDS = {"date_format"}   # aceita e ignora (Data é sempre AAAAMMDD desde a v1.010)
VERSION_FIELDS = ("call_profile", "procedure_name", "source_database", "source_table", "ext_database", "ext_schema",
                  "filter_type", "time_column", "dest_catalog", "dest_schema", "dest_table", "default_subblock_unit",
                  "default_subblock_size", "default_subblock_count", "max_parallel", "call_timeout_min", "max_retries",
                  "retry_unknown", "migration_start", "migration_end", "scope_num_min", "scope_num_max", "description")
_FILTER_ALIASES = {"DATA": "DATE", "DATE": "DATE", "NUMERO": "NUMBER", "NÚMERO": "NUMBER", "NUMBER": "NUMBER",
                   "SEM_FILTRO": "NONE", "SEM FILTRO": "NONE", "SEMFILTRO": "NONE", "NONE": "NONE"}
_FILTER_PT = {"DATE": "DATA", "NUMBER": "NUMERO", "NONE": "SEM_FILTRO"}

_UNIT_ALIASES = {"DAY": "DAY", "DIA": "DAY", "DIAS": "DAY", "D": "DAY",
                 "WEEK": "WEEK", "SEMANA": "WEEK", "SEMANAS": "WEEK", "W": "WEEK",
                 "MONTH": "MONTH", "MES": "MONTH", "MÊS": "MONTH", "MESES": "MONTH", "M": "MONTH",
                 "YEAR": "YEAR", "ANO": "YEAR", "ANOS": "YEAR", "Y": "YEAR"}
_UNIT_PT = {"DAY": "DIA", "WEEK": "SEMANA", "MONTH": "MES", "YEAR": "ANO"}
_TRUE = {"sim", "s", "true", "t", "1", "yes", "y", "x"}
_FALSE = {"nao", "não", "n", "false", "f", "0", "no", ""}
_TABLE_ID = re.compile(r"^[A-Z0-9_]{1,100}$")
MAX_ROWS = 2000

# na ordem de FIELDS
EXAMPLE_ROWS = [
    ["FATO_VENDAS", "netezza", "sim", "SP_CARGA_EVENTUAL", "SP_CARGA_EVENTUAL", "DWA", "FATO_VENDAS", "", "", "DATA",
     "SK_DATA", "migracao", "bronze", "fato_vendas", "MES", "1", "", "2", "240", "2", "não", "2015-01-01", "2025-12-31",
     "", "", "Banco/esquema da external table vazios: usa o padrão da conexão"],
    ["FATO_SAUDE", "netezza", "sim", "SP_CARGA_EVENTUAL", "SP_CARGA_EVENTUAL", "DWA", "FATO_SAUDE", "STAGE", "ADMIN", "DATA",
     "DT_REFERENCIA", "migracao", "bronze", "fato_saude", "MES", "3", "", "1", "480", "1", "não", "01/01/2018", "31/12/2025",
     "", "", "Subbloco trimestral; external table em STAGE.ADMIN"],
    ["FATO_CONTRATO", "netezza", "sim", "SP_CARGA_EVENTUAL", "SP_CARGA_EVENTUAL", "DWA", "FATO_CONTRATO", "", "", "NUMERO",
     "NR_CONTRATO", "migracao", "bronze", "fato_contrato", "", "", "20", "4", "120", "2", "não", "", "",
     "1", "250000000", "Faixa de 1 a 250 milhões dividida em 20 subblocos (maior valor primeiro)"],
    ["DIM_AGENCIA", "netezza", "sim", "SP_CARGA_EVENTUAL", "SP_CARGA_EVENTUAL", "DWA", "DIM_AGENCIA", "", "", "SEM_FILTRO",
     "", "migracao", "bronze", "dim_agencia", "", "", "", "1", "60", "1", "não", "", "", "", "",
     "Tabela inteira numa chamada; o destino é apagado inteiro em reprocesso"],
]


def template_csv() -> str:
    """Modelo para preencher: cabeçalho + exemplos. Separador ';' e BOM para abrir direto no Excel."""
    out = io.StringIO()
    w = csv.writer(out, delimiter=";", lineterminator="\r\n")
    w.writerow(FIELDS)
    w.writerows(EXAMPLE_ROWS)
    return "﻿" + out.getvalue()


def export_csv(tables: list[dict]) -> str:
    out = io.StringIO()
    w = csv.writer(out, delimiter=";", lineterminator="\r\n")
    w.writerow(FIELDS)
    for t in tables:
        w.writerow([
            t["table_config_id"], t["source_id"], "sim" if t["active"] else "não", t.get("call_profile") or "",
            t["procedure_name"], t.get("source_database") or "", t["source_table"], t.get("ext_database") or "",
            t.get("ext_schema") or "", _FILTER_PT[t.get("filter_type") or "DATE"], t.get("time_column") or "",
            t.get("dest_catalog") or "", t.get("dest_schema") or "", t.get("dest_table") or "",
            _UNIT_PT[t["default_subblock_unit"]], t["default_subblock_size"], t.get("default_subblock_count") or 10,
            t["max_parallel"], t["call_timeout_min"], t["max_retries"], "sim" if t["retry_unknown"] else "não",
            t["migration_start"].isoformat() if t["migration_start"] else "",
            t["migration_end"].isoformat() if t["migration_end"] else "",
            "" if t.get("scope_num_min") is None else t["scope_num_min"],
            "" if t.get("scope_num_max") is None else t["scope_num_max"], t["description"] or ""])
    return "﻿" + out.getvalue()


@dataclass
class ParsedRow:
    line: int
    raw: dict
    values: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)
    action: str = ""            # NOVA | NOVA_VERSAO | SEM_ALTERACAO | ERRO
    changes: list = field(default_factory=list)


def decode(data: bytes) -> str:
    for enc in ("utf-8-sig", "cp1252"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("não foi possível ler o arquivo: use UTF-8 ou o padrão do Excel")


def _bool(v: str, default: bool) -> bool:
    s = (v or "").strip().lower()
    if s == "":
        return default
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    raise ValueError(f"use sim/não (recebido {v!r})")


def _int(v: str, default: int, lo: int, hi: int) -> int:
    s = (v or "").strip()
    if s == "":
        return default
    if not re.fullmatch(r"\d+", s):
        raise ValueError(f"número inteiro inválido: {v!r}")
    n = int(s)
    if not lo <= n <= hi:
        raise ValueError(f"valor fora do intervalo {lo}–{hi}: {n}")
    return n


def _date(v: str) -> date | None:
    s = (v or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"data inválida: {v!r} (use AAAA-MM-DD ou DD/MM/AAAA)")


def parse(text: str, sources) -> list[ParsedRow]:
    """sources: lista de ids ou dict id -> linha da conexão (para validar a chamada com os padrões dela)."""
    source_rows = sources if isinstance(sources, dict) else {s: {} for s in sources}
    sources = list(source_rows)
    """Lê e valida o CSV. Não consulta o estado atual; a classificação nova/versão é feita depois."""
    text = text.lstrip("﻿")
    if not text.strip():
        raise ValueError("arquivo vazio")
    first = text.splitlines()[0]
    delim = ";" if first.count(";") >= first.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    header = [h.strip().lower() for h in (reader.fieldnames or [])]
    reader.fieldnames = header
    missing = [c for c, req, _ in COLUMNS if req and c not in header]
    if missing:
        raise ValueError(f"colunas obrigatórias ausentes no cabeçalho: {', '.join(missing)}")
    unknown = [h for h in header if h and h not in FIELDS and h not in LEGACY_FIELDS]
    if unknown:
        raise ValueError(f"colunas desconhecidas: {', '.join(unknown)} (baixe o modelo para ver as colunas aceitas)")

    rows: list[ParsedRow] = []
    seen: dict[str, int] = {}
    for i, raw in enumerate(reader, start=2):
        raw = {k: (v or "").strip() for k, v in raw.items() if k}
        if not any(raw.values()):
            continue  # linha em branco
        if len(rows) >= MAX_ROWS:
            raise ValueError(f"limite de {MAX_ROWS} linhas por arquivo")
        r = ParsedRow(line=i, raw=raw)
        v = r.values

        def check(name, fn):
            try:
                v[name] = fn(raw.get(name, ""))
            except ValueError as e:
                r.errors.append(f"{name}: {e}")

        tid = raw.get("table_config_id", "").upper()
        if not _TABLE_ID.match(tid):
            r.errors.append("table_config_id: obrigatório, só letras maiúsculas, números e _")
        elif tid in seen:
            r.errors.append(f"table_config_id {tid} repetido (linha {seen[tid]})")
        else:
            seen[tid] = i
        v["table_config_id"] = tid
        src = raw.get("source_id") or (sources[0] if sources else "")
        if src not in sources:
            r.errors.append(f"source_id: origem {src!r} não cadastrada ({', '.join(sources) or 'nenhuma'})")
        v["source_id"] = src
        check("active", lambda s: _bool(s, True))
        profile = (raw.get("call_profile") or DEFAULT_PROFILE).upper()
        if profile not in PROFILES:
            r.errors.append(f"call_profile: use {' ou '.join(PROFILES)} (recebido {profile!r})")
        v["call_profile"] = profile if profile in PROFILES else DEFAULT_PROFILE
        v["procedure_name"] = (raw.get("procedure_name") or PROFILES[v["call_profile"]]["default_procedure"]).upper()
        for k in ("source_database", "source_table", "ext_database", "ext_schema", "time_column"):
            v[k] = (raw.get(k) or "").upper() or None
        ft_raw = (raw.get("filter_type") or "DATA").upper()
        if ft_raw not in _FILTER_ALIASES:
            r.errors.append(f"filter_type: use DATA, NUMERO ou SEM_FILTRO (recebido {ft_raw!r})")
        ft = v["filter_type"] = _FILTER_ALIASES.get(ft_raw, "DATE")
        v["date_format"] = "YYYYMMDD"
        if ft == "NONE":
            v["time_column"] = None
        elif ft == "NUMBER" and not v["time_column"]:
            r.errors.append("time_column: obrigatória no filtro NUMERO")
        if not v["source_table"]:
            r.errors.append("source_table: obrigatório")
        elif v["call_profile"] in PROFILES:
            r.errors += [f"chamada: {e}" for e in validate_table_config(v, source_rows.get(src) or {})]
        for k in ("dest_catalog", "dest_schema", "dest_table"):
            v[k] = raw.get(k) or None
        r.errors += [f"destino: {e}" for e in validate_destination(v)]
        unit = (raw.get("default_subblock_unit") or "MES").upper()
        if unit not in _UNIT_ALIASES:
            r.errors.append(f"default_subblock_unit: use DIA, SEMANA, MES ou ANO (recebido {unit!r})")
        v["default_subblock_unit"] = _UNIT_ALIASES.get(unit, "MONTH")
        check("default_subblock_size", lambda s: _int(s, 1, 1, 1000))
        check("default_subblock_count", lambda s: _int(s, 10, 1, faixa.MAX_COUNT))
        check("max_parallel", lambda s: _int(s, 2, 1, 200))
        check("call_timeout_min", lambda s: _int(s, 240, 1, 10080))
        check("max_retries", lambda s: _int(s, 2, 0, 10))
        check("retry_unknown", lambda s: _bool(s, False))
        check("migration_start", _date)
        check("migration_end", _date)
        for col, key in (("scope_min", "scope_num_min"), ("scope_max", "scope_num_max")):
            try:
                v[key] = faixa.parse_int(raw[col], col) if (raw.get(col) or "").strip() else None
            except ValueError as e:
                r.errors.append(str(e))
        if ft != "DATE":
            v["migration_start"] = v["migration_end"] = None
        if ft != "NUMBER":
            v["scope_num_min"] = v["scope_num_max"] = None
        if v.get("migration_start") and v.get("migration_end") and v["migration_start"] > v["migration_end"]:
            r.errors.append("migration_start maior que migration_end")
        if bool(v.get("migration_start")) != bool(v.get("migration_end")) and not any("migration" in e for e in r.errors):
            r.errors.append("informe migration_start e migration_end juntos (ou deixe os dois vazios)")
        lo, hi = v.get("scope_num_min"), v.get("scope_num_max")
        if lo is not None and hi is not None and lo > hi:
            r.errors.append("scope_min maior que scope_max")
        if (lo is None) != (hi is None) and not any("scope_" in e for e in r.errors):
            r.errors.append("informe scope_min e scope_max juntos (ou deixe os dois vazios)")
        v["description"] = raw.get("description") or None
        rows.append(r)
    if not rows:
        raise ValueError("nenhuma linha de dados no arquivo")
    return rows


def classify(rows: list[ParsedRow], current: dict[str, dict]) -> list[ParsedRow]:
    """Compara com a configuração atual: NOVA, NOVA_VERSAO (com as mudanças) ou SEM_ALTERACAO."""
    for r in rows:
        if r.errors:
            r.action = "ERRO"
            continue
        cur = current.get(r.values["table_config_id"])
        if cur is None:
            r.action = "NOVA"
            continue
        for f in ("source_id", "active", *VERSION_FIELDS):
            old, new = cur.get(f), r.values.get(f)
            if (old or None) != (new or None) and old != new:
                r.changes.append({"field": f, "old": old, "new": new})
        r.action = "NOVA_VERSAO" if r.changes else "SEM_ALTERACAO"
    return rows
