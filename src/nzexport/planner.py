"""Geração de subblocos a partir da faixa total (inclusiva).

Data: cortes de calendário (dia, semana, mês, ano). Número: a faixa dividida numa quantidade de subblocos.
As funções de intervalo (subtract_ranges, merge_contiguous) servem para datas e para inteiros.
"""
from dataclasses import dataclass
from datetime import date, timedelta

UNITS = ("DAY", "WEEK", "MONTH", "YEAR")
MAX_SUBBLOCKS_PER_TABLE = 5000


@dataclass(frozen=True)
class Subblock:
    seq: int
    start: date | int    # início enviado à procedure (data ou número)
    end: date | int      # fim enviado à procedure (inclusivo)


def _one(v):
    """Passo unitário do tipo: um dia para datas, 1 para inteiros."""
    return timedelta(days=1) if isinstance(v, date) else 1


def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def _advance(d: date, unit: str, size: int) -> date:
    if unit == "DAY":
        return d + timedelta(days=size)
    if unit == "WEEK":
        return d + timedelta(weeks=size)
    if unit == "MONTH":
        return _add_months(d, size) if d.day == 1 else _add_months_keep_day(d, size)
    if unit == "YEAR":
        return _add_months(d, 12 * size) if (d.day, d.month) == (1, 1) else _add_months_keep_day(d, 12 * size)
    raise ValueError(f"unidade inválida: {unit}")


def _add_months_keep_day(d: date, n: int) -> date:
    first = _add_months(date(d.year, d.month, 1), n)
    nxt = _add_months(first, 1)
    return first.replace(day=min(d.day, (nxt - timedelta(days=1)).day))


def _align(d: date, unit: str) -> date:
    """Início do período de calendário que contém d."""
    if unit == "WEEK":
        return d - timedelta(days=d.weekday())
    if unit == "MONTH":
        return d.replace(day=1)
    if unit == "YEAR":
        return date(d.year, 1, 1)
    return d


def plan(period_start: date, period_end: date, unit: str, size: int,
         calendar_aligned: bool = True) -> list[Subblock]:
    """Divide [period_start, period_end] (inclusivo) em subblocos contíguos e sem sobreposição.

    Com calendar_aligned, os cortes caem no início de semana/mês/ano; o primeiro e o
    último subbloco são truncados aos limites do período total.
    """
    if unit not in UNITS:
        raise ValueError(f"unidade inválida: {unit}")
    if size < 1:
        raise ValueError("tamanho do subbloco deve ser >= 1")
    if period_start > period_end:
        raise ValueError("data inicial maior que a data final")

    upper = period_end + timedelta(days=1)
    cursor = period_start
    base = _align(period_start, unit) if calendar_aligned else period_start
    if calendar_aligned and unit == "MONTH" and size > 1 and 12 % size == 0:
        # bimestre/trimestre/semestre: cortes contados a partir de janeiro
        base = date(base.year, (base.month - 1) // size * size + 1, 1)
    k = 1
    blocks: list[Subblock] = []
    while cursor < upper:
        # cortes sempre calculados a partir da base, para não acumular desvio de fim de mês
        nxt = _advance(base, unit, k * size)
        k += 1
        if nxt <= cursor:
            continue
        stop = min(nxt, upper)
        blocks.append(Subblock(seq=len(blocks) + 1, start=cursor, end=stop - timedelta(days=1)))
        if len(blocks) > MAX_SUBBLOCKS_PER_TABLE:
            raise ValueError(f"mais de {MAX_SUBBLOCKS_PER_TABLE} subblocos; aumente o tamanho do subbloco")
        cursor = stop
    return blocks


def plan_count(lo: int, hi: int, count: int) -> list[Subblock]:
    """Divide a faixa inteira [lo, hi] (inclusiva) em `count` subblocos contíguos de tamanho igual.

    Passo = teto(tamanho / count); o último subbloco é truncado em hi. Com count maior que a faixa,
    sai um subbloco por valor.
    """
    if count < 1:
        raise ValueError("a quantidade de subblocos deve ser >= 1")
    if lo > hi:
        raise ValueError("valor inicial maior que o valor final")
    step = -(-(hi - lo + 1) // count)
    if -(-(hi - lo + 1) // step) > MAX_SUBBLOCKS_PER_TABLE:
        raise ValueError(f"mais de {MAX_SUBBLOCKS_PER_TABLE} subblocos; diminua a quantidade")
    return [Subblock(seq=i + 1, start=a, end=min(a + step - 1, hi)) for i, a in enumerate(range(lo, hi + 1, step))]


def plan_count_pieces(lo: int, hi: int, count: int, pieces: list[tuple[int, int]]) -> list[Subblock]:
    """Cortes da faixa pedida inteira, recortados aos pedaços que ainda faltam (as faixas já executadas
    saem sem deslocar os cortes)."""
    blocks: list[Subblock] = []
    for b in plan_count(lo, hi, count):
        for p_lo, p_hi in pieces:
            a, z = max(b.start, p_lo), min(b.end, p_hi)
            if a <= z:
                blocks.append(Subblock(seq=len(blocks) + 1, start=a, end=z))
    if len(blocks) > MAX_SUBBLOCKS_PER_TABLE:
        raise ValueError(f"mais de {MAX_SUBBLOCKS_PER_TABLE} subblocos; diminua a quantidade")
    return blocks


def subtract_ranges(start, end, excluded: list[tuple]) -> list[tuple]:
    """Retorna as partes de [start, end] (inclusivo) que não estão em nenhum intervalo excluído (inclusivos).
    Serve para datas e para inteiros."""
    one = _one(start)
    pieces = []
    cursor = start
    for ex_start, ex_end in sorted(excluded):
        if ex_end < cursor or ex_start > end:
            continue
        if ex_start > cursor:
            pieces.append((cursor, ex_start - one))
        cursor = max(cursor, ex_end + one)
        if cursor > end:
            break
    if cursor <= end:
        pieces.append((cursor, end))
    return pieces


def plan_pieces(pieces: list[tuple[date, date]], unit: str, size: int,
                calendar_aligned: bool = True) -> list[Subblock]:
    """Planeja vários intervalos disjuntos com numeração contínua de subblocos."""
    blocks: list[Subblock] = []
    for p_start, p_end in pieces:
        for b in plan(p_start, p_end, unit, size, calendar_aligned):
            blocks.append(Subblock(seq=len(blocks) + 1, start=b.start, end=b.end))
    if len(blocks) > MAX_SUBBLOCKS_PER_TABLE:
        raise ValueError(f"mais de {MAX_SUBBLOCKS_PER_TABLE} subblocos; aumente o tamanho do subbloco")
    return blocks


def merge_contiguous(ranges: list[tuple]) -> list[tuple]:
    out: list[list] = []
    for s, e in sorted(ranges):
        if out and s <= out[-1][1] + _one(s):
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]
