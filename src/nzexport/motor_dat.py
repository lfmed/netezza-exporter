"""Motor de leitura do .dat — reaproveitado do motor PLHDWA (lfmed/plhdwa-carga-netezza, src/plhdwa_motor.py).

Copiado SEM alteração (leitura com o schema da tabela + FAILFAST, limpeza do DAT com as 3 regex do cliente nos
modos CLIENTE/AUTO/PARALELA/DIRETA/MULTILINHA, diagnóstico da 1ª linha inválida e classificação de erros):
  ErroCarga, _p, _leitura, ler_dat, ler_dat_escape, ler_header, _RX_LIMPEZA, modo_limpeza, _aplicar_regex,
  _linhas_limpas, linhas_com_aspa_aberta, _linhas_posicionadas, _LayoutMudou, preparar_cliente, _preparar_cliente,
  _schema_ddl, _opcoes_direta, ler_limpo_direto, primeira_linha_invalida_direta, preparar_dat, apagar_tmp,
  explicar_erro, classificar.

AJUSTADO para este projeto (sem CTL, sem Db2; o app controla os subblocos):
  * CTX: só spark, dbutils, limpeza_pedaco_mb e as pastas da landing vindas da conexão do app;
  * caminho_tmp: pasta de limpeza própria (o .dat da SP fica em tmp/);
  * limpeza_ativa: opção 'limpeza' do formato do arquivo no cadastro da tabela.
NOVO (seção "Carga do subbloco"): metadados do destino, conferência do header, APPEND sem validação de período ou
conteúdo, limpeza do período no destino (reprocesso e erro), retomada pelo userMetadata e movimento para a pasta de erro.
"""
import re
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

from .file_format import CLEAN_MODES, predicado_periodo

CTX = {"spark": None, "dbutils": None, "limpeza_pedaco_mb": 0, "pasta_limpeza": None}
MODOS_LIMPEZA = CLEAN_MODES


def iniciar(spark, dbutils, pasta_limpeza: str, limpeza_pedaco_mb: int = 0):
    CTX.update(spark=spark, dbutils=dbutils, pasta_limpeza=pasta_limpeza.rstrip("/"),
               limpeza_pedaco_mb=limpeza_pedaco_mb)


def limpeza_ativa(meta):
    """Opção 'limpeza' do formato do arquivo (padrão: ligada, como no PLHDWA)."""
    return _p(meta, "limpeza", "true").lower() == "true"


def caminho_tmp(arq):
    return f"{(arq.get('pasta_limpeza') or CTX['pasta_limpeza']).rstrip('/')}/{arq['base']}"


# =============================================================================
# Reaproveitado do motor PLHDWA (sem alteração)
# =============================================================================
class ErroCarga(Exception):
    """Erro de negócio ou de processamento. tipo = status gravado no log (ERRO_*)."""
    def __init__(self, tipo, mensagem, mover=True):
        super().__init__(f"{tipo}: {mensagem}")
        self.tipo, self.mensagem, self.mover = tipo, mensagem[:3000], mover


def _p(meta, chave, padrao=""):
    """Opção do arquivo na linha do objeto (ctl_parametros); vazia/nula = padrão. Booleanos viram "true"/"false"."""
    v = meta["params"].get(chave)
    if isinstance(v, bool):
        return "true" if v else "false"
    return padrao if v in (None, "") else str(v)


def _leitura(arq, meta):
    """(caminho, opções do leitor CSV) da carga: o DAT original ou, com a limpeza, o arquivo limpo em pasta_tmp.
    Arquivo limpo (pedido do cliente): header na 1ª linha, delimitador da tabela, multiLine, aspas simples ('),
    escape '\\' (padrão do Spark) e UTF-8 (o arquivo limpo é gravado em UTF-8). Sem multiLine, o header já saiu
    na limpeza (as partes são lidas em paralelo e o header de cada parte não existe)."""
    tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
    o = {"sep": _p(meta, "delimitador", "|"), "nullValue": _p(meta, "null_value"), "timestampFormat": tsf,
         "timestampNTZFormat": tsf, "dateFormat": _p(meta, "date_format", "yyyy-MM-dd")}
    if arq.get("dat_limpo"):
        modo = arq.get("limpeza_modo")
        multi = modo in ("MULTILINHA", "CLIENTE")       # CLIENTE: partes sem header, lidas com multiLine em paralelo
        o.update(header=str(_p(meta, "header", "true") == "true" and modo == "MULTILINHA").lower(),
                 multiLine=str(multi).lower(),
                 quote=_p(meta, "limpeza_quote", "'"), escape="\\", encoding="UTF-8",
                 lineSep="\n")                   # CR do dado (escapado no Netezza) não é fim de linha
        return arq["dat_limpo"], o
    o.update(header=_p(meta, "header", "true"), encoding=_p(meta, "encoding", "ISO-8859-15"), quote=_p(meta, "quote"))
    return arq["dat"], o


def ler_dat(arq, meta):
    """CSV nativo, com as opções do arquivo da ctl_parametros e o schema da tabela. FAILFAST: linha inválida = erro.
    Com a limpeza ativa, lê o arquivo limpo (pasta_tmp); sem ela, o DAT original (com ESCAPECHAR e sem aspas,
    usa ler_dat_escape)."""
    if arq.get("limpeza_modo") == "DIRETA":
        return ler_limpo_direto(arq, meta)
    if not arq.get("dat_limpo") and _p(meta, "escape") and not _p(meta, "quote"):
        return ler_dat_escape(arq, meta)
    caminho, o = _leitura(arq, meta)
    return (CTX["spark"].read.format("csv").schema(meta["schema"]).options(**o)
            .option("enforceSchema", "false")                 # confere o header com o schema
            .option("mode", "FAILFAST").load(caminho))


def ler_dat_escape(arq, meta):
    """Arquivo sem aspas e com ESCAPECHAR (ex.: 2\\|3 = '2|3' e \\\\ = '\\'). Só funções nativas, sem UDF."""
    sep, esc = _p(meta, "delimitador", "|"), _p(meta, "escape")
    e, d, marca = re.escape(esc), re.escape(sep), "\u0002"
    # Lê cada linha inteira como uma coluna (separador improvável), respeitando o encoding.
    linhas = (CTX["spark"].read.format("csv").schema("linha STRING").option("sep", "\u0001").option("quote", "")
              .option("encoding", _p(meta, "encoding", "ISO-8859-15")).option("header", "false").load(arq["dat"]))
    if _p(meta, "header", "true") == "true":
        linhas = linhas.filter(F.col("linha") != ler_header(arq))
    campos = F.split(F.regexp_replace("linha", e + e, marca), f"(?<!{e}){d}", -1)   # | não escapado
    n = len(meta["schema"].fields)
    df = (linhas.select(campos.alias("c"))
          .filter(F.when(F.size("c") != n, F.raise_error(F.concat(
              F.lit(f"ERRO_DADOS: linha com "), F.size("c").cast("string"), F.lit(f" campos (esperado {n})"))))
              .otherwise(F.lit(True))))
    tsf, nulo = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]"), _p(meta, "null_value")
    cols = []
    for i, campo in enumerate(meta["schema"].fields):
        v = F.translate(F.regexp_replace(F.col("c")[i], e + "(.)", "$1"), marca, esc)   # tira o escape
        v = F.when(v == F.lit(nulo), F.lit(None)).otherwise(v)
        if isinstance(campo.dataType, T.TimestampNTZType):
            v = F.to_timestamp_ntz(v, F.lit(tsf))
        elif not isinstance(campo.dataType, T.StringType):
            v = v.cast(campo.dataType)                                                     # ANSI: inválido = erro
        cols.append(v.alias(campo.name))
    return df.select(cols)


def ler_header(arq):
    return CTX["dbutils"].fs.head(arq["dat"], 65536).split("\n", 1)[0].rstrip("\r")


_RX_LIMPEZA = [(r'\\(?![\\\'"|nrt])', ''),   # barra que não escapa \ ' " | n r t: sai
               (r'\\\|', '|'),                 # \| vira | (o texto vem entre aspas simples)
               (r'\\null', '')]                # \null vira vazio (NULL)


def modo_limpeza(meta):
    """Coluna limpeza_modo da ctl_parametros (nula = CLIENTE)."""
    m = _p(meta, "limpeza_modo").upper()
    return m if m in MODOS_LIMPEZA else "CLIENTE"


def _aplicar_regex(linhas):
    """As 3 regex do cliente, só nas linhas com barra invertida (nas outras não mudam nada e são a grande maioria;
    medido em 5 GB: regex em toda linha 61 s · só nas linhas com barra 16 s, mesmo resultado)."""
    limpo = F.col("value")
    for rx, novo in _RX_LIMPEZA:
        limpo = F.regexp_replace(limpo, rx, novo)
    return linhas.withColumn("value", F.when(F.col("value").contains("\\"), limpo).otherwise(F.col("value")))


def _linhas_limpas(arq, meta, tirar_header, regex=True, bloco=False):
    """Linhas do DAT com o encoding da tabela e as 3 regex do cliente aplicadas (coluna value)."""
    # Fim de linha = LF (lineSep): por padrão o Spark também quebra linha em CR solto, e o Netezza escapa CR do dado
    # como "\\<CR>": sem isso, o registro seria cortado e o CR do dado viraria LF. CR no fim da linha (arquivo CRLF)
    # e não escapado sai.
    linhas = (CTX["spark"].read.format("csv").schema("value STRING").option("sep", "\u0001").option("quote", "")
              .option("header", "false").option("lineSep", "\n").option("encoding", _p(meta, "encoding", "ISO-8859-15"))
              .load(arq["dat"]))
    if bloco:                                         # início do pedaço do arquivo que cada tarefa leu (ordem no arquivo)
        linhas = linhas.select("value", F.col("_metadata.file_block_start").alias("_bloco"))
    v = F.col("value")
    linhas = linhas.withColumn("value", F.when(v.endswith("\r"), F.regexp_replace(v, r"(?<!\\)\r$", "")).otherwise(v))
    if tirar_header and _p(meta, "header", "true") == "true":
        linhas = linhas.filter(F.col("value") != F.lit(ler_header(arq)))
    return _aplicar_regex(linhas) if regex else linhas


def linhas_com_aspa_aberta(arq, meta):
    """Detecção (modo AUTO): linhas com número ímpar de aspas não escapadas = um registro continua na linha de baixo.
    Uma contagem em paralelo no arquivo inteiro. Se errar, erra para o lado seguro (vai para MULTILINHA)."""
    q = re.escape(_p(meta, "limpeza_quote", "'"))
    aspas = F.size(F.split(F.col("value"), f"(?<!\\\\){q}")) - 1
    return (_linhas_limpas(arq, meta, tirar_header=False)
            .filter(F.col("value").contains(_p(meta, "limpeza_quote", "'")) & (aspas % 2 == 1)).count())


def _linhas_posicionadas(arq, meta):
    """Linhas do DAT (sem header) com a posição: _bloco = início do pedaço do arquivo lido por cada tarefa (ordem
    no arquivo) e _pos = ordem da linha DENTRO do pedaço; _impar = 1 se a linha tem número ímpar de aspas não
    escapadas (abre ou fecha um registro que ocupa mais de uma linha). A regex do cliente vem DEPOIS da posição, para
    um filtro por posição não precisar rodar a regex no arquivo inteiro.
    _pos NÃO é o monotonically_increasing_id puro: ele embute o número da tarefa, e o Spark pode dar outro número
    de tarefa ao mesmo pedaço em outra execução (visto no teste de 80 GB). Tirando a tarefa, sobra a ordem da linha
    dentro da tarefa, que é estável (num arquivo só, cada tarefa lê um pedaço; conferido em preparar_cliente)."""
    q = _p(meta, "limpeza_quote", "'")
    linhas = (_linhas_limpas(arq, meta, tirar_header=True, regex=False, bloco=True)
              .withColumn("_pos", F.monotonically_increasing_id()
                          - F.spark_partition_id().cast("long") * F.lit(8589934592)))   # 2^33: sai a tarefa
    linhas = _aplicar_regex(linhas)
    aspas = F.size(F.split(F.col("value"), f"(?<!\\\\){re.escape(q)}")) - 1
    return linhas.withColumn("_impar", (F.col("value").contains(q) & (aspas % 2 == 1)).cast("int"))


class _LayoutMudou(Exception):
    """Os pedaços do arquivo não saíram iguais entre as passadas (o motor refaz a limpeza)."""


def preparar_cliente(arq, meta, destino, tentativas=3):
    """Modo CLIENTE (padrão): a abordagem do cliente (arquivo limpo em pasta_tmp em VÁRIAS partes, gravadas em
    paralelo, lidas com multiLine em paralelo), com os ajustes que evitam perder ou corromper registros:
      - o header sai antes de gravar (o CSV com header=True descartaria a 1ª linha de cada parte);
      - um registro em mais de uma linha que caiu na fronteira entre duas partes seria cortado ao meio. Uma
        passada em paralelo resume cada pedaço (1ª e última linha com aspa aberta, quantidade); o driver encadeia
        os pedaços na ordem do arquivo e acha esses registros (no máximo 1 por fronteira); eles saem das partes e
        são gravados inteiros numa parte a mais. Nada é reordenado e nenhuma linha se perde.
    As passadas são jobs Spark separados: os pedaços do arquivo precisam ser os mesmos em todas. O motor confere:
    todas as linhas da fronteira achadas antes de gravar, e nº de partes gravadas = nº de pedaços (opcional:
    limpeza_pedaco_mb fixa o tamanho do pedaço, ao custo de ~2x no tempo).
    Se não conferir, apaga a tmp e refaz (até 'tentativas'); se ainda assim não conferir, para sem gravar nada."""
    if int(CTX["limpeza_pedaco_mb"] or 0) > 0:      # opcional: a conferência abaixo já cobre pedaços que mudem
        CTX["spark"].conf.set("spark.sql.files.maxPartitionBytes", f"{int(CTX['limpeza_pedaco_mb'])}m")
    for t in range(tentativas):
        try:
            return _preparar_cliente(arq, meta, destino) + (f" (tentativa {t + 1})" if t else "")
        except _LayoutMudou as e:
            print(f"modo CLIENTE: {e}; refazendo a limpeza")
            CTX["dbutils"].fs.rm(destino, True)
    raise ErroCarga("ERRO_EXECUCAO", f"modo CLIENTE: os pedaços do arquivo mudaram entre as passadas em {tentativas} "
                                     "tentativas", mover=False)


def _preparar_cliente(arq, meta, destino):
    df = _linhas_posicionadas(arq, meta)
    resumo = sorted((df.groupBy("_bloco").agg(
        F.min("_pos").alias("ini"), F.max("_pos").alias("fim"), F.sum("_impar").alias("impares"),
        F.min(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("pri_impar"),
        F.max(F.when(F.col("_impar") == 1, F.col("_pos"))).alias("ult_impar")).collect()), key=lambda r: r["_bloco"])
    if any(int(r["ini"]) != 0 for r in resumo):       # uma tarefa leu mais de um pedaço: a posição não é confiável
        raise _LayoutMudou("uma tarefa leu mais de um pedaço do arquivo")
    faixas, aberto, total_impares = [], False, 0
    for i, r in enumerate(resumo):
        total_impares += int(r["impares"] or 0)
        if aberto:                                    # o pedaço começa no meio de um registro do pedaço anterior
            if r["pri_impar"] is None:
                raise ErroCarga("ERRO_DADOS", "registro com quebra de linha maior que um pedaço inteiro do arquivo")
            faixas.append((resumo[i - 1]["_bloco"], resumo[i - 1]["ult_impar"], resumo[i - 1]["fim"]))   # início do registro
            faixas.append((r["_bloco"], r["ini"], r["pri_impar"]))                                    # continuação
        aberto = (int(aberto) + int(r["impares"] or 0)) % 2 == 1
    fronteira = F.lit(False)
    for b, a, z in faixas:                            # (pedaço, posição dentro do pedaço): estável entre execuções
        fronteira = fronteira | ((F.col("_bloco") == F.lit(b)) & F.col("_pos").between(a, z))
    registros = []
    if faixas:                                        # poucos registros: montados inteiros no driver, ANTES de gravar
        linhas_f = {(r["_bloco"], r["_pos"]): r["value"]
                    for r in df.filter(fronteira).select("_bloco", "_pos", "value").collect()}
        esperadas = {(b, p) for b, a, z in faixas for p in range(int(a), int(z) + 1)}
        if set(linhas_f) != esperadas:
            raise _LayoutMudou(f"linhas da fronteira: {len(set(linhas_f) & esperadas)} de {len(esperadas)} achadas")
        for j in range(0, len(faixas), 2):            # cada par de faixas = 1 registro (início + continuação)
            partes = [linhas_f[(b, p)] for b, a, z in faixas[j:j + 2] for p in range(int(a), int(z) + 1)]
            registros.append(("\n".join(partes),))    # o registro inteiro num valor só: a ordem das linhas fica garantida
    df.filter(~fronteira).select("value").write.mode("overwrite").text(destino)
    gravadas = len([f for f in CTX["dbutils"].fs.ls(destino) if f.name.startswith("part-")])
    if gravadas != len(resumo):
        raise _LayoutMudou(f"{gravadas} parte(s) gravada(s) para {len(resumo)} pedaço(s) analisado(s)")
    if registros:
        CTX["spark"].createDataFrame(registros, "value STRING").write.mode("append").text(destino)
    return (f"CLIENTE: {len(resumo)} parte(s) em paralelo, {total_impares} linha(s) com aspa aberta, "
            f"{len(registros)} registro(s) na fronteira entre partes reunido(s)")


def _schema_ddl(meta, extra=None):
    campos = [f"`{f.name}` {f.dataType.simpleString()}" for f in meta["schema"].fields]
    return ", ".join(campos + ([f"`{extra}` string"] if extra else []))


def _opcoes_direta(meta):
    tsf = _p(meta, "timestamp_format", "yyyy-MM-dd HH:mm[:ss]")
    return {"sep": _p(meta, "delimitador", "|"), "quote": _p(meta, "limpeza_quote", "'"), "escape": "\\",
            "nullValue": _p(meta, "null_value"), "timestampFormat": tsf, "timestampNTZFormat": tsf,
            "dateFormat": _p(meta, "date_format", "yyyy-MM-dd")}


def ler_limpo_direto(arq, meta):
    """Modo DIRETA: as regex e a separação das colunas na MESMA leitura (from_csv com as opções do cliente), em
    paralelo e sem arquivo temporário. Só para arquivos sem registro em mais de uma linha. FAILFAST: linha inválida
    = erro (o motor mostra a 1ª linha inválida)."""
    linhas = _linhas_limpas(arq, meta, tirar_header=True)
    return (linhas.select(F.from_csv("value", _schema_ddl(meta), {**_opcoes_direta(meta), "mode": "FAILFAST"}).alias("r"))
            .select(*[F.col(f"r.`{f.name}`").alias(f.name) for f in meta["schema"].fields]))


def primeira_linha_invalida_direta(arq, meta):
    """Diagnóstico do modo DIRETA: a 1ª linha que não vira registro válido (PERMISSIVE + coluna de corrompida)."""
    linhas = _linhas_limpas(arq, meta, tirar_header=True)
    r = (linhas.select("value", F.from_csv("value", _schema_ddl(meta, "_corrompida"),
                                           {**_opcoes_direta(meta), "mode": "PERMISSIVE",
                                            "columnNameOfCorruptRecord": "_corrompida"}).alias("r"))
         .filter("r._corrompida IS NOT NULL").select("value").limit(1).collect())
    return r[0]["value"] if r else None


def preparar_dat(arq, meta):
    """Limpeza do DAT antes da carga. Decide o modo e:
       CLIENTE    (padrão) abordagem do cliente em partes paralelas + multiLine, com os ajustes (preparar_cliente);
       AUTO       detecta se há registro em mais de uma linha: nenhum = PARALELA, algum = CLIENTE;
       DIRETA     nada a gravar: a limpeza acontece na própria leitura da carga (ler_limpo_direto);
       PARALELA   regex + arquivo limpo em pasta_tmp em várias partes (sem header), lido em paralelo;
       MULTILINHA arquivo limpo em UM arquivo (coalesce(1)) + multiLine. NÃO recomendado em arquivo grande: uma
                  tarefa só, e no teste de 80 GB o coalesce(1) não manteve a ordem das linhas (o header foi parar no
                  meio do arquivo).
    Devolve (modo efetivo, segundos, detalhe)."""
    inicio = time.time()
    modo, detalhe = modo_limpeza(meta), ""
    if modo == "AUTO":
        abertas = linhas_com_aspa_aberta(arq, meta)
        modo = "CLIENTE" if abertas else "PARALELA"      # medido: PARALELA é a mais rápida sem multilinha
        detalhe = f"AUTO: {abertas} linha(s) com aspa aberta"
    arq["limpeza_modo"] = modo
    if modo == "CLIENTE":
        arq["dat_limpo"] = caminho_tmp(arq)
        detalhe = preparar_cliente(arq, meta, arq["dat_limpo"]) + (f"; {detalhe}" if detalhe else "")
    if modo in ("PARALELA", "MULTILINHA"):
        destino = caminho_tmp(arq)
        linhas = _linhas_limpas(arq, meta, tirar_header=(modo == "PARALELA"))
        if modo == "MULTILINHA":
            linhas = linhas.coalesce(1)               # 1 arquivo, na ordem original (repartition embaralharia)
        linhas.write.mode("overwrite").text(destino)
        arq["dat_limpo"] = destino
    return modo, time.time() - inicio, detalhe


def apagar_tmp(arq):
    arq.pop("limpeza_modo", None)
    caminho = arq.pop("dat_limpo", None)
    if caminho:
        try:
            CTX["dbutils"].fs.rm(caminho, True)
        except Exception as e:
            print(f"não consegui apagar {caminho}: {e}")


def explicar_erro(e, arq, meta):
    """Como classificar(), mas quando o Spark só diz "erro ao ler o arquivo" (FAILED_READ_FILE), relê o arquivo
    em modo tolerante e mostra a 1ª linha inválida e o motivo. Custo extra só quando já houve erro."""
    tipo, msg = classificar(e)
    if tipo == "ERRO_DADOS" and arq.get("limpeza_modo") == "DIRETA" and "ERRO_DADOS:" not in msg:
        try:
            ruim = primeira_linha_invalida_direta(arq, meta)
            if ruim is not None:
                return tipo, ("ERRO_DADOS: linha inválida depois da limpeza (campos a mais/menos, tipo incompatível ou "
                              f"registro em mais de uma linha: use limpeza_modo AUTO ou MULTILINHA). Linha: {ruim[:400]}")
        except Exception as diag:
            print(f"diagnóstico do erro não foi possível: {diag}")
        return tipo, msg
    sem_diagnostico = not arq.get("dat_limpo") and _p(meta, "escape") and not _p(meta, "quote")
    if tipo != "ERRO_DADOS" or "FAILED_READ_FILE" not in msg or sem_diagnostico:
        return tipo, msg
    try:
        sep = _p(meta, "delimitador", "|")
        n = len(meta["schema"].fields)
        esquema = T.StructType(meta["schema"].fields + [T.StructField("_corrompida", T.StringType())])
        caminho, o = _leitura(arq, meta)                     # mesmas opções da carga (DAT ou arquivo limpo)
        ruim = (CTX["spark"].read.format("csv").schema(esquema).options(**o)
                .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrompida")
                .load(caminho).filter("_corrompida IS NOT NULL")
                .select("*").limit(1).collect())   # todas as colunas: com poda, o Spark não analisa a coluna ruim
        if not ruim:
            print("diagnóstico: nenhuma linha inválida encontrada no modo tolerante")
            return tipo, msg
        linha = ruim[0]["_corrompida"]
        campos = linha.split(sep)
        if len(campos) != n:
            motivo = f"linha com {len(campos)} campos (esperado {n}); se há '{sep}' dentro de um texto, falta ESCAPECHAR"
        else:
            motivo = "valor incompatível com o tipo de alguma coluna (ex.: texto em campo numérico ou data inválida)"
        return tipo, f"ERRO_DADOS: {motivo}. 1ª linha inválida: {linha[:400]}"
    except Exception as diag:
        print(f"diagnóstico do erro não foi possível: {diag}")
        return tipo, msg


def classificar(e):
    """Traduz exceções do Spark/Delta em (tipo, mensagem). Erros de dado viram ERRO_DADOS."""
    msg = str(e)
    if "ERRO_DADOS" in msg:
        trecho = msg[msg.index("ERRO_DADOS"):].split("\n")[0]
        return "ERRO_DADOS", trecho
    sinais = ("MALFORMED", "Malformed", "CAST_INVALID_INPUT", "CANNOT_PARSE", "NumberFormat", "FAILFAST",
              "REPLACE_WHERE", "replaceWhere", "CSV header", "does not conform", "DATETIME", "ARITHMETIC_OVERFLOW",
              "NUMERIC_VALUE_OUT_OF_RANGE", "CANNOT_CAST")
    if any(s in msg for s in sinais):
        return "ERRO_DADOS", msg[:2000]
    return "ERRO_EXECUCAO", msg[:2000]


# =============================================================================
# Carga do subbloco (novo neste projeto)
# =============================================================================
_NOME = re.compile(r"[A-Za-z0-9_]+")


def metadados(destino: str, opcoes: dict, col_tempo: str | None) -> dict:
    """Destino existe, é Delta e tem a coluna de tempo (usada para limpar o período). ERRO_CADASTRO se não."""
    partes = destino.split(".")
    if len(partes) != 3 or not all(_NOME.fullmatch(p) for p in partes):
        raise ErroCarga("ERRO_CADASTRO", f"destino inválido: {destino!r}", mover=False)
    try:
        schema = CTX["spark"].table(destino).schema
        formato = (CTX["spark"].sql(f"DESCRIBE DETAIL {destino}").first()["format"] or "").lower()
    except Exception as e:
        raise ErroCarga("ERRO_CADASTRO", f"tabela destino {destino} não encontrada ({str(e)[:300]})", mover=False)
    if formato != "delta":
        raise ErroCarga("ERRO_CADASTRO", f"tabela destino {destino} não é Delta ({formato})", mover=False)
    nomes = {f.name.lower(): f for f in schema.fields}
    campo = nomes.get((col_tempo or "").lower())
    if col_tempo and campo is None:
        raise ErroCarga("ERRO_CADASTRO", f"coluna de tempo {col_tempo} não existe em {destino}", mover=False)
    return {"tabela": destino, "params": opcoes, "schema": schema,
            "col_tempo": campo.name if campo else None,
            "tipo_tempo": campo.dataType.simpleString() if campo else None}


def conferir_header(arq, meta):
    """Header × colunas da tabela (estrutura, não conteúdo): mesmo teste do precheck do PLHDWA."""
    if _p(meta, "header", "true") != "true":
        return
    header = [c.strip().strip("'\"").upper() for c in ler_header(arq).split(_p(meta, "delimitador", "|"))]
    esperado = [f.name.upper() for f in meta["schema"].fields]
    if header != esperado:
        novas = [c for c in header if c not in esperado]
        faltam = [c for c in esperado if c not in header]
        raise ErroCarga("ERRO_SCHEMA", f"header diferente da tabela {meta['tabela']}: novas={novas[:10]} "
                                       f"faltando={faltam[:10]} ({len(header)} colunas no arquivo, {len(esperado)} na tabela)")


def tamanho(caminho: str) -> int | None:
    try:
        return CTX["dbutils"].fs.ls(caminho)[0].size
    except Exception:
        return None


def esperar_arquivo(caminho: str, minutos: float, a_cada_s: int = 15) -> int:
    """A SP grava o .dat no datasource: espera até 'minutos' pelo arquivo completo (tamanho estável)."""
    limite, anterior = time.time() + minutos * 60, None
    while True:
        t = tamanho(caminho)
        if t is not None and t == anterior:
            return t
        if time.time() > limite:
            if t is not None:
                return t
            raise ErroCarga("ARQUIVO_AUSENTE", f"{caminho} não encontrado após {minutos} min", mover=False)
        anterior = t
        time.sleep(a_cada_s if t is None else 5)


def ja_carregado(meta, tag: str):
    """Retomada: se um commit deste subbloco já existe no destino (userMetadata), devolve (versão, linhas)."""
    h = (CTX["spark"].sql(f"DESCRIBE HISTORY {meta['tabela']} LIMIT 200")
         .filter(F.col("userMetadata") == tag).orderBy(F.col("version").desc()).first())
    if not h:
        return None
    m = h.operationMetrics or {}
    return h.version, int(m.get("numOutputRows", 0) or 0)


def limpar_periodo(meta, ini: str, fim: str) -> int:
    """Remove do destino as linhas do período do subbloco (o app garante que o período é só dele)."""
    if not meta.get("col_tempo"):
        raise ErroCarga("ERRO_CADASTRO", "sem coluna de tempo: não é possível limpar o período no destino", mover=False)
    try:
        pred = predicado_periodo(meta["col_tempo"], meta["tipo_tempo"], ini, fim)
    except ValueError as e:
        raise ErroCarga("ERRO_CADASTRO", str(e), mover=False)
    r = CTX["spark"].sql(f"DELETE FROM {meta['tabela']} WHERE {pred}").first()
    return int(r["num_affected_rows"]) if r and "num_affected_rows" in r.asDict() else 0


def append(arq, meta, tag: str):
    """APPEND do .dat no destino num único commit, sem validação de período ou conteúdo. Devolve (versão, linhas)."""
    t = meta["tabela"]
    try:
        (ler_dat(arq, meta).write.format("delta").mode("append")
         .option("userMetadata", tag).saveAsTable(t))
    except ErroCarga:
        raise
    except Exception as e:
        tipo, msg = explicar_erro(e, arq, meta)
        raise ErroCarga(tipo, msg)
    feito = ja_carregado(meta, tag)
    if not feito:
        raise ErroCarga("ERRO_EXECUCAO", f"commit do append não encontrado no histórico de {t}", mover=False)
    return feito


def mover_para_erro(caminho: str, pasta_erro: str) -> str | None:
    destino = f"{pasta_erro.rstrip('/')}/{caminho.rsplit('/', 1)[-1]}"
    try:
        CTX["dbutils"].fs.mv(caminho, destino)
        return destino
    except Exception as e:
        print(f"não consegui mover {caminho} para {destino}: {e}")
        return None


def apagar(caminho: str):
    try:
        CTX["dbutils"].fs.rm(caminho)
    except Exception as e:
        print(f"não consegui apagar {caminho}: {e}")
