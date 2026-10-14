# Plano — Job de importação do `.dat` para a tabela destino (Delta)

Situação: v1.002 (2026-10-14). Referência analisada: `lfmed/plhdwa-carga-netezza`, o motor `plhdwa_motor.py` e o job
`carga_arquivo.py`.

## 1. O que o projeto de referência faz e o que muda aqui

O motor PLHDWA espera um **par `.dat` + `.CTL`** na landing S3. A chegada do CTL (file arrival trigger) dispara o
despachante. Para cada arquivo, ele:

1. lê o cadastro do objeto na `ctl_parametros` e o layout da tabela destino (`ERRO_CADASTRO` se não baterem);
2. valida o CTL (código, data de movimento, nr_cpr);
3. **limpa o DAT** com as 3 regex do cliente (modo `CLIENTE`, em partes paralelas, com encoding e multiLine);
4. faz o **precheck**: header × schema da tabela e uma amostra com detector de formato;
5. **carrega num único commit Delta** com `replaceWhere` (FATO: data + nr_cpr; DIM: `1 = 1`), schema da tabela e
   `FAILFAST`;
6. grava a controle no Db2 e o log em Delta, **apaga o par** no sucesso ou **move para `erro/`** no erro de negócio.

Aqui não existe CTL e quem controla é o app. O app já sabe, por subbloco, **a tabela, o período (ini, fim), a
coluna de tempo e o destino**, que são exatamente as informações que o CTL trazia. O arquivo gerado pela
`SP_CARGA_EVENTUAL` também é conhecido: `DATAOBJECT '/tmp/<TABELA>_<ini>_<fim>.dat'` no datasource
`PLH_DATASOURCE_DWA`, com `DELIM '|'`, `IncludeHeader true`, `escapechar '\'` e textos entre aspas simples
(`quote_literal`). É o mesmo formato que o motor já trata no modo de limpeza `CLIENTE`.

## 2. Fluxo proposto (tudo controlado pelo app, por subbloco)

```
App ─ pedido ─▶ Lakebase (subblocos)
                  │
   dispatcher ────┤ 1. CALL SP_CARGA_EVENTUAL(...)     → subbloco EXPORTED (arquivo esperado registrado)
                  │                                      └─ dispara o importador (run-now)
   importador ────┤ 2. arquivo existe? → limpeza → precheck → REPLACE WHERE no Delta
   (job novo)     │ 3. sucesso: LOADED + métricas (linhas, versão Delta) e APAGA o .dat
                  │    erro:    IMPORT_FAILED (mensagem); o .dat fica para repetir a importação
                  ▼
App: progresso por subbloco nas duas fases; painel de completude conta só LOADED
```

### Estados do subbloco

`PENDING → RUNNING (procedure) → EXPORTED → IMPORTING → LOADED`

Com erro, os estados são:
- `FAILED`: falha na exportação;
- `IMPORT_FAILED`: falha na importação. Repetir a importação não chama o Netezza de novo.

Os estados atuais `UNKNOWN`, `CANCELED` e `SUPERSEDED` continuam valendo. O `DONE` de hoje passa a significar
**"carregado no Delta"**. A completude e o fechamento do pedido passam a exigir `LOADED`.

## 3. Reaproveitamento do motor (`plhdwa_motor.py`)

| Parte do motor | Decisão | Ajuste |
|---|---|---|
| `_leitura`, `ler_dat`, `ler_dat_escape`, `ler_header` (leitura com schema da tabela + `FAILFAST`) | **reusar** | opções do arquivo vêm do cadastro da tabela no app, com padrões do formato da SP |
| Limpeza (`preparar_dat`, `preparar_cliente`, `_linhas_limpas`, `_aplicar_regex`, `apagar_tmp`, `limpar_tmp_orfaos`) | **reusar** | pasta de limpeza separada (`limpeza/`), porque o `.dat` da SP já fica em `tmp/` |
| `precheck` (header × schema, amostra, detector de formato) | **reusar** | sem o detector de `nr_cpr` |
| `explicar_erro`, `classificar`, `ErroCarga` (1ª linha inválida, tipos de erro) | **reusar** | — |
| `carregar` (`replaceWhere` + `userMetadata` + métricas do `DESCRIBE HISTORY`) | **reusar com ajuste** | predicado: `<coluna_tempo> BETWEEN ini AND fim`, tipado (inteiro AAAAMMDD, DATE ou texto); `userMetadata = subblock_id` |
| `validacoes_fato` | **adaptar** | linha com coluna de tempo **fora do período do subbloco** = `ERRO_DADOS` (nada é gravado) |
| `metadados` (cadastro + layout do destino) | **adaptar** | lê o cadastro do Lakebase em vez da `ctl_parametros`; confere se o destino existe, **é Delta** e tem a coluna de tempo |
| `processar_arquivo` | **adaptar (simplificar)** | sem CTL e sem Db2; status e métricas gravados no Lakebase; retomada pelo `userMetadata` (se o commit existe, só marca LOADED) |
| `apagar_par`, `mover_para_erro` | **adaptar** | só o `.dat`. No erro, o arquivo **fica no lugar** para repetir a importação (opcional: mover para `erro/` ao cancelar) |
| CTL (`ler_nome`, `validar_ctl`, `ler_conteudo_ctl`), sequência de geração, `NOME_INVALIDO` | **não usar** | o app já sabe tabela, período e destino |
| Controle no Db2 (`ibm-db`, `sensibilizar_controle`, …) | **não usar** | a controle é o Lakebase do app. Sem o `ibm-db`, não há a restrição de máquina x86 |
| Logs Delta (`log_carga_evento`, `log_lote`), despachante, vigia, gatilho de chegada de arquivo, dashboards | **não usar** | o app e o dispatcher fazem esse papel |

O código reaproveitado entra como **módulo do bundle deste projeto** (`src/importer/motor_dat.py`), copiado com
crédito à origem e com as alterações marcadas. Não haverá dependência em tempo de execução do outro repositório.

## 4. Job importador

- **Notebook** `src/importar_dat.py` no job `nzexport-importer`: serverless, `max_concurrent_runs = 1` com fila. É
  disparado pelo dispatcher quando um subbloco vira `EXPORTED` e pelo agendador do app (rede de segurança).
- **Fila com vaga por arquivo**, a mesma ideia do despachante do motor:
  - threads com limite global configurável no app;
  - **no máximo 1 arquivo por tabela destino ao mesmo tempo**, porque REPLACE WHERE concorrentes na mesma tabela
    Delta geram conflito;
  - tabelas diferentes rodam em paralelo;
  - claim com lease no Lakebase, como no dispatcher;
  - encerra sozinho quando não há trabalho.
- **Passos por subbloco:**
  1. Confere se o `.dat` existe e o tamanho. Se não existir, espera até N minutos, porque a SP pode estar terminando
     de gravar, e depois falha com `ARQUIVO_AUSENTE`.
  2. Confere o destino: existe, é Delta e tem a coluna de tempo. Se não, falha com `ERRO_CADASTRO`.
  3. Faz a limpeza (modo `CLIENTE`), o precheck e a carga com `replaceWhere` do período.
  4. Registra linhas inseridas e removidas, a versão Delta e a duração. **Apaga o `.dat`** e a pasta de limpeza.
  5. Marca o subbloco como `LOADED`.
- **Alternativa mais simples** (não recomendada): um run por subbloco, com os parâmetros na chamada. Cada run paga o
  arranque do serverless (~1–2 min), o que pesa em migrações com centenas de subblocos.

## 5. Mudanças no app

| Onde | O quê |
|---|---|
| Conexão Netezza | **Landing S3** do datasource `PLH_DATASOURCE_DWA`: raiz `s3://<bucket>/<prefixo>` + pasta do `.dat` (padrão `tmp`) + pasta de limpeza. Botão para testar a leitura da landing |
| Cadastro da tabela | **Formato do arquivo**, com os padrões da SP: delimitador `\|`, header, quote `'`, escape `\`, encoding, null, formatos de data e hora. Também a limpeza (liga/desliga, modo) |
| Pedido / subbloco | Duas fases por subbloco (exportação e importação): arquivo, tamanho, linhas, versão Delta, erro. Ações: **repetir importação**, **reexportar** (ver ponto 7.6) e cancelar |
| Painel | Completude conta só `LOADED`; nova coluna "exportado, aguardando importação" |
| Administração | Limite global de importações simultâneas |
| Lakebase | Colunas de importação no subbloco + tabela `export_import_attempt` (histórico por tentativa) |

## 6. Infraestrutura e permissões

- **External location** cobrindo o bucket/prefixo do datasource, com `READ FILES` e `WRITE FILES` (o importador
  apaga o arquivo) para a identidade do job. É o mesmo pré-requisito do motor PLHDWA.
- `USE CATALOG`, `USE SCHEMA`, `SELECT` e `MODIFY` nas tabelas destino para a identidade do job. Recomendado: um
  service principal dedicado como `run_as`. Hoje os jobs rodam com o usuário do deploy.
- `BROWSE` nos catálogos destino para o SP do app (o navegador do catálogo). Já concedido no
  `stable_classic_pg4xe1_catalog`.
- Environment serverless com `databricks-sdk`, `psycopg` e `psycopg-pool`. Sem `ibm-db`.

## 7. Pontos a confirmar antes de construir

1. **Caminho S3 do `PLH_DATASOURCE_DWA`**: bucket e prefixo. O `.dat` fica em `<raiz>/tmp/<TABELA>_<ini>_<fim>.dat`?
2. **Encoding do `.dat`**: a external table usa `NVARCHAR`, então provavelmente sai em UTF-8. O motor usa
   ISO-8859-15 (LATIN9) por padrão.
3. **As tabelas destino já existem?** O motor tem o `converter_ddl_netezza.py`, que cria a tabela Delta a partir da
   DDL do Netezza. Dá para trazer isso para o app depois ("criar destino a partir do Netezza").
4. **Tipo da coluna de tempo no destino**: inteiro `AAAAMMDD` ou `DATE`. Define o predicado do REPLACE WHERE.
5. **Arquivo com erro de importação**: fica no lugar para repetir (proposta) ou vai para `erro/`?
6. **Reexportação**: a SP faz `CREATE EXTERNAL TABLE` com nome fixo por tabela e período. Reexportar o mesmo
   subbloco falha com "já existe". Opções:
   - a SP passa a fazer `DROP ... IF EXISTS` antes do `CREATE`;
   - o app executa `DROP TABLE <banco>.<esquema>.<TABELA>_<ini>_<fim> IF EXISTS` antes de chamar a SP. Isso exige
     esse privilégio para o usuário da conexão.
7. **Limpeza do cliente** (as 3 regex): ligada por padrão, como no PLHDWA?

## 8. Entregas (uma versão por ajuste)

| Versão | Entrega |
|---|---|
| 1.003 | Banco de dados e telas: landing S3 na conexão, formato do arquivo no cadastro, estados de importação |
| 1.004 | Módulo `motor_dat.py` (código do PLHDWA adaptado) + notebook e job `nzexport-importer` + testes unitários do predicado e da validação de período |
| 1.005 | Integração: o dispatcher dispara o importador; telas do pedido e do subbloco com as duas fases; repetir importação; completude por `LOADED` |
| 1.006 | Teste ponta a ponta com um `.dat` no formato da SP (amostra real, como o `testes/fatparcela.dat`) numa external location de teste e numa tabela destino temporária, ambas removidas no fim. Sem simulação no app |
