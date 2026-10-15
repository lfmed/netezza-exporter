# Plano — tipo de filtro no cadastro da tabela (Data · Número · Sem filtro)

Versão alvo: **v1.010** (um ajuste pedido = +0.001). Estado de partida: v1.009.

## 1. Pedido

No cadastro da tabela, o cliente escolhe o **tipo de filtro** que o app usa para quebrar a migração em subblocos:

| Tipo | Coluna do filtro | Faixa enviada à SP (`p_dtini` / `p_dtfim`) | Escopo da migração | Subblocos |
|---|---|---|---|---|
| **Data** (atual) | obrigatória | `AAAAMMDD` (ano, mês e dia sem separador), fim inclusivo | primeiro e último dia | por calendário: dia, semana, mês ou ano (como hoje) |
| **Número** | obrigatória | valores inteiros mínimo e máximo, fim inclusivo | valor mínimo e máximo | **quantidade de subblocos** |
| **Sem filtro** | — | filtro sempre verdadeiro: a tabela inteira | a tabela inteira | um único subbloco |

## 2. Decisões fechadas com o usuário (2026-10-15)

1. A `SP_CARGA_EVENTUAL` **concatena** `p_dtini`/`p_dtfim` num `BETWEEN` sem converter para data. Por isso o filtro
   numérico usa o **mesmo perfil e a mesma procedure**, sem mudar a SP.
2. **Número**: só **inteiros**, que podem ser negativos, no intervalo do `bigint`.
3. **Número**: a faixa é dividida pela **quantidade de subblocos** informada.
4. **Sem filtro** entra neste plano. O app envia um **filtro sempre verdadeiro**: `p_NomeColunaTempo='1'`, `p_dtini='1'`,
   `p_dtfim='1'`, o que vira `WHERE 1 BETWEEN 1 AND 1`. O `.dat` esperado é `/tmp/<TABELA>_1_1.dat`.
5. **Sem filtro**: em reprocesso, ou na limpeza depois de um erro de importação, o app faz **`DELETE` da tabela destino
   inteira**. Assume-se que o destino é exclusivo dessa tabela do Netezza.
6. **Número**: os subblocos rodam do **maior valor para o menor**, no mesmo espírito do "mais recente primeiro" das datas.

## 3. Como o app trata cada tipo hoje (validação contra o código)

Hoje o "período" é **sempre uma data**. Estes pontos mudam:

| Onde | Hoje | Impacto |
|---|---|---|
| `schema.sql` · `export_subblock.data_range` | `daterange` + `EXCLUDE gist (table_config_id, data_range &&)` | a faixa vira genérica |
| `schema.sql` · `export_request_table.period_start/end` | `date NOT NULL`; `subblock_unit IN (DAY,WEEK,MONTH,YEAR)` | faixa numérica, quantidade e "tabela inteira" |
| `schema.sql` · `table_config_version` | `time_column`, `date_format`, `migration_start/end date` | `filter_type`, escopo numérico e quantidade padrão |
| `schema.sql` · `v_migration_coverage` / `mr_days` | `datemultirange`, conta dias | conta "unidades" (dias ou valores); sem filtro = 1 unidade |
| `planner.py` | só calendário (`plan`, `subtract_ranges`, `merge_contiguous` com `date`) | adicionar o plano por quantidade e o bloco único |
| `procedure.build_call` | `data_ini/data_fim: date`, `strftime`; `time_column` validada por `_IDENT` | faixa genérica; constante `'1'` no sem filtro |
| `dispatcher.py` | `lower(data_range)`, `upper-1`, `ORDER BY lower(...) DESC` | ler a faixa genérica (a ordem decrescente já serve) |
| `importer.py` / `motor_dat.limpar_periodo` / `file_format.predicado_periodo` | `to_char(... 'YYYYMMDD')`; predicado aceita só 8 dígitos | número = `BETWEEN min AND max`; sem filtro = `DELETE` total |
| `repo.py` | `TableRequest(period_start/end: date)`, `review`, `find_overlaps`, `gap_rows`, `table_ranges` | faixa genérica |
| `table_csv.py` | colunas e validação só de data | `filter_type`, `scope_min/max`, `default_subblock_count` |
| Telas | `admin_table_form`, `request_new`, `request_review`, `request_detail`, `_request_progress`, `subblock`, `coverage`, `dashboard` | campos e exibição por tipo |
| `pos_deploy.py` | confere a coluna de tempo no destino | Número: coluna inteira no destino; Sem filtro: nada |

A SP e a external table **não mudam**, conforme o contrato do projeto. A importação continua só com append, sem validar
range nem conteúdo.

## 4. Desenho

### 4.1 Faixa genérica (chave única para os três tipos)
- Nova coluna `export_subblock.key_range int8range`, semiaberta `[lo, hi+1)`, que substitui `data_range`:
  - **Data**: dias desde 1970-01-01 (`d - DATE '1970-01-01'`). Mantém a contiguidade: 31/01 → 01/02 = +1.
  - **Número**: o próprio valor, `[min, max+1)`.
  - **Sem filtro**: `[0, 1)` fixo.
- `EXCLUDE USING gist (table_config_id WITH =, key_range WITH &&)` com os mesmos estados que reservam a faixa hoje.
- Funções SQL `key_lo_txt` e `key_hi_txt (filter_type, key_range)` montam o texto para a SP e as telas (`AAAAMMDD`,
  número ou `1`).
- O tipo de filtro **não pode mudar** depois que a tabela tem subblocos (de qualquer estado). O app bloqueia a nova
  versão do cadastro e explica o motivo, porque as faixas antigas deixariam de ser comparáveis.
- Migração: dev não tem dados (0 tabelas, 0 subblocos em 2026-10-15). Mesmo assim, o `schema.sql` converte
  `data_range → key_range` de forma idempotente antes de apagar a coluna antiga.

### 4.2 Cadastro (`table_config_version`)
- `filter_type text NOT NULL DEFAULT 'DATE' CHECK (filter_type IN ('DATE','NUMBER','NONE'))`.
- `scope_num_min bigint` e `scope_num_max bigint` (Número), com `CHECK min <= max`. Na Data continuam
  `migration_start/end`.
- `default_subblock_count int` (Número, padrão 10, de 1 a 5000).
- **Data**: formato sempre `AAAAMMDD`. A opção `YYYY-MM-DD` sai da tela e do CSV, e não há cadastro usando essa opção.
- Perfil: **Número** e **Sem filtro** só no perfil `SP_CARGA_EVENTUAL`. O perfil de 3 parâmetros continua só com Data.

### 4.3 Planner
- `plan_count(lo, hi, n)`: passo = `ceil((hi - lo + 1) / n)`, cortes em `lo + k·passo`, último truncado em `hi`. Com
  n maior que o tamanho da faixa, o resultado é um subbloco por valor. O limite de 5000 subblocos continua.
- **Lacunas e revisão**: a quantidade vale para a faixa pedida. Os cortes são calculados sobre a faixa pedida inteira e
  depois descontam as faixas já executadas (`subtract_ranges` genérico). Assim os pedaços restantes não deslocam os
  cortes.
- **"Criar pedido para as lacunas"**: usa a quantidade padrão do cadastro sobre o **escopo** da tabela. Cada lacuna é
  cortada nos mesmos limites, o mesmo alinhamento que o calendário dá às datas.
- `plan_whole()`: um subbloco `[0, 1)`.
- `subtract_ranges` e `merge_contiguous` passam a operar com inteiros. Na Data, a conversão de e para dias fica nas
  bordas (tela, repo).

### 4.4 Chamada da SP (`procedure.build_call`)
- Recebe `(lo_txt, hi_txt)` já formatados pelo tipo e valida:
  - Data `^\d{8}$`;
  - Número `^-?\d{1,19}$` (valores montados pelo app, nunca texto livre);
  - Sem filtro `'1'`.
- **Sem filtro**: `p_NomeColunaTempo='1'`, `p_dtini='1'`, `p_dtfim='1'`, uma exceção explícita ao `_IDENT` só para esse
  tipo.
- Arquivo esperado: `/tmp/<TABELA>_<lo>_<hi>.dat`. Num número negativo o nome fica com `-`, por exemplo
  `FATO_X_-100_-1.dat`. **Item a validar no E2E real**: a SP pode gerar outro nome para negativos.

### 4.5 Importador
- Busca `filter_type` com o subbloco, e `ini`/`fim` vêm de `key_lo_txt`/`key_hi_txt`.
- Na `predicado_periodo`, ganham um ramo:
  - **Número**: `` `col` BETWEEN <lo> AND <hi> `` (inteiros validados). Exige uma coluna inteira no destino
    (`tinyint` a `bigint`, ou `decimal(p,0)`); fora disso é erro de cadastro.
  - **Sem filtro**: `limpar_periodo` faz `DELETE FROM <destino>` sem `WHERE`, **só** nesse tipo e só no reprocesso ou na
    limpeza depois de um erro. O evento registra `rows_cleaned`.
- Retomada por `DESCRIBE HISTORY`/`userMetadata`: não muda.

### 4.6 Dispatcher
- Lê `lo/hi` em texto já formatado. `ORDER BY lower(key_range) DESC` dá "mais recente primeiro" na Data e "maior valor
  primeiro" no Número. Sem filtro tem um subbloco só.

### 4.7 Cobertura e telas
- `v_migration_coverage` com `int8multirange` e `range_agg(key_range)`, com contagem em **unidades**:
  - Data = dias;
  - Número = valores;
  - Sem filtro = 1, e fica COMPLETA quando o subbloco único chega a DONE.
- As colunas `*_days` viram `*_units` (`DROP VIEW` + `CREATE`). Os rótulos na tela seguem o tipo: "dias", "valores" ou
  "tabela inteira".
- Um filtro Jinja `faixa(sb)` exibe `01/01/2016 – 31/01/2016`, `1.000.000 – 1.999.999` ou `tabela inteira`.
  Ele é usado em `request_detail`, `_request_progress`, `subblock`, `coverage` e `request_review`.
- **Cadastro** (`admin_table_form`): um seletor "Tipo de filtro" mostra ou esconde os campos:
  - coluna;
  - escopo em datas ou em números;
  - unidade de calendário ou quantidade.

  A **prévia da chamada** acompanha o tipo, por exemplo `CALL SP_CARGA_EVENTUAL('DWA','FATO_X','STAGE','ADMIN','1','1','1')`.
- **Novo pedido** (`request_new`): cada linha se adapta ao tipo da tabela escolhida:
  - Data: datas e subbloco por calendário;
  - Número: mínimo, máximo e quantidade;
  - Sem filtro: só "tabela inteira".
- O conferidor do destino na tela avisa quando a coluna do filtro numérico não é inteira.

### 4.8 CSV do cadastro
- Novas colunas:
  - `filter_type` (`DATA` | `NUMERO` | `SEM_FILTRO`, padrão `DATA`);
  - `scope_min` e `scope_max` (inteiros, para Número);
  - `default_subblock_count`.
- `date_format` sai.
- A validação depende do tipo:
  - Número exige a coluna e `min <= max`;
  - Sem filtro ignora coluna, escopo e subbloco.
- O modelo ganha um exemplo de cada tipo.

### 4.9 Pós-deploy
- Na conferência da tabela destino:
  - Número exige uma coluna inteira;
  - Data mantém a regra atual;
  - Sem filtro não confere coluna.

## 5. Etapas de implantação

| # | Etapa | Entrega |
|---|---|---|
| 1 | Modelo | `schema.sql`: `filter_type`, escopo numérico, quantidade, `key_range` + EXCLUDE, funções de texto, views em unidades; migração idempotente de `data_range` |
| 2 | Núcleo | `planner` (contagem, bloco único, faixas inteiras), `procedure.build_call`, `file_format.predicado_periodo` |
| 3 | Fluxo | `repo` (TableRequest genérico, review, overlaps, gaps, cobertura), `dispatcher`, `importer`/`motor_dat.limpar_periodo` |
| 4 | Telas | cadastro, novo pedido, revisão, detalhe, subbloco, cobertura, painel; filtro `faixa` |
| 5 | CSV e pós-deploy | `table_csv` (colunas e exemplos), conferência da coluna no destino |
| 6 | Testes unitários | planner por contagem (bordas, n > faixa, negativos, 5000), `build_call` dos 3 tipos, predicado Número e Sem filtro, CSV por tipo, regra "tipo não muda com subblocos" |
| 7 | Teste em branch Lakebase temporário | migrar o branch, cadastrar uma tabela de cada tipo, criar pedidos, conferir o EXCLUDE (sobreposição numérica bloqueada), a revisão que exclui faixas executadas e a cobertura |
| 8 | E2E do importador | estender `scripts/teste_importacao.py` com uma tabela numérica (.dat por faixa, limpeza `BETWEEN`) e uma sem filtro (`_1_1.dat`, `DELETE` total no reprocesso) |
| 9 | Deploy dev | `bump_version.py` → `bundle deploy` → `nzexport_pos_deploy` → `bundle run nzexport_app`; health `1.010`; telas via HTTP |
| 10 | Registro | `README.md`, `DEPLOY.md`, `CLAUDE.md`, `CHANGELOG.md`; commit `v1.010`, tag e push |

## 6. Riscos e pendências

- **Sem filtro com `'1'`**: depende de a SP concatenar a coluna sem colocar aspas ou prefixo. Confirmar no primeiro teste
  real contra o Netezza (`conn_test` + um pedido de uma tabela pequena).
- **Nome do .dat com número negativo**: confirmar que a SP gera `<TABELA>_-100_-1.dat`. Se não gerar, ajustar o nome
  esperado no app, sem mexer na SP.
- **`DELETE` total no sem filtro**: só é seguro se o destino for exclusivo. A tela do cadastro avisa isso quando o tipo
  for Sem filtro.
- **Mudança de tipo**: bloqueada com subblocos existentes. Para trocar, o caminho é criar outro identificador lógico.
- **Faixa numérica muito grande com quantidade pequena**: os subblocos ficam enormes, porque o app não conhece a
  distribuição dos valores. O cliente ajusta a quantidade, como faz hoje com o tamanho do subbloco.
