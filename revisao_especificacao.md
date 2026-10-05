# Revisão crítica — Exportador Netezza → Databricks

Revisão de `especificacao_exportador_netezza_databricks.md` (2026-10-05).

## 0. Decisões confirmadas (2026-10-05) — prevalecem sobre o restante do documento

| Decisão | Impacto no desenho |
|---|---|
| Fase atual = **só gerar arquivos** para tirar os dados do Netezza. A importação para Delta será outro job, fora deste escopo. | O app precisa de um **hook pós-exportação** configurável que dispare um job externo. Nada de escrita em Delta neste desenvolvimento. |
| **Rota A**: o cliente vai entregar uma procedure que faz todo o trabalho. Assinatura: `(tabela, data_ini, data_fim)`. `data_ini`/`data_fim` = **subbloco**. | Fica descartado: B/C, unload, pyarrow e Spark. O worker só chama a procedure via `nzpy`. Toda a robustez (planejamento, claim, lease, retomada, dedup) fica do lado Databricks/Lakebase. |
| A procedure **ainda não existe**. | Criar um adaptador `ProcedureClient` com implementação real (`nzpy`) e **fake** (testes e desenvolvimento sem Netezza). O nome e os parâmetros da procedure ficam configuráveis por tabela. |
| Rede: **serverless via NCC**. | Job serverless com `nzpy` como dependência do environment. O app não fala com o Netezza. |

### Consequências da assinatura mínima `(tabela, ini, fim)`

A procedure não recebe `request_id`, `subblock_id` nem tentativa, e não oferece status consultável. Portanto:

1. **Não dá para reconciliar no Netezza.** Se a conexão cair no meio da execução, o subbloco fica `UNKNOWN`. A única saída segura é **reexecutar**, e isso só é seguro se a procedure for **idempotente por (tabela, ini, fim)**, ou seja, se sobrescrever o mesmo destino determinístico. **Pedir isso ao cliente como requisito do contrato.**
2. **Sucesso = retorno sem exceção.** Pedir ao cliente que a procedure **retorne um resultset** (mesmo opcional) com `status, linhas, qtd_arquivos, bytes, caminho`. Sem isso, o manifesto e as métricas dependem de o Databricks conseguir **listar o destino**.
3. **Destino dos arquivos.** Se a procedure grava no host do Netezza, o Databricks não enxerga os arquivos e o hook de importação não tem o que ler. Para o hook funcionar, o destino precisa ser S3/ADLS acessível por uma **external location/Volume** no UC.
4. **Cancelamento.** Não há cancelamento na procedure. "Parar" = não iniciar novos subblocos. A chamada em curso termina ou expira pelo timeout do statement ou do `nzpy`.
5. **Concorrência no Netezza.** Fica 100% controlada pelo dispatcher (`source_connection.max_sessions`).
6. **Intervalo semiaberto.** Confirmar se a procedure interpreta `data_fim` como exclusiva. Se for inclusiva, o planner precisa enviar `fim - 1 unidade`, senão os subblocos se sobrepõem.

### Hook pós-exportação (novo)

```sql
CREATE TABLE post_export_hook (
  hook_id          text PRIMARY KEY,
  table_config_id  text,                 -- NULL = vale para todas
  trigger_level    text NOT NULL,        -- SUBBLOCK | REQUEST_TABLE | REQUEST
  databricks_job_id bigint NOT NULL,
  enabled          boolean NOT NULL DEFAULT false
);
CREATE TABLE hook_invocation (
  invocation_id    uuid PRIMARY KEY,
  hook_id          text REFERENCES post_export_hook,
  request_id uuid, request_table_id uuid, subblock_id uuid,
  job_run_id       bigint, status text, error_message text,
  created_at timestamptz DEFAULT now(),
  UNIQUE (hook_id, request_table_id, subblock_id)   -- não dispara duas vezes
);
```

O dispatcher, ao publicar um subbloco ou tabela, chama `jobs.run_now(job_id, job_parameters={table, data_ini, data_fim, manifest_path, request_id})`. Falha no hook **não** reverte a exportação. Fica registrada e pode ser redisparada pela UI.

### Contrato a solicitar ao cliente (procedure)

- Parâmetros: `p_tabela varchar`, `p_data_ini timestamp`, `p_data_fim timestamp` (exclusiva).
- Idempotente: reexecutar com os mesmos parâmetros sobrescreve o mesmo destino, sem duplicar arquivos.
- Destino determinístico, por exemplo `<raiz>/<tabela>/<ini>_<fim>/`, em S3/ADLS acessível pelo UC.
- Retorna um resultset com `status, linhas, arquivos, bytes, caminho`, ou lança exceção com código e mensagem.
- Não usa arquivo temporário com o nome final (escreve e só depois renomeia), ou grava um marcador `_SUCCESS`.
- Tempo máximo esperado por subbloco e número de chamadas simultâneas suportadas.

## 1. Resumo do veredito

A direção geral está certa: o app funciona como plano de controle, o trabalho roda em Job, o checkpoint é por subbloco, há reconciliação antes de retry e o manifesto é o contrato. O que não está fechado:

1. **Lacuna central: quem grava os arquivos e onde.** A spec diz que a procedure Netezza gera os arquivos e recomenda Parquet com destino em UC Volume. Isso só funciona se o Netezza for **NPS 11.x** (Cloud Pak / NPSaaS / on-prem recente) com `EXTERNAL DATASOURCE` em S3/ADLS e CTAS `FORMAT 'PARQUET'`. Num Netezza legado (TwinFin/Striper/Mako, NPS 7.x), a procedure só escreve texto delimitado no filesystem do host. Os arquivos não chegam ao Volume e não existe Parquet.
2. **Objetivo indefinido: migração ou entrega recorrente de arquivos?** A pasta se chama "migração", mas a spec descreve um produto de exportação recorrente com periodicidade. Se o destino final for Delta no Databricks, arquivo + manifesto vira um passo intermediário. Gravar direto em Delta com `replaceWhere` por subbloco é idempotente e elimina boa parte do problema de duplicidade.
3. **Armazenamento do estado indefinido ("Delta e/ou Lakebase").** Claim concorrente, lease, transição condicional e bloqueio de sobreposição são carga OLTP. Precisa ser **Lakebase (Postgres)**. Delta como fonte de verdade traz conflitos de concorrência otimista e latência de warehouse na UI.
4. **Escopo de MVP grande demais.** São 14 estados de pedido, 10 de subbloco, pausa e cancelamento separados, dashboard de periodicidade e reprocessamento versionado, tudo antes do piloto.
5. **Inconsistências no documento:** veja a seção 6.

## 2. Validação ponto a ponto

| Tema | Spec | Avaliação |
|---|---|---|
| App como plano de controle, sem chamadas longas | ✔ | Correto. O app **também não deve** testar conectividade com o Netezza (item 4.2). O app é serverless e normalmente não tem rota de rede até o Netezza. Essa validação é do Job. |
| Um Job reutilizável, metadata-driven | ✔ | Correto. Veja em 4 a proposta de *dispatcher* em vez de um run por pedido. |
| `For each` para fan-out | ⚠ | Concorrência máxima de 100 por For each e input de até 48 KB via task values. Mais importante: **o limite de concorrência vale só para o run.** Dois pedidos simultâneos dobram as sessões no Netezza. A spec exige limite global por origem, e o For each não entrega isso. |
| Spark | ⚠ | Se a procedure escreve os arquivos, o worker só espera I/O do Netezza e o Spark não tem papel. Uma task Python com pool de threads é suficiente e mais barata. |
| Pausa vs cancelar vs retomar | ⚠ | Na prática, pausar e cancelar com retomada são a mesma coisa: parar de iniciar subblocos e deixar os ativos terminarem ou expirarem. Unificar em **Parar** + **Retomar**. |
| Chave de unicidade inclui o "período total" | ✘ | **Erro conceitual.** O mesmo mês exportado em dois pedidos com períodos totais diferentes geraria chaves diferentes e a duplicidade não seria detectada. A chave deve usar o **intervalo de dados do subbloco**. A sobreposição deve ser verificada como **interseção de intervalos**, não por igualdade de chave, porque pedidos podem usar granularidades diferentes (mensal × diário). |
| `config_version` e formato na chave | ⚠ | Mudar a versão da config ou o formato liberaria duplicidade silenciosamente. Decidir de forma explícita. Sugestão: duplicidade = mesma tabela + contexto + interseção de dados. Formato e versão só entram via reprocessamento autorizado. |
| Netezza executa o filtro | ✔ | Só é eficiente se a coluna de data estiver correlacionada com a ordem física (zone maps) ou se houver `ORGANIZE ON`. Medir no benchmark. |
| Consistência entre tabelas | ⚠ | Deixar explícito: sem snapshot, cada subbloco representa um momento diferente. Para migração isso costuma ser aceitável com reconciliação de contagens no final. |
| Identidade | ⚠ | Recomendo **SP do app** para Job e Lakebase, com autorização no próprio app (grupos) e auditoria do usuário solicitante. OBO (user auth) complica disparar Jobs e acessar segredos. |

## 3. Alternativas de implantação (onde a extração acontece)

| Rota | Como | Prós | Contras | Quando usar |
|---|---|---|---|---|
| **A. Spec atual: procedure grava os arquivos** | Job chama a procedure via `nzpy`; a procedure faz CTAS em external table no S3/ADLS (NPS 11.x) | Lógica fica no Netezza, onde já foi homologada; o Databricks só orquestra | Depende da versão do NPS; o Netezza precisa de credencial do storage; contrato de procedure assíncrona, status e cancelamento exige **desenvolvimento pesado em NZPLSQL**; checksum e atomicidade ficam no lado Netezza | Cliente exige que a lógica fique no Netezza **e** o NPS suporta object storage |
| **B. Databricks puxa com unload remoto (recomendada para avaliar)** | Worker Python usa `nzpy` com transient external table `REMOTESOURCE 'PYTHON'` (unload em bloco para o cliente), converte para Parquet com pyarrow e grava em staging no Volume. A procedure, se existir, só prepara ou valida | Unload em bloco é o caminho mais rápido de saída do Netezza; **o Databricks controla arquivos, atomicidade, manifesto e checksum**; qualquer versão do Netezza; a procedure fica simples | Banda de rede Netezza→Databricks; disco local do worker | Caso geral, principalmente migração |
| **C. Spark JDBC particionado** | `spark.read.jdbc` com `partitionColumn` no range do subbloco e gravação direta em Delta/Parquet | Nativo, simples, grava Delta direto | JDBC linha a linha é lento no Netezza; N partições = N sessões e N scans | Tabelas pequenas e médias, fallback |
| **D. Ferramentas** | Lakebridge (análise, conversão de SQL/procedures e reconciliação), Qlik/Informatica, AWS SCT extraction agents | Reaproveita produto pronto | Licença, menos controle sobre a UX pedida | Se o projeto for migração completa (DDL + código + dados) |

**Recomendação:** fazer um **spike comparativo A × B (× C)** na tabela piloto antes de construir o app. Se o destino final for Delta, a rota **B ou C gravando Delta com `replaceWhere` por subbloco** é a mais simples e robusta. Se o produto for entrega de arquivos para terceiros, a rota B mantém o manifesto sob controle do Databricks.

## 4. Desenho de execução proposto (no lugar de um run por pedido com For each)

**Dispatcher único com fila no Lakebase:**

- Job `netezza-export-dispatcher` com 1 task Python (wheel), `max_concurrent_runs = 1` e fila habilitada.
- O app grava o pedido e os subblocos no Lakebase e chama `run-now`. Se já houver run ativo, o novo fica na fila. Um schedule a cada N minutos serve de rede de segurança.
- O worker drena a fila de **todos** os pedidos com claim `SELECT … FOR UPDATE SKIP LOCKED` e lease (`lease_expires_at`).
- **Limite global por origem** = semáforo na tabela `source_connection.max_sessions`. Limite por tabela e por pedido = filtros no claim. A concorrência efetiva fica em `min(global, tabela, pedido)`.
- Parar/cancelar = flag no banco. O worker verifica antes do claim, antes da chamada, no heartbeat e antes de publicar. Cancelar o run Databricks fica como último recurso.
- Reconciliador: o próprio loop verifica leases expirados e consulta o status no Netezza pela `idempotency_key` antes de liberar um subbloco.
- Compute: serverless se houver rota de rede (NCC/Private Link até o Netezza). Caso contrário, Job compute clássico single-node numa VPC/VNet com rota para o Netezza. O `nzpy` é Python puro e instala via pip.

Ganhos: concorrência global garantida, cancelamento simples, um só compute, sem limite de 48 KB do For each e sem task por subbloco.

## 5. Modelo de dados revisado (Lakebase / Postgres)

Mudanças principais em relação à spec: **tabela de tentativas** (a spec perde o histórico porque sobrescreve `attempt_number`/`error`), **tabela de arquivos**, **config versionada e imutável**, **intervalos como `tstzrange [)`** com **constraint de exclusão** para bloquear sobreposição no banco, lease explícito, e campos de estado da tabela calculados em view em vez de denormalizados no pedido.

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE source_connection (
  source_id        text PRIMARY KEY,
  secret_scope     text NOT NULL,          -- só referência; nunca a senha
  max_sessions     int  NOT NULL
);

CREATE TABLE table_config (
  table_config_id  text PRIMARY KEY,       -- nome lógico
  source_id        text NOT NULL REFERENCES source_connection,
  active           boolean NOT NULL DEFAULT true
);

CREATE TABLE table_config_version (       -- imutável; pedido aponta para a versão
  table_config_id  text REFERENCES table_config,
  config_version   int,
  source_database  text NOT NULL, source_schema text NOT NULL, source_table text NOT NULL,
  filter_column    text NOT NULL, filter_type text NOT NULL, filter_timezone text NOT NULL,
  procedure_name   text, procedure_version text, procedure_mode text,  -- sync|async|none
  default_subblock interval NOT NULL, min_subblock interval NOT NULL,
  max_parallel     int NOT NULL, call_timeout interval NOT NULL,
  expected_periodicity interval, periodicity_tolerance interval,
  allowed_formats  text[] NOT NULL, output_root text NOT NULL,
  allowed_columns  text[],
  created_by text, created_at timestamptz DEFAULT now(),
  PRIMARY KEY (table_config_id, config_version)
);

CREATE TABLE export_request (
  request_id       uuid PRIMARY KEY,
  request_name     text, context_key text NOT NULL,     -- cliente/contexto (falta na spec)
  requested_by     text NOT NULL,
  status           text NOT NULL,          -- DRAFT|SUBMITTED|RUNNING|STOPPING|STOPPED|FAILED|COMPLETED
  output_format    text NOT NULL, compression text,
  max_parallel     int, deadline_at timestamptz,
  stop_requested_at timestamptz, stop_requested_by text,
  created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now()
);

CREATE TABLE export_request_table (
  request_table_id uuid PRIMARY KEY,
  request_id       uuid NOT NULL REFERENCES export_request,
  table_config_id  text NOT NULL, config_version int NOT NULL,
  data_range       tstzrange NOT NULL,     -- período total, sempre [início, fim)
  subblock_interval interval NOT NULL, calendar_aligned boolean NOT NULL,
  is_reprocess     boolean NOT NULL DEFAULT false,
  reprocess_reason text, approved_by text,
  FOREIGN KEY (table_config_id, config_version) REFERENCES table_config_version
);

CREATE TABLE export_subblock (
  subblock_id      uuid PRIMARY KEY,
  request_table_id uuid NOT NULL REFERENCES export_request_table,
  table_config_id  text NOT NULL, context_key text NOT NULL,  -- denormalizado para a constraint
  seq              int NOT NULL,
  data_range       tstzrange NOT NULL,
  status           text NOT NULL,          -- PENDING|RUNNING|UNKNOWN|SUCCEEDED|PUBLISHED|FAILED|CANCELED|SKIPPED|SUPERSEDED
  current_attempt  int NOT NULL DEFAULT 0,
  lease_owner      text, lease_expires_at timestamptz,
  updated_at       timestamptz DEFAULT now(),
  -- impede duas cargas ativas/publicadas com interseção para a mesma tabela+contexto
  EXCLUDE USING gist (table_config_id WITH =, context_key WITH =, data_range WITH &&)
    WHERE (status NOT IN ('CANCELED','FAILED','SKIPPED','SUPERSEDED'))
);
CREATE INDEX ON export_subblock (status, lease_expires_at);

CREATE TABLE export_attempt (              -- histórico de cada tentativa (falta na spec)
  attempt_id       uuid PRIMARY KEY,
  subblock_id      uuid NOT NULL REFERENCES export_subblock,
  attempt_no       int NOT NULL,
  idempotency_key  text NOT NULL UNIQUE,
  databricks_job_run_id bigint, databricks_task_run_id bigint,
  netezza_execution_id text,
  status           text NOT NULL,
  started_at timestamptz, finished_at timestamptz, last_heartbeat_at timestamptz,
  row_count bigint, bytes_written bigint, file_count int,
  error_class text, error_code text, error_message text,
  UNIQUE (subblock_id, attempt_no)
);

CREATE TABLE export_file (                 -- consultável (a spec só tinha array no manifesto)
  file_id uuid PRIMARY KEY,
  attempt_id uuid NOT NULL REFERENCES export_attempt,
  path text NOT NULL UNIQUE, stage text NOT NULL,  -- STAGING|FINAL|DELETED
  size_bytes bigint, row_count bigint, checksum text
);

CREATE TABLE export_manifest (
  manifest_id uuid PRIMARY KEY,
  request_table_id uuid NOT NULL REFERENCES export_request_table,
  manifest_version int NOT NULL, path text NOT NULL,
  schema_snapshot jsonb, total_rows bigint, total_bytes bigint, file_count int,
  validation_status text, published_at timestamptz,
  UNIQUE (request_table_id, manifest_version)
);

CREATE TABLE export_event (                -- append-only
  event_id bigserial PRIMARY KEY,
  request_id uuid, request_table_id uuid, subblock_id uuid, attempt_id uuid,
  event_type text NOT NULL, previous_state text, new_state text,
  actor_type text NOT NULL, actor_id text NOT NULL,
  event_time timestamptz DEFAULT now(), details jsonb
);

-- Indicadores do dashboard calculados, não gravados no pedido:
-- view table_load_status: último range PUBLISHED, próximo esperado
-- (último fim + periodicidade), atraso, falhas recentes, pedidos conflitantes.
```

Notas:
- O **reprocessamento** marca os subblocos anteriores como `SUPERSEDED` e cria novos na mesma transação. A constraint de exclusão continua válida.
- O **retry** vira nova linha em `export_attempt` e o subbloco é o mesmo.
- O **analítico** (custo, histórico, Genie/AI-BI) registra o banco Lakebase como catálogo no UC, ou um job copia snapshots para Delta. O Lakebase continua como fonte de verdade.
- Campos da spec que saem: `total_period_*` no subbloco (redundante), `partition_lower/upper_bound` (redundante com `data_range`), `last_successful_*`/`next_expected_*`/`*_status` em `export_request_table` (vão para view), `job_id/run_id` no cabeçalho (vão para attempt/run).

## 6. Inconsistências no documento

- `RECONCILIATION_PENDING` aparece em 8.3, mas não está na máquina de estados 5.2.
- Seções 8.3 e 8.4 numeradas duas vezes.
- Há menções a "conexão JDBC" (9, 11.2, Fase 1) numa arquitetura baseada em `nzpy`. As fontes JDBC no final são irrelevantes, a menos que a rota C seja adotada.
- Fonte de "cancelamento assíncrono" cita doc de Power Platform. Trocar pela Jobs API (`runs/cancel`).
- O lease é citado (Fase 3, critérios), mas não existe no modelo de dados.
- A chave de unicidade exige "cliente ou contexto", mas nenhum modelo tem essa coluna.
- 4.2 pede "filtro específico para cada tabela" e 11.3 proíbe filtro livre, mas não há modelo para filtros tipados.
- 4.2 "espaço disponível no destino": em object storage isso praticamente não se aplica. Trocar por quota/permissão.
- O app validando conectividade com o Netezza (4.2) não é possível pela rede do app serverless na maioria dos ambientes.

## 7. Estados simplificados

- **Pedido:** `DRAFT → SUBMITTED → RUNNING → (STOPPING → STOPPED) | FAILED | COMPLETED`. "Parcialmente concluído" é derivado (STOPPED/FAILED com subblocos PUBLISHED). `EXPIRED` fica para a fase de retenção.
- **Subbloco:** `PENDING → RUNNING → SUCCEEDED → PUBLISHED`, com `UNKNOWN` (reconciliação), `FAILED` (retryable no attempt), `CANCELED`, `SKIPPED`, `SUPERSEDED`.

## 8. Stack do app

Flask + Jinja2 + Gunicorn como Databricks App, Lakebase via psycopg (pool e token OAuth renovado), Databricks SDK para `jobs.run_now`/`runs.get`, e deploy de app, job e permissões via Databricks Asset Bundle. Atualização de status por polling curto (htmx ou fetch a cada 5–10 s). O dashboard operacional fica no app. Visões analíticas podem ser um AI/BI dashboard sobre o catálogo Lakebase.

## 9. Plano de ação

| Fase | Entregas | Gate de saída |
|---|---|---|
| **0. Contrato e decisões (1 sem)** | Enviar ao cliente o contrato da procedure (seção 0); confirmar o destino dos arquivos (S3/ADLS + external location); identidade (SP do app); tabela piloto | Contrato aceito pelo cliente |
| **1. Conectividade (paralelo, 2–3 dias)** | Script em `scripts/` rodando como job serverless via NCC: `nzpy` conecta e executa `SELECT 1` e uma procedure *stub* no Netezza | Rede e driver validados |
| **2. Núcleo sem UI (2–3 sem)** | Schema Lakebase com migrações; wheel `exporter` (planner, claim/lease, `ProcedureClient` real + fake, worker, publicação de manifesto, **hook**); Job dispatcher via DAB; CLI para criar pedido | E2E com o fake; retomada após kill do run; nenhum subbloco duplicado; hook disparado uma única vez |
| **3. Robustez (1–2 sem)** | Classificação de erros e retry com backoff; parar/retomar; reprocessamento com SUPERSEDED; testes de caos (queda de rede, kill de cluster, cliques duplicados) | Critérios técnicos da spec aprovados |
| **4. App MVP (2–3 sem)** | Flask: catálogo, novo pedido com validação de sobreposição, revisão, lista e detalhe com progresso por subbloco, parar/retomar; autorização por grupo; auditoria | Piloto com usuários operacionais |
| **5. Operação (1–2 sem)** | Dashboard de periodicidade (view `table_load_status`), alertas, runbooks, custo por pedido via system tables | Rollout gradual por tabela |

## 10. Perguntas bloqueadoras

1. Objetivo: migração (o destino final é Delta no Databricks) ou entrega recorrente de arquivos para consumidores externos?
2. Versão/distribuição do Netezza (NPS 11.x com object storage ou legado 7.x)? On-prem ou cloud?
3. A procedure já existe? Ela grava arquivos onde? É síncrona ou assíncrona? Existe tabela de status?
4. Há rota de rede do Databricks (serverless ou clássico) até o Netezza?
5. Volume: maior tabela (linhas/TB), número de tabelas fato, sessões concorrentes permitidas.
6. Quem são os usuários (internos do cliente? grupos?) e se o acesso é igual para todos.
7. Pedidos podem se sobrepor legitimamente (mesma tabela e período para contextos diferentes)?
8. Cloud (AWS/Azure/GCP) e se Apps + Lakebase estão habilitados no workspace.
