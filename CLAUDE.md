# nzexport — contexto para novas sessões

Registro consolidado das conversas de 2026-10-05 a 2026-10-14 (v0.990 → v1.006). Leia antes de mexer no projeto:
ele evita re-perguntar decisões já fechadas e re-descobrir o ambiente.

## O que é
Databricks App que **controla a migração de tabelas fato do Netezza para Delta**, garantindo a **completude** de cada
tabela. As tabelas **não têm periodicidade**: cada uma tem um escopo (`migration_start`/`migration_end`) e o app mede a
cobertura e as lacunas. Fluxo por **subbloco** (subperíodo):
1. o **dispatcher** chama `SP_CARGA_EVENTUAL` no Netezza, que gera um `.dat` na landing S3;
2. o **importador** faz **append** do `.dat` na tabela Delta de destino e apaga o arquivo;
3. em erro: apaga o período do subbloco no Delta, move o `.dat` para a pasta de erro e marca `IMPORT_FAILED`, o que
   libera o período para um novo pedido.

Documentos: `README.md` (operação), `DEPLOY.md` (deploy no cliente), `CHANGELOG.md`, `revisao_especificacao.md`
(revisão da especificação original `especificacao_exportador_netezza_databricks.md`), `plano_job_importacao.md`.

## Regras do usuário (obrigatórias)
- **Nunca Streamlit.** Stack: Flask + Jinja2 + Gunicorn, Lakebase via psycopg3, Databricks App.
- **Versão**: cada ajuste pedido = +0.001 (desde 0.990). `scripts/bump_version.py "<descrição>"` (atualiza
  `src/VERSION` e `CHANGELOG.md`), deploy, commit `vX.XXX: ...`, `git tag -a vX.XXX`, `git push --follow-tags` no repo
  **privado** `lfmed/netezza-exporter` (conta gh `lfmed`). Não agrupe ajustes pedidos separadamente.
- **Sem simulação**: o driver fake e os dados simulados foram removidos na v1.001. Não reintroduza.
- **Credenciais nunca no Git**: host, porta, usuário e senha do Netezza ficam só no secret scope, que já existe no ambiente.
  O app e os jobs só **leem** (READ). Nada de WRITE/MANAGE para o app, e o scope não é recurso do bundle (v1.008).
- **Escopo da SP**: o app só **chama** a SP e passa os parâmetros. O controle interno é da SP (não alterar a procedure
  nem recriar a external table).
- **Dados**: só append, **sem validação de range ou conteúdo**. O app garante os subblocos.
- Perfil Databricks: sempre `--profile fe-vm-stable-classic-pg4xe1` (nunca escolher outro sozinho).
- Salve tudo dentro do projeto (auxiliares em `scripts/`); nunca em `/tmp`.
- Nada compartilhado com terceiros; tudo privado ao usuário.
- Identidade visual Sicoob, igual ao app "Marketplace de Dados": `logo-sicoob.png` real, sidebar `#00353e`, teal `#2fa890`.

## Ambiente dev (implantado)
| Item | Valor |
|---|---|
| Workspace | https://fevm-stable-classic-pg4xe1.cloud.databricks.com (profile `fe-vm-stable-classic-pg4xe1`) |
| App | `netezza-exporter` — https://netezza-exporter-7474646973581105.aws.databricksapps.com |
| SP do app | `88d6e7b2-9da3-4ca1-890e-fab9e0eb9604` (`app-32k2l4`) |
| Lakebase | projeto `netezza-exporter`, branch `production`, endpoint `projects/netezza-exporter/branches/production/endpoints/primary`, schema `nzexport` |
| Backup Lakebase | branch `backup-antes-remover-simulacao` (sem expiração; apagar só se o usuário pedir) |
| Jobs | dispatcher `65950468575247`, importer `1045038738828699`, conn_test `1073163387876744`, pos_deploy (criado pelo bundle) |
| Secret scope | `nzexport-netezza`, chaves `<source_id>-host` / `-port` (URL exibida) e `-user` / `-password`; app com READ (conexão padrão `netezza`). O **nome** do scope vem só da variável `secret_scope` do bundle (app: `NETEZZA_SECRET_SCOPE`; jobs: parâmetro `--secret-scope`); o app mostra e nunca grava (v1.012) |
| Catálogo dev | `stable_classic_pg4xe1_catalog`; warehouse para testes `9981e1dca61efe86` |
| Health | `GET /api/health` → `{"ok":true,"version":"..."}` (`/healthz` é interceptado pela plataforma) |

**Pendente do lado do usuário:** host, credenciais e landing do Netezza no app. Os targets `hml`/`prd` do
`databricks.yml` têm placeholders `<...>` à espera dos valores do cliente.

## Deploy (sempre nesta ordem)
```bash
databricks bundle deploy -t dev --profile fe-vm-stable-classic-pg4xe1
databricks bundle run nzexport_pos_deploy -t dev --profile fe-vm-stable-classic-pg4xe1   # migra o banco, grants, ACLs, BROWSE, checagens
databricks bundle run nzexport_app -t dev --profile fe-vm-stable-classic-pg4xe1
```
- Não existe `src/app.yaml`: a configuração do app fica em `databricks.yml` (`config.command`/`env` + `value_from`).
  Se o app subir com `python app.py` (publicado sem o bundle), `app.py` sobe o mesmo gunicorn e loga as variáveis
  ausentes (v1.014). Log em stdout, nível `NZEXPORT_LOG_LEVEL`.
- Política de serverless do app: variável `serverless_policy_id` → `budget_policy_id` do app (vazia = padrão do
  workspace, hoje `79898ab9-344d-47f6-a180-de5a332ef681`); placeholders em hml/prd (v1.009).
- O projeto Lakebase é recurso do bundle com `prevent_destroy`; em dev foi vinculado
  (`bundle deployment bind nzexport_lakebase projects/netezza-exporter`). O secret scope saiu do bundle na v1.008.
- Resultado esperado hoje do pós-deploy: sem erros; avisos de host, credenciais e landing ainda não configurados.
- Para ver as páginas via HTTP: token de `databricks auth token --profile ...` como Bearer.

## Arquitetura (src/)
- `app.py` → `webapp.create_app()`. `webapp/__init__.py`: rotas, filtros (`dt`, `num`, `secs`, `pct`, `dur`) e
  fuso `America/Sao_Paulo`. `webapp/services.py`: cliente, banco, disparo de jobs, secrets e uma thread **scheduler**
  (advisory lock 4207311) que dispara o dispatcher e o importador quando há trabalho devido.
  `webapp/catalog.py`: navegador do UC (cache de 60 s).
- `nzexport/db.py`: pool psycopg com token OAuth (`generate_database_credential`), `tx()`/`query()`/`one()`. Para um
  UPDATE use `tx()`, não `query()`.
  `resolve_endpoint`: `lakebase_endpoint` é o caminho `projects/.../endpoints/primary`; se vier o host
  (`ep-....database...com[/banco]`, erro real do cliente em 2026-10-15), procura o endpoint pelo host.
- `nzexport/schema.sql`: migrações idempotentes, aplicadas por `migrate.py` (no pós-deploy ou por
  `scripts/migrate_local.py`). Para mudar as colunas de uma view, faça `DROP VIEW` e depois `CREATE`.
- `nzexport/faixa.py` (v1.010): tipo de filtro `DATE`/`NUMBER`/`NONE` e a faixa genérica. Valores nativos: date, int
  ou 0; chave inteira: dias desde 1970-01-01, o valor ou 0. `to_key`/`from_key`/`units`/`fmt_range`.
- `nzexport/repo.py`: todas as consultas e ações (pedidos, revisão que exclui períodos já executados, retry/cancel,
  cobertura, vazão). Lê `key_lo`/`key_hi` e converte com `with_natives`. `planner.py`: calendário (Data),
  `plan_count`/`plan_count_pieces` (Número, quantidade) e intervalos genéricos (máx. 5000).
  `lifecycle.py`: finaliza pedidos e dispara hooks.
- `nzexport/dispatcher.py`: claim com `FOR UPDATE SKIP LOCKED`, lease e heartbeat, concorrência
  = mín(global da conexão, tabela, pedido); processa **o período mais recente / maior valor primeiro**; retry com backoff; um reaper
  marca leases expiradas como `UNKNOWN`.
- `nzexport/procedure.py`: perfis `SP_CARGA_EVENTUAL` (7 args, padrão) e `TABELA_INI_FIM` (3 args, só Data).
  - Número: `p_dtini`/`p_dtfim` = mínimo/máximo. Sem filtro: coluna, ini e fim = `'1'` (`WHERE 1 BETWEEN 1 AND 1`).
  - O `nzpy` substitui `?` **sem escapar**, por isso os valores são validados por regex e o app monta os literais.
  - A sessão usa o banco da external table.
  - Saída da SP: `/tmp/<TABELA>_<ini>_<fim>.dat` no datasource `PLH_DATASOURCE_DWA`, `DELIM '|'`, header, escape `\`.
- `nzexport/motor_dat.py`: funções copiadas **verbatim** do motor PLHDWA (`lfmed/plhdwa-carga-netezza`), com leitura
  FAILFAST, limpeza CLIENTE/AUTO/PARALELA/DIRETA/MULTILINHA e `explicar_erro`, mais helpers próprios.
  `importer.py` + `importar_dat.py` (notebook):
  - append com `userMetadata=nzexport:<subblock_id>`, que permite retomar via `DESCRIBE HISTORY`;
  - uma importação por tabela destino de cada vez;
  - num reprocessamento, apaga o período antes do append (`BETWEEN` na Data/Número; Sem filtro: `DELETE` total).
- `nzexport/file_format.py`: opções do `.dat` com padrões do PLHDWA (ISO-8859-15, `|`, header...).
  `table_csv.py`: importação e exportação do cadastro de tabelas por CSV.
- Entradas de job: `run_dispatcher.py`, `run_conn_test.py`, `importar_dat.py`, `pos_deploy.py`. Em
  `spark_python_task` não existe `__file__`: use `inspect.currentframe().f_code.co_filename`.
- Identidade no serverless: `User` não tem `application_id`. Use `getattr(...)`; um `user_name` sem `@` é um SP.

## Modelo de estados
- Subbloco: `PENDING → RUNNING → EXPORTED → IMPORTING → DONE`, mais `FAILED`, `IMPORT_FAILED`, `UNKNOWN`, `CANCELED`
  e `SUPERSEDED`.
  - **Reservam o período** (EXCLUDE btree_gist em `key_range int8range [)`, desde a v1.010; antes `data_range`): PENDING, RUNNING, EXPORTED, IMPORTING, DONE, UNKNOWN e
    FAILED.
  - **Liberam o período**: CANCELED, SUPERSEDED e IMPORT_FAILED.
  - Um subbloco **cancelado é definitivo** no pedido (não pode ser repetido); o período volta num novo pedido.
  - Só FAILED e UNKNOWN podem ser repetidos.
- Pedido: SUBMITTED, RUNNING, STOPPING, STOPPED, FAILED, COMPLETED e CANCELED, mais `scheduled_start_at` e
  `deadline_at`.
- Cobertura: `v_migration_coverage` (multirange `range_agg`; IMPORT_FAILED conta como lacuna).
- Vazão: `v_subblock_throughput` (v1.006). A taxa no Netezza é linhas ÷ duração da SP; a taxa no Delta é linhas ÷
  duração da importação. A SP não devolve contagem, então a vazão só existe depois da importação. As médias são
  ponderadas.

## Testes e verificação
- Unitários: `.venv/bin/python -m pytest tests` (35 testes).
- E2E real do importador: `.venv/bin/python scripts/teste_importacao.py`. Cria um branch Lakebase e um schema/volume
  temporários, roda os jobs reais (importador nos 3 tipos de filtro e dispatcher sem Netezza), confere e limpa.
- Modelo e telas no branch: `.venv/bin/python scripts/teste_filtro_branch.py` (migração `data_range`→`key_range`, pedidos
  por tipo, cobertura e formulários pelo Flask local).
- Para testar sem tocar a produção: crie um **branch Lakebase temporário** (`ttl`), migre-o e aponte os jobs/scripts para o
  endpoint dele; apague o branch no fim. Branch sem expiração exige `no_expiry: true` (um ttl de 30 dias falhou).
- Screenshots: Chrome headless em background, salvos em `scripts/screenshots/` (gitignored).
- O scheduler do app implantado roda em paralelo e pode interferir em testes feitos no branch `production`.

## Histórico de versões (resumo)
0.990 visual Sicoob · 0.991 `/api/health` · 0.992 logo real · 0.993 repetir/cancelar falhas · 0.994 agendamento ·
0.995 ordenar por completude · 0.996 cadastro por CSV · 0.997 cancelado é definitivo · 0.998 mais recente primeiro ·
0.999 chamada da `SP_CARGA_EVENTUAL` · 1.000 tela de conexão Netezza · 1.001 remove simulação · 1.002 destino UC com
navegador · 1.003 landing e formato do arquivo · 1.004 job importador · 1.005 deploy repetível (targets, pós-deploy,
secret scope) · 1.006 vazão (reg/s) por subbloco, tabela e histórica · 1.007 host/porta no secret scope · 1.008 app só
lê o scope · 1.009 política de serverless do app · 1.010 tipo de filtro (Data, Número, Sem filtro) · 1.011 `lakebase_endpoint` aceita o host · 1.012 nome do
secret scope só pela configuração do deploy · 1.013 erro real do Lakebase no lugar do `PoolTimeout` · 1.014 app sobe e se diagnostica com
`python app.py` (config do bundle não aplicada); health 503 com o motivo.
