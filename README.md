# Exportador Netezza (Databricks App + Job + Lakebase)

Este projeto é o plano de controle para tirar dados do Netezza em subblocos. Cada subbloco faz uma chamada de
`procedure(tabela, data_ini, data_fim)`, com `data_fim` **inclusiva**. Quem grava os arquivos é a procedure. O sistema só
precisa saber se ela terminou: retorno sem exceção significa concluído.

- **App**: https://netezza-exporter-7474646973581105.aws.databricksapps.com (workspace `fe-vm-stable-classic-pg4xe1`)
- **Lakebase**: projeto `netezza-exporter`, branch `production`, schema `nzexport`
- **Jobs**: `nzexport-dispatcher` (fila e chamadas da procedure) e `nzexport-conn-test` (teste de conexão)

## Arquitetura

```
App Flask (SP do app) ──grava pedido/subblocos──▶ Lakebase (nzexport)
      │                                                ▲
      └──run-now──▶ Job nzexport-dispatcher (serverless)──┘
                       │  claim com FOR UPDATE SKIP LOCKED + lease/heartbeat
                       ├─ nzpy ─▶ CALL procedure('TABELA', 'AAAA-MM-DD', 'AAAA-MM-DD')
                       └─ hook ─▶ run-now do job de importação (ex.: Delta), parâmetros do período
```

**Concorrência.** O menor dos três limites vale:
1. **Global por origem**, em `Administração → Origens → Limite global`. Soma todos os pedidos.
2. **Por tabela**, em `Administração → Tabela → Máx. chamadas simultâneas`.
3. **Por pedido**, definido ao criar o pedido. Pode ser alterado durante a execução, na tela do pedido.

**Completude da migração (painel).** Cada tabela tem um **escopo de migração**: primeiro e último dia de
dados a migrar, definidos em Administração. O painel compara o escopo com os subblocos e mostra:
- **% migrado**: dias do escopo com subbloco concluído.
- **Em execução**: dias pendentes ou executando.
- **Lacunas**: dias do escopo sem nenhum subbloco vivo, seja porque nunca foram pedidos, foram cancelados ou falharam
  em definitivo.
- **Falha e desconhecido**: dias com problema.

Status por tabela: `COMPLETA`, `EM_ANDAMENTO`, `INCOMPLETA`, `COM_FALHAS`, `ESTADO_DESCONHECIDO`, `NAO_INICIADA` e
`SEM_ESCOPO`. O botão **Criar pedido para as lacunas** monta um pedido com exatamente os intervalos que faltam. A
migração está garantida quando todas as tabelas aparecem como `COMPLETA`.

**Duplicidade.** Uma constraint `EXCLUDE` no Postgres impede dois subblocos vivos da mesma tabela com período
sobreposto. Isso vale para interseção parcial e para granularidades diferentes. Para exportar de novo um período,
crie o pedido como **Reprocessar** e informe a justificativa. Os subblocos antigos ficam como `SUPERSEDED`.

**Dispatcher.** É o Job que efetivamente chama a procedure. O app só grava pedidos; o dispatcher pega os
subblocos prontos de **todos** os pedidos, respeita os limites de concorrência, faz heartbeat/lease, aplica retries e
marca o resultado. Quando não há trabalho, ele encerra sozinho, e assim não consome compute parado.

**Agendamento.** O pedido pode ter **início agendado**. Antes da data e hora informadas, nenhuma chamada é feita. Uma
checagem do app, a cada minuto, dispara o dispatcher na hora certa. Na tela do pedido é possível
**Iniciar agora** ou **reagendar**. O **prazo/janela final** impede que subblocos novos comecem depois do horário.
Datas e horas usam o fuso `America/Sao_Paulo`.

**Falhas.** O subbloco que falhou depois dos retries automáticos **continua reservando o período** até alguém decidir:
- **Repetir** (um ou todos): volta para a fila.
- **Cancelar** (com motivo): libera o período, que vira lacuna no painel e pode entrar em um novo pedido.

**Importação para o Delta.** Quando a tabela tem destino no Databricks, a SP gera o `.dat` e o subbloco fica
`EXPORTED`. O Job **`nzexport-importer`** (notebook `src/importar_dat.py`) é disparado pelo dispatcher e pelo app. Ele
carrega o arquivo na tabela destino e apaga o `.dat`, e o subbloco fica `DONE`. Regras:
- **Append puro**, sem validação de período ou conteúdo. O app garante que os subblocos não se sobrepõem. Só a
  estrutura é conferida: o header precisa bater com as colunas da tabela, e uma linha ilegível aborta (`FAILFAST`).
- **Erro**: o período do subbloco é **limpo** na tabela destino, o `.dat` vai para a pasta de erro e o subbloco fica
  `IMPORT_FAILED`, o que **libera o período** para um novo pedido (aparece como lacuna).
- **Reprocessamento**: o período é limpo no destino antes do append.
- **Retomada**: se o commit do subbloco já está no histórico Delta (`userMetadata = nzexport:<subbloco>`), ele só
  conclui, sem carregar de novo.
- **Concorrência**: no máximo uma importação por tabela destino ao mesmo tempo. Tabelas diferentes rodam em
  paralelo, até o limite da conexão.
- **Código de leitura**: a leitura, a limpeza do DAT (3 regex do cliente, modo `CLIENTE`) e o diagnóstico de erros
  vêm do motor PLHDWA (`src/nzexport/motor_dat.py`, com as origens e os ajustes documentados no topo).
- **Configuração**: landing (caminho S3 ou volume, pastas do `.dat`, de erro e de limpeza) em **Conexão Netezza**.
  Formato do arquivo e destino no **cadastro da tabela**.
- **Teste ponta a ponta**: `scripts/teste_importacao.py`, com Job real e ambiente temporário removido no fim.

**Estados do subbloco.**
- `PENDING → RUNNING → DONE` é o caminho normal.
- `FAILED` indica erro da procedure ou de conexão, depois de esgotar os retries automáticos. Aguarda repetir ou
  cancelar.
- `UNKNOWN` indica que a chamada foi enviada e a conexão caiu ou o worker parou. Nesse caso, é preciso reconciliar
  manualmente ou reexecutar, o que só é seguro se a procedure for idempotente.

**Ações.**
- **Parar**: não inicia subblocos novos e deixa terminar os que estão em execução.
- **Retomar**: continua os pendentes, com opção de repetir os falhos e os desconhecidos.
- **Encerrar**: cancela os pendentes e os falhos e libera o período.

## Operação

Deploy em qualquer ambiente (dev, hml, prd): veja **[DEPLOY.md](DEPLOY.md)**. Em resumo:

```bash
databricks bundle deploy -t <alvo>
databricks bundle run nzexport_pos_deploy -t <alvo>   # banco, grants, ACLs e conferência dos pré-requisitos
databricks bundle run nzexport_app -t <alvo>
```

Desenvolvimento:

```bash
.venv/bin/python -m pytest -q tests                  # testes unitários
.venv/bin/python scripts/teste_importacao.py         # E2E do importador (ambiente temporário, removido no fim)
```

## Configurar o Netezza

1. Em **🔌 Conexão Netezza**, informe:
   - host, porta (5480), banco da sessão, TLS, timeout e limite global de sessões;
   - **usuário e senha**, que são gravados no secret scope `nzexport-netezza` e nunca no banco do app;
   - os **padrões** de `p_banco`, `p_banco_ext_table` e `p_esquema_ext_table`.
2. Clique em **Testar conexão**. O teste roda como Job serverless, pela mesma rede (NCC) do dispatcher, e:
   - autentica;
   - mostra o usuário e o banco da sessão, e a versão do Netezza;
   - verifica se a procedure existe nos bancos das external tables.
3. Em cada tabela, use o perfil **SP_CARGA_EVENTUAL** e informe `p_tabela`, `p_NomeColunaTempo` e, se forem
   diferentes do padrão da conexão, `p_banco` e o banco e o esquema da external table. A tela mostra a prévia
   exata do `CALL`.

### Chamada da `SP_CARGA_EVENTUAL`

```
CALL SP_CARGA_EVENTUAL(p_banco, p_tabela, p_banco_ext_table, p_esquema_ext_table, p_dtini, p_dtfim, p_NomeColunaTempo)
```

- `p_dtini`/`p_dtfim` vêm do subbloco, em `AAAAMMDD`, com o fim inclusivo. A procedure filtra com
  `BETWEEN p_dtini AND p_dtfim`, sem aspas, então a coluna de tempo deve ser numérica `AAAAMMDD`.
- A sessão é aberta no **banco da external table**, porque a procedure faz `INSERT INTO esquema.tabela` sem o banco.
- Os valores são validados (só letras, números e `_`) antes de montar o comando, porque o `nzpy` não escapa
  parâmetros e a procedure concatena os valores em SQL dinâmico.
- Cada tentativa registra o comando, o retorno da procedure e o arquivo esperado:
  `STAGE.ADMIN.<TABELA>_<ini>_<fim>` → `/tmp/<TABELA>_<ini>_<fim>.dat`.
- Cadastro incompleto gera falha `CONFIG`, sem chamar o Netezza e sem retry.
- Atenção: a procedure faz `CREATE EXTERNAL TABLE` com nome fixo por tabela e período. Repetir um subbloco cuja
  external table já foi criada falha com "já existe". Mantenha "Repetir resultado desconhecido" desligado.

## Contrato pedido ao cliente (procedure)

- Assinatura: `(p_tabela varchar, p_data_ini date, p_data_fim date)`, com `p_data_fim` inclusiva. O app envia as datas
  como `'AAAA-MM-DD'`.
- Erro de negócio ou de execução: lançar exceção, que vira `FAILED` com a mensagem retornada.
- **Idempotente** por (tabela, ini, fim): reexecutar sobrescreve o mesmo destino, sem duplicar. Só com isso é
  seguro habilitar "Repetir resultado desconhecido" na tabela.
- Informar quantas chamadas simultâneas o Netezza suporta, para configurar o limite global.

## Estrutura

- `src/nzexport/`: núcleo com `schema.sql`, `planner.py` (subblocos), `procedure.py` (chamada via nzpy), `repo.py` e `dispatcher.py`
- `src/webapp/`: app Flask e templates Jinja
- `src/run_dispatcher.py`, `src/run_conn_test.py`: entrypoints dos jobs
- `scripts/`: migração local e smoke test
- `revisao_especificacao.md`: revisão crítica da especificação original
