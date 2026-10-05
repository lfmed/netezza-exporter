# Exportador Netezza (Databricks App + Job + Lakebase)

Este projeto é o plano de controle para tirar dados do Netezza em subblocos. Cada subbloco faz uma chamada de
`procedure(tabela, data_ini, data_fim)`, com `data_fim` **inclusiva**. Quem grava os arquivos é a procedure. O sistema só
precisa saber se ela terminou: retorno sem exceção significa concluído.

- **App**: https://netezza-exporter-7474646973581105.aws.databricksapps.com (workspace `fe-vm-stable-classic-pg4xe1`)
- **Lakebase**: projeto `netezza-exporter`, branch `production`, schema `nzexport`
- **Jobs**: `nzexport-dispatcher` (fila e chamadas da procedure) e `nzexport-hook-example` (exemplo de hook)

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

```bash
P=fe-vm-stable-classic-pg4xe1
databricks bundle deploy --profile $P            # app + jobs
databricks bundle run nzexport_app --profile $P  # publica o código do app
.venv/bin/python scripts/migrate_local.py --grant-role <sp_client_id>   # schema + grants (o job também aplica a cada run)
.venv/bin/python -m pytest -q tests              # testes do planner
.venv/bin/python scripts/smoke_local.py          # E2E local com procedure simulada
```

## Ligar no Netezza real (hoje a origem usa o driver `fake`)

1. Crie o secret scope com as chaves `host`, `port` (5480), `database`, `user` e `password`:
   ```bash
   databricks secrets create-scope netezza --profile $P
   databricks secrets put-secret netezza host --profile $P   # repita para port, database, user, password
   ```
   O identity que executa o Job precisa de `READ` no scope.
2. Configure o NCC do workspace serverless com a rota privada até o Netezza.
3. Em **Administração → Origens**, mude o driver para `nzpy`, informe o secret scope e ajuste o limite global.
4. Em cada tabela, informe o nome da procedure (ex.: `ADMIN.SP_EXPORTA_TABELA`) e o valor do parâmetro *tabela*.
   O timeout da chamada vira o timeout de socket do `nzpy`.

## Contrato pedido ao cliente (procedure)

- Assinatura: `(p_tabela varchar, p_data_ini date, p_data_fim date)`, com `p_data_fim` inclusiva. O app envia as datas
  como `'AAAA-MM-DD'`.
- Erro de negócio ou de execução: lançar exceção, que vira `FAILED` com a mensagem retornada.
- **Idempotente** por (tabela, ini, fim): reexecutar sobrescreve o mesmo destino, sem duplicar. Só com isso é
  seguro habilitar "Repetir resultado desconhecido" na tabela.
- Informar quantas chamadas simultâneas o Netezza suporta, para configurar o limite global.

## Estrutura

- `src/nzexport/`: núcleo com `schema.sql`, `planner.py` (subblocos), `procedure.py` (fake/nzpy), `repo.py` e `dispatcher.py`
- `src/webapp/`: app Flask e templates Jinja
- `src/run_dispatcher.py`, `src/hook_example.py`: entrypoints dos jobs
- `scripts/`: migração local e smoke test
- `revisao_especificacao.md`: revisão crítica da especificação original
