# Especificação de implementação — Exportador de tabelas Netezza para arquivos via Databricks

## 1. Objetivo

Construir um Databricks App para que usuários autorizados possam:

* selecionar várias tabelas fato do Netezza;
* informar, para cada tabela, o período total de dados a exportar;
* informar o período de cada subbloco de dados, que será a unidade de execução e checkpoint;
* configurar uma duração máxima ou janela operacional para a execução;
* iniciar a geração de arquivos em um Databricks Job;
* acompanhar o progresso por tabela e por bloco de processamento;
* cancelar uma execução com segurança;
* retomar uma execução cancelada, interrompida ou que tenha falhado;
* consultar o histórico, os arquivos gerados, os erros e as métricas da execução;
* acessar um dashboard interno para acompanhar a periodicidade das cargas e impedir a geração de arquivos duplicados.

O produto não deve executar chamadas longas ao Netezza dentro do processo web do app. O app deve funcionar como plano de controle. O Databricks Job deve executar um worker Python que se conecta ao Netezza usando `nzpy` e chama uma procedure previamente homologada. A procedure no Netezza será responsável pela geração dos arquivos e/ou pela execução da lógica de exportação definida pelo cliente.

## 2. Validação do entendimento

A necessidade é tecnicamente coerente com Databricks, desde que o requisito de “retomar a qualquer momento” seja interpretado como retomada a partir do último subbloco confirmado, e não como continuação exata de uma chamada `nzpy` ou de uma procedure que foi interrompida no meio da execução.

Lakeflow Jobs oferece tarefas, parâmetros, dependências, execução paralela, loops `For each`, retries, monitoramento e APIs para disparar, consultar e cancelar runs. Isso atende ao papel de orquestração do projeto ([Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs), [controle de fluxo](https://docs.databricks.com/aws/en/jobs/control-flow)).

Para tabelas muito grandes, o controle de volume e concorrência deve ser feito pela procedure no Netezza, usando o período total e o período de cada subbloco. O Databricks Job deve controlar a quantidade de chamadas simultâneas, timeouts, retries e reconciliação de chamadas cujo resultado não seja conhecido.

O Databricks App é adequado para a interface, configuração e acompanhamento, mas seu estado não deve depender da memória do processo. O estado persistente deve ficar em tabelas Databricks e/ou Lakebase, e os dados devem ser processados por Jobs ([Key concepts in Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/key-concepts), [Best practices for Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)).

## 3. Decisões recomendadas

### 3.1 Arquitetura principal

A execução deve ser orientada pela procedure do Netezza. O Databricks não deve reimplementar a lógica de extração nem ler a tabela fato diretamente, salvo se isso for definido como fallback futuro.

Adotar os seguintes componentes:

* Databricks App: interface web, validação dos pedidos, disparo de Jobs, comandos de cancelamento/retomada, consulta de status e dashboard interno de cargas.
* Tabelas de controle em Unity Catalog: pedidos, tabelas selecionadas, blocos, estados, eventos, manifestos e erros.
* Lakeflow Job de controle: recebe o identificador do pedido e distribui o trabalho.
* Tarefa de planejamento: valida a configuração, calcula blocos, registra o plano e identifica blocos já concluídos.
* Tarefas de execução: chamam a procedure do Netezza via `nzpy`, passando o pedido e o subbloco correspondente.
* Tarefa de reconciliação: consulta o resultado da procedure, atualiza o estado do subbloco e identifica chamadas interrompidas ou órfãs.
* Tarefa de publicação: valida os resultados informados pelo Netezza, grava o manifesto e publica o resultado como disponível para download/consumo.
* Object storage governado por Unity Catalog Volumes ou external location: destino dos arquivos.
* Segredos e configuração de conexão: credenciais, biblioteca `nzpy`, endpoint, certificado e parâmetros de rede.

Databricks Apps suporta recursos como Jobs, secrets, volumes e Lakebase, além de autorização por identidade do app ou do usuário. A implementação deve usar apenas os recursos necessários e aplicar menor privilégio ([Key concepts in Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/key-concepts), [Configure authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)).

### 3.2 Modelo de execução

Não criar dinamicamente um Job diferente para cada tabela. Usar um Job reutilizável, orientado por metadados:

1. O app grava o pedido e suas tabelas em tabelas de controle.
2. O app dispara um único Job com `request_id`.
3. O Job lê a configuração persistida.
4. O Job gera ou reutiliza os subblocos de cada tabela.
5. Um `For each` ou mecanismo equivalente executa as chamadas da procedure com concorrência limitada.
6. Cada subbloco confirma o resultado da procedure de forma idempotente ou reconciliável.
7. A publicação só ocorre depois de todos os subblocos obrigatórios concluídos.

O padrão metadata-driven evita hardcode do conjunto de tabelas e permite adicionar ou retirar tabelas sem alterar a definição do Job ([Use a control table to drive a For each job](https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial)).

### 3.3 Período total e subblocos de dados

O cliente define explicitamente dois níveis de período:

* **período total**: intervalo completo que deve ser exportado para a tabela;
* **período do subbloco**: duração de cada unidade de dados que será executada, acompanhada, cancelada e retomada individualmente.

O sistema deve materializar os subblocos dentro do período total. Por exemplo, para um período total de janeiro a dezembro e subblocos mensais, serão criados doze subblocos. O último subbloco deve ser truncado no limite final do período total quando o intervalo não for múltiplo exato da duração configurada.

O subbloco é a unidade mínima de controle funcional e operacional. Para cada subbloco, o sistema deve saber:

* se está pendente, em execução, concluído, cancelado ou com erro;
* qual Job/task o está processando;
* qual intervalo de dados ele representa;
* quais arquivos foram produzidos;
* quantas linhas e bytes foram processados;
* se pode ser retomado sem repetir subblocos concluídos.

A granularidade do subbloco deve ser configurável por tabela, pois fatos diferentes terão volume, distribuição e coluna de data diferentes. O sistema não deve substituir silenciosamente o período informado pelo cliente; pode apenas rejeitar valores inválidos ou sugerir uma configuração mais adequada antes da confirmação.

Recomendação inicial:

* passar o período do subbloco para a procedure;
* garantir que a procedure aplique o filtro no Netezza;
* evitar que o Job envie intervalos sobrepostos;
* evitar uma única chamada para todo o período total histórico, salvo quando o cliente aprovar essa exceção.

## 4. Escopo funcional

### 4.1 Cadastro e metadados de tabelas

O sistema deve manter um catálogo controlado das tabelas exportáveis. Para cada tabela:

* identificador lógico;
* banco e schema Netezza;
* nome físico validado;
* coluna de filtro de negócio;
* tipo da coluna de filtro;
* parâmetros e versão da procedure;
* estratégia de execução síncrona ou assíncrona;
* periodicidade mínima do subbloco;
* limite recomendado de linhas ou tamanho;
* projeção de colunas permitidas;
* formato de saída permitido;
* caminho raiz de saída;
* política de retenção;
* periodicidade esperada da carga;
* tolerância de atraso;
* política de sobreposição de períodos;
* política de duplicidade de arquivos;
* status ativo/inativo;
* regras de autorização.

O usuário não deve fornecer SQL livre como mecanismo principal. A aplicação deve montar a consulta a partir de metadados confiáveis e parâmetros validados. Se houver necessidade de filtros adicionais, eles devem ser modelados como campos tipados e aprovados por tabela.

### 4.2 Criação de um pedido

O usuário deve poder criar um pedido com:

* nome ou descrição;
* uma ou mais tabelas;
* filtro específico para cada tabela;
* início e fim do período total por tabela;
* período/duração de cada subbloco por tabela;
* inclusão ou exclusão do limite final, definida de forma consistente;
* opção de gerar subblocos alinhados ao calendário ou a partir do início informado;
* formato: preferencialmente Parquet; CSV apenas quando houver requisito explícito;
* compressão;
* caminho de destino;
* limite de paralelismo solicitado, sujeito a limites administrativos;
* prazo máximo ou janela operacional;
* política de sobrescrita ou criação de uma nova versão;
* opção de validação completa ou validação amostral;
* confirmação de que o pedido pode gerar grande volume de dados.

O pedido deve ser validado antes de disparar o Job:

* tabela pertence ao catálogo permitido;
* o período não se sobrepõe a uma carga concluída ou em execução;
* o subbloco não possui arquivo ou manifesto válido já publicado;
* a periodicidade da tabela permite a carga solicitada;
* usuário tem permissão para solicitar a tabela;
* coluna de filtro é a configurada para a tabela;
* intervalo é válido;
* início é menor que fim;
* formato e caminho são permitidos;
* volume estimado não ultrapassa o limite configurado;
* não existe pedido conflitante, ou o conflito foi explicitamente autorizado;
* há conectividade com Netezza e espaço disponível no destino.

### 4.3 Acompanhamento

A tela de acompanhamento deve mostrar:

* status geral do pedido;
* percentual por subblocos concluídos, não apenas percentual de tempo;
* tabelas em execução, concluídas, pendentes e com erro;
* intervalo solicitado por tabela;
* número estimado e efetivo de linhas;
* tamanho dos arquivos;
* quantidade de arquivos;
* início, última atualização e duração;
* Job run ID e task run ID, quando disponíveis;
* mensagens de erro resumidas;
* caminho do manifesto e dos arquivos publicados;
* timestamps do último heartbeat;
* indicação de que o progresso é estimado quando o volume total ainda não é conhecido.

O app deve consultar as tabelas de controle como fonte primária do progresso e usar a Jobs API para enriquecer com o estado operacional do Job. Não depender somente da interface nativa do Databricks Jobs para a experiência do usuário.

### 4.4 Dashboard interno de cargas e periodicidade

O app deve possuir uma área interna, restrita a usuários operacionais e administradores, para acompanhar a saúde das cargas por tabela. O dashboard deve permitir filtrar por tabela, status, período, cliente, origem e atraso.

Indicadores mínimos por tabela:

* periodicidade configurada;
* última carga solicitada;
* última carga iniciada;
* última carga concluída com sucesso;
* último período total processado;
* último subbloco concluído;
* próximo período esperado;
* atraso em relação à periodicidade;
* quantidade de pedidos sobrepostos;
* quantidade de subblocos duplicados ou rejeitados;
* arquivos publicados por período;
* status da última procedure;
* falhas e retries recentes;
* data da última validação de duplicidade.

O dashboard deve destacar visualmente:

* tabela dentro da periodicidade esperada;
* tabela atrasada;
* tabela com pedido conflitante;
* tabela com intervalo já processado;
* tabela com arquivo potencialmente duplicado;
* tabela com procedure em estado desconhecido;
* tabela com falhas recorrentes.

### 4.5 Validação de periodicidade e duplicidade

Antes de disparar o Job, o app deve validar cada tabela individualmente. A validação deve:

1. localizar pedidos e subblocos já concluídos para a mesma tabela;
2. comparar o período solicitado com os períodos já processados;
3. detectar interseção total ou parcial entre intervalos;
4. verificar se já existe arquivo ou manifesto para a mesma chave de negócio;
5. verificar se existe outra execução ativa para a mesma tabela e intervalo;
6. validar se o intervalo respeita a periodicidade esperada;
7. bloquear automaticamente duplicidades, salvo quando o usuário selecionar explicitamente “reprocessar”;
8. exigir justificativa e gerar uma nova versão quando houver reprocessamento autorizado.

A chave de unicidade recomendada deve combinar, no mínimo:

* tabela lógica;
* cliente ou contexto de execução;
* período total;
* subbloco;
* versão da configuração da procedure;
* formato e compressão;
* versão do reprocessamento.

A periodicidade não deve ser inferida apenas pela data de criação do pedido. Deve ser calculada a partir do período de dados processado e da configuração da tabela.

### 4.6 Cancelamento

O botão Cancelar deve:

1. registrar uma intenção de cancelamento na tabela do pedido;
2. impedir que novos blocos sejam iniciados;
3. permitir que a tarefa atual observe o sinal entre etapas seguras;
4. solicitar o cancelamento do run ou da task no Databricks;
5. esperar a confirmação do estado terminal;
6. marcar blocos não iniciados como cancelados ou pendentes, conforme a política escolhida;
7. deixar claro que uma tarefa já executando no Netezza pode demorar até o timeout ou até o driver aceitar o cancelamento;
8. preservar blocos confirmados e seus manifestos temporários para a retomada.

O cancelamento deve ser eventual, não síncrono. O app deve exibir “cancelamento solicitado” antes de exibir “cancelado”. A Jobs API suporta consultar e cancelar runs, mas a mudança de estado ocorre de forma assíncrona ([Use Databricks data on Microsoft Power Platform](https://docs.databricks.com/aws/en/integrations/msft-power-platform/usage), [Configure and edit Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/configure-job)).

### 4.7 Retomada

A retomada deve aceitar três cenários:

* retomar somente subblocos pendentes;
* repetir subblocos com erro;
* criar uma nova tentativa completa, preservando a versão anterior para auditoria.

A retomada nunca deve sobrescrever silenciosamente um arquivo parcialmente escrito. Cada subbloco deve ser tratado pela procedure com uma chave idempotente e publicado somente após a confirmação do resultado. Se já houver publicação válida para a mesma chave, o subbloco deve ser ignorado ou validado, não duplicado.

A retomada deve manter o mesmo `request_id` e incrementar `attempt_number`, ou criar um `resume_id` ligado ao pedido original. A decisão deve ser uniforme em todo o produto.

## 5. Máquina de estados

### 5.1 Pedido

Estados recomendados:

* `DRAFT`: configuração em edição.
* `VALIDATING`: validações funcionais e técnicas em andamento.
* `READY`: pronto para execução.
* `QUEUED`: enviado para o Job, aguardando capacidade.
* `RUNNING`: pelo menos um bloco em execução.
* `PAUSE_REQUESTED`: usuário solicitou pausa; novos blocos não iniciam.
* `PAUSED`: execução interrompida em um ponto seguro.
* `CANCEL_REQUESTED`: cancelamento solicitado.
* `CANCELED`: nenhum processamento continuará sem ação explícita do usuário.
* `FAILED`: falha que exige intervenção ou retomada.
* `PUBLISHING`: validação e publicação do resultado.
* `COMPLETED`: todos os blocos válidos e manifesto publicado.
* `PARTIALLY_COMPLETED`: alguns resultados foram preservados, mas o pedido não foi concluído.
* `EXPIRED`: janela ou retenção do pedido expirou.

### 5.2 Subbloco

Estados recomendados:

* `PLANNED`
* `CLAIMED`
* `RUNNING`
* `SUCCEEDED_TEMPORARY`
* `PUBLISHED`
* `FAILED_RETRYABLE`
* `FAILED_FINAL`
* `CANCEL_REQUESTED`
* `CANCELED`
* `SKIPPED_ALREADY_PUBLISHED`

Toda transição deve registrar ator, timestamp, tentativa, mensagem e origem da transição. Atualizações de estado devem ser condicionais para evitar que dois workers processem o mesmo bloco.

## 6. Modelo de dados lógico

### 6.1 `export_table_config`

Configuração governada das tabelas:

* `table_config_id`
* `source_system`
* `source_database`
* `source_schema`
* `source_table`
* `business_filter_column`
* `business_filter_type`
* `procedure_name`
* `procedure_version`
* `procedure_execution_mode`
* `default_subblock_period`
* `procedure_timeout_minutes`
* `max_parallel_blocks`
* `expected_periodicity`
* `periodicity_unit`
* `periodicity_tolerance`
* `overlap_policy`
* `duplicate_file_policy`
* `allowed_formats`
* `output_root`
* `active`
* `config_version`
* `created_at`
* `updated_at`

### 6.2 `export_request`

Cabeçalho do pedido:

* `request_id`
* `request_name`
* `requested_by`
* `requested_at`
* `status`
* `requested_format`
* `compression`
* `output_root`
* `max_duration_minutes`
* `requested_parallelism`
* `job_id`
* `run_id`
* `attempt_number`
* `cancel_requested_at`
* `pause_requested_at`
* `started_at`
* `completed_at`
* `last_heartbeat_at`
* `error_summary`
* `created_at`
* `updated_at`

### 6.3 `export_request_table`

Configuração específica de cada tabela no pedido:

* `request_id`
* `request_table_id`
* `table_config_id`
* `source_table_snapshot`
* `filter_start`
* `filter_end`
* `filter_end_inclusive`
* `block_granularity`
* `estimated_rows`
* `estimated_bytes`
* `status`
* `published_path`
* `published_manifest_path`
* `last_successful_period_start`
* `last_successful_period_end`
* `next_expected_period_start`
* `next_expected_period_end`
* `periodicity_status`
* `overlap_status`
* `duplicate_status`

### 6.4 `export_subblock`

Unidade de execução, acompanhamento e retomada. O nome anterior `export_block` pode ser usado apenas como nome técnico legado, mas o conceito funcional deve ser sempre chamado de subbloco.

Cada subbloco deve representar uma interseção entre o período total solicitado e o período configurado pelo cliente.

Campos principais:

* `request_id`
* `request_table_id`
* `subblock_id`
* `subblock_sequence`
* `total_period_start`
* `total_period_end`
* `subblock_period_start`
* `subblock_period_end`
* `partition_lower_bound`
* `partition_upper_bound`
* `status`
* `attempt_number`
* `worker_run_id`
* `temporary_path`
* `final_path`
* `row_count`
* `file_count`
* `bytes_written`
* `checksum`, se aplicável
* `started_at`
* `finished_at`
* `last_heartbeat_at`
* `error_code`
* `error_message`
* `duplicate_check_status`
* `duplicate_check_reason`
* `duplicate_check_at`

### 6.5 `export_event`

Auditoria de comandos e transições:

* `event_id`
* `request_id`
* `request_table_id`
* `subblock_id`
* `event_type`
* `previous_state`
* `new_state`
* `actor_type`
* `actor_id`
* `event_time`
* `details`

### 6.6 `export_manifest`

Contrato de saída:

* `request_id`
* `request_table_id`
* `source_table`
* `filter_start`
* `filter_end`
* `source_schema_snapshot`
* `output_format`
* `compression`
* `total_rows`
* `total_bytes`
* `file_count`
* `files`
* `created_at`
* `validation_status`
* `manifest_version`
* `uniqueness_key`
* `periodicity_validation_status`
* `overlap_validation_status`

## 7. Desenho do Job Databricks

### 7.1 Tarefas lógicas

O Job deve conter, conceitualmente, as seguintes etapas:

1. **Load request**: carregar o pedido pelo `request_id` e validar que ele pode executar.
2. **Plan subblocks**: materializar os subblocos a partir do período total e do período de subbloco informado pelo cliente, reutilizando os subblocos já publicados quando a retomada for solicitada.
3. **Estimate and guardrails**: calcular ou validar estimativas por subbloco e aplicar limites de volume, concorrência e duração.
4. **Execute subblocks**: abrir conexão com o Netezza via `nzpy` e chamar a procedure para cada subbloco, respeitando a concorrência configurada.
5. **Reconcile subblocks**: confirmar o retorno da procedure, registrar arquivos, contagens, bytes, identificadores de execução e mensagens de erro.
6. **Validate outputs**: validar os resultados informados pela procedure, ausência de arquivos incompletos e registro de métricas.
7. **Publish manifest**: publicar apenas quando os subblocos necessários estiverem válidos.
8. **Finalize request**: atualizar o estado final e registrar o resumo operacional.

As tarefas podem ser notebooks, Python scripts ou Python wheel; a escolha deve privilegiar empacotamento testável e reutilizável. Jobs suportam essas modalidades e controle de fluxo por dependências, `For each`, condicionais e retries ([Configure and edit tasks](https://docs.databricks.com/aws/en/jobs/configure-task)).

### 7.2 Fan-out por tabela e por subbloco

A unidade natural de fan-out deve ser o subbloco definido para cada tabela. Uma tabela fato de centenas de milhões de linhas pode exigir muitos subblocos, enquanto outra pode terminar em um único subbloco. O período total e o período de subbloco podem ser diferentes para cada tabela do mesmo pedido.

Estratégia recomendada:

* fan-out lógico pelos subblocos planejados de cada tabela;
* cada subbloco representa um intervalo explícito dentro do período total;
* limite global de workers;
* limite específico por tabela;
* pesos ou prioridades para evitar que várias tabelas muito grandes sobrecarreguem o Netezza;
* filas ou grupos de capacidade para pedidos concorrentes.

Não criar uma task permanente por subbloco no Job se o número puder crescer muito. Usar um Job estável que leia a fila de subblocos ou um `For each` controlado por tabela de lookup. O Job tem limites de tarefas, concorrência e chamadas de API que devem ser respeitados ([Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs), [Resource limits](https://docs.databricks.com/aws/en/resources/limits)).

### 7.3 Idempotência

A chamada da procedure deve ser repetível ou reconciliável para a mesma combinação de:

* tabela;
* período total do pedido;
* subbloco;
* versão do schema;
* versão da configuração;
* formato e compressão.

Antes de considerar o subbloco concluído, o worker deve verificar o status retornado ou persistido pela procedure. A publicação final deve ocorrer somente após a validação registrar o subbloco como concluído.

## 8. Execução da procedure no Netezza via `nzpy`

### 8.1 Estratégia padrão

A estratégia padrão será executar uma procedure no Netezza a partir de uma tarefa Python do Databricks Job. O Job não fará a leitura direta da tabela fato; ele controlará as chamadas da procedure por subbloco.

Para cada tabela, a especificação deve registrar:

* biblioteca `nzpy` homologada;
* versão da biblioteca e do Python;
* endpoint, banco e schema Netezza;
* modo de autenticação;
* nome e versão da procedure;
* assinatura, parâmetros e tipos da procedure;
* modo síncrono ou assíncrono;
* identificador idempotente da chamada;
* número máximo de chamadas simultâneas;
* timeout de conexão e execução;
* comportamento de retry;
* comportamento de cancelamento;
* mecanismo de consulta de status;
* tabela ou mecanismo de auditoria no Netezza.

### 8.2 Contrato obrigatório da procedure

A procedure deve possuir um contrato operacional explícito. No mínimo, deve receber ou derivar:

* `request_id`;
* `subblock_id`;
* nome lógico da tabela autorizada;
* período total solicitado;
* início e fim do subbloco;
* formato e compressão de saída;
* destino ou identificador lógico do destino;
* número da tentativa;
* modo de execução: iniciar, consultar, cancelar ou reconciliar.

A procedure deve retornar ou persistir:

* identificador da execução no Netezza;
* status: recebido, executando, concluído, falho ou cancelado;
* timestamp de início e fim;
* contagem de linhas;
* quantidade de arquivos;
* bytes gerados;
* caminhos ou identificadores dos arquivos;
* mensagem e código de erro;
* informação suficiente para consulta posterior após a conexão `nzpy` ser encerrada.

Se a procedure for síncrona e permanecer aberta até terminar, o Job deve controlar timeout, retry e reconciliação de conexão perdida. Se for assíncrona, o Job deve separar a operação em duas etapas: solicitar a execução e consultar o status até um estado terminal.

### 8.3 Idempotência e reconciliação

Cada chamada deve usar uma chave idempotente formada, no mínimo, por pedido, tabela, subbloco e tentativa. Antes de iniciar uma nova chamada, o worker deve consultar o status persistido no Netezza ou na tabela de controle.

Se a conexão `nzpy` cair depois que a procedure foi aceita, o sistema não deve simplesmente repetir a chamada. Primeiro deve consultar o identificador da execução ou a chave idempotente para descobrir se o Netezza já iniciou ou concluiu o subbloco.

Uma chamada só deve ser marcada como definitivamente falha quando a reconciliação confirmar que ela não foi aceita, foi rejeitada ou terminou com erro. Caso contrário, deve ficar em `RECONCILIATION_PENDING` até a situação ser resolvida.

### 8.4 Data e tempo

Cada tabela deve ter seu próprio filtro. O contrato deve definir explicitamente:

* timezone de interpretação;
* formato de data/hora;
* se o limite final é inclusivo ou exclusivo;
* comportamento para valores NULL;
* tratamento de registros fora do intervalo;
* estabilidade da fonte durante a extração.

Recomendar intervalo semiaberto `[início, fim)`, pois reduz sobreposição entre subblocos consecutivos. A implementação deve confirmar com o cliente se isso é compatível com o padrão de negócio.

### 8.3 Consistência da origem

O sistema deve registrar a expectativa de consistência:

* snapshot lógico do Netezza, se disponível e viável;
* execução da procedure baseada em timestamp, aceitando que a origem continue mudando;
* janela de congelamento operacional;
* tolerância para registros inseridos ou alterados durante a execução.

Sem uma estratégia de snapshot ou watermark, duas tabelas ou dois blocos podem representar momentos diferentes. Isso deve ser documentado como propriedade do produto, não escondido como detalhe de implementação.

### 8.4 Formato de saída

Recomendação:

* Parquet como formato padrão para volume grande e consumo analítico;
* CSV somente por compatibilidade externa;
* compressão configurável, com padrão apropriado ao formato;
* múltiplos arquivos por tabela e subbloco, acompanhados de manifesto;
* nunca prometer um único arquivo físico para uma tabela muito grande sem validar limites de tamanho, tempo e memória.

O usuário deve consumir o manifesto, não inferir os arquivos apenas por listagem de diretório.

## 9. Pausa, cancelamento e retomada: semântica detalhada

A aplicação deve distinguir quatro operações:

* **Pausar**: não iniciar novos subblocos; permitir que a chamada da procedure atual finalize ou seja cancelada conforme política.
* **Cancelar**: encerrar a execução e deixar o pedido em estado terminal, preservando resultados válidos.
* **Retomar**: continuar os subblocos pendentes ou falhos a partir dos checkpoints.
* **Reprocessar**: invalidar deliberadamente os resultados selecionados e executar novamente.

A pausa não deve ser implementada como suspensão arbitrária do cluster ou da conexão JDBC. A unidade segura de pausa é o subbloco. A UI deve mostrar explicitamente qual subbloco está em execução e quais ainda estão pendentes.

Cada worker deve consultar o sinal de controle:

* antes de iniciar um subbloco;
* antes de chamar a procedure;
* durante a consulta de status, quando a procedure for assíncrona;
* antes de publicar;
* em intervalos de heartbeat.

Se o worker perder o heartbeat, um reconciliador deve classificar o subbloco como potencialmente órfão e consultar o status da procedure antes de liberá-lo para retomada. O timeout deve evitar duas chamadas para o mesmo subbloco simultaneamente.

## 10. App e experiência do usuário

### 10.1 Telas

* **Catálogo de tabelas**: lista de tabelas permitidas, coluna de filtro, granularidade padrão e limites.
* **Novo pedido**: seleção de tabelas, filtros por tabela, formato, destino e parâmetros operacionais.
* **Revisão**: resumo dos intervalos, estimativas, riscos, custo e confirmação.
* **Execuções**: histórico, status, filtros e ordenação.
* **Detalhe da execução**: progresso geral, progresso por tabela, blocos, logs resumidos, arquivos e ações.
* **Administração**: configurações de conexão lógica, limites, tabelas, grupos autorizados e retenção.

### 10.2 Ações condicionais

* `Iniciar`: apenas para pedidos válidos e ainda não executados.
* `Pausar`: somente enquanto houver processamento ativo.
* `Cancelar`: enquanto o pedido não estiver em estado terminal.
* `Retomar`: para estados pausados, cancelados, parcialmente concluídos ou falhos.
* `Reprocessar`: ação mais restrita, exigindo confirmação.
* `Baixar manifesto`: quando houver manifesto publicado.
* `Copiar caminho`: quando o usuário tiver autorização para o destino.

### 10.3 Atualização de status

Preferir atualização periódica curta ou mecanismo de eventos quando disponível. A UI deve suportar atraso e inconsistência temporária entre o estado do Job e o estado das tabelas de controle.

## 11. Segurança e governança

### 11.1 Identidade

Escolher explicitamente entre:

* identidade do app/service principal, quando todos os usuários devem operar com o mesmo conjunto de permissões; ou
* autorização em nome do usuário, quando o acesso às tabelas, ao Job ou aos destinos deve respeitar as permissões individuais.

Para dados de clientes, a recomendação é preferir autorização por usuário quando houver necessidade de segregação por pessoa ou grupo. Para um serviço operacional centralizado, usar service principal dedicado com permissões mínimas e trilha de auditoria do usuário solicitante.

### 11.2 Segredos e Netezza

* não armazenar senha no código, parâmetros do Job, logs ou tabela de pedido;
* usar secret scope ou conexão JDBC governada, de acordo com os recursos disponíveis no ambiente;
* mascarar URL, usuário sensível e mensagens de erro;
* restringir conectividade de rede ao endpoint Netezza;
* homologar TLS/certificados;
* registrar somente identificadores não sensíveis;
* estabelecer rotação de credenciais.

### 11.3 Controle de SQL

* não aceitar nome de tabela, schema ou coluna arbitrário sem validação contra catálogo;
* não concatenar filtros livres recebidos da interface;
* manter allowlist de tabelas e colunas;
* validar tipo, formato e faixa dos parâmetros;
* limitar colunas selecionáveis;
* aplicar limites de volume e duração;
* registrar a configuração efetiva usada na execução.

### 11.4 Auditoria

Auditar:

* quem criou o pedido;
* quem iniciou, pausou, cancelou, retomou ou reprocessou;
* qual configuração de tabela foi usada;
* qual Job/run processou o dado;
* quais arquivos foram publicados;
* quais permissões foram avaliadas;
* quais erros ocorreram;
* quando o resultado foi expirado ou removido.

## 12. Observabilidade e operação

### 12.1 Métricas por pedido e bloco

* duração total;
* duração de planejamento;
* duração da chamada da procedure;
* duração da reconciliação;
* linhas reportadas pela procedure;
* bytes e arquivos reportados pela procedure;
* arquivos gerados;
* taxa de transferência;
* número de retries;
* chamadas simultâneas máximas ao Netezza;
* falhas por código;
* blocos órfãos;
* tempo em fila;
* DBUs/custo aproximado;
* atraso desde o último heartbeat.

### 12.2 Alertas

* pedido acima da duração esperada;
* falhas repetidas da mesma tabela;
* taxa de erro de conexão;
* atraso da tabela em relação à periodicidade;
* tentativa de intervalo sobreposto;
* tentativa de publicação duplicada;
* volume acima da estimativa;
* número de conexões próximo do limite;
* blocos órfãos;
* falta de espaço no destino;
* Job em fila por tempo excessivo;
* cancelamento que não chega a estado terminal;
* divergência entre contagem esperada e publicada.

Lakeflow Jobs fornece histórico de runs, status, logs, métricas, notificações e tabelas de sistema para análise operacional e de custo ([Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs), [Monitor Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/monitor)). O produto deve complementar isso com métricas de domínio persistidas nas tabelas de controle.

## 13. Computação

O processamento pesado deve ocorrer em Job compute, não no App. Serverless Jobs pode ser avaliado para as tarefas compatíveis, mas a decisão final depende de conectividade com o Netezza, instalação do driver, requisitos de rede, capacidade de memória e comportamento do JDBC. A documentação recomenda serverless para tarefas suportadas, mas também registra limitações e requisitos de compatibilidade ([Run Lakeflow Jobs with serverless compute](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs), [Configure compute for jobs](https://docs.databricks.com/aws/en/jobs/compute)).

O sizing deve ser validado por benchmark com pelo menos:

* uma tabela pequena;
* uma tabela média;
* uma tabela muito grande;
* tabela larga com strings grandes;
* tabela com distribuição de dados desigual;
* janela com muitos blocos;
* dois pedidos concorrentes.

A concorrência deve ser controlada em dois níveis:

* concorrência de subblocos no Databricks;
* concorrência de chamadas de procedures no Netezza.

## 14. Tratamento de erros

Classificar erros em:

* **Validação**: configuração inválida, permissão ou intervalo incorreto; não fazer retry automático.
* **Conectividade transitória**: timeout, conexão recusada ou perda de rede; retry limitado com backoff.
* **Execução da procedure**: erro síncrono, status desconhecido ou falha no mecanismo assíncrono; reconciliar antes de repetir.
* **Capacidade da origem**: excesso de sessões, fila ou degradação do Netezza; reduzir concorrência e alertar.
* **Erro de dados**: conversão de tipo, valor inválido ou schema incompatível; bloquear o bloco e solicitar correção.
* **Destino**: falta de espaço, permissão ou erro de escrita; retry apenas quando for transitório.
* **Controle**: conflito de estado, lease expirado ou pedido duplicado; reconciliar de forma idempotente.
* **Periodicidade/duplicidade**: intervalo sobreposto, arquivo já publicado ou periodicidade violada; bloquear ou encaminhar para reprocessamento autorizado.
* **Cancelamento**: tratar como estado operacional, não como falha inesperada.

Cada retry deve criar ou atualizar uma tentativa rastreável, sem apagar a evidência da tentativa anterior.

## 15. Plano de implementação

### Fase 0 — Descoberta e decisões bloqueadoras

* confirmar cloud, região e workspace;
* confirmar se o cliente está em AWS, Azure ou GCP;
* confirmar se Databricks Apps está habilitado;
* confirmar versão e distribuição do Netezza;
* homologar `nzpy`, versão do Python e comportamento de conexão;
* documentar a assinatura das procedures;
* validar rede, DNS, firewall, TLS e latência;
* inventariar tabelas fato e suas colunas de filtro;
* medir volume, cardinalidade, distribuição e crescimento;
* definir formato de saída e consumidores;
* decidir identidade do app e modelo de autorização;
* definir política de consistência da origem;
* estabelecer limites de concorrência e janela operacional.

Saída: documento de decisões arquiteturais, matriz de tabelas e benchmark de conectividade.

### Fase 1 — Fundação de governança e controle

* criar catálogo de tabelas exportáveis;
* definir periodicidade esperada, tolerância e política de duplicidade por tabela;
* criar tabelas de pedidos, tabelas, blocos, eventos e manifestos;
* definir estados e transições;
* configurar segredos/conexão JDBC;
* configurar destinos governados;
* definir permissões de usuários, grupos, app e Jobs;
* estabelecer retenção e auditoria.

Saída: modelo de controle operacional validado sem extração de grande volume.

### Fase 2 — Extração de uma tabela piloto

* selecionar uma tabela representativa;
* implementar planejamento de intervalo e blocos;
* validar que o período total e o período do subbloco chegam corretamente à procedure;
* validar execução síncrona ou assíncrona da procedure;
* validar retorno de status, contagem, tamanho e caminhos;
* publicar manifesto;
* medir linhas, bytes, arquivos e duração;
* testar retries e idempotência;
* testar cancelamento durante conexão, execução da procedure, consulta de status e publicação.

Saída: fluxo de ponta a ponta com uma tabela e retomada comprovada.

### Fase 3 — Job parametrizado e fan-out

* transformar o fluxo em Job reutilizável por `request_id`;
* implementar fan-out por blocos;
* impor limites de concorrência;
* adicionar heartbeat e lease de bloco;
* implementar reconciliador de blocos órfãos;
* implementar retries classificados;
* implementar publicação atômica do manifesto;
* integrar métricas de Job com métricas de domínio.

Saída: Job capaz de processar várias tabelas com filtros diferentes.

### Fase 4 — Databricks App

* construir catálogo e formulário de pedido;
* construir dashboard interno de cargas e periodicidade;
* validar entrada contra metadados e períodos já processados;
* criar tela de revisão e estimativa;
* disparar Job;
* consultar status e histórico;
* implementar pausa, cancelamento, retomada e reprocessamento;
* exibir progresso por tabela e bloco;
* exibir manifesto, caminhos e erros;
* aplicar autorização e auditoria.

Saída: aplicação utilizável por usuários autorizados.

### Fase 5 — Escala e hardening

* testar tabelas muito grandes;
* testar múltiplos pedidos concorrentes;
* testar falha de driver, rede, cluster e destino;
* testar reinício do App sem perda de estado;
* testar duplicidade de cliques e chamadas concorrentes;
* testar mudança de schema;
* testar retenção e limpeza;
* configurar alertas, dashboards e runbooks;
* executar revisão de segurança;
* documentar operação e suporte.

Saída: release candidata para produção.

### Fase 6 — Rollout controlado

* liberar para grupo piloto;
* processar tabelas de baixo risco;
* revisar métricas e incidentes;
* ajustar granularidade e concorrência por tabela;
* liberar tabelas de maior volume gradualmente;
* formalizar SLOs e responsabilidades;
* congelar contratos de metadados e manifestos.

## 16. Critérios de aceite

### Funcionais

* usuário autorizado consegue criar um pedido com duas ou mais tabelas e filtros de data diferentes;
* cada tabela gera seus próprios blocos e arquivos;
* o pedido pode ser disparado sem editar a definição do Job;
* o progresso aparece por tabela e por bloco;
* o usuário consegue solicitar pausa/cancelamento;
* a tela identifica exatamente o subbloco em execução e os subblocos restantes;
* o pedido pode ser retomado sem duplicar subblocos já publicados;
* o manifesto lista todos os arquivos publicados;
* o histórico preserva pedidos, tentativas, comandos e erros.

### Técnicos

* a app pode reiniciar sem perder pedidos ou progresso;
* o Job pode falhar e ser retomado a partir do último checkpoint válido;
* o worker não processa o mesmo bloco simultaneamente em duas tentativas;
* a procedure recebe e aplica corretamente o período do subbloco;
* chamadas simultâneas ao Netezza ficam abaixo do limite configurado;
* a validação de periodicidade é determinística e auditável;
* a validação de sobreposição cobre interseções totais e parciais;
* a chave de unicidade impede a publicação duplicada do mesmo subbloco;
* chamadas perdidas podem ser reconciliadas sem duplicação;
* credenciais não aparecem em código, parâmetros ou logs;
* arquivos temporários incompletos não são publicados como finais;
* schema, contagem e tamanho são registrados por bloco e por tabela;
* timeout e retry possuem comportamento previsível;
* os limites de Jobs e APIs são respeitados.

### Operacionais

* existe runbook para falha de conexão Netezza;
* existe runbook para bloco órfão;
* existe runbook para falta de espaço;
* existe runbook para divergência de contagem;
* existem alertas de duração, erro e capacidade;
* há visibilidade do custo aproximado por pedido;
* há trilha de auditoria suficiente para investigar um pedido.

## 17. Riscos e mitigação

| Risco | Impacto | Mitigação |
|---|---|---|
| Netezza não suporta a concorrência planejada | Degradação ou indisponibilidade da origem | Limite global por origem, limite por tabela, benchmark e circuit breaker |
| Filtro de data não é seletivo | Extração lenta e alto custo | Estatísticas, plano de execução, particionamento e revisão de modelo |
| Coluna de particionamento é enviesada | Blocos desbalanceados | Profiling, predicados customizados e granularidade adaptativa |
| Cancelamento não interrompe imediatamente a consulta | Usuário interpreta como falha | Estado assíncrono, timeout explícito e sinalização de “cancelamento solicitado” |
| Arquivo parcial é tratado como final | Dados incompletos | Staging, validação e publicação atômica via manifesto |
| Schema muda durante a execução | Falha ou inconsistência | Snapshot de schema, política de compatibilidade e bloqueio de publicação |
| App reinicia e perde memória | Perda de status na UI | Estado somente em tabelas persistentes/Lakebase |
| Job possui tasks demais | Limites de plataforma ou complexidade operacional | Fan-out por fila/lookup e Job reutilizável |
| Vários pedidos competem | Saturação de Databricks ou Netezza | Fila, quotas, prioridades e limite de runs simultâneos |
| Filtros livres causam SQL injection | Risco de segurança | Catálogo allowlist, parâmetros tipados e ausência de SQL arbitrário |

## 18. Questões que precisam ser fechadas antes da construção

* O app será usado por usuários internos Databricks, usuários federados ou usuários externos?
* O acesso aos dados deve ser igual para todos ou por usuário/grupo?
* O destino será Volume, external location ou outro storage?
* O consumidor exige CSV, Parquet ou ambos?
* O resultado deve ser um arquivo único ou um conjunto de arquivos acompanhado de manifesto?
* O cliente precisa de snapshot consistente entre tabelas?
* Qual é a coluna de data de cada fato?
* Qual é a melhor coluna de particionamento técnico de cada tabela?
* O Netezza permite quantas sessões concorrentes para esse usuário e ambiente?
* Qual é o volume máximo esperado por pedido?
* Qual é o prazo esperado por tabela e por pedido?
* O cliente precisa de notificações por e-mail, Slack ou webhook?
* Qual é a retenção dos arquivos e dos manifestos?
* Pode haver pedidos sobrepostos para a mesma tabela e intervalo?
* Qual é a periodicidade esperada de cada tabela?
* Qual tolerância de atraso deve gerar alerta ou bloqueio?
* Reprocessamentos devem substituir arquivos, criar nova versão ou apenas gerar novos arquivos?
* O Job deve rodar em serverless ou classic compute por causa da rede, da biblioteca `nzpy` e do acesso ao Netezza?
* A procedure é síncrona ou assíncrona?
* A procedure grava os arquivos diretamente ou retorna dados para o Job gravar?
* Existe uma tabela de controle/status no Netezza para consultar e cancelar uma execução?

## 19. Recomendação final

Implementar primeiro um MVP com uma tabela piloto, dois níveis de checkpoint, Parquet, manifesto, cancelamento assíncrono e retomada por bloco. Só depois generalizar para todas as tabelas fato.

A decisão mais importante é não modelar o produto como “executar uma procedure gigante e apenas acompanhar o Job”. O modelo robusto é “planejar subblocos, chamar a procedure por subbloco, reconciliar o status, confirmar os resultados e publicar um manifesto”. Isso torna cancelamento, retomada, auditoria, concorrência e evolução das tabelas controláveis.

## 20. Fontes técnicas

* [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs)
* [Control the flow of tasks within Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/control-flow)
* [Configure and edit tasks in Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/configure-task)
* [Use a control table to drive a For each job](https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial)
* [Query databases using JDBC](https://docs.databricks.com/aws/en/archive/connectors/jdbc)
* [JDBC DataFrameReader](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframereader/jdbc)
* [JDBC connection](https://docs.databricks.com/aws/en/connect/jdbc-connection)
* [Key concepts in Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/key-concepts)
* [Configure authorization in a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
* [Best practices for Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)
* [Run Lakeflow Jobs with serverless compute](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs)
* [Configure compute for jobs](https://docs.databricks.com/aws/en/jobs/compute)
* [Resource limits](https://docs.databricks.com/aws/en/resources/limits)
