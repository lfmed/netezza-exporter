# Changelog

Versão em `src/VERSION`, exibida no cabeçalho do app e no log do dispatcher. Cada ajuste soma 0.001
(`scripts/bump_version.py`) e recebe a tag git `v<versão>`.

## 1.014 — 2026-10-15
- App resistente à publicação sem a configuração do bundle: com o comando padrão 'python app.py' (que fazia o app sair com 'app exited unexpectedly'), app.py sobe o mesmo gunicorn na porta do Databricks Apps; log de início em stdout com versão, comando e variáveis do bundle (ok/AUSENTE) e a orientação de publicar com bundle deploy + bundle run; nível do log em NZEXPORT_LOG_LEVEL; /api/health responde 503 com o erro e missing_env; DEPLOY.md com o roteiro de diagnóstico.

## 1.013 — 2026-10-15
- Falha ao conectar no Lakebase com a causa real: o PoolTimeout passa a trazer o último erro do Postgres (host, usuário e mensagem); o pós-deploy para na falha de banco, sem seguir para as conferências que mascaravam o erro, e diz quem resolve (role da identidade dos jobs com o comando create-role, dono do schema ou rede); DEPLOY.md explica a identidade dos jobs no Lakebase.

## 1.012 — 2026-10-15
- Nome do secret scope do Netezza só pela configuração do deploy (variável secret_scope do bundle): o app recebe em NETEZZA_SECRET_SCOPE e o dispatcher e o teste de conexão no parâmetro --secret-scope; o app mostra o nome na conexão e na Administração, sem campo e sem gravar (coluna source_connection.secret_scope removida).

## 1.011 — 2026-10-15
- lakebase_endpoint aceita também o host do Postgres (ep-....database...com, com /banco ou URL postgresql://) e encontra o endpoint pelo host; o pós-deploy não quebra mais com NotFound 'GET /postgres/ep-...': mostra o caminho certo (projects/<projeto>/branches/production/endpoints/primary) como aviso ou erro claro; descrição da variável e DEPLOY.md explicam o formato.

## 1.010 — 2026-10-15
- Tipo de filtro no cadastro da tabela: Data (AAAAMMDD, subbloco por calendário), Número (coluna inteira, escopo mínimo-máximo dividido numa quantidade de subblocos, maior valor primeiro, limpeza BETWEEN no destino) e Sem filtro (tabela inteira numa chamada com filtro sempre verdadeiro '1','1','1', .dat <TABELA>_1_1.dat, DELETE total do destino em reprocesso/erro). Faixa genérica int8range (key_range) no lugar de data_range, com migração; cobertura em unidades; tipo travado quando há subblocos; telas, pedido, revisão, CSV (filter_type, scope_min/max, default_subblock_count) e pós-deploy (coluna do filtro no destino) por tipo; opção YYYY-MM-DD removida.

## 1.009 — 2026-10-15
- Variável serverless_policy_id no bundle: política de serverless (budget policy) aplicada ao app; vazia = política padrão do workspace; placeholders em hml/prd.

## 1.008 — 2026-10-15
- App só lê o secret scope do Netezza, que já existe no ambiente: sai a edição de usuário e senha (status host/porta/usuário/senha só leitura); o scope deixa de ser recurso do bundle (sem MANAGE para grupo, sem grupo_owner); o pós-deploy só confere a leitura (jobs e app) e não altera ACLs; colunas credentials_set_* removidas.

## 1.007 — 2026-10-15
- Host e porta do Netezza movidos para o secret scope (chaves <id>-host e <id>-port, porta opcional com padrão 5480): saem do banco e da tela; a tela da conexão e a Administração mostram só a URL de conexão (jdbc:netezza://host:porta/banco), sem editar; o pós-deploy confere o host no scope.

## 1.006 — 2026-10-14
- Vazão por subbloco: registros/s na chamada da SP (Netezza) e na carga do Delta (com MB/s), média ponderada por tabela no pedido e vazão histórica da tabela na página de completude.

## 1.005 — 2026-10-14
- Deploy repetível no ambiente do cliente: targets dev/hml/prd com variáveis (app, projeto Lakebase, secret scope, catálogos de destino, svc_hash, grupo dono); projeto Lakebase e secret scope como recursos do bundle (prevent_destroy); config do app no bundle (sem valores fixos); job nzexport_pos_deploy idempotente (migração, grants do app no banco, ACLs do scope, BROWSE nos catálogos e conferência dos pré-requisitos com relatório na Administração); DEPLOY.md.

## 1.004 — 2026-10-14
- Job importador nzexport-importer: carrega o .dat de cada subbloco exportado na tabela destino por append (leitura/limpeza do motor PLHDWA), apaga o arquivo; em erro limpa o período no destino, move o .dat para erro/ e libera o subbloco; retomada pelo histórico Delta; 1 importação por tabela destino; dispatcher e app disparam o importador; telas com as importações e linhas carregadas.

## 1.003 — 2026-10-14
- Configuração da importação: caminho da landing (S3 ou volume) e pastas do .dat, de erro e de limpeza na tela da conexão, importações simultâneas e espera pelo arquivo; formato do .dat no cadastro da tabela (padrões do PLHDWA); estados EXPORTED/IMPORTING/IMPORT_FAILED e histórico de importação no banco.

## 1.002 — 2026-10-14
- Destino no Databricks no cadastro da tabela (catálogo, schema e tabela do Unity Catalog) com navegador do catálogo no app, conferência da tabela (existe, tipo, colunas, coluna de tempo) e colunas de destino no CSV.

## 1.001 — 2026-10-14
- Removido tudo o que era simulação: driver fake (código, telas e banco), dados simulados (tabelas de exemplo, pedidos, subblocos, eventos, hook e job de exemplo) e o smoke test simulado; conexão 'netezza' passa a ser nzpy aguardando host e credenciais. Backup no branch Lakebase backup-antes-remover-simulacao.

## 1.000 — 2026-10-14
- Módulo de configuração da conexão Netezza no app: host, porta, banco, TLS, timeout, limite global e padrões de p_banco/p_banco_ext_table/p_esquema_ext_table; usuário e senha gravados no secret scope (nunca no banco nem exibidos); Testar conexão via Job (mesma rede do dispatcher) verificando autenticação, versão e existência da procedure.

## 0.999 — 2026-10-14
- Chamada da SP_CARGA_EVENTUAL (7 parâmetros): p_banco, p_tabela, p_banco_ext_table, p_esquema_ext_table e p_NomeColunaTempo no cadastro da tabela (banco/esquema com padrão da conexão), p_dtini/p_dtfim do subbloco em AAAAMMDD; prévia da chamada; validação estrita; sessão no banco da external table; retorno e arquivo esperado registrados por tentativa; cadastro inválido = falha CONFIG sem retry; CSV com as novas colunas.

## 0.998 — 2026-10-06
- Subblocos executados do período mais recente para o mais antigo (dentro de cada pedido/tabela); listas do pedido e da revisão na mesma ordem.

## 0.997 — 2026-10-05
- Subbloco cancelado é definitivo no pedido: não pode ser repetido nem reprocessado ali (bloqueado no servidor e sem botão na tela); o período só volta a ser executado em um novo pedido.

## 0.996 — 2026-10-05
- Administração: carga de tabelas exportáveis via CSV (modelo com exemplos, exportação no mesmo formato, prévia com novas/novas versões/erros por linha, importação tudo-ou-nada).

## 0.995 — 2026-10-05
- Painel: ordenação da tabela de completude por percentual migrado (menor/maior), nome, lacunas ou falhas; cabeçalhos clicáveis e preferência lembrada no navegador.

## 0.994 — 2026-10-05
- Agendamento do início da execução (data/hora futura) com disparo automático pelo app; Iniciar agora / reagendar; datas e horas no fuso America/Sao_Paulo (corrige prazo interpretado em UTC).

## 0.993 — 2026-10-05
- Subblocos com falha reservam o período até decisão: repetir (um ou todos) ou cancelar com motivo; cancelados viram lacunas e podem entrar em novo pedido. Hook de tabela só dispara sem subblocos cancelados.

## 0.992 — 2026-10-05
- Logo real do Sicoob e padrão visual do app Marketplace de Dados (menu lateral, cores teal/verde-escuro, banners, badges).

## 0.991 — 2026-10-05
- Health check movido de /healthz (interceptado pela plataforma) para /api/health, retornando a versão.

## 0.990 — 2026-10-05
- Identidade visual Sicoob: Turquesa, Verde-escuro e Verde-claro, tipografia Asap.
- Início do controle de versão (versão no cabeçalho, rodapé, `/api/health` e log do Job).
- Base já entregue: pedidos por subbloco, dispatcher com limites de concorrência, completude da migração
  com lacunas, exclusão automática de períodos já executados, hook pós-exportação.
