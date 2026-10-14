# Changelog

Versão em `src/VERSION`, exibida no cabeçalho do app e no log do dispatcher. Cada ajuste soma 0.001
(`scripts/bump_version.py`) e recebe a tag git `v<versão>`.

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
