# Changelog

Versão em `src/VERSION`, exibida no cabeçalho do app e no log do dispatcher. Cada ajuste soma 0.001
(`scripts/bump_version.py`) e recebe a tag git `v<versão>`.

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
