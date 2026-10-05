# Changelog

Versão em `src/VERSION`, exibida no cabeçalho do app e no log do dispatcher. Cada ajuste soma 0.001
(`scripts/bump_version.py`) e recebe a tag git `v<versão>`.

## 0.990 — 2026-10-05
- Identidade visual Sicoob: Turquesa, Verde-escuro e Verde-claro, tipografia Asap.
- Início do controle de versão (versão no cabeçalho, rodapé, `/healthz` e log do Job).
- Base já entregue: pedidos por subbloco, dispatcher com limites de concorrência, completude da migração
  com lacunas, exclusão automática de períodos já executados, hook pós-exportação.
