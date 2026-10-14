# Deploy do nzexport no ambiente do cliente

O projeto é um **Databricks Asset Bundle**. O deploy cria e atualiza:
- o app;
- os jobs: dispatcher, importador, teste de conexão e pós-deploy;
- o **projeto Lakebase**;
- o **secret scope** do Netezza.

O job **`nzexport_pos_deploy`** configura o restante (banco, permissões) e confere os pré-requisitos.

## 1. Pré-requisitos (uma vez por ambiente, fora do bundle)

| # | O quê | Quem |
|---|---|---|
| 1 | Workspace com **Databricks Apps**, **Lakebase** e **serverless** habilitados | admin do workspace |
| 2 | **Rede**: NCC do serverless com rota privada até o Netezza (host e porta 5480) | rede / infra |
| 3 | **Landing S3** do datasource `PLH_DATASOURCE_DWA`: role IAM, storage credential e **external location** cobrindo o caminho, com `READ FILES` e `WRITE FILES` para o service principal dos jobs. A mesma do PLHDWA pode ser usada | AWS + admin do Unity Catalog |
| 4 | **Service principal dos jobs** (`svc_hash`), com quem faz o deploy no papel *Service Principal: User* (ou o próprio SP fazendo o deploy no CI/CD) | admin da conta |
| 5 | **Tabelas destino** (Delta) já criadas, com `SELECT` e `MODIFY` para o `svc_hash` | donos das tabelas |
| 6 | Databricks CLI ≥ 1.18 autenticada no workspace | quem faz o deploy |

O pós-deploy confere os itens 3 e 5, além de credenciais e conexão. Ele aponta **o que falta e quem resolve**.

## 2. Configurar o target

Em `databricks.yml`, preencha o target (`hml` ou `prd`):

| Variável | Exemplo | Para quê |
|---|---|---|
| `workspace.host` | `https://dbc-xxxx.cloud.databricks.com` | workspace do ambiente |
| `svc_hash` | `a1b2c3d4-...` | application ID do SP: `run_as` dos jobs e pasta do bundle |
| `app_name` | `netezza-exporter` | nome do app |
| `lakebase_project` / `lakebase_endpoint` | `netezza-exporter-prd` / `projects/netezza-exporter-prd/branches/production/endpoints/primary` | banco do app |
| `secret_scope` | `nzexport-netezza` | credenciais do Netezza |
| `dest_catalogs` | `migracao` (vírgula para vários) | catálogos que o app navega (`BROWSE`) |
| `grupo_owner` | `grp-engenharia-dados` | grupo com `MANAGE` no secret scope |

No CI/CD, qualquer variável pode vir de `BUNDLE_VAR_<nome>` (por exemplo, `BUNDLE_VAR_svc_hash`).

## 3. Deploy (todo deploy, sempre nesta ordem)

```bash
databricks bundle validate -t prd
databricks bundle deploy   -t prd
databricks bundle run nzexport_pos_deploy -t prd     # banco, grants, ACLs, BROWSE e conferência
databricks bundle run nzexport_app        -t prd     # publica o código do app
```

O pós-deploy é **idempotente**:
- só **adiciona** permissões;
- termina com **erro** apenas quando algo obrigatório falha (app, banco ou secret scope);
- **avisos** são pré-requisitos externos.

O relatório aparece no log do job e em **Administração → Último pós-deploy**.

> O projeto Lakebase e o secret scope têm `prevent_destroy`: um `bundle destroy` não apaga a migração nem as credenciais.

## 4. Credenciais do Netezza (valor nunca no Git)

Escolha uma das formas:
- **Pelo app**: em **🔌 Conexão Netezza**, informe usuário e senha. O app grava no secret scope e não as exibe depois.
- **Pelo CI/CD**, logo após o deploy, com o valor vindo do cofre do pipeline:
  ```bash
  databricks secrets put-secret nzexport-netezza netezza-user     --string-value "$NZ_USER"
  databricks secrets put-secret nzexport-netezza netezza-password --string-value "$NZ_PASSWORD"
  ```
  As chaves são `<id da conexão>-user` e `<id da conexão>-password`. A conexão padrão é `netezza`.

## 5. Primeiro uso no app

1. **🔌 Conexão Netezza**: host, porta, banco, padrões de `p_banco` e do banco e esquema da external table, e o
   **caminho da landing** (`s3://...`) com as pastas do `.dat`, de erro e de limpeza. Depois, **Testar conexão**.
2. **Administração → Tabelas exportáveis**: cadastre as tabelas pela tela ou por **CSV** (botão "Baixar modelo CSV"),
   com procedure, coluna de tempo, destino e escopo da migração.
3. Rode `nzexport_pos_deploy` de novo. Ele confere as tabelas destino e a landing já configuradas.
4. **Painel da migração → Criar pedido para as lacunas**.

## 6. Ambiente de desenvolvimento (`dev`)

O target `dev` aponta para o workspace de desenvolvimento, e os jobs rodam como quem fez o deploy. O projeto
Lakebase e o secret scope desse workspace foram vinculados ao bundle com `databricks bundle deployment bind`.
Num workspace em que eles já existam, faça o mesmo antes do primeiro deploy:

```bash
databricks bundle deployment bind nzexport_lakebase projects/<projeto> -t <alvo>
databricks bundle deployment bind netezza <secret_scope> -t <alvo>
```
