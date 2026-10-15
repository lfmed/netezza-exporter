# Deploy do nzexport no ambiente do cliente

O projeto é um **Databricks Asset Bundle**. O deploy cria e atualiza:
- o app;
- os jobs: dispatcher, importador, teste de conexão e pós-deploy;
- o **projeto Lakebase**.

O **secret scope** do Netezza não é criado pelo bundle: ele já existe no ambiente, e o app e os jobs só o **leem**.

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
| 7 | **Secret scope** do Netezza (`secret_scope`) com as chaves `<id>-host`, `<id>-port` (opcional), `<id>-user` e `<id>-password`, e `READ` para o `svc_hash` e para o service principal do app | administrador do scope |

O pós-deploy confere os itens 3, 5 e 7, além da conexão. Ele aponta **o que falta e quem resolve**.

## 2. Configurar o target

Em `databricks.yml`, preencha o target (`hml` ou `prd`):

| Variável | Exemplo | Para quê |
|---|---|---|
| `workspace.host` | `https://dbc-xxxx.cloud.databricks.com` | workspace do ambiente |
| `svc_hash` | `a1b2c3d4-...` | application ID do SP: `run_as` dos jobs e pasta do bundle |
| `app_name` | `netezza-exporter` | nome do app |
| `lakebase_project` / `lakebase_endpoint` | `netezza-exporter-prd` / `projects/netezza-exporter-prd/branches/production/endpoints/primary` | banco do app |
| `secret_scope` | `nzexport-netezza` | nome do scope existente com host, porta e credenciais do Netezza; informado só aqui (o app mostra o nome, sem gravar nem permitir alterar) |
| `serverless_policy_id` | `79898ab9-...` | política de serverless (budget policy) do app; vazio = padrão do workspace |
| `dest_catalogs` | `migracao` (vírgula para vários) | catálogos que o app navega (`BROWSE`) |

> `lakebase_endpoint` é o **caminho do endpoint** (`projects/<projeto>/branches/production/endpoints/primary`), não o host
> `ep-....database.<região>.cloud.databricks.com/databricks_postgres`. Para ver o caminho:
> `databricks postgres list-endpoints projects/<projeto>/branches/production` (campo `name`). Desde a v1.011 o app aceita
> também o host e procura o endpoint correspondente; o pós-deploy avisa e mostra o caminho a colocar no target.
> Antes disso, o host gerava `NotFound: No API found for 'GET /postgres/ep-...'`.

> **Identidade dos jobs no Lakebase.** O pós-deploy migra o banco como o `svc_hash` (`run_as`), que vira dono do
> schema `nzexport`. Se o projeto Lakebase foi criado por outra identidade (ex.: um usuário fez o `bundle deploy`), o
> `svc_hash` não tem role no Postgres e a conexão falha. Até a v1.012 isso aparecia como `PoolTimeout: couldn't get a
> connection after 30.00 sec`; desde a v1.013 o pós-deploy para nesse ponto e mostra o erro do Postgres com o comando.
> Correção (dono do projeto):
> ```bash
> databricks postgres create-role projects/<projeto>/branches/production --role-id <svc_hash> \
>   --json '{"spec": {"identity_type": "SERVICE_PRINCIPAL", "postgres_role": "<svc_hash>", "auth_method": "LAKEBASE_OAUTH_V1"}}'
> # e, conectado como dono:  GRANT CREATE ON DATABASE databricks_postgres TO "<svc_hash>";
> ```
> Ou faça o `bundle deploy` com o próprio `svc_hash` (CI/CD), que assim cria o projeto e já é o dono.

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
- termina com **erro** apenas quando algo obrigatório falha (app, banco ou leitura do secret scope);
- não altera as ACLs do secret scope;
- **avisos** são pré-requisitos externos.

O relatório aparece no log do job e em **Administração → Último pós-deploy**.

> O projeto Lakebase tem `prevent_destroy`: um `bundle destroy` não apaga a migração. O secret scope fica fora do bundle.

## 4. Servidor e credenciais do Netezza (valor nunca no Git)

Host, porta, usuário e senha ficam só no secret scope, que já existe no ambiente. O app **só lê** o scope: mostra a URL
de conexão (`jdbc:netezza://host:porta/banco`) e se as credenciais existem, mas não grava nem edita nada.
O administrador do scope (ou o CI/CD, com os valores do cofre do pipeline) grava as chaves e dá leitura:
```bash
databricks secrets put-secret nzexport-netezza netezza-host     --string-value "$NZ_HOST"
databricks secrets put-secret nzexport-netezza netezza-port     --string-value "5480"   # opcional, padrão 5480
databricks secrets put-secret nzexport-netezza netezza-user     --string-value "$NZ_USER"
databricks secrets put-secret nzexport-netezza netezza-password --string-value "$NZ_PASSWORD"
databricks secrets put-acl    nzexport-netezza <svc_hash>                 READ
databricks secrets put-acl    nzexport-netezza <service principal do app> READ
```
As chaves são `<id da conexão>-host`, `-port`, `-user` e `-password`. A conexão padrão é `netezza`.

## 5. Primeiro uso no app

1. **🔌 Conexão Netezza**: confira a URL de conexão e informe o banco, padrões de `p_banco` e do banco e esquema da external table, e o
   **caminho da landing** (`s3://...`) com as pastas do `.dat`, de erro e de limpeza. Depois, **Testar conexão**.
2. **Administração → Tabelas exportáveis**: cadastre as tabelas pela tela ou por **CSV** (botão "Baixar modelo CSV"),
   com procedure, tipo de filtro (Data, Número ou Sem filtro), coluna do filtro, destino e escopo da migração.
3. Rode `nzexport_pos_deploy` de novo. Ele confere as tabelas destino e a landing já configuradas.
4. **Painel da migração → Criar pedido para as lacunas**.

## 6. Ambiente de desenvolvimento (`dev`)

O target `dev` aponta para o workspace de desenvolvimento, e os jobs rodam como quem fez o deploy. O projeto
Lakebase desse workspace foi vinculado ao bundle com `databricks bundle deployment bind`.
Num workspace em que ele já exista, faça o mesmo antes do primeiro deploy:

```bash
databricks bundle deployment bind nzexport_lakebase projects/<projeto> -t <alvo>
```
