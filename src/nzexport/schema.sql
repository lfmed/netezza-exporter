-- Modelo de controle do exportador Netezza. Idempotente: pode ser reaplicado.
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE SCHEMA IF NOT EXISTS nzexport;
SET search_path = nzexport, public;

-- Origem Netezza e limite global de sessões simultâneas (procedures em paralelo)
CREATE TABLE IF NOT EXISTS source_connection (
  source_id        text PRIMARY KEY,
  display_name     text NOT NULL,
  driver           text NOT NULL DEFAULT 'fake' CHECK (driver IN ('fake', 'nzpy')),
  secret_scope     text,               -- chaves: host, port, database, user, password
  max_sessions     int  NOT NULL DEFAULT 4 CHECK (max_sessions BETWEEN 1 AND 200),
  connect_timeout_s int NOT NULL DEFAULT 30,
  driver_options   jsonb NOT NULL DEFAULT '{}'::jsonb,  -- fake: min_s, max_s, fail_pct
  updated_at       timestamptz NOT NULL DEFAULT now(),
  updated_by       text
);

CREATE TABLE IF NOT EXISTS table_config (
  table_config_id  text PRIMARY KEY,   -- nome lógico exibido no app
  source_id        text NOT NULL REFERENCES source_connection,
  active           boolean NOT NULL DEFAULT true,
  current_version  int NOT NULL DEFAULT 1,
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- Versões imutáveis: cada pedido aponta para a versão usada
CREATE TABLE IF NOT EXISTS table_config_version (
  table_config_id  text NOT NULL REFERENCES table_config,
  config_version   int  NOT NULL,
  source_table     text NOT NULL,      -- valor enviado no parâmetro "tabela" da procedure
  procedure_name   text NOT NULL,      -- ex.: ADMIN.SP_EXPORTA_TABELA
  default_subblock_unit text NOT NULL DEFAULT 'MONTH' CHECK (default_subblock_unit IN ('DAY','WEEK','MONTH','YEAR')),
  default_subblock_size int NOT NULL DEFAULT 1 CHECK (default_subblock_size > 0),
  max_parallel     int  NOT NULL DEFAULT 2 CHECK (max_parallel > 0),
  call_timeout_min int  NOT NULL DEFAULT 240 CHECK (call_timeout_min > 0),
  max_retries      int  NOT NULL DEFAULT 2 CHECK (max_retries >= 0),
  retry_unknown    boolean NOT NULL DEFAULT false,  -- só se a procedure for idempotente
  migration_start  date,               -- escopo da migração: primeiro dia de dados no Netezza
  migration_end    date,               -- último dia de dados a migrar (inclusivo)
  description      text,
  created_by       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_config_id, config_version)
);

DROP VIEW IF EXISTS v_table_load_status;
ALTER TABLE table_config_version ADD COLUMN IF NOT EXISTS migration_start date;
ALTER TABLE table_config_version ADD COLUMN IF NOT EXISTS migration_end date;
ALTER TABLE table_config_version DROP COLUMN IF EXISTS expected_periodicity_unit;
ALTER TABLE table_config_version DROP COLUMN IF EXISTS expected_periodicity_size;
ALTER TABLE table_config_version DROP COLUMN IF EXISTS periodicity_tolerance_days;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'table_config_version_scope_chk') THEN
    ALTER TABLE table_config_version ADD CONSTRAINT table_config_version_scope_chk
      CHECK (migration_start IS NULL OR migration_end IS NULL OR migration_start <= migration_end);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS export_request (
  request_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  request_name     text NOT NULL,
  requested_by     text NOT NULL,
  status           text NOT NULL CHECK (status IN
                   ('SUBMITTED','RUNNING','STOPPING','STOPPED','FAILED','COMPLETED','CANCELED')),
  max_parallel     int  NOT NULL CHECK (max_parallel BETWEEN 1 AND 200),
  deadline_at      timestamptz,        -- janela operacional: nada novo inicia depois disso
  notes            text,
  stop_requested_at timestamptz,
  stop_requested_by text,
  status_reason    text,
  submitted_at     timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS export_request_status_idx ON export_request (status, submitted_at);

CREATE TABLE IF NOT EXISTS export_request_table (
  request_table_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id       uuid NOT NULL REFERENCES export_request ON DELETE CASCADE,
  ordinal          int  NOT NULL,
  table_config_id  text NOT NULL,
  config_version   int  NOT NULL,
  period_start     date NOT NULL,
  period_end       date NOT NULL,      -- inclusiva, como o usuário informa
  subblock_unit    text NOT NULL CHECK (subblock_unit IN ('DAY','WEEK','MONTH','YEAR')),
  subblock_size    int  NOT NULL CHECK (subblock_size > 0),
  calendar_aligned boolean NOT NULL DEFAULT true,
  is_reprocess     boolean NOT NULL DEFAULT false,
  reprocess_reason text,
  CHECK (period_start <= period_end),
  CHECK (NOT is_reprocess OR coalesce(length(trim(reprocess_reason)), 0) > 0),
  FOREIGN KEY (table_config_id, config_version) REFERENCES table_config_version
);
CREATE INDEX IF NOT EXISTS export_request_table_req_idx ON export_request_table (request_id);

-- Subbloco = 1 chamada da procedure (tabela, data_ini, data_fim inclusiva)
CREATE TABLE IF NOT EXISTS export_subblock (
  subblock_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  request_table_id uuid NOT NULL REFERENCES export_request_table ON DELETE CASCADE,
  request_id       uuid NOT NULL,
  table_config_id  text NOT NULL,
  seq              int  NOT NULL,
  data_range       daterange NOT NULL,   -- semiaberto [ini, fim+1)
  status           text NOT NULL CHECK (status IN
                   ('PENDING','RUNNING','DONE','FAILED','UNKNOWN','CANCELED','SUPERSEDED')),
  attempts         int  NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz,
  lease_owner      text,
  lease_expires_at timestamptz,
  started_at       timestamptz,
  finished_at      timestamptz,
  last_error       text,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (request_table_id, seq)
);
CREATE INDEX IF NOT EXISTS export_subblock_status_idx ON export_subblock (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS export_subblock_req_idx ON export_subblock (request_id, status);

-- Bloqueio de duplicidade no banco: dois subblocos vivos da mesma tabela não podem se sobrepor
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'export_subblock_no_overlap') THEN
    ALTER TABLE export_subblock ADD CONSTRAINT export_subblock_no_overlap
      EXCLUDE USING gist (table_config_id WITH =, data_range WITH &&)
      WHERE (status IN ('PENDING','RUNNING','DONE','UNKNOWN'));
  END IF;
END $$;

-- Histórico de cada chamada da procedure
CREATE TABLE IF NOT EXISTS export_attempt (
  attempt_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  subblock_id      uuid NOT NULL REFERENCES export_subblock ON DELETE CASCADE,
  attempt_no       int  NOT NULL,
  databricks_run_id bigint,
  worker           text,
  status           text NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','UNKNOWN','ABANDONED')),
  call_statement   text,
  started_at       timestamptz NOT NULL DEFAULT now(),
  finished_at      timestamptz,
  error_class      text,
  error_message    text,
  UNIQUE (subblock_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS export_event (
  event_id         bigserial PRIMARY KEY,
  request_id       uuid,
  subblock_id      uuid,
  event_type       text NOT NULL,
  actor            text NOT NULL,
  details          jsonb,
  event_time       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS export_event_req_idx ON export_event (request_id, event_time DESC);

-- Hook pós-exportação: dispara um job (ex.: importação para Delta)
CREATE TABLE IF NOT EXISTS post_export_hook (
  hook_id          text PRIMARY KEY,
  description      text,
  table_config_id  text REFERENCES table_config,   -- NULL = todas as tabelas
  trigger_level    text NOT NULL CHECK (trigger_level IN ('REQUEST_TABLE','REQUEST')),
  job_id           bigint NOT NULL,
  enabled          boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS hook_invocation (
  invocation_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  hook_id          text NOT NULL REFERENCES post_export_hook ON DELETE CASCADE,
  request_id       uuid NOT NULL,
  request_table_id uuid,
  job_run_id       bigint,
  status           text NOT NULL CHECK (status IN ('TRIGGERED','FAILED')),
  error_message    text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT (hook_id, request_id, request_table_id)
);

-- Execuções do dispatcher (para o app saber se precisa disparar o Job)
CREATE TABLE IF NOT EXISTS dispatcher_run (
  run_id           bigint PRIMARY KEY,
  started_at       timestamptz NOT NULL DEFAULT now(),
  heartbeat_at     timestamptz NOT NULL DEFAULT now(),
  finished_at      timestamptz,
  status           text NOT NULL DEFAULT 'RUNNING',
  in_flight        int NOT NULL DEFAULT 0
);

-- Progresso por tabela do pedido
DROP VIEW IF EXISTS v_request_table_progress;
CREATE VIEW v_request_table_progress AS
SELECT rt.request_id, rt.request_table_id, rt.ordinal, rt.table_config_id, rt.config_version,
       rt.period_start, rt.period_end, rt.subblock_unit, rt.subblock_size, rt.is_reprocess,
       count(s.*)                                         AS total,
       count(*) FILTER (WHERE s.status = 'DONE')          AS done,
       count(*) FILTER (WHERE s.status = 'RUNNING')       AS running,
       count(*) FILTER (WHERE s.status = 'PENDING')       AS pending,
       count(*) FILTER (WHERE s.status = 'FAILED')        AS failed,
       count(*) FILTER (WHERE s.status = 'UNKNOWN')       AS unknown,
       count(*) FILTER (WHERE s.status IN ('CANCELED','SUPERSEDED')) AS canceled,
       min(s.started_at) AS started_at, max(s.finished_at) AS last_finished_at
FROM export_request_table rt
LEFT JOIN export_subblock s ON s.request_table_id = rt.request_table_id
GROUP BY rt.request_table_id;

-- Dias contidos num multirange de datas
CREATE OR REPLACE FUNCTION mr_days(m datemultirange) RETURNS int LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce(sum(upper(r) - lower(r)), 0)::int FROM unnest(coalesce(m, '{}'::datemultirange)) r
$$;

-- Completude da migração por tabela: escopo x períodos concluídos, em execução e com problema
DROP VIEW IF EXISTS v_migration_coverage;
CREATE VIEW v_migration_coverage AS
WITH cur AS (
  SELECT tc.source_id, tc.active, v.*,
         CASE WHEN v.migration_start IS NOT NULL AND v.migration_end IS NOT NULL
              THEN datemultirange(daterange(v.migration_start, v.migration_end, '[]')) END AS scope
  FROM table_config tc
  JOIN table_config_version v ON v.table_config_id = tc.table_config_id AND v.config_version = tc.current_version
), mr AS (
  SELECT s.table_config_id,
         range_agg(s.data_range) FILTER (WHERE s.status = 'DONE')                         AS done_mr,
         range_agg(s.data_range) FILTER (WHERE s.status IN ('PENDING','RUNNING'))          AS inflight_mr,
         range_agg(s.data_range) FILTER (WHERE s.status = 'UNKNOWN')                       AS unknown_mr,
         range_agg(s.data_range) FILTER (WHERE s.status IN ('PENDING','RUNNING','DONE','UNKNOWN')) AS live_mr,
         range_agg(s.data_range) FILTER (WHERE s.status = 'FAILED')                        AS failed_mr,
         count(*) FILTER (WHERE s.status = 'RUNNING')  AS running,
         count(*) FILTER (WHERE s.status = 'PENDING')  AS pending,
         max(s.finished_at) FILTER (WHERE s.status = 'DONE') AS last_success_at
  FROM export_subblock s GROUP BY s.table_config_id
), x AS (
  SELECT c.table_config_id, c.source_id, c.active, c.source_table, c.procedure_name, c.config_version,
         c.migration_start, c.migration_end, c.scope,
         coalesce(m.done_mr, '{}')     AS done_mr,
         coalesce(m.inflight_mr, '{}') AS inflight_mr,
         coalesce(m.unknown_mr, '{}')  AS unknown_mr,
         -- falhas ainda não cobertas por outro subbloco vivo
         coalesce(m.failed_mr, '{}') - coalesce(m.live_mr, '{}') AS failed_open_mr,
         -- lacunas: parte do escopo sem nenhum subbloco vivo (nunca pedida, cancelada ou falha)
         CASE WHEN c.scope IS NOT NULL THEN c.scope - coalesce(m.live_mr, '{}') END AS missing_mr,
         CASE WHEN c.scope IS NOT NULL THEN coalesce(m.done_mr, '{}') - c.scope END AS done_outside_mr,
         coalesce(m.running, 0) AS running, coalesce(m.pending, 0) AS pending, m.last_success_at
  FROM cur c LEFT JOIN mr m ON m.table_config_id = c.table_config_id
)
SELECT x.table_config_id, x.source_id, x.active, x.source_table, x.procedure_name, x.config_version,
       x.migration_start, x.migration_end, x.running, x.pending, x.last_success_at,
       mr_days(x.scope)                              AS scope_days,
       mr_days(x.done_mr * coalesce(x.scope, x.done_mr)) AS done_days,
       mr_days(x.inflight_mr * coalesce(x.scope, x.inflight_mr)) AS inflight_days,
       mr_days(x.unknown_mr * coalesce(x.scope, x.unknown_mr))  AS unknown_days,
       mr_days(x.failed_open_mr * coalesce(x.scope, x.failed_open_mr)) AS failed_days,
       mr_days(x.missing_mr)                         AS missing_days,
       mr_days(x.done_outside_mr)                    AS done_outside_days,
       (SELECT count(*) FROM unnest(x.missing_mr))   AS gap_count,
       x.missing_mr::text AS missing_ranges,
       x.failed_open_mr::text AS failed_ranges,
       x.unknown_mr::text AS unknown_ranges,
       CASE
         WHEN NOT x.active THEN 'INATIVA'
         WHEN x.scope IS NULL THEN 'SEM_ESCOPO'
         WHEN mr_days(x.unknown_mr * x.scope) > 0 THEN 'ESTADO_DESCONHECIDO'
         WHEN mr_days(x.done_mr * x.scope) = mr_days(x.scope) THEN 'COMPLETA'
         WHEN x.running + x.pending > 0 THEN 'EM_ANDAMENTO'
         WHEN mr_days(x.failed_open_mr * x.scope) > 0 THEN 'COM_FALHAS'
         WHEN mr_days(x.done_mr * x.scope) = 0 THEN 'NAO_INICIADA'
         ELSE 'INCOMPLETA'
       END AS coverage_status
FROM x;
