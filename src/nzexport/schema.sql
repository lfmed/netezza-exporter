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
  expected_periodicity_unit text CHECK (expected_periodicity_unit IN ('DAY','WEEK','MONTH','YEAR')),
  expected_periodicity_size int CHECK (expected_periodicity_size > 0),
  periodicity_tolerance_days int NOT NULL DEFAULT 0,
  description      text,
  created_by       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_config_id, config_version)
);

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

-- Saúde da carga por tabela: último período exportado x periodicidade esperada
DROP VIEW IF EXISTS v_table_load_status;
CREATE VIEW v_table_load_status AS
WITH cur AS (
  SELECT tc.source_id, tc.active, v.*
  FROM table_config tc
  JOIN table_config_version v ON v.table_config_id = tc.table_config_id
                             AND v.config_version = tc.current_version
), agg AS (
  SELECT s.table_config_id,
         min(lower(s.data_range)) FILTER (WHERE s.status = 'DONE')      AS first_exported_date,
         max(upper(s.data_range)) FILTER (WHERE s.status = 'DONE') - 1  AS last_exported_date,
         max(s.finished_at) FILTER (WHERE s.status = 'DONE')            AS last_success_at,
         count(*) FILTER (WHERE s.status = 'RUNNING')                   AS running,
         count(*) FILTER (WHERE s.status = 'PENDING')                   AS pending,
         count(*) FILTER (WHERE s.status = 'UNKNOWN')                   AS unknown,
         count(*) FILTER (WHERE s.status = 'FAILED')                    AS failed
  FROM export_subblock s GROUP BY s.table_config_id
), fails AS (
  SELECT s.table_config_id, count(*) AS failed_attempts_7d
  FROM export_attempt a JOIN export_subblock s USING (subblock_id)
  WHERE a.status IN ('FAILED','UNKNOWN') AND a.started_at > now() - interval '7 days'
  GROUP BY s.table_config_id
), gaps AS (
  -- buracos entre o primeiro e o último dia exportado
  SELECT table_config_id, count(*) AS gap_count FROM (
    SELECT table_config_id, lower(data_range) AS lo,
           lag(upper(data_range)) OVER (PARTITION BY table_config_id ORDER BY lower(data_range)) AS prev_hi
    FROM export_subblock WHERE status = 'DONE') x
  WHERE prev_hi IS NOT NULL AND lo > prev_hi GROUP BY table_config_id
), last_req AS (
  SELECT DISTINCT ON (rt.table_config_id) rt.table_config_id, r.request_id, r.status AS last_request_status,
         r.submitted_at AS last_request_at
  FROM export_request_table rt JOIN export_request r USING (request_id)
  ORDER BY rt.table_config_id, r.submitted_at DESC
)
SELECT c.table_config_id, c.source_id, c.active, c.source_table, c.procedure_name, c.config_version,
       c.expected_periodicity_unit, c.expected_periodicity_size, c.periodicity_tolerance_days,
       a.first_exported_date, a.last_exported_date, a.last_success_at,
       coalesce(a.running, 0) AS running, coalesce(a.pending, 0) AS pending,
       coalesce(a.unknown, 0) AS unknown, coalesce(a.failed, 0) AS failed,
       coalesce(f.failed_attempts_7d, 0) AS failed_attempts_7d,
       coalesce(g.gap_count, 0) AS gap_count,
       lr.request_id AS last_request_id, lr.last_request_status, lr.last_request_at,
       (a.last_exported_date + 1) AS next_expected_start,
       CASE WHEN c.expected_periodicity_unit IS NULL THEN NULL
            ELSE (a.last_exported_date + 1
                  + make_interval(days   => CASE c.expected_periodicity_unit WHEN 'DAY'  THEN c.expected_periodicity_size WHEN 'WEEK' THEN 7 * c.expected_periodicity_size ELSE 0 END,
                                  months => CASE c.expected_periodicity_unit WHEN 'MONTH' THEN c.expected_periodicity_size WHEN 'YEAR' THEN 12 * c.expected_periodicity_size ELSE 0 END))::date - 1
       END AS next_expected_end,
       CASE
         WHEN NOT c.active THEN 'INATIVA'
         WHEN coalesce(a.unknown, 0) > 0 THEN 'ESTADO_DESCONHECIDO'
         WHEN coalesce(f.failed_attempts_7d, 0) >= 3 THEN 'FALHAS_RECORRENTES'
         WHEN a.last_exported_date IS NULL THEN 'SEM_CARGA'
         WHEN c.expected_periodicity_unit IS NULL THEN 'SEM_PERIODICIDADE'
         -- atrasada quando o próximo período esperado já terminou (+ tolerância) e não foi exportado
         WHEN (a.last_exported_date + 1
               + make_interval(days   => CASE c.expected_periodicity_unit WHEN 'DAY'  THEN c.expected_periodicity_size WHEN 'WEEK' THEN 7 * c.expected_periodicity_size ELSE 0 END,
                               months => CASE c.expected_periodicity_unit WHEN 'MONTH' THEN c.expected_periodicity_size WHEN 'YEAR' THEN 12 * c.expected_periodicity_size ELSE 0 END)
               + make_interval(days => c.periodicity_tolerance_days))::date <= current_date
           THEN 'ATRASADA'
         ELSE 'EM_DIA'
       END AS load_health
FROM cur c
LEFT JOIN agg a ON a.table_config_id = c.table_config_id
LEFT JOIN fails f ON f.table_config_id = c.table_config_id
LEFT JOIN gaps g ON g.table_config_id = c.table_config_id
LEFT JOIN last_req lr ON lr.table_config_id = c.table_config_id;
