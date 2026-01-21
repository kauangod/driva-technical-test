CREATE EXTENSION IF NOT EXISTS pgcrypto; -- Utilizada para gerar o uuid na inserção.
CREATE SCHEMA IF NOT EXISTS api_enrichments_seed;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE api_enrichments_seed.enriquecimentos (
  id UUID PRIMARY KEY,
  id_workspace UUID,
  workspace_name TEXT,
  total_contacts INTEGER,
  contact_type TEXT,
  status TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE bronze.enriquecimentos (
  id UUID PRIMARY KEY,
  payload_json JSONB,
  dw_ingested_at TIMESTAMP,
  dw_updated_at TIMESTAMP,
  execution_id TEXT,
  source_page INTEGER
);

CREATE TABLE gold.enriquecimentos (
  id_enriquecimento UUID PRIMARY KEY,
  id_workspace UUID,
  nome_workspace TEXT,
  total_contatos INTEGER,
  tipo_contato TEXT,
  status_processamento TEXT,
  duracao_processamento_minutos FLOAT,
  tempo_por_contato_minutos FLOAT,
  processamento_sucesso BOOLEAN,
  categoria_tamanho_job TEXT,
  necessita_reprocessamento BOOLEAN,
  data_criacao TIMESTAMP,
  data_atualizacao TIMESTAMP,
  data_atualizacao_dw TIMESTAMP
);

CREATE TABLE control.api_ingestion_runs (
  execution_id TEXT PRIMARY KEY,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status TEXT
);

CREATE TABLE control.dw_pipeline_state (
  execution_id TEXT PRIMARY KEY,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  records_counter INTEGER,
  status TEXT
);

INSERT INTO api_enrichments_seed.enriquecimentos (
  id,
  id_workspace,
  workspace_name,
  total_contacts,
  contact_type,
  status,
  created_at,
  updated_at
)
SELECT
  gen_random_uuid(),
  gen_random_uuid(),
  'Workspace Teste ' || (floor(random() * 5000) + 1)::int,
  (random() * 1500)::int,
  CASE WHEN random() > 0.5 THEN 'COMPANY' ELSE 'PERSON' END,
  CASE
    WHEN random() < 0.2 THEN 'CANCELED'
    WHEN random() < 0.7 THEN 'COMPLETED'
    WHEN random() < 0.85 THEN 'FAILED'
    ELSE 'PROCESSING'
  END,
  now() - (random() * interval '1 day'),
  now()
FROM generate_series(1, 5000) gs;

CREATE INDEX idx_gold_enriquecimentos_id_workspace
  ON gold.enriquecimentos (id_workspace); -- Índice para otimização de consultas pelo campo mais utilizado nos consumos via dashboard.

