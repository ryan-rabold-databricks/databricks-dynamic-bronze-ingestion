-- Unity Catalog assets for the dynamic bronze ingestion framework.
-- Concrete names below match the bundle variables in databricks.yml. To promote to another
-- environment, change the catalog name in databricks.yml and re-render (or run this via a job
-- task with parameters). External locations are created separately by aws/setup_uc_storage.py
-- because they depend on the storage credential created during the AWS IAM-trust dance.

CREATE CATALOG IF NOT EXISTS banner_bronze
  COMMENT 'Bronze layer + ingestion control plane for the dynamic ingestion framework';

CREATE SCHEMA IF NOT EXISTS banner_bronze.control
  COMMENT 'Control plane: source_config + Auto Loader schema/checkpoint metadata volume';

CREATE SCHEMA IF NOT EXISTS banner_bronze.crm
  COMMENT 'Bronze tables for the CRM domain';

CREATE SCHEMA IF NOT EXISTS banner_bronze.erp
  COMMENT 'Bronze tables for the ERP domain';

-- Volume holding Auto Loader schemaLocation/checkpoint metadata for every pipeline group.
-- NEVER colocate this with source data (causes permission + listing conflicts).
CREATE VOLUME IF NOT EXISTS banner_bronze.control.pipeline_metadata
  COMMENT 'Auto Loader schema + checkpoint metadata, namespaced by pipeline_group/source_id';

-- The control plane. One row per (source -> bronze table). Onboarding a source = INSERT one row.
CREATE TABLE IF NOT EXISTS banner_bronze.control.source_config (
  source_id              STRING  NOT NULL COMMENT 'Unique logical source id',
  domain                 STRING  NOT NULL COMMENT 'Business domain (maps to a landing bucket)',
  landing_bucket         STRING  NOT NULL COMMENT 'S3 bucket name (no s3:// prefix) used for event routing',
  landing_prefix         STRING  NOT NULL COMMENT 'Key prefix within the bucket used for event routing',
  landing_path           STRING  NOT NULL COMMENT 'Full s3:// path Auto Loader reads (the source/table root)',
  path_regex             STRING           COMMENT 'Optional regex applied to _metadata.file_path for fine filtering',
  file_format            STRING  NOT NULL COMMENT 'Auto Loader cloudFiles.format: csv|json|parquet|avro|orc|text|binaryFile',
  reader_options         MAP<STRING,STRING>        COMMENT 'Extra Auto Loader/reader options (e.g. header, delimiter)',
  schema_hints           STRING           COMMENT 'Auto Loader cloudFiles.schemaHints',
  schema_evolution_mode  STRING           COMMENT 'addNewColumns|rescue|failOnNewColumns|none (default addNewColumns)',
  target_catalog         STRING  NOT NULL COMMENT 'Destination catalog',
  target_schema          STRING  NOT NULL COMMENT 'Destination schema',
  target_table           STRING  NOT NULL COMMENT 'Destination table name',
  pipeline_group         STRING  NOT NULL COMMENT 'Which SDP pipeline owns this table (immutable once set!)',
  cadence                STRING           COMMENT 'Informational: weekly|monthly|daily|adhoc',
  t_shirt_size           STRING           COMMENT 'Informational sizing hint: S|M|L',
  is_active              BOOLEAN NOT NULL COMMENT 'Whether this source participates in routing + ingestion',
  created_at             TIMESTAMP        COMMENT 'Row creation time',
  updated_at             TIMESTAMP        COMMENT 'Row last-update time'
)
COMMENT 'Config-driven control plane for landing -> bronze ingestion'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
