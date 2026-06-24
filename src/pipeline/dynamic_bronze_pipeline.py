"""
Dynamic bronze ingestion pipeline (single file, reused by every pipeline group).

This one file backs ALL bronze SDP pipelines. Each deployed pipeline passes a different
`pipeline_group` in its configuration; the file reads the control config table, filters to the
rows for THIS group, and metaprograms one Auto Loader streaming table per active source.

Adding a new source that fits an existing group = INSERT one row into source_config. No code
change, no redeploy. Adding a new group = add one ~6-line pipeline block to databricks.yml.

Key design point: Auto Loader (cloudFiles) discovers new files from its OWN checkpoint state.
The orchestrator never tells this pipeline which file to read -- it only decides whether to wake
the pipeline at all. That is why intermittent sources and replays "just work".

Pipeline configuration parameters (set in databricks.yml -> resources.pipelines.*.configuration):
  pipeline_group        e.g. "pg_crm"
  config_table          e.g. "banner_bronze.control.source_config"
  schema_location_base  UC volume path, e.g. "/Volumes/banner_bronze/control/pipeline_metadata/pg_crm"
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

PIPELINE_GROUP = spark.conf.get("pipeline_group")
CONFIG_TABLE = spark.conf.get("config_table")
SCHEMA_LOCATION_BASE = spark.conf.get("schema_location_base")

# Read config ONCE at graph-construction time. The set of tables a pipeline owns is fixed for the
# duration of an update; new sources are picked up on the next update (which is exactly when the
# orchestrator wakes the pipeline).
_configs = (
    spark.read.table(CONFIG_TABLE)
    .where((F.col("is_active") == True) & (F.col("pipeline_group") == F.lit(PIPELINE_GROUP)))
    .collect()
)


def _build_streaming_table(cfg: dict) -> None:
    """Define one Auto Loader -> bronze streaming table for a single source config row.

    Wrapped in a function so each table closes over its OWN cfg (avoids the classic late-binding
    bug when defining decorated functions inside a loop).
    """
    target = f"{cfg['target_catalog']}.{cfg['target_schema']}.{cfg['target_table']}"
    fmt = cfg["file_format"]
    landing_path = cfg["landing_path"]
    schema_location = f"{SCHEMA_LOCATION_BASE}/{cfg['source_id']}"
    reader_options = cfg.get("reader_options") or {}
    schema_hints = cfg.get("schema_hints")
    schema_evolution_mode = cfg.get("schema_evolution_mode") or "addNewColumns"
    path_regex = cfg.get("path_regex")

    @dp.table(
        name=target,
        comment=(
            f"Bronze raw ingest for source '{cfg['source_id']}' "
            f"(domain={cfg['domain']}, group={PIPELINE_GROUP}, format={fmt})"
        ),
        table_properties={
            "quality": "bronze",
            "ingestion.source_id": cfg["source_id"],
            "ingestion.pipeline_group": PIPELINE_GROUP,
        },
    )
    def _ingest():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            # Auto Loader checkpoint + schema tracking. MUST be set for Python cloudFiles ingestion.
            # Lives in a UC volume (never the source data location) and is what makes checkpoints
            # durable across intermittent deliveries.
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
            .option("cloudFiles.inferColumnTypes", "true")
            # Recursively walk {table}/{batch_date}/ partitions under the source path.
            .option("recursiveFileLookup", "true")
        )
        if schema_hints:
            reader = reader.option("cloudFiles.schemaHints", schema_hints)
        for key, value in reader_options.items():
            reader = reader.option(key, value)

        df = reader.load(landing_path)

        # Optional fine-grained regex filter on the file path (Auto Loader path globbing is coarse;
        # rlike on _metadata.file_path gives true regex when a source needs it).
        if path_regex:
            df = df.where(F.col("_metadata.file_path").rlike(path_regex))

        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("_source_id", F.lit(cfg["source_id"]))
            .withColumn("_pipeline_group", F.lit(PIPELINE_GROUP))
        )


if not _configs:
    # A pipeline with zero active sources is valid (e.g. all sources paused). Emit nothing.
    print(f"[dynamic_bronze] No active sources for pipeline_group={PIPELINE_GROUP}")

for _row in _configs:
    _build_streaming_table(_row.asDict())
