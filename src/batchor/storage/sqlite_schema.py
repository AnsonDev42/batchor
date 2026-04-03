from sqlalchemy import Column, Float, Integer, MetaData, String, Table, Text


METADATA = MetaData()

RUNS_TABLE = Table(
    "runs",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("status", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("provider_config_json", Text, nullable=False),
    Column("chunk_policy_json", Text, nullable=False),
    Column("retry_policy_json", Text, nullable=False),
    Column("batch_metadata_json", Text, nullable=False),
    Column("schema_name", String, nullable=True),
    Column("structured_output_module", String, nullable=True),
    Column("structured_output_qualname", String, nullable=True),
    Column("artifacts_exported_at", String, nullable=True),
    Column("artifact_export_root", Text, nullable=True),
)

ITEMS_TABLE = Table(
    "items",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("item_id", String, primary_key=True),
    Column("item_index", Integer, nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("metadata_json", Text, nullable=False),
    Column("prompt", Text, nullable=False),
    Column("system_prompt", Text, nullable=True),
    Column("request_artifact_path", String, nullable=True),
    Column("request_artifact_line", Integer, nullable=True),
    Column("request_sha256", String, nullable=True),
    Column("status", String, nullable=False),
    Column("attempt_count", Integer, nullable=False),
    Column("active_batch_id", String, nullable=True),
    Column("active_custom_id", String, nullable=True),
    Column("active_submission_tokens", Integer, nullable=False),
    Column("output_text", Text, nullable=True),
    Column("output_json", Text, nullable=True),
    Column("raw_response_json", Text, nullable=True),
    Column("error_json", Text, nullable=True),
)

BATCHES_TABLE = Table(
    "batches",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("provider_batch_id", String, primary_key=True),
    Column("local_batch_id", String, nullable=False),
    Column("status", String, nullable=False),
    Column("custom_ids_json", Text, nullable=False),
    Column("output_file_id", String, nullable=True),
    Column("error_file_id", String, nullable=True),
    Column("output_artifact_path", String, nullable=True),
    Column("error_artifact_path", String, nullable=True),
)

RUN_RETRY_STATE_TABLE = Table(
    "run_retry_state",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("consecutive_failures", Integer, nullable=False),
    Column("total_failures", Integer, nullable=False),
    Column("backoff_sec", Float, nullable=False),
    Column("next_retry_at", String, nullable=True),
    Column("last_error_class", String, nullable=True),
)

RUN_INGEST_STATE_TABLE = Table(
    "run_ingest_state",
    METADATA,
    Column("run_id", String, primary_key=True),
    Column("source_kind", String, nullable=False),
    Column("source_ref", Text, nullable=False),
    Column("source_fingerprint", String, nullable=False),
    Column("next_item_index", Integer, nullable=False),
    Column("ingestion_complete", Integer, nullable=False),
)
