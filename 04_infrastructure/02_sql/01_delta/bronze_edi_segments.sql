CREATE TABLE IF NOT EXISTS catalog.schema.bronze_edi_segments (
    file_name STRING,
    segment_id STRING,
    elements ARRAY<STRING>,
    raw_segment STRING,
    seg_order BIGINT,
    ingested_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true)
