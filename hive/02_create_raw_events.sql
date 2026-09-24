USE education_monitoring;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_events (
    student_id STRING,
    event STRING
)
STORED AS PARQUET
LOCATION '/online-education/raw-events';