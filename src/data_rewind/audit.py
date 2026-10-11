
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)


RUN_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), True),
    StructField("error_message", StringType(), True),
])


INCIDENT_SCHEMA = StructType([
    StructField("incident_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("incident_type", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("affected_key_count", LongType(), False),
    StructField("status", StringType(), False),
    StructField("detected_at", TimestampType(), False),
    StructField("recovery_version", LongType(), True),
    StructField("verification_passed", StringType(), True),
])


def record_pipeline_run(
    spark,
    run_id,
    status,
    started_at,
    finished_at=None,
    error_message=None,
    table_name="rewind_pipeline_runs",
):
    record = [(
        run_id,
        "data_rewind",
        status,
        started_at,
        finished_at,
        error_message,
    )]

    df = spark.createDataFrame(record, schema=RUN_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table_name)


def record_incident(
    spark,
    run_id,
    incident_type,
    source_table,
    affected_key_count,
    status="DETECTED",
    recovery_version=None,
    verification_passed=None,
    table_name="rewind_incident_audit",
):
    record = [(
        f"inc_{uuid4().hex[:12]}",
        run_id,
        incident_type,
        source_table,
        int(affected_key_count),
        status,
        datetime.now(timezone.utc).replace(tzinfo=None),
        recovery_version,
        str(verification_passed).lower()
        if verification_passed is not None else None,
    )]

    df = spark.createDataFrame(record, schema=INCIDENT_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table_name)

    return record[0][0]
