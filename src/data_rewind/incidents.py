
from pyspark.sql import functions as F


def find_duplicate_keys(df, key_column="order_id"):
    """Return keys that occur more than once."""
    return (
        df.groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )


def detect_duplicate_incident(
    source_table,
    run_id,
    record_incident,
    key_column="order_id",
):
    """
    Detect duplicate keys in a Delta table and record an incident.
    """
    source = spark.table(source_table)

    duplicates = find_duplicate_keys(source, key_column)
    affected_key_count = duplicates.count()

    if affected_key_count == 0:
        print(f"No duplicate keys found in {source_table}.")
        return None

    incident_id = record_incident(
        run_id=run_id,
        incident_type="DUPLICATE_ORDER_IDS",
        source_table=source_table,
        affected_key_count=affected_key_count,
        status="DETECTED",
    )

    print("Incident detected:", incident_id)
    print("Affected keys:", affected_key_count)

    return incident_id
