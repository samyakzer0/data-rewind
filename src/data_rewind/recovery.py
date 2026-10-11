
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def recover_gold(spark, incident_id):
    # Confirm the incident exists before changing any data.
    audit = DeltaTable.forName(spark, "rewind_incident_audit")

    incident_count = (
        spark.table("rewind_incident_audit")
        .filter(F.col("incident_id") == incident_id)
        .count()
    )

    if incident_count != 1:
        raise ValueError(
            f"Expected exactly one audit record for {incident_id}; "
            f"found {incident_count}."
        )

    baseline = spark.table("rewind_gold_baseline")
    restored_data = baseline.drop("baseline_captured_at")

    # Restore the trusted baseline into the canonical Gold table.
    (
        restored_data.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("gold_daily_revenue")
    )

    restored = spark.table("gold_daily_revenue")
    keys = ["order_date", "status"]

    # Compare keys and business metrics, including missing rows.
    comparison = (
        baseline.alias("b")
        .join(restored.alias("r"), on=keys, how="full_outer")
        .filter(
            ~F.col("b.total_revenue").eqNullSafe(
                F.col("r.total_revenue")
            )
            | ~F.col("b.valid_order_count").eqNullSafe(
                F.col("r.valid_order_count")
            )
        )
    )

    mismatch_count = comparison.count()
    verified = mismatch_count == 0

    recovery_version = (
        spark.sql("DESCRIBE HISTORY gold_daily_revenue")
        .orderBy(F.col("version").desc())
        .first()["version"]
    )

    audit.update(
        condition=F.col("incident_id") == incident_id,
        set={
            "status": F.lit(
                "VERIFIED" if verified else "RECOVERY_FAILED"
            ),
            "recovery_version": F.lit(int(recovery_version)),
            "verification_passed": F.lit(str(verified).lower()),
        },
    )

    print("Incident:", incident_id)
    print("Recovery version:", recovery_version)
    print("Mismatched rows:", mismatch_count)
    print("Recovery verified:", verified)

    return verified
