from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_orders(orders: DataFrame):
    """
    Separate valid orders from rejected orders.

    Reject records with:
    - Missing order ID
    - Missing amount
    - Negative amount
    - Duplicate order ID
    """
    order_counts = (
        orders.groupBy("order_id")
        .count()
        .withColumnRenamed("count", "id_count")
    )

    checked = orders.join(order_counts, on="order_id", how="left")

    checked = checked.withColumn(
        "rejection_reason",
        F.concat_ws(
            ", ",
            F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
            F.when(F.col("amount").isNull(), F.lit("missing_amount")),
            F.when(F.col("amount") < 0, F.lit("negative_amount")),
            F.when(F.col("id_count") > 1, F.lit("duplicate_order_id")),
        ),
    )

    valid_orders = (
        checked.filter(F.col("rejection_reason") == "")
        .drop("id_count", "rejection_reason")
    )

    rejected_orders = checked.filter(
        F.col("rejection_reason") != ""
    ).drop("id_count")

    return valid_orders, rejected_orders