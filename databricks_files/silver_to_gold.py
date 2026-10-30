from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.materialized_view(
    name="gold_ecommerce_orders",
    comment="Cleaned e-commerce orders with standardized strings, typed numerics, and calculated columns"
)
def gold_ecommerce_orders():
    df = spark.read.table("dbacademy.e_com.silver_ecommerce_orders")

    result = df

    # Calculated columns
    result = result.withColumn(
        "discount_amount",
        F.round(F.col("unit_price") * F.col("quantity") * F.col("discount_pct") / F.lit(100.0), 2),
    ).withColumn(
        "sale",
        F.round(F.col("unit_price") * F.col("quantity") - F.coalesce(F.col("discount_amount"), F.lit(0.0)), 2),
    ).withColumn(
        "sale_month",
        F.date_format(F.col("order_date"), "MMMM"),
    )

    # IQR outlier detection on sale (quartiles computed with percentile_approx)
    quartiles = result.agg(
        F.percentile_approx("sale", 0.25).alias("q1"),
        F.percentile_approx("sale", 0.75).alias("q3"),
    )
    iqr_sale_outlier = (
        result.crossJoin(quartiles)
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("upper_bound", F.col("q3") + F.col("iqr") * F.lit(1.5))
        .withColumn("lower_bound", F.col("q1") - F.col("iqr") * F.lit(1.5))
        .filter((F.col("sale") < F.col("upper_bound")) & (F.col("sale") > F.col("lower_bound")))
        .drop("q1", "q3", "iqr", "upper_bound", "lower_bound")
    )

    return iqr_sale_outlier
