from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_proj.retail_silver.product_catalog",
    comment="Standardized and quality-validated product catalog from bronze layer",
    cluster_by=["category"],
)
# --- Critical constraints: halt the pipeline on violation ---
@dp.expect_or_fail("product_id_not_null", "product_id IS NOT NULL")
@dp.expect_or_fail("unit_price_not_null", "unit_price IS NOT NULL")
# --- Cleansing: drop rows that cannot be corrected ---
@dp.expect_or_drop("product_name_not_null", "product_name IS NOT NULL")
@dp.expect_or_drop("unit_price_positive", "unit_price > 0")
# --- Monitoring: warn but keep the record ---
@dp.expect("category_not_null", "category IS NOT NULL")
@dp.expect("brand_not_null", "brand IS NOT NULL")
@dp.expect("updated_at_not_null", "updated_at IS NOT NULL")
@dp.expect("launch_date_not_future", "launch_date <= current_date()")
def product_catalog():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("retail_proj.postgres_bronze.product_catalog")
        .filter(F.col("is_active") == True)
        .select(
            # Primary key — preserve as-is
            F.col("product_id"),

            # Descriptive strings — strip whitespace + normalize case
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            F.upper(F.trim(F.col("category"))).alias("category"),
            F.upper(F.trim(F.col("subcategory"))).alias("subcategory"),
            F.initcap(F.trim(F.col("brand"))).alias("brand"),
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),

            # Numeric / date / boolean — pass through as-is
            F.col("unit_price"),
            F.col("launch_date"),
            F.col("updated_at"),

            # Derived: price-based segmentation
            F.when(F.col("unit_price") > 50000, "Premium")
             .when(F.col("unit_price") > 10000, "Mid Range")
             .otherwise("Budget")
             .alias("product_segment"),

            # Audit — timestamp when this row was processed into silver
            F.current_timestamp().alias("_silver_loaded_at"),

            # SCD Type 2 validity window
            F.col("__START_AT"),
            F.col("__END_AT"),

            # Derived: true when __END_AT is null (record is the current live version)
            F.col("__END_AT").isNull().alias("is_active"),
        )
    )
