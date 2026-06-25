# Databricks notebook source
# DBTITLE 1,Load CSV files with Auto Loader to bronze table
# Read CSV files using Auto Loader

from pyspark.sql import functions as F

# ── Paths & config ─────────────────────────────────────────────────────────────
SOURCE_PATH      = "/Volumes/retail_proj/volume/blob_source/transaction_source/"
TARGET_TABLE     = "retail_proj.blob_bronze.transactions"
CHECKPOINT_PATH  = "/Volumes/retail_proj/volume/blob_source/_checkpoints/transactions"

# ── Schema hints (derived from source CSV) ─────────────────────────────────────
schema_hints = """
    transaction_id       STRING,
    opportunity_name     STRING,
    product_id           STRING,
    store_id             STRING,
    quantity             INT,
    selling_price        INT,
    discount_amount      INT,
    transaction_timestamp STRING,
    payment_mode         STRING,
    sales_channel        STRING
"""

# ── Auto Loader read stream ─────────────────────────────────────────────────────
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/_schema")
        .option("cloudFiles.inferColumnTypes", "false")   # use explicit hints above
        .load(SOURCE_PATH)
        # ── audit / lineage columns ──────────────────────────────────────────
        .withColumn("_source_file",         F.col("_metadata.file_path"))
        .withColumn("_ingestion_timestamp",  F.current_timestamp())
        .drop("_rescued_data")             # drop Auto Loader rescue column if clean
)

# ── Write stream → Delta bronze table ──────────────────────────────────────────
query = (
    df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)        # process all available files then stop
        .toTable(TARGET_TABLE)
)

query.awaitTermination()
print(f" Auto Loader run complete. Data written to {TARGET_TABLE}")

# COMMAND ----------

%sql
select count(*) from retail_q.blob_bronze.transactions
