# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Azure Capstone: Bronze to Silver Pipeline
# MAGIC **Description:** Ingests raw sales CSV from ADLS Gen2 Bronze layer, applies data cleansing, schema standardization,
# MAGIC validation rules with rejection reasons, deduplication, and writes outputs in Delta Lake format to Silver.

# COMMAND ----------
# Cell 1: Notebook Widgets & Parameters (Must be defined first for automated orchestration)
dbutils.widgets.text(
    "input_path",
    "abfss://bronze@zaid123.dfs.core.windows.net/uploads/sales.csv",
    "Input CSV path"
)
dbutils.widgets.text(
    "storage_account",
    "zaid123",
    "Storage Account Name"
)
dbutils.widgets.text(
    "pipeline_run_id",
    "manual_run",
    "Orchestration Run ID"
)

input_path = dbutils.widgets.get("input_path")
storage_account = dbutils.widgets.get("storage_account")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

print(f"Executing Bronze-to-Silver with:")
print(f"  Input Path: {input_path}")
print(f"  Storage Account: {storage_account}")
print(f"  Pipeline Run ID: {pipeline_run_id}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from datetime import datetime

silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
silver_cleaned_path = f"{silver_base}/cleaned_sales"
silver_rejected_path = f"{silver_base}/rejected_sales"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Read Raw CSV from Bronze
# COMMAND ----------
try:
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )
    raw_count = raw_df.count()
    print(f"Successfully loaded {raw_count} raw records from {input_path}")
except Exception as e:
    print(f"Error reading from {input_path}: {e}")
    raise e

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Standardize & Type-Cast
# COMMAND ----------
df_typed = (
    raw_df
    .withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
    .withColumn("customer_id", F.trim(F.col("customer_id").cast(StringType())))
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("batch_id", F.lit(pipeline_run_id))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Data Quality Validation with Explicit Reasons
# COMMAND ----------
# Rule 1: Missing or blank order_id
r_order_id = F.col("order_id").isNull() | (F.col("order_id") == "")

# Rule 2: Invalid or unparseable order_date
r_date = F.col("order_date").isNull()

# Rule 3: Missing or non-positive quantity
r_qty = F.col("quantity").isNull() | (F.col("quantity") <= 0)

# Rule 4: Missing or negative unit_price
r_price = F.col("unit_price").isNull() | (F.col("unit_price") < 0)

# Rule 5: Missing category
r_cat = F.col("category").isNull() | (F.col("category") == "")

# Rule 6: Missing city
r_city = F.col("city").isNull() | (F.col("city") == "")

df_tagged = df_typed.withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(r_order_id, F.lit("Missing/blank order_id")),
        F.when(r_date, F.lit("Invalid order_date")),
        F.when(r_qty, F.lit("Quantity must be > 0")),
        F.when(r_price, F.lit("Unit price must be >= 0")),
        F.when(r_cat, F.lit("Missing category")),
        F.when(r_city, F.lit("Missing city"))
    )
)

is_valid = F.col("rejection_reason") == ""

valid_raw_df = df_tagged.filter(is_valid).drop("rejection_reason")
rejected_df = df_tagged.filter(~is_valid)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Deterministic Deduplication of Valid Records
# COMMAND ----------
# Keep the latest order record if duplicate order_ids exist within the batch
from pyspark.sql.window import Window

window_spec = Window.partitionBy("order_id").orderBy(F.col("order_date").desc_nulls_last())

deduped_valid_df = (
    valid_raw_df
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("total_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
)

valid_count = deduped_valid_df.count()
rejected_count = rejected_df.count()

print(f"Transformation Summary:")
print(f"  Raw Records Read: {raw_count}")
print(f"  Valid Cleansed Records: {valid_count}")
print(f"  Rejected Records: {rejected_count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Idempotent Write to Delta Lake Silver Layer
# COMMAND ----------
# Write valid records
(
    deduped_valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_cleaned_path)
)
print(f"Saved valid records to Delta: {silver_cleaned_path}")

# Write rejected records
if rejected_count > 0:
    (
        rejected_df.write
        .format("delta")
        .mode("append")
        .save(silver_rejected_path)
    )
    print(f"Appended rejected records to Delta: {silver_rejected_path}")
else:
    # Ensure empty Delta table exists with schema
    (
        rejected_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_rejected_path)
    )
    print("No rejected records in this batch; initialized rejected Delta schema.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verification & Self-Audit Read-Back
# COMMAND ----------
cleaned_check = spark.read.format("delta").load(silver_cleaned_path)
print(f"Verification: Read-back cleaned_sales count = {cleaned_check.count()}")
cleaned_check.printSchema()

# Return execution metrics for ADF
dbutils.notebook.exit(f"SUCCESS: Read {raw_count}, Valid {valid_count}, Rejected {rejected_count}")
