# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Azure Capstone: Incremental Ingestion & Control Table Engine
# MAGIC **Description:** Manages the file processing lifecycle, checks file identity hashes/timestamps,
# MAGIC updates the Delta control table (`file_processing_history`), applies idempotent Delta MERGE into Silver,
# MAGIC and triggers Gold recalculation.

# COMMAND ----------
# Cell 1: Widgets
dbutils.widgets.text("storage_account", "zaid123", "Storage Account Name")
dbutils.widgets.text("pipeline_run_id", "manual_run", "Pipeline Run ID")
dbutils.widgets.text("target_file", "", "Target File Name (optional)")

storage_account = dbutils.widgets.get("storage_account")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_file = dbutils.widgets.get("target_file")

print(f"Incremental Ingestion Controller:")
print(f"  Storage Account: {storage_account}")
print(f"  Pipeline Run ID: {pipeline_run_id}")
print(f"  Target File: {target_file if target_file else 'All Eligible Files'}")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, IntegerType
)
from datetime import datetime, timezone
import hashlib

bronze_uploads_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/uploads"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
control_table_path = f"{silver_base}/control/file_processing_history"
silver_cleaned_path = f"{silver_base}/cleaned_sales"
silver_rejected_path = f"{silver_base}/rejected_sales"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Ensure Control Table Exists
# COMMAND ----------
control_schema = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("file_identity", StringType(), False),
    StructField("file_size", LongType(), True),
    StructField("status", StringType(), False),
    StructField("records_read", IntegerType(), True),
    StructField("records_valid", IntegerType(), True),
    StructField("records_rejected", IntegerType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("pipeline_run_id", StringType(), True)
])

try:
    control_df = spark.read.format("delta").load(control_table_path)
    print(f"Loaded existing control table with {control_df.count()} history entries.")
except Exception:
    print(f"Initializing new Delta control table at {control_table_path}")
    empty_control_df = spark.createDataFrame([], control_schema)
    empty_control_df.write.format("delta").mode("overwrite").save(control_table_path)
    control_df = spark.read.format("delta").load(control_table_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Discover Eligible Bronze Files
# COMMAND ----------
file_list = dbutils.fs.ls(bronze_uploads_path)
csv_files = [f for f in file_list if f.name.lower().endswith(".csv")]

print(f"Discovered {len(csv_files)} CSV files in {bronze_uploads_path}:")
for f in csv_files:
    print(f"  - {f.name} ({f.size} bytes)")

# Query successfully processed files and their identities
successful_history = (
    control_df
    .filter(F.col("status") == "SUCCESS")
    .select("file_path", "file_identity")
    .collect()
)
processed_map = {row["file_path"]: row["file_identity"] for row in successful_history}

eligible_files = []
for f in csv_files:
    if target_file and f.name != target_file:
        continue
    # Compute file identity from path and modification timestamp/size
    file_id = hashlib.sha256(f"{f.path}_{f.size}_{f.modificationTime}".encode()).hexdigest()
    if f.path not in processed_map or processed_map[f.path] != file_id:
        eligible_files.append((f, file_id))

print(f"\nFiles eligible for ingestion: {len(eligible_files)}")
for f, fid in eligible_files:
    print(f"  -> {f.name} [ID: {fid[:12]}...]")

if len(eligible_files) == 0:
    print("No new or modified files to process. All files are up to date.")
    dbutils.notebook.exit("SUCCESS: 0 new files processed.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Process Eligible Files with Concurrency Claim & Delta Merge
# COMMAND ----------
from delta.tables import DeltaTable

for file_info, file_id in eligible_files:
    start_time = datetime.now(timezone.utc)
    print(f"\nProcessing: {file_info.name}...")

    # Record Claim: Status = PROCESSING
    claim_record = spark.createDataFrame([{
        "file_name": file_info.name,
        "file_path": file_info.path,
        "file_identity": file_id,
        "file_size": file_info.size,
        "status": "PROCESSING",
        "records_read": 0,
        "records_valid": 0,
        "records_rejected": 0,
        "started_at": start_time,
        "processed_at": None,
        "error_message": None,
        "pipeline_run_id": pipeline_run_id
    }], schema=control_schema)

    claim_record.write.format("delta").mode("append").save(control_table_path)

    try:
        # Read file
        raw_df = spark.read.option("header", True).option("inferSchema", True).csv(file_info.path)
        read_count = raw_df.count()

        # Type-cast & clean
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
            .withColumn("source_file", F.lit(file_info.name))
        )

        # Validation rules
        r_order_id = F.col("order_id").isNull() | (F.col("order_id") == "")
        r_date = F.col("order_date").isNull()
        r_qty = F.col("quantity").isNull() | (F.col("quantity") <= 0)
        r_price = F.col("unit_price").isNull() | (F.col("unit_price") < 0)
        r_cat = F.col("category").isNull() | (F.col("category") == "")
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
        valid_df = (
            df_tagged
            .filter(is_valid)
            .drop("rejection_reason")
            .withColumn("total_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
            .dropDuplicates(["order_id"])
        )
        rejected_df = df_tagged.filter(~is_valid)

        v_count = valid_df.count()
        r_count = rejected_df.count()

        # Idempotent MERGE into Silver cleaned_sales
        if DeltaTable.isDeltaTable(spark, silver_cleaned_path):
            silver_delta = DeltaTable.forPath(spark, silver_cleaned_path)
            (
                silver_delta.alias("target")
                .merge(
                    valid_df.alias("source"),
                    "target.order_id = source.order_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            print(f"Merged {v_count} records into {silver_cleaned_path}")
        else:
            valid_df.write.format("delta").mode("overwrite").save(silver_cleaned_path)
            print(f"Created initial {silver_cleaned_path} with {v_count} records")

        if r_count > 0:
            rejected_df.write.format("delta").mode("append").save(silver_rejected_path)
            print(f"Appended {r_count} rejected records to {silver_rejected_path}")

        # Record SUCCESS in control table
        end_time = datetime.now(timezone.utc)
        success_record = spark.createDataFrame([{
            "file_name": file_info.name,
            "file_path": file_info.path,
            "file_identity": file_id,
            "file_size": file_info.size,
            "status": "SUCCESS",
            "records_read": read_count,
            "records_valid": v_count,
            "records_rejected": r_count,
            "started_at": start_time,
            "processed_at": end_time,
            "error_message": None,
            "pipeline_run_id": pipeline_run_id
        }], schema=control_schema)

        # Update control history
        control_delta = DeltaTable.forPath(spark, control_table_path)
        (
            control_delta.alias("c")
            .merge(
                success_record.alias("s"),
                "c.file_path = s.file_path AND c.pipeline_run_id = s.pipeline_run_id AND c.status = 'PROCESSING'"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Successfully finished processing {file_info.name} (Valid: {v_count}, Rejected: {r_count})")

    except Exception as e:
        err_msg = str(e)[:500]
        end_time = datetime.now(timezone.utc)
        print(f"FAILED processing {file_info.name}: {err_msg}")
        fail_record = spark.createDataFrame([{
            "file_name": file_info.name,
            "file_path": file_info.path,
            "file_identity": file_id,
            "file_size": file_info.size,
            "status": "FAILED",
            "records_read": 0,
            "records_valid": 0,
            "records_rejected": 0,
            "started_at": start_time,
            "processed_at": end_time,
            "error_message": err_msg,
            "pipeline_run_id": pipeline_run_id
        }], schema=control_schema)

        control_delta = DeltaTable.forPath(spark, control_table_path)
        (
            control_delta.alias("c")
            .merge(
                fail_record.alias("f"),
                "c.file_path = f.file_path AND c.pipeline_run_id = f.pipeline_run_id AND c.status = 'PROCESSING'"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        raise e

dbutils.notebook.exit("SUCCESS: Incremental batch completed.")
