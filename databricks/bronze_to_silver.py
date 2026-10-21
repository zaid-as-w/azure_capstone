# Databricks notebook source

from pyspark.sql import functions as F

# Source data
storage_account = "zaid123"

bronze_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "uploads/sales.csv"
)

silver_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
    "cleaned_sales"
)

rejected_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
    "rejected_sales"
)

# Read the raw CSV
raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)

# Standardize and convert data types
df = (
    raw_df
    .withColumn("order_id", F.col("order_id").cast("string"))
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)

# Remove duplicate order IDs, keeping the first row
deduplicated_df = df.dropDuplicates(["order_id"])

# Define basic validation rules
valid_condition = (
    F.col("order_id").isNotNull()
    & (F.trim(F.col("order_id")) != "")
    & F.col("order_date").isNotNull()
    & F.col("quantity").isNotNull()
    & (F.col("quantity") > 0)
    & F.col("unit_price").isNotNull()
    & (F.col("unit_price") >= 0)
    & F.col("category").isNotNull()
    & (F.col("category") != "")
    & F.col("city").isNotNull()
    & (F.col("city") != "")
)

valid_df = deduplicated_df.filter(valid_condition)
rejected_df = deduplicated_df.filter(~valid_condition)

# Calculate total sales
valid_df = valid_df.withColumn(
    "total_sales",
    F.round(F.col("quantity") * F.col("unit_price"), 2)
)

# Save valid records as Delta
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

# Save rejected records for data quality review
(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .save(rejected_path)
)

print("Silver transformation completed.")
print("Input records:", raw_df.count())
print("Records after deduplication:", deduplicated_df.count())
print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())


# COMMAND ----------


silver_df = spark.read.format("delta").load(silver_path)

display(silver_df)

print("Silver record count:", silver_df.count())

silver_df.printSchema()

