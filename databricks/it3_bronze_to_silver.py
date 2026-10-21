# Databricks notebook source
storage_account = "zaid123"

bronze_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "uploads/sales.csv"
)

silver_base_path = (
    f"abfss://silver@{storage_account}.dfs.core.windows.net/"
)

# Check that the uploaded CSV is accessible
display(
    dbutils.fs.ls(
        f"abfss://bronze@{storage_account}.dfs.core.windows.net/uploads/"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("Input records:", raw_df.count())

raw_df.printSchema()
display(raw_df.limit(10))

# COMMAND ----------

df = (
    raw_df
    .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
    .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)

display(df.limit(10))

# COMMAND ----------

deduplicated_df = df.dropDuplicates(["order_id"])

print("Records before deduplication:", df.count())
print("Records after deduplication:", deduplicated_df.count())

# COMMAND ----------

valid_condition = (
    F.col("order_id").isNotNull()
    & (F.col("order_id") != "")
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

print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())

display(rejected_df)

# COMMAND ----------

valid_df = valid_df.withColumn(
    "total_sales",
    F.round(
        F.col("quantity") * F.col("unit_price"),
        2
    )
)

display(valid_df)

# COMMAND ----------

silver_path = silver_base_path + "cleaned_sales"

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

print("Cleaned sales saved successfully.")

# COMMAND ----------

rejected_path = silver_base_path + "rejected_sales"

(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .save(rejected_path)
)

print("Rejected sales saved successfully.")

# COMMAND ----------

cleaned_check = spark.read.format("delta").load(silver_path)
rejected_check = spark.read.format("delta").load(rejected_path)

print("Input records:", raw_df.count())
print("After deduplication:", deduplicated_df.count())
print("Valid records:", cleaned_check.count())
print("Rejected records:", rejected_check.count())

display(cleaned_check.limit(10))

# COMMAND ----------

dbutils.widgets.text(
    "input_path",
    "abfss://bronze@zaid123.dfs.core.windows.net/uploads/sales.csv",
    "Input CSV path"
)

input_path = dbutils.widgets.get("input_path")

print("Input file:", input_path)
