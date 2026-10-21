# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Azure Capstone: Silver to Gold Analytics Pipeline
# MAGIC **Description:** Aggregates cleansed Silver Delta data into business-ready Gold analytical datasets:
# MAGIC 1. `gold_category_sales`
# MAGIC 2. `gold_city_sales`
# MAGIC 3. `gold_monthly_sales`
# MAGIC Registers tables in Unity Catalog for Databricks SQL querying.

# COMMAND ----------
# Cell 1: Widgets & Parameters
dbutils.widgets.text("storage_account", "zaid123", "Storage Account Name")
dbutils.widgets.text("catalog_name", "zaid_capstone_dbx_7405607366206759", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "default", "Catalog Schema")

storage_account = dbutils.widgets.get("storage_account")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Executing Silver-to-Gold with:")
print(f"  Storage Account: {storage_account}")
print(f"  Catalog: {catalog_name}")
print(f"  Schema: {schema_name}")

# COMMAND ----------
from pyspark.sql import functions as F

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/cleaned_sales"
gold_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/gold"

# Read Cleaned Silver Dataset
silver_df = spark.read.format("delta").load(silver_path)
total_silver_records = silver_df.count()
print(f"Loaded {total_silver_records} records from Silver cleaned_sales.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Gold Category Sales Aggregate
# COMMAND ----------
gold_category_sales = (
    silver_df
    .groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("total_sales"), 2).alias("total_revenue"),
        F.round(F.sum("total_sales") / F.countDistinct("order_id"), 2).alias("avg_order_value")
    )
    .sort(F.col("total_revenue").desc())
)

cat_path = f"{gold_base_path}/gold_category_sales"
(
    gold_category_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(cat_path)
)
print(f"Written gold_category_sales ({gold_category_sales.count()} categories) to {cat_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Gold City Sales Aggregate
# COMMAND ----------
gold_city_sales = (
    silver_df
    .groupBy("city")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("total_sales"), 2).alias("total_revenue"),
        F.round(F.sum("total_sales") / F.countDistinct("order_id"), 2).alias("avg_order_value")
    )
    .sort(F.col("total_revenue").desc())
)

city_path = f"{gold_base_path}/gold_city_sales"
(
    gold_city_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(city_path)
)
print(f"Written gold_city_sales ({gold_city_sales.count()} cities) to {city_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Gold Monthly Sales Trend
# COMMAND ----------
gold_monthly_sales = (
    silver_df
    .withColumn("sales_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("sales_month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("total_sales"), 2).alias("total_revenue"),
        F.round(F.sum("total_sales") / F.countDistinct("order_id"), 2).alias("avg_order_value")
    )
    .sort(F.col("sales_month").asc())
)

monthly_path = f"{gold_base_path}/gold_monthly_sales"
(
    gold_monthly_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(monthly_path)
)
print(f"Written gold_monthly_sales ({gold_monthly_sales.count()} months) to {monthly_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Register Tables in Unity Catalog
# COMMAND ----------
# Register or refresh table mappings in Unity Catalog
try:
    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")

    # Cleaned Silver Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS cleaned_sales
        USING DELTA
        LOCATION '{silver_path}'
    """)

    # Rejected Silver Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS rejected_sales
        USING DELTA
        LOCATION '{silver_base}/rejected_sales'
    """)

    # Gold Category Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_category_sales
        USING DELTA
        LOCATION '{cat_path}'
    """)

    # Gold City Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_city_sales
        USING DELTA
        LOCATION '{city_path}'
    """)

    # Gold Monthly Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_monthly_sales
        USING DELTA
        LOCATION '{monthly_path}'
    """)

    print(f"Successfully registered / verified all Delta tables in {catalog_name}.{schema_name}")
except Exception as e:
    print(f"Notice: Table registration check: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Verification Queries
# COMMAND ----------
cat_check = spark.read.format("delta").load(cat_path)
city_check = spark.read.format("delta").load(city_path)
monthly_check = spark.read.format("delta").load(monthly_path)

print(f"Validation: Categories = {cat_check.count()}, Cities = {city_check.count()}, Months = {monthly_check.count()}")

dbutils.notebook.exit(f"SUCCESS: Categories={cat_check.count()}, Cities={city_check.count()}, Months={monthly_check.count()}")
