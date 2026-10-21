# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Azure Capstone: Discover Bronze Files
# MAGIC **Description:** Lists all available files in `abfss://bronze@{storage_account}.dfs.core.windows.net/uploads/`
# MAGIC and returns a structured JSON list for Azure Data Factory dynamic iteration or monitoring.

# COMMAND ----------
dbutils.widgets.text("storage_account", "zaid123", "Storage Account Name")
storage_account = dbutils.widgets.get("storage_account")

bronze_uploads_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/uploads"

import json

file_list = dbutils.fs.ls(bronze_uploads_path)
csv_files = [
    {
        "name": f.name,
        "path": f.path,
        "size": f.size,
        "modificationTime": f.modificationTime
    }
    for f in file_list if f.name.lower().endswith(".csv")
]

print(f"Found {len(csv_files)} CSV files in {bronze_uploads_path}")
result_json = json.dumps({"files": csv_files, "count": len(csv_files)})

dbutils.notebook.exit(result_json)
