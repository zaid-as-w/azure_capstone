# Azure Cloud Data Engineering Capstone — Troubleshooting Guide

This guide documents known issues encountered during the capstone development, their root cause analysis, and the verified resolutions.

---

## Issue 1: `Invalid configuration value detected for fs.azure.account.key`

### Symptoms
When running Databricks notebooks or attempting to access ADLS Gen2, PySpark throws:
```text
Invalid configuration value detected for fs.azure.account.key
```
or `AZURE_INVALID_CREDENTIALS_CONFIGURATION`.

### Root Cause Analysis
1. In traditional / non-governed environments, developers often configured Spark with:
   `spark.conf.set("fs.azure.account.key.<storage-account>.dfs.core.windows.net", "<storage-key>")`
2. However, in modern Azure Databricks deployments with **Unity Catalog**, the storage container access is governed through:
   - **Azure Databricks Access Connector** (system or user-assigned Managed Identity with `Storage Blob Data Contributor` on the storage account).
   - **Unity Catalog Storage Credential** (`capstone_storage_credential`).
   - **Unity Catalog External Location** (`capstone_bronze_location` and `capstone_silver_location`).
3. Setting an invalid or manual account key conflicts with Unity Catalog's Managed Identity authorization mechanism and causes authentication rejection.

### Resolution
- **Do not configure storage account keys in Spark or notebooks.**
- Rely strictly on Unity Catalog external locations:
  - `abfss://bronze@zaid123.dfs.core.windows.net/`
  - `abfss://silver@zaid123.dfs.core.windows.net/`
- Databricks automatically uses the Access Connector's managed identity to sign and authorize ADLS Gen2 requests via Azure RBAC.

---

## Issue 2: `SparkContext is not supported on serverless compute`

### Symptoms
When executing code in Serverless compute or shared Unity Catalog clusters:
```text
org.apache.spark.SparkUnsupportedOperationException: SparkContext is not supported on serverless compute
```
or errors when referencing `spark.sparkContext` or JVM internals.

### Root Cause Analysis
- Databricks Serverless Compute and Single-User / Shared Unity Catalog compute isolate user code to prevent JVM-level container escapes and unauthorized access.
- Access to `sc` (`SparkContext`) or JVM reflection is disabled by design in serverless compute environments.

### Resolution
- Use high-level DataFrame and SQL APIs exclusively:
  - Use `spark.read` and `df.write` rather than RDD operations.
  - Use `dbutils.widgets` for parameter passing.
  - Avoid calling `spark.sparkContext._jvm` or `sc.parallelize`.
- For ADF orchestration, configure the Databricks activity to use either:
  1. The existing interactive cluster (`capstone-cluster` / `1020-163146-bndzglvu`) configured with auto-termination.
  2. Serverless compute where supported by ADF without JVM dependencies.

---

## Issue 3: Inverted Notebook Parameter Definition Order

### Symptoms
In ADF pipeline execution, the pipeline fails with `NameError: name 'input_path' is not defined`.

### Root Cause
In `it3_bronze_to_silver.py`, the `dbutils.widgets.text("input_path", ...)` call was placed in the last cell of the notebook, after cells that attempted to read `input_path`.

### Resolution
- Parameter widgets must be defined and parsed in **Cell 1** at the very top of the notebook before any data transformations occur.
- Provide a default fallback path so the notebook can be executed interactively during development as well as headless via ADF.

---

## Issue 4: Duplicate Ingestion upon Pipeline Retry

### Symptoms
If an ADF pipeline fails mid-way (e.g. during Gold processing) and is retried, running Silver transformations in naive `append` mode creates duplicate rows.

### Resolution
- Maintain an ingestion control table in Delta format (`silver/control/file_processing_history`).
- Calculate file identity using path and content hash/ETag.
- Use deterministic deduplication rules and idempotent MERGE operations based on `order_id`.
- Recompute Gold aggregates deterministically from the authoritative Silver state.
