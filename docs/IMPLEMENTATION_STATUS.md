# Azure Cloud Data Engineering Capstone — Implementation Status

**Last Updated:** 2026-10-21  
**Project Lead:** Zaid  
**Environment:** Azure for Students (`my-group` / `databricks-managed-rg`)

---

## Iteration Progress Tracker

| Iteration | Title | Status | Verification |
| :---: | :--- | :---: | :--- |
| **1** | Local Streamlit Application | ✅ **COMPLETED & VERIFIED** | `app.py` — 3-tab dashboard with upload, analytics, and quality views. Runs locally via `streamlit run app.py`. |
| **2** | Upload to ADLS Gen2 Bronze | ✅ **COMPLETED & VERIFIED** | `azure_storage.py` uses `DefaultAzureCredential`. Verified `uploads/sales.csv` (530 bytes) exists in `bronze` container via `az storage blob list`. |
| **3** | Bronze-to-Silver PySpark Processing | ✅ **COMPLETED & REPAIRED** | `notebooks/nb_bronze_to_silver.py` deployed to `/azure-capstone/nb_bronze_to_silver`. Widget parameter order fixed. 6 explicit validation rules with `rejection_reason` column. Run verified via ADF — **Result: SUCCESS**. |
| **4** | Azure Data Factory Setup | ✅ **COMPLETED & VERIFIED** | `PL_Sales_Bronze_To_Silver` deployed to `zaid-capstone-adf`. Run ID `e9b7df43-cd24-11f1-ab17-c03532654138` — **Status: Succeeded** (94.6s). Linked service `LS_AzureDatabricks` re-credentialed via ARM REST API. |
| **5** | Gold Analytics Layer | ✅ **COMPLETED & VERIFIED** | `notebooks/nb_silver_to_gold.py` deployed. Gold tables created in Unity Catalog via SQL Warehouse DDL. Verified: Categories=3, Cities=3, Months=3. Gold Delta files confirmed in `silver/gold/` paths. |
| **6** | Streamlit Analytics Dashboard | ✅ **COMPLETED** | `app.py` rebuilt with 3 tabs: Executive Analytics (Plotly charts, 5 KPI metrics), Ingestion tab, Data Quality tab. Dual-mode: live Databricks SQL + local fallback. |
| **7** | Direct Databricks SQL Integration | ✅ **COMPLETED & VERIFIED** | `databricks_client.py` created. `test_databricks.py` passed: 5 tables registered, live KPI query returned total_revenue=₹128,600, total_orders=10, total_customers=9. SQL Warehouse `44847d5087ca2301` RUNNING. |
| **8** | Full ADF Orchestration | ✅ **COMPLETED & VERIFIED** | `PL_Sales_Bronze_To_Silver_To_Gold` deployed. Run ID `3ed11096-cd25-11f1-a16a-c03532654138` — **Status: Succeeded** (130.4s). Both activities (Bronze→Silver, Silver→Gold) confirmed successful. |
| **9** | Incremental Ingestion & File Tracking | ✅ **COMPLETED** | `notebooks/nb_file_ingestion_control.py` deployed. `PL_Sales_Incremental_Ingestion` pipeline created. Delta control table at `silver/control/file_processing_history`. Uses SHA256 file identity hash, PROCESSING/SUCCESS/FAILED statuses, idempotent Delta MERGE. ⚠️ Not yet ADF-test-run (cluster auto-terminated; requires manual trigger). |
| **10** | Data Quality Monitoring & Failure Recovery | ✅ **COMPLETED** | Explicit validation rules in `nb_bronze_to_silver.py` (6 rules, row-level `rejection_reason` column). `rejected_sales` Delta table tracks all rejections. Test dataset `data/sales_invalid_records.csv` created. |
| **11** | Security, Optimization & Cost Control | ✅ **COMPLETED** | DefaultAzureCredential (keyless) for Streamlit→ADLS. Unity Catalog external locations for storage isolation. No storage keys in code. Auto-termination 15min (cluster), 10min (SQL warehouse). Cluster: Standard_D4ds_v4 (appropriate for student capstone). |
| **12** | End-to-End Testing & Final Delivery | ✅ **COMPLETED** | End-to-end run verified: Bronze file present → ADF pipeline Succeeded → Silver cleaned_sales=10 records → Gold 3 tables created → SQL Warehouse query returns live KPIs → Dashboard functional. README, docs, .gitignore, deployment scripts all created. |

---

## Verified Pipeline Runs

| Pipeline | Run ID | Status | Duration |
|---|---|---|---|
| `PL_Sales_Bronze_To_Silver` | `e9b7df43-cd24-11f1-ab17-c03532654138` | **Succeeded** | 94.6s |
| `PL_Sales_Bronze_To_Silver_To_Gold` | `3ed11096-cd25-11f1-a16a-c03532654138` | **Succeeded** | 130.4s |

## Verified Live SQL Query Results

| Metric | Value |
|---|---|
| Total Revenue | ₹128,600 |
| Total Orders | 10 |
| Unique Customers | 9 |
| Total Units Sold | 38 |
| Avg Order Value | ₹12,860 |

**By Category:** Electronics ₹117,000 | Clothing ₹8,400 | Food ₹3,200  
**By City:** Belagavi ₹57,200 | Bagalkot ₹48,800 | Hubballi ₹22,600  
**By Month:** Jan-26 ₹57,400 | Feb-26 ₹21,400 | Mar-26 ₹49,800

---

## Remaining Caveats

1. `PL_Sales_Incremental_Ingestion` pipeline: Logic complete and deployed; a live ADF test run was not executed (cluster had auto-terminated). Trigger manually and verify control table is populated.
2. Gold table Unity Catalog registration uses `CREATE TABLE ... LOCATION`. After running `nb_silver_to_gold.py` via ADF in future runs, the table definitions persist but data is automatically current from the Delta path.
3. ADF Linked Service uses PAT (auto-generated). For production, switch to Managed Identity passthrough on the Databricks linked service.
