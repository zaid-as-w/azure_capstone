# Azure Cloud Data Engineering Capstone — Deployment Log

**Deployment Target:** Azure Subscription `902c80dd-782a-446e-9516-d760fef91a3a`  
**Resource Group:** `my-group`  
**Execution Agent:** Autonomous Data Engineering Agent  

---

## Log Entries

### Entry 001: Environment Audit & Discovery (2026-10-21)
- **Objective:** Verify Azure CLI authentication, subscription details, existing cloud resources, and local codebase.
- **Commands Executed:**
  - `az version` → 2.91.0
  - `az account show` → Azure for Students (`902c80dd-782a-446e-9516-d760fef91a3a`)
  - `az resource list --resource-group my-group` → `zaid123`, `zaidstorage`, `zaid-capstone-adf`, `zaid-capstone-dbx`, `zaid-capstone-connector`
  - Databricks REST API → Workspace active; cluster `capstone-cluster` (auto-stop 15 min); SQL warehouse (auto-stop 10 min); Unity Catalog with bronze and silver external locations.
- **Findings:** Preserved all existing resources. Identified widget parameter ordering bug in `it3_bronze_to_silver.py`.

---

### Entry 002: Notebooks Created & Uploaded (2026-10-21)
- **Objective:** Author and deploy all pipeline notebooks to Databricks workspace.
- **Notebooks Created:**
  - `notebooks/nb_bronze_to_silver.py` → `/azure-capstone/nb_bronze_to_silver`
  - `notebooks/nb_silver_to_gold.py` → `/azure-capstone/nb_silver_to_gold`
  - `notebooks/nb_file_ingestion_control.py` → `/azure-capstone/nb_file_ingestion_control`
  - `notebooks/nb_discover_bronze_files.py` → `/azure-capstone/nb_discover_bronze_files`
- **Method:** Databricks Workspace REST API (`/api/2.0/workspace/import`) with Azure AD Bearer token.
- **Result:** All 4 notebooks confirmed present in `/azure-capstone/`.

---

### Entry 003: ADF Pipelines Deployed (2026-10-21)
- **Objective:** Deploy all 3 ADF pipelines via `az datafactory pipeline create`.
- **Pipelines Deployed:**
  - `PL_Sales_Bronze_To_Silver` (1 activity)
  - `PL_Sales_Bronze_To_Silver_To_Gold` (2 activities, chained)
  - `PL_Sales_Incremental_Ingestion` (2 activities, chained)
- **Linked Service Updated:** `LS_AzureDatabricks` re-credentialed via ARM REST API with fresh PAT.

---

### Entry 004: Gold Tables Registered in Unity Catalog (2026-10-21)
- **Objective:** Register Silver and Gold Delta paths as Unity Catalog tables.
- **Method:** Python `databricks-sql-connector` via SQL Warehouse `44847d5087ca2301`.
- **Tables Registered:**
  - `zaid_capstone_dbx_7405607366206759.default.cleaned_sales` → `abfss://silver@zaid123.dfs.core.windows.net/cleaned_sales`
  - `zaid_capstone_dbx_7405607366206759.default.rejected_sales` → `abfss://silver@zaid123.dfs.core.windows.net/rejected_sales`
  - `zaid_capstone_dbx_7405607366206759.default.gold_category_sales` → `abfss://silver@zaid123.dfs.core.windows.net/gold/gold_category_sales`
  - `zaid_capstone_dbx_7405607366206759.default.gold_city_sales` → `abfss://silver@zaid123.dfs.core.windows.net/gold/gold_city_sales`
  - `zaid_capstone_dbx_7405607366206759.default.gold_monthly_sales` → `abfss://silver@zaid123.dfs.core.windows.net/gold/gold_monthly_sales`
- **Verification:** `SHOW TABLES` returned 5 tables. ✅

---

### Entry 005: ADF End-to-End Pipeline Test Runs (2026-10-21)
- **Objective:** Execute and verify both primary pipelines.

| Pipeline | Run ID | Status | Duration |
|---|---|---|---|
| `PL_Sales_Bronze_To_Silver` | `e9b7df43-cd24-11f1-ab17-c03532654138` | **Succeeded** | 94.6s |
| `PL_Sales_Bronze_To_Silver_To_Gold` | `3ed11096-cd25-11f1-a16a-c03532654138` | **Succeeded** | 130.4s |

- **Live SQL KPI Verification (post-Gold):**
  - Total Revenue: ₹128,600 | Orders: 10 | Customers: 9 | Units: 38

---

### Entry 006: Final Documentation & Delivery (2026-10-21)
- README.md, .gitignore, IMPLEMENTATION_STATUS.md, RESOURCE_INVENTORY.md, TROUBLESHOOTING.md all finalized.
- `scripts/deploy_adf_pipelines.ps1` created for repeatable deployment.
- Test datasets created: `data/sales.csv`, `data/sales_batch_002.csv`, `data/sales_invalid_records.csv`.
- Project is GitHub-ready (no secrets in any tracked file).
