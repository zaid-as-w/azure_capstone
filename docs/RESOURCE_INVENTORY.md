# Azure Cloud Data Engineering Capstone — Resource Inventory

**Generated / Audited:** 2026-10-21  
**Subscription Name:** Azure for Students  
**Subscription ID:** `902c80dd-782a-446e-9516-d760fef91a3a`  
**Tenant ID:** `3fc4af91-b6ce-4b12-909c-1209b4cd6d07`  
**Primary Resource Group:** `my-group` (Region: `malaysiawest`)  
**Databricks Managed Resource Group:** `databricks-managed-rg` (Region: `eastasia`)  

---

## 1. Storage Resources (ADLS Gen2)

| Resource Name | Type | Region | SKU / Kind | HNS Enabled | Containers Verified | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `zaid123` | Storage Account | `malaysiawest` | Standard_LRS / StorageV2 | **Yes** (ADLS Gen2) | `bronze`, `silver`, `mycontainer` | Primary Medallion data lake |
| `zaidstorage` | Storage Account | `malaysiawest` | Standard_LRS / StorageV2 | No | N/A | Auxiliary storage |
| `dbstoragenqayccs5zc5ma` | Storage Account | `eastasia` | Standard_ZRS / StorageV2 | Yes | `unity-catalog-storage` | Databricks internal root storage |

### Verified Storage Paths
- **Bronze Input Path:** `abfss://bronze@zaid123.dfs.core.windows.net/uploads/`
  - Sample file verified: `uploads/sales.csv` (530 bytes)
- **Silver Cleaned Path:** `abfss://silver@zaid123.dfs.core.windows.net/cleaned_sales` (Delta format)
- **Silver Rejected Path:** `abfss://silver@zaid123.dfs.core.windows.net/rejected_sales` (Delta format)
- **Silver Control Path:** `abfss://silver@zaid123.dfs.core.windows.net/control/file_processing_history` (Delta format)
- **Gold Path Base:** `abfss://silver@zaid123.dfs.core.windows.net/gold/`
  - `gold_category_sales`
  - `gold_city_sales`
  - `gold_monthly_sales`

---

## 2. Azure Databricks

| Item | Details |
| :--- | :--- |
| **Workspace Name** | `zaid-capstone-dbx` |
| **Workspace URL** | `https://adb-7405607366206759.19.azuredatabricks.net` |
| **Workspace Resource ID** | `/subscriptions/902c80dd-782a-446e-9516-d760fef91a3a/resourceGroups/my-group/providers/Microsoft.Databricks/workspaces/zaid-capstone-dbx` |
| **Pricing Tier** | Premium |
| **Region** | `eastasia` |

### Existing Clusters
1. **`capstone-cluster`** (ID: `1020-163146-bndzglvu`)
   - Spark Version: `18.x-scala2.13`
   - State: `TERMINATED` (starts on-demand)
   - Auto-termination: 15 minutes (cost-controlled)
   - Note: Referenced by existing ADF linked service `LS_AzureDatabricks`
2. **`azure-capstone-cluster`** (ID: `1020-160817-xb3c13ne`)
   - Spark Version: `18.x-scala2.13`
   - State: `TERMINATED`
   - Auto-termination: 15 minutes

### SQL Warehouses
1. **`Serverless Starter Warehouse`** (ID: `44847d5087ca2301`)
   - State: `STOPPED`
   - Size: `Small`
   - Auto-stop: 10 minutes
   - HTTP Path: `/sql/1.0/warehouses/44847d5087ca2301`

### Unity Catalog Governance
- **Catalog:** `zaid_capstone_dbx_7405607366206759`
- **Schemas:** `default`, `information_schema`
- **Storage Credentials:**
  - `capstone_storage_credential` (Azure Access Connector Managed Identity: `/subscriptions/902c80dd-782a-446e-9516-d760fef91a3a/resourceGroups/my-group/providers/Microsoft.Databricks/accessConnectors/zaid-capstone-connector`)
- **External Locations:**
  - `capstone_bronze_location`: `abfss://bronze@zaid123.dfs.core.windows.net/`
  - `capstone_silver_location`: `abfss://silver@zaid123.dfs.core.windows.net/`

---

## 3. Azure Data Factory

| Item | Details |
| :--- | :--- |
| **Factory Name** | `zaid-capstone-adf` |
| **Resource Group** | `my-group` |
| **Region** | `malaysiawest` |
| **Linked Services** | `LS_AzureDatabricks` (points to `adb-7405607366206759.19.azuredatabricks.net` and cluster `1020-163146-bndzglvu`), `AzureDatabricks1`, `AzureDatabricksDeltaLake1` |
| **Pipelines** | To be deployed: `PL_Sales_Bronze_To_Silver`, `PL_Sales_Bronze_To_Silver_To_Gold` |

---

## 4. Local Development Environment

| Item | Details |
| :--- | :--- |
| **Path** | `C:\Users\Zaid\Desktop\azure-capstone` |
| **Python** | Python 3.12 (`.venv`) |
| **Streamlit** | Streamlit 1.66.0 |
| **Azure CLI** | 2.91.0 |
| **Source Data** | `data/sales.csv` (10 rows, columns: `order_id`, `order_date`, `customer_id`, `category`, `city`, `quantity`, `unit_price`) |
