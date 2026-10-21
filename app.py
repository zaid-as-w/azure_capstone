import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

# Local modules
from azure_storage import upload_file
import databricks_client


# --------------------------------------------------
# Page Configuration & Styling
# --------------------------------------------------
st.set_page_config(
    page_title="Azure Cloud Data Engineering & Analytics Platform",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for rich dashboard aesthetic
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #475569;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .status-badge {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        border-radius: 6px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">☁️ Azure Cloud Data Engineering Platform</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">End-to-End Medallion Architecture: Streamlit ➔ ADLS Gen2 Bronze ➔ PySpark / Delta Silver ➔ Gold Aggregations ➔ Databricks SQL</div>',
    unsafe_allow_html=True
)

# --------------------------------------------------
# Sidebar Configuration & Environment Status
# --------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/color/96/azure-1.png", width=64)
    st.subheader("Cloud Architecture Status")
    
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "zaid123")
    st.caption(f"**Storage Account:** `{storage_account}`")
    st.caption(f"**Workspace:** `zaid-capstone-dbx`")
    st.caption(f"**Data Factory:** `zaid-capstone-adf`")
    st.caption(f"**Warehouse ID:** `44847d5087ca2301`")
    
    st.divider()
    st.subheader("Data Source Mode")
    datasource_mode = st.radio(
        "Select Analytics Backend:",
        ["Databricks SQL Warehouse (Direct)", "Local Silver Fallback"],
        help="Connect directly to Databricks SQL Warehouse over port 443 with keyless Azure AD Bearer auth, or use local CSV preview."
    )
    
    if st.button("🔄 Test Databricks SQL Connection", use_container_width=True):
        with st.spinner("Connecting to Databricks SQL Warehouse..."):
            res = databricks_client.test_connection()
            if res.get("success"):
                st.success(f"Connected as {res['user']} to {res['catalog']}.{res['schema']}")
            else:
                st.error(f"Connection error: {res.get('error')}")

# --------------------------------------------------
# Navigation Tabs
# --------------------------------------------------
tab_dashboard, tab_ingestion, tab_quality = st.tabs([
    "📊 Executive Sales Analytics",
    "🚀 Ingestion & ADF Orchestration",
    "🛡️ Data Quality & Audit Logs"
])

# ==================================================
# TAB 1: EXECUTIVE SALES ANALYTICS
# ==================================================
with tab_dashboard:
    st.header("Executive Sales & Commercial KPIs")

    # Load data based on selected mode
    df_category = pd.DataFrame()
    df_city = pd.DataFrame()
    df_monthly = pd.DataFrame()
    kpis = {"total_revenue": 0.0, "total_orders": 0, "total_units_sold": 0, "total_customers": 0, "avg_order_value": 0.0}
    is_live_data = False

    if datasource_mode == "Databricks SQL Warehouse (Direct)":
        try:
            with st.spinner("Executing live analytical queries on Databricks SQL Warehouse..."):
                df_category = databricks_client.get_gold_category_sales()
                df_city = databricks_client.get_gold_city_sales()
                df_monthly = databricks_client.get_gold_monthly_sales()
                kpis = databricks_client.get_overall_kpis()
                is_live_data = True
        except Exception as e:
            st.warning(f"Live Databricks SQL query encountered an issue: {e}. Falling back to local data preview.")

    # Local fallback if live SQL fails or if selected
    if not is_live_data:
        local_path = Path("data/sales.csv")
        if local_path.exists():
            df_local = pd.read_csv(local_path)
            df_local["total_sales"] = df_local["quantity"] * df_local["unit_price"]
            df_local["order_date"] = pd.to_datetime(df_local["order_date"])
            df_local["sales_month"] = df_local["order_date"].dt.strftime("%Y-%m")
            
            kpis = {
                "total_revenue": float(df_local["total_sales"].sum()),
                "total_orders": int(df_local["order_id"].nunique()),
                "total_units_sold": int(df_local["quantity"].sum()),
                "total_customers": int(df_local["customer_id"].nunique()),
                "avg_order_value": float(round(df_local["total_sales"].sum() / df_local["order_id"].nunique(), 2))
            }
            
            df_category = (
                df_local.groupby("category", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum")
                )
                .sort_values("total_revenue", ascending=False)
            )
            df_category["avg_order_value"] = (df_category["total_revenue"] / df_category["total_orders"]).round(2)

            df_city = (
                df_local.groupby("city", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum")
                )
                .sort_values("total_revenue", ascending=False)
            )
            df_city["avg_order_value"] = (df_city["total_revenue"] / df_city["total_orders"]).round(2)

            df_monthly = (
                df_local.groupby("sales_month", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum")
                )
                .sort_values("sales_month", ascending=True)
            )
            df_monthly["avg_order_value"] = (df_monthly["total_revenue"] / df_monthly["total_orders"]).round(2)

    # ------------------ KPI Metrics Row ------------------
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Revenue", f"₹{kpis['total_revenue']:,.2f}")
    col2.metric("Total Orders", f"{kpis['total_orders']:,}")
    col3.metric("Units Sold", f"{kpis['total_units_sold']:,}")
    col4.metric("Unique Customers", f"{kpis['total_customers']:,}")
    col5.metric("Avg Order Value", f"₹{kpis['avg_order_value']:,.2f}")

    st.divider()

    # ------------------ Interactive Visualizations ------------------
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("Revenue by Category (Gold Layer)")
        if not df_category.empty:
            fig_cat = px.bar(
                df_category,
                x="category",
                y="total_revenue",
                color="total_revenue",
                color_continuous_scale="Blues",
                text="total_revenue",
                hover_data=["total_orders", "total_quantity_sold", "avg_order_value"],
                labels={"total_revenue": "Revenue (₹)", "category": "Product Category"}
            )
            fig_cat.update_traces(texttemplate="₹%{text:,.0f}", textposition="outside")
            fig_cat.update_layout(height=380, showlegend=False, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("No category data available.")

    with row1_col2:
        st.subheader("Revenue by City (Gold Layer)")
        if not df_city.empty:
            fig_city = px.bar(
                df_city,
                x="city",
                y="total_revenue",
                color="total_revenue",
                color_continuous_scale="Teal",
                text="total_revenue",
                hover_data=["total_orders", "total_quantity_sold", "avg_order_value"],
                labels={"total_revenue": "Revenue (₹)", "city": "City"}
            )
            fig_city.update_traces(texttemplate="₹%{text:,.0f}", textposition="outside")
            fig_city.update_layout(height=380, showlegend=False, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_city, use_container_width=True)
        else:
            st.info("No city data available.")

    # ------------------ Monthly Revenue Trend ------------------
    st.subheader("Monthly Revenue Trend (Gold Layer)")
    if not df_monthly.empty:
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=df_monthly["sales_month"],
            y=df_monthly["total_revenue"],
            mode="lines+markers+text",
            text=[f"₹{v:,.0f}" for v in df_monthly["total_revenue"]],
            textposition="top center",
            line=dict(color="#2563EB", width=3),
            marker=dict(size=8, color="#1D4ED8"),
            name="Revenue"
        ))
        fig_trend.update_layout(
            height=350,
            xaxis_title="Billing Month",
            yaxis_title="Total Revenue (₹)",
            margin=dict(l=10, r=10, t=30, b=10)
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No monthly trend data available.")

    # ------------------ Detailed Expandable Tables ------------------
    with st.expander("🔍 View Detailed Gold Datasets & Query Results"):
        subtab_cat, subtab_city, subtab_month = st.tabs(["Category Table", "City Table", "Monthly Table"])
        with subtab_cat:
            st.dataframe(df_category, use_container_width=True)
        with subtab_city:
            st.dataframe(df_city, use_container_width=True)
        with subtab_month:
            st.dataframe(df_monthly, use_container_width=True)


# ==================================================
# TAB 2: INGESTION & ADF ORCHESTRATION
# ==================================================
with tab_ingestion:
    st.header("Bronze Ingestion & Pipeline Orchestration")
    
    st.subheader("1. Upload Sales CSV to ADLS Gen2 Bronze")
    uploaded_file = st.file_uploader(
        "Upload sales CSV file for ingestion into bronze/uploads/",
        type=["csv"],
        key="adls_uploader"
    )

    if uploaded_file is not None:
        st.write(f"**Filename:** `{uploaded_file.name}` | **Size:** `{uploaded_file.size:,} bytes`")
        try:
            preview_df = pd.read_csv(uploaded_file)
            st.dataframe(preview_df.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"CSV read error: {e}")
            preview_df = None

        if st.button("📤 Upload to ADLS Gen2 Bronze", type="primary", disabled=preview_df is None):
            try:
                with st.spinner("Uploading to ADLS Gen2 Bronze container..."):
                    result = upload_file(uploaded_file)
                st.success("Successfully uploaded to Azure Data Lake Storage!")
                st.write("**Container:**", result["container"])
                st.write("**Blob Path:**", result["blob_name"])
                st.code(result["url"])
            except Exception as e:
                st.error(f"Upload failed: {e}")

    st.divider()
    st.subheader("2. Azure Data Factory Pipeline Execution")
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        st.info("**Primary Medallion Pipeline:** `PL_Sales_Bronze_To_Silver_To_Gold`\n\nChains Bronze ingestion, schema validation, deduplication, and Gold analytical aggregations.")
    with col_p2:
        st.info("**Incremental Control Pipeline:** `PL_Sales_Incremental_Ingestion`\n\nDiscovers unconsumed Bronze files, prevents duplicates via Delta MERGE, and logs status to the control table.")

    st.caption("Trigger runs directly via Azure Data Factory CLI/REST API: `az datafactory pipeline create-run --factory-name zaid-capstone-adf --resource-group my-group --name <pipeline-name>`")


# ==================================================
# TAB 3: DATA QUALITY & AUDIT LOGS
# ==================================================
with tab_quality:
    st.header("Data Quality Monitoring & Delta Control Table")
    
    st.markdown("""
    ### Validation Rules Applied in PySpark:
    1. **Order Identifier:** `order_id` must be non-null and non-empty.
    2. **Order Date:** `order_date` must parse into a valid `DateType` (`YYYY-MM-DD`).
    3. **Quantity:** `quantity` must be a positive integer (`> 0`).
    4. **Unit Price:** `unit_price` must be non-negative (`>= 0.0`).
    5. **Category:** `category` must be a trimmed non-empty string.
    6. **City:** `city` must be a trimmed non-empty string.
    7. **Deduplication:** Deduplication on `order_id` retains the latest order entry deterministically.
    """)
    
    st.divider()
    st.subheader("Delta Ingestion Control Table (`file_processing_history`)")
    
    try:
        df_control = databricks_client.get_ingestion_control_history()
        if not df_control.empty:
            st.dataframe(df_control, use_container_width=True)
        else:
            st.info("No processing history entries found yet in Delta control table.")
    except Exception as e:
        st.warning(f"Could not load control table: {e}")
