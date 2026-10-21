"""
Azure Cloud Data Engineering & Analytics Platform
Executive Lakehouse BI Dashboard - Enterprise Grade
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Local architecture integration modules
from azure_storage import upload_file
import databricks_client


# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & METADATA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Azure Lakehouse Intelligence | Medallion Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------------------------------------------------------
# HIGH-END MODERN CSS STYLING (Google Fonts, Glassmorphism, Micro-animations)
# -----------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* Global typography */
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        color: #0f172a;
    }

    /* Main container padding */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
        max-width: 100%;
    }

    /* Top App Header Card */
    .enterprise-header {
        background: linear-gradient(135deg, #0b192c 0%, #1e3a8a 50%, #1d4ed8 100%);
        border-radius: 16px;
        padding: 24px 32px;
        color: #ffffff;
        margin-bottom: 24px;
        box-shadow: 0 10px 25px -5px rgba(30, 58, 138, 0.25), 0 8px 10px -6px rgba(30, 58, 138, 0.2);
        position: relative;
        overflow: hidden;
    }
    .enterprise-header::before {
        content: "";
        position: absolute;
        top: -50%;
        right: -10%;
        width: 320px;
        height: 320px;
        background: radial-gradient(circle, rgba(56, 189, 248, 0.18) 0%, rgba(255,255,255,0) 70%);
        border-radius: 50%;
        pointer-events: none;
    }
    .header-badge-row {
        display: flex;
        gap: 8px;
        align-items: center;
        margin-bottom: 10px;
        flex-wrap: wrap;
    }
    .platform-pill {
        background: rgba(255, 255, 255, 0.14);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 255, 255, 0.2);
        color: #e0f2fe;
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        padding: 4px 10px;
        border-radius: 9999px;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .status-live-dot {
        width: 7px;
        height: 7px;
        background-color: #22c55e;
        border-radius: 50%;
        box-shadow: 0 0 8px #22c55e;
        animation: pulse-dot 2s infinite;
    }
    @keyframes pulse-dot {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(34, 197, 94, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }
    }
    .header-title {
        font-size: 1.85rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin: 0;
        line-height: 1.2;
        color: #ffffff;
    }
    .header-desc {
        font-size: 0.95rem;
        color: #93c5fd;
        margin-top: 6px;
        margin-bottom: 0;
        font-weight: 400;
    }
    .architecture-breadcrumb {
        margin-top: 16px;
        padding-top: 14px;
        border-top: 1px solid rgba(255, 255, 255, 0.12);
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 0.78rem;
        color: #cbd5e1;
        flex-wrap: wrap;
    }
    .breadcrumb-step {
        background: rgba(15, 23, 42, 0.35);
        padding: 3px 10px;
        border-radius: 6px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        font-weight: 600;
        color: #f1f5f9;
    }
    .breadcrumb-arrow {
        color: #38bdf8;
        font-weight: 700;
    }

    /* KPI Metric Cards */
    .kpi-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 18px 20px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05), 0 1px 2px rgba(0, 0, 0, 0.03);
        transition: all 0.25s ease-in-out;
        position: relative;
        overflow: hidden;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 20px -3px rgba(15, 23, 42, 0.08), 0 4px 6px -2px rgba(15, 23, 42, 0.03);
        border-color: #cbd5e1;
    }
    .kpi-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 12px;
    }
    .kpi-label {
        font-size: 0.73rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #64748b;
    }
    .kpi-icon-wrap {
        width: 36px;
        height: 36px;
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.15rem;
    }
    .icon-blue { background: #eff6ff; color: #2563eb; }
    .icon-emerald { background: #ecfdf5; color: #059669; }
    .icon-indigo { background: #eef2ff; color: #4f46e5; }
    .icon-violet { background: #f5f3ff; color: #7c3aed; }
    .icon-amber { background: #fffbeb; color: #d97706; }

    .kpi-value {
        font-size: 1.85rem;
        font-weight: 800;
        color: #0f172a;
        letter-spacing: -0.03em;
        line-height: 1.1;
    }
    .kpi-footer {
        margin-top: 10px;
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 0.74rem;
        font-weight: 600;
    }
    .badge-positive {
        color: #059669;
        background: #ecfdf5;
        padding: 2px 7px;
        border-radius: 5px;
        display: inline-flex;
        align-items: center;
        gap: 3px;
    }
    .badge-neutral {
        color: #475569;
        background: #f1f5f9;
        padding: 2px 7px;
        border-radius: 5px;
    }

    /* Modern Card Container for charts & content */
    .content-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 22px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
        margin-bottom: 20px;
    }
    .content-card-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: #0f172a;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .content-card-subtitle {
        font-size: 0.8rem;
        color: #64748b;
        margin-bottom: 16px;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #f1f5f9;
        padding: 6px;
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        margin-bottom: 20px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 9px 18px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.86rem;
        color: #475569;
        border: none;
        background-color: transparent;
        transition: all 0.2s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #0f172a;
        background-color: rgba(255, 255, 255, 0.6);
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff !important;
        color: #1e3a8a !important;
        box-shadow: 0 2px 5px rgba(0, 0, 0, 0.06);
    }

    /* Pipeline Status Pill Cards */
    .pipeline-card {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .pipeline-status-badge {
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 3px 8px;
        border-radius: 9999px;
    }
    .badge-success-solid {
        background: #dcfce7;
        color: #15803d;
        border: 1px solid #bbf7d0;
    }
    .badge-ready-solid {
        background: #e0f2fe;
        color: #0369a1;
        border: 1px solid #bae6fd;
    }

    /* Rule item card */
    .rule-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 12px 16px;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 12px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .rule-num {
        background: #eff6ff;
        color: #1d4ed8;
        font-weight: 700;
        font-size: 0.78rem;
        width: 28px;
        height: 28px;
        border-radius: 7px;
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
    }

    /* Streamlit overrides */
    div[data-testid="stMetricValue"] {
        font-size: 1.85rem !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em !important;
    }
    [data-testid="stSidebar"] {
        background-color: #f8fafc;
        border-right: 1px solid #e2e8f0;
    }
    
    /* Code block styling */
    pre, code {
        font-family: 'JetBrains Mono', monospace !important;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# TOP ENTERPRISE HERO BANNER
# -----------------------------------------------------------------------------
st.markdown("""
<div class="enterprise-header">
    <div class="header-badge-row">
        <span class="platform-pill"><span class="status-live-dot"></span> Azure Cloud Data Lakehouse</span>
        <span class="platform-pill">⚡ Databricks SQL Engine</span>
        <span class="platform-pill">🛡️ Unity Catalog Governed</span>
        <span class="platform-pill">📁 Delta Lake Parquet</span>
    </div>
    <h1 class="header-title">Enterprise Sales Analytics Platform</h1>
    <p class="header-desc">Multi-tier Medallion architecture delivering clean, validated, and aggregated commercial intelligence.</p>
    <div class="architecture-breadcrumb">
        <span class="breadcrumb-step">📤 CSV Upload</span>
        <span class="breadcrumb-arrow">➔</span>
        <span class="breadcrumb-step">🥉 ADLS Gen2 Bronze</span>
        <span class="breadcrumb-arrow">➔</span>
        <span class="breadcrumb-step">⚙️ ADF Orchestration</span>
        <span class="breadcrumb-arrow">➔</span>
        <span class="breadcrumb-step">🥈 PySpark Silver Delta</span>
        <span class="breadcrumb-arrow">➔</span>
        <span class="breadcrumb-step">🥇 Gold Aggregates</span>
        <span class="breadcrumb-arrow">➔</span>
        <span class="breadcrumb-step">📊 Databricks SQL Warehouse</span>
    </div>
</div>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# SIDEBAR CONTROLS & ENVIRONMENT STATUS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px;">
        <img src="https://img.icons8.com/color/96/azure-1.png" width="38" />
        <div>
            <div style="font-weight: 800; font-size: 1.05rem; color: #0f172a; line-height: 1.2;">Azure Data Lake</div>
            <div style="font-size: 0.72rem; color: #64748b; font-weight: 600; text-transform: uppercase;">Infrastructure Monitor</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Cloud Resources Status Box
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "zaid123")
    st.markdown(f"""
    <div style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px; margin-bottom: 16px; font-size: 0.78rem;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #64748b;">Storage:</span>
            <span style="font-weight: 600; color: #0f172a; font-family: 'JetBrains Mono', monospace;">{storage_account}</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #64748b;">Workspace:</span>
            <span style="font-weight: 600; color: #0f172a;">zaid-capstone-dbx</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #64748b;">Data Factory:</span>
            <span style="font-weight: 600; color: #0f172a;">zaid-capstone-adf</span>
        </div>
        <div style="display: flex; justify-content: space-between;">
            <span style="color: #64748b;">SQL Warehouse:</span>
            <span style="font-weight: 600; color: #059669;">● Serverless Active</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-size: 0.78rem; font-weight: 700; text-transform: uppercase; color: #475569; letter-spacing: 0.05em; margin-bottom: 6px;'>Analytics Data Backend</div>", unsafe_allow_html=True)
    datasource_mode = st.radio(
        "Backend Selection",
        ["Databricks SQL Warehouse (Direct)", "Local Silver Fallback"],
        index=0,
        label_visibility="collapsed",
        help="Connect directly to Databricks Serverless SQL Warehouse over port 443 with keyless Azure AD Bearer auth, or use local CSV preview."
    )

    col_btn, col_ref = st.columns([3, 1])
    with col_btn:
        if st.button("⚡ Test SQL Connection", use_container_width=True):
            with st.spinner("Pinging SQL Warehouse..."):
                res = databricks_client.test_connection()
                if res.get("success"):
                    st.success(f"Connected: {res['catalog']}.{res['schema']}")
                else:
                    st.error(f"Error: {res.get('error')[:60]}...")
    with col_ref:
        if st.button("🔄", help="Refresh Dashboard Data", use_container_width=True):
            st.rerun()

    st.markdown("---")
    st.markdown("""
    <div style="font-size: 0.72rem; color: #64748b; line-height: 1.5;">
        <b>Security Mode:</b> Keyless Azure AD RBAC<br>
        <b>Format:</b> Delta Lake (ACID Transactional)<br>
        <b>Governance:</b> Unity Catalog External Tables<br>
        <b>Compute:</b> Serverless Auto-Suspend (10m)
    </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# MAIN DASHBOARD TABS
# -----------------------------------------------------------------------------
tab_analytics, tab_orchestration, tab_governance = st.tabs([
    "📊 Executive Performance BI",
    "🚀 Bronze Ingestion & ADF Orchestration",
    "🛡️ Data Quality & Audit Logs",
])


# =============================================================================
# TAB 1: EXECUTIVE PERFORMANCE BI (DASHBOARD)
# =============================================================================
with tab_analytics:
    # Fetch Data
    df_category = pd.DataFrame()
    df_city = pd.DataFrame()
    df_monthly = pd.DataFrame()
    kpis = {
        "total_revenue": 0.0,
        "total_orders": 0,
        "total_units_sold": 0,
        "total_customers": 0,
        "avg_order_value": 0.0,
    }
    is_live_data = False

    if datasource_mode == "Databricks SQL Warehouse (Direct)":
        try:
            with st.spinner("Querying Databricks Serverless SQL Warehouse..."):
                df_category = databricks_client.get_gold_category_sales()
                df_city = databricks_client.get_gold_city_sales()
                df_monthly = databricks_client.get_gold_monthly_sales()
                kpis = databricks_client.get_overall_kpis()
                is_live_data = True
        except Exception as e:
            st.warning(f"Live Databricks SQL query encountered an issue ({e}). Showing local fallback dataset.")

    # Local fallback
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
                "avg_order_value": float(round(df_local["total_sales"].sum() / df_local["order_id"].nunique(), 2)),
            }

            df_category = (
                df_local.groupby("category", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum"),
                )
                .sort_values("total_revenue", ascending=False)
            )
            df_category["avg_order_value"] = (df_category["total_revenue"] / df_category["total_orders"]).round(2)

            df_city = (
                df_local.groupby("city", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum"),
                )
                .sort_values("total_revenue", ascending=False)
            )
            df_city["avg_order_value"] = (df_city["total_revenue"] / df_city["total_orders"]).round(2)

            df_monthly = (
                df_local.groupby("sales_month", as_index=False)
                .agg(
                    total_orders=("order_id", "nunique"),
                    total_quantity_sold=("quantity", "sum"),
                    total_revenue=("total_sales", "sum"),
                )
                .sort_values("sales_month", ascending=True)
            )
            df_monthly["avg_order_value"] = (df_monthly["total_revenue"] / df_monthly["total_orders"]).round(2)

    # ------------------ TOP METRIC CARDS ------------------
    col_kpi1, col_kpi2, col_kpi3, col_kpi4, col_kpi5 = st.columns(5)

    source_tag = "Live Unity Catalog" if is_live_data else "Local Bronze CSV"

    with col_kpi1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-header">
                <span class="kpi-label">Gross Revenue</span>
                <div class="kpi-icon-wrap icon-emerald">₹</div>
            </div>
            <div class="kpi-value">₹{kpis['total_revenue']:,.0f}</div>
            <div class="kpi-footer">
                <span class="badge-positive">● {source_tag}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_kpi2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-header">
                <span class="kpi-label">Total Orders</span>
                <div class="kpi-icon-wrap icon-blue">📦</div>
            </div>
            <div class="kpi-value">{kpis['total_orders']:,}</div>
            <div class="kpi-footer">
                <span class="badge-neutral">Deduplicated IDs</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_kpi3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-header">
                <span class="kpi-label">Units Sold</span>
                <div class="kpi-icon-wrap icon-violet">📊</div>
            </div>
            <div class="kpi-value">{kpis['total_units_sold']:,}</div>
            <div class="kpi-footer">
                <span class="badge-neutral">Sum of Quantities</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_kpi4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-header">
                <span class="kpi-label">Unique Customers</span>
                <div class="kpi-icon-wrap icon-indigo">👥</div>
            </div>
            <div class="kpi-value">{kpis['total_customers']:,}</div>
            <div class="kpi-footer">
                <span class="badge-neutral">Distinct Count</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_kpi5:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-header">
                <span class="kpi-label">Avg Order Value</span>
                <div class="kpi-icon-wrap icon-amber">⚡</div>
            </div>
            <div class="kpi-value">₹{kpis['avg_order_value']:,.0f}</div>
            <div class="kpi-footer">
                <span class="badge-neutral">Revenue / Order</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom: 24px;'></div>", unsafe_allow_html=True)

    # ------------------ CHARTS ROW 1: REVENUE TREND & CATEGORY SHARE ------------------
    row1_left, row1_right = st.columns([1.6, 1.2])

    with row1_left:
        st.markdown("""
        <div class="content-card">
            <div class="content-card-title">📈 Monthly Revenue Trajectory</div>
            <div class="content-card-subtitle">Aggregated via Gold Delta Table (gold_monthly_sales)</div>
        """, unsafe_allow_html=True)

        if not df_monthly.empty:
            fig_trend = go.Figure()
            # Gradient Area Trace
            fig_trend.add_trace(go.Scatter(
                x=df_monthly["sales_month"],
                y=df_monthly["total_revenue"],
                mode="lines+markers",
                name="Total Revenue",
                line=dict(color="#2563eb", width=3.5, shape="spline", smoothing=1.1),
                marker=dict(size=8, color="#1e40af", line=dict(color="#ffffff", width=2)),
                fill="tozeroy",
                fillcolor="rgba(37, 99, 235, 0.12)",
                hovertemplate="<b>%{x}</b><br>Revenue: ₹%{y:,.0f}<extra></extra>"
            ))

            fig_trend.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(
                    showgrid=False,
                    color="#64748b",
                    tickfont=dict(family="Plus Jakarta Sans", size=11, weight="bold")
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="#f1f5f9",
                    color="#64748b",
                    tickformat="₹~s",
                    tickfont=dict(family="Plus Jakarta Sans", size=11)
                ),
                hovermode="x unified"
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("No monthly trend data found.")

        st.markdown("</div>", unsafe_allow_html=True)

    with row1_right:
        st.markdown("""
        <div class="content-card">
            <div class="content-card-title">🍩 Category Revenue Distribution</div>
            <div class="content-card-subtitle">Contribution by Product Category (gold_category_sales)</div>
        """, unsafe_allow_html=True)

        if not df_category.empty:
            custom_colors = ["#2563eb", "#38bdf8", "#818cf8", "#34d399", "#fbbf24"]
            fig_donut = go.Figure(data=[go.Pie(
                labels=df_category["category"],
                values=df_category["total_revenue"],
                hole=0.6,
                marker=dict(colors=custom_colors, line=dict(color="#ffffff", width=2)),
                textinfo="label+percent",
                textposition="outside",
                hovertemplate="<b>%{label}</b><br>Revenue: ₹%{value:,.0f}<br>Share: %{percent}<extra></extra>"
            )])
            fig_donut.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                font=dict(family="Plus Jakarta Sans", size=12)
            )
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No category data found.")

        st.markdown("</div>", unsafe_allow_html=True)

    # ------------------ CHARTS ROW 2: CITY BREAKDOWN & CATEGORY VOLUME ------------------
    row2_left, row2_right = st.columns(2)

    with row2_left:
        st.markdown("""
        <div class="content-card">
            <div class="content-card-title">🏙️ Commercial Performance by City</div>
            <div class="content-card-subtitle">Revenue and volume rankings from Gold Layer (gold_city_sales)</div>
        """, unsafe_allow_html=True)

        if not df_city.empty:
            fig_city = px.bar(
                df_city,
                x="city",
                y="total_revenue",
                color="total_revenue",
                color_continuous_scale=[[0, "#bae6fd"], [0.5, "#0284c7"], [1, "#0369a1"]],
                text="total_revenue",
                hover_data=["total_orders", "total_quantity_sold", "avg_order_value"],
                labels={"total_revenue": "Revenue (₹)", "city": "City"}
            )
            fig_city.update_traces(
                texttemplate="₹%{text:,.0f}",
                textposition="outside",
                marker=dict(line=dict(color="#ffffff", width=1))
            )
            fig_city.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                coloraxis_showscale=False,
                xaxis=dict(showgrid=False, color="#64748b", tickfont=dict(weight="bold")),
                yaxis=dict(showgrid=True, gridcolor="#f1f5f9", color="#64748b", tickformat="₹~s")
            )
            st.plotly_chart(fig_city, use_container_width=True)
        else:
            st.info("No city data found.")

        st.markdown("</div>", unsafe_allow_html=True)

    with row2_right:
        st.markdown("""
        <div class="content-card">
            <div class="content-card-title">🏷️ Category Volume vs Revenue</div>
            <div class="content-card-subtitle">Units sold vs revenue generation efficiency</div>
        """, unsafe_allow_html=True)

        if not df_category.empty:
            fig_cat_bar = px.bar(
                df_category,
                x="category",
                y="total_quantity_sold",
                color="category",
                color_discrete_sequence=["#6366f1", "#0ea5e9", "#10b981"],
                text="total_quantity_sold",
                hover_data=["total_revenue", "avg_order_value"],
                labels={"total_quantity_sold": "Units Sold", "category": "Product Category"}
            )
            fig_cat_bar.update_traces(
                texttemplate="%{text} units",
                textposition="outside",
                marker=dict(line=dict(color="#ffffff", width=1))
            )
            fig_cat_bar.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                xaxis=dict(showgrid=False, color="#64748b", tickfont=dict(weight="bold")),
                yaxis=dict(showgrid=True, gridcolor="#f1f5f9", color="#64748b")
            )
            st.plotly_chart(fig_cat_bar, use_container_width=True)
        else:
            st.info("No category data found.")

        st.markdown("</div>", unsafe_allow_html=True)

    # ------------------ DETAILED QUERY VIEWER ------------------
    with st.expander("🔍 Inspect Registered Gold Tables in Unity Catalog", expanded=False):
        sub_cat, sub_city, sub_month = st.tabs(["📁 gold_category_sales", "📁 gold_city_sales", "📁 gold_monthly_sales"])
        with sub_cat:
            st.dataframe(df_category, use_container_width=True)
        with sub_city:
            st.dataframe(df_city, use_container_width=True)
        with sub_month:
            st.dataframe(df_monthly, use_container_width=True)


# =============================================================================
# TAB 2: BRONZE INGESTION & ADF ORCHESTRATION
# =============================================================================
with tab_orchestration:
    st.markdown("""
    <div class="content-card">
        <div class="content-card-title">📤 Step 1: Upload Sales Dataset to ADLS Gen2 Bronze Container</div>
        <div class="content-card-subtitle">Raw CSV is uploaded directly into <code>bronze/uploads/</code> using Azure Identity (DefaultAzureCredential)</div>
    """, unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Select or drop CSV sales dataset for ingestion:",
        type=["csv"],
        key="adls_uploader_v2",
        help="Upload sales CSV file conforming to the Medallion ingestion schema."
    )

    if uploaded_file is not None:
        col_file_info1, col_file_info2, col_file_info3 = st.columns(3)
        col_file_info1.metric("Filename", uploaded_file.name)
        col_file_info2.metric("File Size", f"{uploaded_file.size:,} bytes")
        col_file_info3.metric("Format", "CSV (UTF-8)")

        try:
            preview_df = pd.read_csv(uploaded_file)
            st.markdown("<div style='font-size: 0.82rem; font-weight: 700; color: #475569; margin: 12px 0 6px 0;'>Payload Preview (First 5 Rows):</div>", unsafe_allow_html=True)
            st.dataframe(preview_df.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"Error reading uploaded CSV payload: {e}")
            preview_df = None

        if st.button("🚀 Push to Azure Data Lake (Bronze)", type="primary", disabled=preview_df is None):
            try:
                with st.spinner("Streaming blob to ADLS Gen2 Bronze container..."):
                    result = upload_file(uploaded_file)
                st.success("Successfully uploaded to ADLS Gen2 Bronze Layer!")
                st.json({
                    "Target Storage Account": os.getenv("AZURE_STORAGE_ACCOUNT", "zaid123"),
                    "Target Container": result.get("container", "bronze"),
                    "Blob Path": result.get("blob_name", ""),
                    "Resource URL": result.get("url", ""),
                    "Status": "STAGED_FOR_ADF_INGESTION"
                })
            except Exception as e:
                st.error(f"Upload failed: {e}")

    st.markdown("</div>", unsafe_allow_html=True)

    # ------------------ ADF PIPELINE SUMMARY ------------------
    st.markdown("""
    <div class="content-card">
        <div class="content-card-title">⚙️ Step 2: Azure Data Factory Orchestration Engine</div>
        <div class="content-card-subtitle">Orchestrates automated PySpark batch execution across the Medallion layers</div>
    """, unsafe_allow_html=True)

    col_pipe1, col_pipe2 = st.columns(2)

    with col_pipe1:
        st.markdown("""
        <div class="pipeline-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <span style="font-weight: 700; color: #0f172a; font-size: 0.95rem;">PL_Sales_Bronze_To_Silver_To_Gold</span>
                <span class="pipeline-status-badge badge-success-solid">SUCCEEDED (130s)</span>
            </div>
            <div style="font-size: 0.78rem; color: #475569; margin-bottom: 10px;">
                End-to-end Medallion pipeline. Ingests raw CSV, enforces 6 validation rules, tags rejections, writes cleaned Delta table, and generates Gold commercial aggregations.
            </div>
            <div style="font-size: 0.72rem; color: #64748b; font-family: 'JetBrains Mono', monospace;">
                Activities: IngestBronzeToSilver ➔ TransformSilverToGold
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_pipe2:
        st.markdown("""
        <div class="pipeline-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <span style="font-weight: 700; color: #0f172a; font-size: 0.95rem;">PL_Sales_Incremental_Ingestion</span>
                <span class="pipeline-status-badge badge-ready-solid">DEPLOYED</span>
            </div>
            <div style="font-size: 0.78rem; color: #475569; margin-bottom: 10px;">
                Incremental file discovery and state management. Computes SHA256 file hashes, records audit history in Delta control table, and performs idempotent MERGE operations.
            </div>
            <div style="font-size: 0.72rem; color: #64748b; font-family: 'JetBrains Mono', monospace;">
                Activities: DiscoverBronzeFiles ➔ IngestWithControlTable
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='font-size: 0.8rem; font-weight: 700; color: #0f172a; margin-top: 14px;'>CLI Run Command:</div>", unsafe_allow_html=True)
    st.code("""az datafactory pipeline create-run --factory-name zaid-capstone-adf --resource-group my-group --name PL_Sales_Bronze_To_Silver_To_Gold""", language="bash")

    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# TAB 3: DATA QUALITY & AUDIT LOGS
# =============================================================================
with tab_governance:
    st.markdown("""
    <div class="content-card">
        <div class="content-card-title">🛡️ Data Quality Rules Engine (PySpark Silver Layer)</div>
        <div class="content-card-subtitle">Every ingested record is evaluated against 6 deterministic data quality constraints before entering the cleaned Delta table</div>
    """, unsafe_allow_html=True)

    col_rules_left, col_rules_right = st.columns(2)

    rules_left = [
        ("1", "Order ID Constraint", "order_id IS NOT NULL AND TRIM(order_id) != ''", "Rejects missing or empty transaction keys"),
        ("2", "Date Format Verification", "TO_DATE(order_date, 'yyyy-MM-dd') IS NOT NULL", "Prevents unparseable or corrupted dates"),
        ("3", "Positive Quantity", "quantity > 0", "Enforces non-negative commercial inventory transactions"),
    ]

    rules_right = [
        ("4", "Unit Price Non-Negative", "unit_price >= 0.0", "Guarantees valid monetary values for revenue calculations"),
        ("5", "Category & City Populated", "category != '' AND city != ''", "Mandatory dimensions for downstream Gold aggregations"),
        ("6", "Deterministic Deduplication", "ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC)", "Retains latest valid version of duplicated records"),
    ]

    with col_rules_left:
        for num, title, expr, desc in rules_left:
            st.markdown(f"""
            <div class="rule-card">
                <div class="rule-num">R{num}</div>
                <div>
                    <div style="font-weight: 700; font-size: 0.82rem; color: #0f172a;">{title}</div>
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; color: #2563eb; margin: 2px 0;">{expr}</div>
                    <div style="font-size: 0.72rem; color: #64748b;">{desc}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    with col_rules_right:
        for num, title, expr, desc in rules_right:
            st.markdown(f"""
            <div class="rule-card">
                <div class="rule-num">R{num}</div>
                <div>
                    <div style="font-weight: 700; font-size: 0.82rem; color: #0f172a;">{title}</div>
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; color: #2563eb; margin: 2px 0;">{expr}</div>
                    <div style="font-size: 0.72rem; color: #64748b;">{desc}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ------------------ DELTA CONTROL TABLE AUDIT LOGS ------------------
    st.markdown("""
    <div class="content-card">
        <div class="content-card-title">📜 Delta Ingestion Control Table (file_processing_history)</div>
        <div class="content-card-subtitle">Real-time audit log of file processing status, record counts, and pipeline execution tokens</div>
    """, unsafe_allow_html=True)

    try:
        df_control = databricks_client.get_ingestion_control_history()
        if not df_control.empty:
            st.dataframe(df_control, use_container_width=True)
        else:
            st.markdown("""
            <div style="background: #f8fafc; border: 1px dashed #cbd5e1; border-radius: 8px; padding: 18px; text-align: center; color: #64748b; font-size: 0.84rem;">
                No incremental files logged yet. Trigger <code>PL_Sales_Incremental_Ingestion</code> to generate audit entries in Unity Catalog.
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.warning(f"Control table query returned: {e}")

    st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------
st.markdown("""
<div style="text-align: center; margin-top: 36px; padding-top: 20px; border-top: 1px solid #e2e8f0; font-size: 0.76rem; color: #94a3b8;">
    Azure Cloud Data Engineering Capstone • Architecture by Zaid • Built with Streamlit, Azure Data Factory, Delta Lake, Unity Catalog & Databricks SQL
</div>
""", unsafe_allow_html=True)
