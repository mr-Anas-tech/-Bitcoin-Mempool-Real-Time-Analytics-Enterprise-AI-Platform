import os
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.connector import connect
from google import genai

# Page Configuration
st.set_page_config(
    page_title="Bitcoin Mempool AI Analytics Platform", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling for Dark Theme, Modern KPI Cards & Upgraded Chat UI
st.markdown("""
    <style>
    .main { background-color: #0E1117; }
    div[data-testid="stMetricValue"] { font-size: 22px; color: #00FFAA; }
    
    /* Modern KPI Cards */
    .kpi-box {
        background: linear-gradient(135deg, #161B22 0%, #1C2128 100%);
        border: 1px solid #30363D;
        border-radius: 10px;
        padding: 14px 18px;
        margin-bottom: 12px;
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.4);
        transition: transform 0.2s ease-in-out;
    }
    .kpi-box:hover {
        transform: translateY(-2px);
        border-color: #58A6FF;
    }
    .kpi-title {
        color: #8B949E;
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 6px;
    }
    .kpi-value {
        color: #58A6FF;
        font-size: 19px;
        font-weight: 700;
        word-wrap: break-word;
    }
    .kpi-alert {
        color: #FF7B72;
        font-size: 16px;
        font-weight: 700;
        word-wrap: break-word;
    }
    .kpi-normal {
        color: #3FB950;
        font-size: 16px;
        font-weight: 700;
        word-wrap: break-word;
    }
    
    /* Sidebar Agent Badge */
    .agent-header {
        background: linear-gradient(90deg, #1f6feb 0%, #388bfd 100%);
        color: white;
        padding: 8px 12px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 14px;
        text-align: center;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# Helper function for rendering KPI Boxes
def render_kpi(title, value, is_alert=False, is_status=False):
    if is_status:
        val_str = str(value).upper()
        val_class = "kpi-normal" if any(x in val_str for x in ["NORMAL", "HEALTHY", "LOW"]) else "kpi-alert"
    else:
        val_class = "kpi-alert" if is_alert else "kpi-value"
        
    st.markdown(f"""
    <div class="kpi-box">
        <div class="kpi-title">{title}</div>
        <div class="{val_class}">{value}</div>
    </div>
    """, unsafe_allow_html=True)

# Main Title Section
st.title("⚡ Bitcoin Mempool Real-Time Analytics Platform")
st.caption("Deployed via Azure App Service / Streamlit | Real-time streaming via Event Hub, Databricks, Snowflake & dbt")
st.caption("Developer: Mahar Anas | GitHub: https://github.com/mr-Anas-tech")

# Snowflake Connection Helper
def get_sf_connection():
    return connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"]
    )

# Cacheable Data Loader with Fallback
@st.cache_data(ttl=60)
def load_mempool_data(table_name, backup_filename):
    os.makedirs("data", exist_ok=True)
    backup_path = os.path.join("data", backup_filename)
    
    try:
        conn = get_sf_connection()
        query = f"SELECT * FROM MEMPOOL_DB.MEMPOOL_DBT.{table_name} ORDER BY 1 DESC LIMIT 500;"
        df = pd.read_sql(query, conn)
        conn.close()
        df.columns = [col.lower() for col in df.columns]
        
        # Save backup copy to local Parquet
        df.to_parquet(backup_path, index=False)
        return df, "Snowflake DB (Live)"
        
    except Exception as e:
        if os.path.exists(backup_path):
            df = pd.read_parquet(backup_path)
            return df, "Parquet Local Backup (Offline)"
        else:
            raise Exception(f"Database Error & Backup Unavailable: {e}")

# Safe value extractor
def safe_val(row, keys, default=0.0):
    for k in keys:
        if k in row and pd.notnull(row[k]) and row[k] != 0:
            return row[k]
    return default

# Load Datasets
try:
    df_hourly, source_h = load_mempool_data("FCT_MEMPOOL_HOURS", "hourly_mempool_backup.parquet")
    df_minutely, source_m = load_mempool_data("FCT_MEMPOOL_MINUTES", "minutely_mempool_backup.parquet")
    
    if "Live" in source_h:
        st.sidebar.success(f"🟢 Data Source: {source_h}")
    else:
        st.sidebar.warning(f"🟡 Data Source: {source_h}")
        
except Exception as e:
    st.sidebar.error(f"❌ Data Load Error: {e}")
    st.stop()

view_option = st.sidebar.selectbox("Select View Horizon", ["Hourly Aggregations", "Minute Real-time Spikes"])

# ----------------- AI AGENT INTEGRATION (GEMINI) -----------------
st.sidebar.markdown("---")
st.sidebar.subheader("🤖 Mempool AI Agent (Gemini)")

# Initialize Chat History
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! I am your Mempool AI Assistant. Ask me anything about live metrics or fee spikes!"}
    ]

# Display Chat History
for msg in st.session_state.messages:
    st.sidebar.chat_message(msg["role"]).write(msg["content"])

# User Chat Input
if user_input := st.sidebar.chat_input("Ask AI Agent..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    st.sidebar.chat_message("user").write(user_input)
    
    # Live Data Context
    latest_h = df_hourly.iloc[0].to_dict() if not df_hourly.empty else {}
    latest_m = df_minutely.iloc[0].to_dict() if not df_minutely.empty else {}
    
    system_instruction = f"""
    You are an expert Bitcoin Mempool Analytics Assistant integrated into a real-time Streamlit dashboard.
    Respond strictly in clear English.
    
    Latest Live Mempool Metrics Context:
    [Hourly Summary]:
    - Network Congestion: {latest_h.get('network_congestion', 'N/A')}
    - Total Tx: {latest_h.get('total_transactions', 'N/A')}
    - Median Fee (P50): {latest_h.get('avg_fee_p50', 'N/A')} sat/vB
    - P99 Fee Spike: {latest_h.get('avg_fee_p99', 'N/A')} sat/vB
    - Fee Spike Alert: {latest_h.get('hourly_fee_alert', 'N/A')}
    
    [Minutely Summary]:
    - Fee Spike Alert (Minute): {latest_m.get('fee_spike_alert', 'N/A')}
    - Peak Fee P99: {latest_m.get('peak_fee_p99', 'N/A')} sat/vB
    - Whale Outlier Alert: {latest_m.get('fee_skewness_alert', 'N/A')}

    Answer user questions accurately, professionally, and concisely based on this data.
    """
    
    try:
        if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
            client = genai.Client(api_key=st.secrets["gemini"]["api_key"])
            
            # Formatting contents for google-genai client
            contents = []
            for msg in st.session_state.messages:
                role = "user" if msg["role"] == "user" else "model"
                contents.append(
                    {"role": role, "parts": [{"text": msg["content"]}]}
                )

                
            response = client.models.generate_content(
                model="gemini-3.1-flash-lite",
                contents=contents,
                config={"system_instruction": system_instruction}
            )
            ai_response = response.text
        else:
            ai_response = "⚠️ Gemini API key missing in .streamlit/secrets.toml!"

    except Exception as err:
        try:
            response = client.models.generate_content(
                model="models/gemini-3.1-flash-lite",
                contents=user_input,
                config={"system_instruction": system_instruction}
            )
            ai_response = response.text
        except Exception as e:
            ai_response = f"AI Error: {e}"

    st.session_state.messages.append({"role": "assistant", "content": ai_response})
    st.sidebar.chat_message("assistant").write(ai_response)

# ----------------- HOURLY VIEW -----------------
if view_option == "Hourly Aggregations":
    st.header("🕒 Hourly Mempool Deep-Dive Analysis")
    latest = df_hourly.iloc[0]

    # SECTION 1: SYSTEM ALERTS
    st.subheader("🚨 1. Network Status & System Alerts")
    a1, a2, a3 = st.columns(3)
    with a1:
        render_kpi("Network Congestion Status", safe_val(latest, ['network_congestion', 'network_congestion_status'], 'N/A'), is_status=True)
    with a2:
        render_kpi("Hourly Fee Alert", safe_val(latest, ['hourly_fee_alert', 'fee_spike_alert'], 'N/A'), is_status=True)
    with a3:
        render_kpi("Hourly Skewness Alert", safe_val(latest, ['hourly_skewness_alert', 'fee_skewness_alert'], 'N/A'), is_status=True)

    st.markdown("---")

    # SECTION 2: VOLUME & BLOCK METRICS
    st.subheader("📦 2. Block Throughput & Size Economics")
    
    tot_tx = safe_val(latest, ['total_transactions', 'tx_count'])
    tot_blocks = safe_val(latest, ['total_blocks', 'block_count'], 1)
    
    avg_tx = safe_val(latest, ['avg_transactions', 'avg_tx_per_block'])
    if avg_tx == 0 and tot_blocks > 0:
        avg_tx = tot_tx / tot_blocks

    max_tx = safe_val(latest, ['max_transactions', 'max_tx_per_block'])
    if max_tx == 0:
        max_tx = tot_tx

    v1, v2, v3, v4, v5, v6 = st.columns(6)
    with v1:
        render_kpi("Total Transactions", f"{int(tot_tx):,}")
    with v2:
        render_kpi("Total Blocks Mined", f"{int(tot_blocks):,}")
    with v3:
        render_kpi("Avg Tx / Block", f"{avg_tx:,.2f}")
    with v4:
        render_kpi("Max Tx / Block", f"{int(max_tx):,}")
    with v5:
        render_kpi("Total Block Size (MB)", f"{(safe_val(latest, ['total_block_size'])/1e6):,.2f}")
    with v6:
        render_kpi("Avg Block VSize", f"{safe_val(latest, ['avg_block_vsize']):,.2f}")

    st.markdown("---")

    # SECTION 3: FEE PERCENTILES
    st.subheader("📊 3. Sat/vB Fee Percentile Spread")
    f1, f2, f3, f4, f5, f6, f7 = st.columns(7)
    with f1:
        render_kpi("P10 Fee", f"{safe_val(latest, ['avg_fee_p10']):.2f}")
    with f2:
        render_kpi("P25 Fee", f"{safe_val(latest, ['avg_fee_p25']):.2f}")
    with f3:
        render_kpi("P50 (Median)", f"{safe_val(latest, ['avg_fee_p50', 'avg_median_fee']):.2f}")
    with f4:
        render_kpi("P75 Fee", f"{safe_val(latest, ['avg_fee_p75']):.2f}")
    with f5:
        render_kpi("P90 Fee", f"{safe_val(latest, ['avg_fee_p90']):.2f}")
    with f6:
        render_kpi("P95 Fee", f"{safe_val(latest, ['avg_fee_p95']):.2f}")
    with f7:
        render_kpi("P99 Fee Spike", f"{safe_val(latest, ['avg_fee_p99', 'max_fee_p99_hourly']):.2f}")

    # Percentile Line Chart
    fee_cols = [c for c in ['avg_fee_p10', 'avg_fee_p50', 'avg_fee_p90', 'avg_fee_p99'] if c in df_hourly.columns]
    if fee_cols:
        time_col = 'time_hour' if 'time_hour' in df_hourly.columns else df_hourly.columns[0]
        df_sorted = df_hourly.sort_values(time_col)
        fig_fees = px.line(
            df_sorted, 
            x=time_col, 
            y=fee_cols,
            title="Hourly Sat/vB Fee Percentile Trends & Spikes",
            template="plotly_dark"
        )
        st.plotly_chart(fig_fees, use_container_width=True)

    st.markdown("---")

    # SECTION 4: WHALE METRICS
    st.subheader("🐋 4. Whale Outliers & Miner Revenue Analysis")
    w1, w2, w3, w4, w5 = st.columns(5)
    with w1:
        render_kpi("Avg Fee Spread", f"{safe_val(latest, ['avg_fee_spread']):.7f}")
    with w2:
        render_kpi("Interquartile Fee (IQR)", f"{safe_val(latest, ['avg_iqr_fee' ,'avg_interquartile_fee']):.7f}")
    with w3:
        render_kpi("Fee Skewness Ratio", f"{safe_val(latest, ['avg_fee_skewness']):.7f}")
    with w4:
        render_kpi("Miner Rev Ratio", f"{safe_val(latest, ['avg_miner_revenue_per_byte' ,'avg_miner_revenue_ratio']):.7f}")
    with w5:
        render_kpi("Block Compactness", f"{safe_val(latest, ['avg_block_compression' ,'avg_block_compactness']):.7f}")

    col_chart1, col_chart2 = st.columns(2)
    time_col = 'time_hour' if 'time_hour' in df_hourly.columns else df_hourly.columns[0]
    df_chart_sorted = df_hourly.sort_values(time_col)
    with col_chart1:
        fig_vol = px.bar(
            df_chart_sorted, 
            x=time_col, 
            y='total_transactions' if 'total_transactions' in df_chart_sorted.columns else df_chart_sorted.columns[1], 
            color='network_congestion' if 'network_congestion' in df_chart_sorted.columns else None,
            title="Hourly Transaction Throughput & Network Congestion",
            template="plotly_dark"
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    with col_chart2:
        fee_col = 'total_fees_collected' if 'total_fees_collected' in df_chart_sorted.columns else 'total_fees'
        fig_rev = px.area(
            df_chart_sorted,
            x=time_col,
            y=fee_col if fee_col in df_chart_sorted.columns else df_chart_sorted.columns[1],
            title="Total Hourly Block Fees Collected",
            template="plotly_dark"
        )
        st.plotly_chart(fig_rev, use_container_width=True)

# ----------------- MINUTE VIEW -----------------
else:
    st.header("⚡ Minute Real-time Level Deep Analytics & Outliers")
    latest_min = df_minutely.iloc[0]

    st.subheader("🚨 1. Real-time System Alerts")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        render_kpi("Fee Spike Alert", safe_val(latest_min, ['fee_spike_alert'], 'N/A'), is_status=True)
    with m2:
        render_kpi("Whale Outlier Alert", safe_val(latest_min, ['fee_skewness_alert'], 'N/A'), is_status=True)
    with m3:
        render_kpi("Congestion Alert", safe_val(latest_min, ['congestion_alert'], 'N/A'), is_status=True)
    with m4:
        render_kpi("Network Congestion", safe_val(latest_min, ['network_congestion'], 'N/A'), is_status=True)

    st.markdown("---")

    st.subheader("📊 2. Minute Level Fee Percentiles (Sat/vB)")
    mp1, mp2, mp3, mp4, mp5 = st.columns(5)
    with mp1:
        render_kpi("P10 Fee", f"{safe_val(latest_min, ['avg_fee_p10']):.2f}")
    with mp2:
        render_kpi("P50 Fee (Median)", f"{safe_val(latest_min, ['avg_fee_p50', 'avg_median_fee']):.2f}")
    with mp3:
        render_kpi("P90 Fee", f"{safe_val(latest_min, ['avg_fee_p90']):.2f}")
    with mp4:
        render_kpi("P99 Fee", f"{safe_val(latest_min, ['avg_fee_p99']):.2f}")
    with mp5:
        render_kpi("Peak Fee (P99)", f"{safe_val(latest_min, ['peak_fee_p99']):.2f}")

    st.markdown("---")

    col_chart3, col_chart4 = st.columns(2)
    min_time_col = 'time_minute' if 'time_minute' in df_minutely.columns else df_minutely.columns[0]
    df_min_sorted = df_minutely.sort_values(min_time_col)
    
    with col_chart3:
        p99_col = 'peak_fee_p99' if 'peak_fee_p99' in df_min_sorted.columns else 'avg_fee_p99'
        fig_spike = px.line(
            df_min_sorted, 
            x=min_time_col, 
            y=p99_col if p99_col in df_min_sorted.columns else df_min_sorted.columns[1],
            title="Real-Time Minute Fee Spikes (p99 Level)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_spike, use_container_width=True)

    with col_chart4:
        skew_col = 'max_fee_skewness_ratio' if 'max_fee_skewness_ratio' in df_min_sorted.columns else 'avg_fee_skewness'
        fig_skew = px.scatter(
            df_min_sorted, 
            x=min_time_col, 
            y=skew_col if skew_col in df_min_sorted.columns else df_min_sorted.columns[1],
            color='fee_skewness_alert' if 'fee_skewness_alert' in df_min_sorted.columns else None,
            title="Fee Skewness / Whale Transaction Detection",
            template="plotly_dark"
        )
        st.plotly_chart(fig_skew, use_container_width=True)

with st.expander("🔍 View Raw Mart Table Data"):
    st.dataframe(df_hourly if view_option == "Hourly Aggregations" else df_minutely)
