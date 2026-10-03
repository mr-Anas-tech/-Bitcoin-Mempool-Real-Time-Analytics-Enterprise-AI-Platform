# ⚡ Bitcoin Mempool Real-Time Analytics & Enterprise AI Platform

An end-to-end, enterprise-grade real-time data engineering, analytics, and intelligence platform built to monitor Bitcoin mempool metrics, transaction throughput, fee anomalies, and whale transaction behaviors at scale.

This repository orchestrates a multi-tier streaming pipeline: **WebSocket Ingestion (Azure Event Hubs) ➔ Lakehouse Storage (ADLS Gen2 Avro) ➔ Distributed ETL (Azure Databricks PySpark) ➔ Data Warehousing (Snowflake) ➔ Analytics Engineering (dbt Core) ➔ Interactive Dashboards (Streamlit & Power BI) ➔ Embedded Generative AI (Google Gemini)**.

---

## 🏛 System Architecture & End-to-End Pipeline
```


┌─────────────────────────────────┐
│ Mempool.space WebSocket Stream  │
│     wss://mempool.space/ws      │
└────────────────┬────────────────┘
│ (Live Streams: blocks, mempool-blocks, stats)
▼
┌─────────────────────────────────────────────────────────────────┐
│ AZURE APP SERVICE INGESTION (mempoolwebservice | Python 3.12) │
│   • Flask Health Probe (Port 8100)                              │
│   • Byte-Aware 1MB Dynamic Batching Engine                      │
└────────────────┬────────────────────────────────────────────────┘
│ (AMQP / TLS 1.3)
▼
┌─────────────────────────────────────────────────────────────────┐
│ AZURE EVENT HUBS (mempoolspace-Events / mempool_realtime...)  │
│   • Azure Native Auto-Capture Enabled                           │
└────────────────┬────────────────────────────────────────────────┘
│ (Apache Avro Binary Format)
▼
┌─────────────────────────────────────────────────────────────────┐
│ AZURE DATA LAKE STORAGE GEN2 (ADLS Gen2 - Bronze Layer)         │
└────────────────┬────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────┐
│ AZURE DATABRICKS (PySpark Medallion Transformation Engine)      │
│   • Bronze ➔ Silver (JSON Exploding & Deduplication)            │
│   • Silver ➔ Gold (Fee Percentile Extraction & Parquet Storage) │
└────────────────┬────────────────────────────────────────────────┘
│ (Parquet Files)
▼
┌─────────────────────────────────────────────────────────────────┐
│ SNOWFLAKE DATA WAREHOUSE (MEMPOOL_DB.STG_MEMPOOL_PARSED)      │
│   • Storage Integration via Azure Service Principal             │
│   • Automated Serverless Ingestion Tasks (50M+ Rows)            │
└────────────────┬────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────┐
│ dbt (DATA BUILD TOOL) TRANSFORMATIONS                           │
│   • Bronze Layer: Incremental Staging (stg_mempool)           │
│   • Silver Layer: Feature Engineering & Window Deduplication    │
│   • Gold Layer: Data Marts (fct_mempool_daily, _hourly, etc)│
└────────────────┬────────────────────────────────────────────────┘
│
┌────────┴──────────────────────────┐
▼                                   ▼
┌───────────────────────────────┐   ┌───────────────────────────────┐
│ STREAMLIT DASHBOARD & AI      │   │ POWER BI EXECUTIVE DASHBOARD  │
│   • Live Snowflake Queries    │   │   • Daily Throughput Analytics│
│   • Local Parquet Fallback    │   │   • Block Efficiency Metrics  │
│   • Google Gemini AI Agent    │   │   • Congestion Breakdown      │
└───────────────────────────────┘   └───────────────────────────────┘
```
---

## 🌐 Ingestion Architecture (Azure Event Hubs & Blob Capture)

### Cloud Infrastructure
* **PaaS Hosting:** Azure App Service (`mempoolwebservice`, Region: West US 3) running Python 3.12 stack with a built-in logging stream and a Flask health probe on port `8100`.
* **Streaming Bus:** Azure Event Hubs (`mempoolspace-Events / mempool_realtime_streamig`) decoupling WebSocket edge ingestion from downstream engines.
* **Auto-Capture Lakehouse Sink:** Automatically persists raw binary message streams into compact **Apache Avro** files stored in ADLS Gen2 (`abfss://bronze@mempoolspaceacc.dfs.core.windows.net/`).
* **Resilience & Dynamic Batching:** Implements client-side dynamic 1MB payload batching (`producer.create_batch()`) to intercept overflow exceptions during network volatility spikes.
### VIDEO:

https://github.com/user-attachments/assets/668aad4b-a665-4d04-b7f5-b729a83c68f6







---

## 🔥 Processing Engine (Azure Databricks PySpark)

The ETL processing engine on Azure Databricks applies a 3-tier **Medallion Architecture**:
1. **Bronze Layer:** Connects securely to ADLS Gen2 using Azure Key Vault secrets (`dbutils.secrets.get()`) to read raw Avro streams.
2. **Silver Layer:** Deserializes raw string payloads dynamically via `schema_of_json()` and flattens array structures with `explode_outer()`.
3. **Gold Layer:** Extracts fee percentile metrics ($P_{10}$ to $P_{99}$) and writes compressed, query-optimized Parquet files to `Mempool_parsing/`.
* **Workflow Orchestration:** Scheduled via Databricks Workflows every 5 hours (`mempool_parsing` pipeline) with enterprise Key Vault secret hardening.

### VIDEO:


https://github.com/user-attachments/assets/dd5ce13a-abdc-4083-81a1-ff0a83521104


---

## ❄️ Data Warehousing & Ingestion (Snowflake)

* **Database Context:** `MEMPOOL_DB`
* **Storage Integration:** `azure_adls_snowflake_int` (Zero-Egress Azure Service Principal Authentication).
* **Automated Ingestion Task:** Serverless background task (`load_mempool_azure_task`) executing on a 360-minute schedule.
* **Scale Proof:** Successfully processed **50,119,413+ active rows** (~1 GB compressed micro-partitions) with sub-second lookup latency.

### VIDEO:

https://github.com/user-attachments/assets/5243c5b2-4deb-4f7c-bb97-69212c0671f4

---

## 💎 Data Modeling & Analytics Engineering (dbt Core + Snowflake)

[![dbt CI/CD Pipeline](https://github.com/mr-Anas-tech/mempool/actions/workflows/ci_cd_dbt.yml/badge.svg)](https://github.com/mr-Anas-tech/mempool/actions)
![dbt Core](https://img.shields.io/badge/dbt--core-1.7.0-orange?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?logo=githubactions)

### dbt Layers Strategy
1. **Bronze (`stg_mempool.sql`):** Incremental materialization with surrogate key hashing (`dbt_utils.generate_surrogate_key`) and dynamic watermark filtering on `enqueued_time`.
2. **Silver (`int_mempool.sql`):** Window deduplication (`ROW_NUMBER() OVER (PARTITION BY mempool_id ORDER BY enqueued_time DESC)`), calculation of Fee Volatility Spreads ($P_{99} - P_{10}$), Fee Skewness Ratios, and Block Compression Efficiency.
3. **Gold (`fct_mempool_daily`, `fct_mempool_hourly`, `fct_mempool_minutely`):**
   * `fct_mempool_daily`: Clustered by `['mempool_date']`.
   * `fct_mempool_hourly`: Multi-column clustered by `['time_hour', 'network_congestion_status']`.
   * `fct_mempool_minutely`: Unclustered high-frequency table to eliminate background re-clustering cost overhead.
### VIDEO:


https://github.com/user-attachments/assets/fffe2aa9-9820-4e24-a422-93d6badbcdb9



### DevOps & Continuous Integration (CI/CD)
Automated via GitHub Actions (`.github/workflows/ci_cd_dbt.yml`) on every pull request and push to `main`. It dynamically injects encrypted repository secrets (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.) into `~/.dbt/profiles.yml` to execute `dbt deps`, `dbt debug`, and `dbt build`.

### VIDEO:




https://github.com/user-attachments/assets/9ac51e63-3cc3-449d-a6cd-2916b67526dc



---

## ⚡ Streamlit Real-Time Analytics & Gemini AI Agent

### Key Capabilities
* **Dual Time-Horizon Dashboards:** Toggles between Hourly Aggregations (`fct_mempool_hourly`) and High-Frequency Minute Real-Time Spikes (`fct_mempool_minutely`).
* **Resilient Dual-Source Fallback System:** Directly queries Snowflake, falling back automatically to cached local Parquet files (`hourly_mempool_backup.parquet` & `minutely_mempool_backup.parquet`) if the database is unreachable.
* **In-App AI Conversational Agent:** Embedded sidebar chatbot powered by `gemini-3.1-flash-lite` that receives real-time mempool context for dynamic network analysis.

---

## 📊 Power BI Executive Dashboard

An executive-level interactive reporting solution built for macro monitoring of network congestion and miner economics.

### Power BI Dashboard Overview:


### High-Level Metrics & Visuals
* **Total Daily Transactions:** Historical metric tracking total processed volume (e.g., **2.62bn** total volume, **242.03M – 370.60M** daily).
* **Total Daily Miner Revenue & Block Size:** Evaluates capacity usage (~**1.59 MB – 1.73 MB**) and miner fee harvests.
* **Fee Volatility & Spread (Line Chart):** Maps $P_{10}$, $P_{99}$, and Median fee trends across time slices.
* **Block Efficiency vs. Fee Skewness (Scatter Plot):** Correlates daily average compression ratios with fee inequality.
* **Network Congestion Breakdown (Donut Chart):** Segments activity into **Low Congestion** (~92-95%), **Moderate Congestion** (~3-6%), and **High Congestion** (~0.1-1.5%).

---

## 🛠 Setup & Local Deployment Guide

### 1. Prerequisites
* Python 3.10+
* Git installed

### 2. Installation
```bash
git clone [https://github.com/mr-Anas-tech/mempool.git](https://github.com/mr-Anas-tech/mempool.git)
cd mempool
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure Secrets
```
 (.streamlit/secrets.toml)
Create a .streamlit/secrets.toml file in the project root:
[snowflake]
user = "YOUR_SNOWFLAKE_USER"
password = "YOUR_SNOWFLAKE_PASSWORD"
account = "YOUR_SNOWFLAKE_ACCOUNT"
warehouse = "YOUR_SNOWFLAKE_WAREHOUSE"
database = "MEMPOOL_DB"
schema = "MEMPOOL_DBT"
role = "YOUR_SNOWFLAKE_ROLE"

[gemini]
api_key = "YOUR_GEMINI_API_KEY"
```
4. Local Execution Steps
 * Run Local Parquet Backup Extractor:

```
  python create_backup.py
 ``` 

 * Execute dbt Models & Validation:
```
   dbt deps
dbt debug
dbt build
```
 * Launch Streamlit Web Platform:
 * ```
   streamlit run eda_analysis.py
``
📂 Repository Structure
```.
├── .github/
│   └── workflows/
│       └── ci_cd_dbt.yml            # CI/CD pipeline for dbt compilation & testing
├── .streamlit/
│   └── secrets.toml                 # Encrypted database & AI credentials
├── data/
│   ├── hourly_mempool_backup.parquet   # Cached offline Parquet data (Hourly)
│   ├── minutely_mempool_backup.parquet # Cached offline Parquet data (Minutely)
│   └── powerbi_dashboard_overview.png  # Dashboard visual previews
├── models/                          # dbt transformation models (Bronze, Silver, Gold)
├── eda_analysis.py                  # Streamlit Dashboard application & Gemini AI integration
├── create_backup.py                 # Automated Snowflake to Parquet exporter script
├── dbt_project.yml                  # dbt configuration parameters
├── requirements.txt                 # Project dependencies
└── README.md                        # Master documentation file
```
👤 Author & Maintainer
 * Developer: Muhammad Anas
 * GitHub Profile: https://github.com/mr-Anas-tech
 * Deployment Platforms: Azure App Service / Databricks / Snowflake / Streamlit / Power BI






