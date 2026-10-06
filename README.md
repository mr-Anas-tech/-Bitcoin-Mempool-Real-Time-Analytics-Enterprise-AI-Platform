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
---

## 🌐 Ingestion Architecture (Azure Event Hubs & Blob Capture)

### Technical Architecture & Implementation
* *PaaS Edge Deployment:* Hosted on Azure App Service (mempoolwebservice, Region: West US 3) running a Python 3.12 stack with continuous background event-loop management, persistent WebSocket connection keep-alives, and a lightweight Flask health-check probe exposed on port 8100 for Azure infrastructure diagnostics.
* *Streaming Messaging Bus:* Utilizes Azure Event Hubs (mempoolspace-Events / mempool_realtime_streamig) to decouple memory-bound WebSocket edge ingestion from downstream data processing engines, guaranteeing zero-data-loss buffering during traffic spikes.
* *Auto-Capture Lakehouse Sink:* Leverages Azure Native Event Hubs Capture to automatically persist high-throughput binary message streams directly into *ADLS Gen2* (abfss://bronze@mempoolspaceacc.dfs.core.windows.net/) in append-only *Apache Avro* format.
* *Resilience & Dynamic Memory Management:* Features a client-side byte-aware dynamic batching mechanism using producer.create_batch(). The engine measures payload sizes before sending, flushing batches at 1MB boundaries to avoid API buffer overflow exceptions during periods of severe network activity.

### Why This Architecture? (Engineering Rationale)
* *Decoupling Production & Processing:* Direct-writing WebSocket payloads to databases causes schema locking, dropped sockets, and socket exhaustion under high load. Event Hubs acts as an elastic buffer that absorbs traffic bursts.
* *Schema Evolution Support:* Storing initial streams in raw Apache Avro ensures schema updates from external WebSocket feeds do not break downstream ETL consumers.
* *Zero Compute Storage Ingestion:* Event Hubs Capture persists raw streams to ADLS Gen2 directly at the storage infrastructure layer without needing continuously running VM compute clusters.

### Business Value
* *100% Data Retention & Zero Loss:* Ensures every block fee surge and mempool anomaly is captured without packet drop.
* *Predictable Ingestion Costs:* Native cloud streaming sinks reduce operational expenses compared to self-managed Kafka clusters.

### VIDEO:
https://github.com/user-attachments/assets/668aad4b-a665-4d04-b7f5-b729a83c68f6

---

## 🔥 Processing Engine (Azure Databricks PySpark)

### Technical Architecture & Medallion Pipeline
The distributed ETL layer on Azure Databricks runs a 3-tier *Medallion Pipeline*:
1. *Bronze Layer:* Connects to ADLS Gen2 storage using Azure Key Vault secret management (dbutils.secrets.get()) to read raw Apache Avro ingestion streams securely.
2. *Silver Layer:* Parses raw payload strings into structured PySpark DataFrames using dynamic schema_of_json() extraction. Array fields are expanded with explode_outer(), missing values handled with fallback defaults, and structural duplicates purged using windowed constraints (dropDuplicates).
3. *Gold Layer:* Computes fee percentile distributions ($P_{10}$ to $P_{99}$), block-space compression efficiency, and dynamic transaction velocity. Writes clean, compressed, partitioned *Apache Parquet* datasets back to ADLS Gen2 (Mempool_parsing/).
* *Automated Workflow Orchestration:* Scheduled via Databricks Workflows every 5 hours (mempool_parsing pipeline) with Key Vault RBAC access controls.

### Why This Architecture? (Engineering Rationale)
* *Distributed Parallel Processing:* Memory-heavy JSON parsing across millions of nested arrays causes single-node Pandas scripts to OOM (Out Of Memory). PySpark distributes JSON schema inferencing and string manipulation across cluster worker nodes.
* *Medallion Standardization:* Separating Raw (Bronze), Cleansed (Silver), and Aggregated (Gold) environments allows data engineers to re-process transformed layers without re-ingesting raw edge streams.

### Business Value
* *Sub-Second Analytics Efficiency:* Converts unstructured JSON text into compressed columnar Parquet, reducing analytical query scan times by up to *85%*.
* *Engineered Fee Metrics:* Computes fee variance ($P_{99} - P_{10}$) to help crypto platforms and miners optimize transaction inclusion strategies.

### VIDEO:
https://github.com/user-attachments/assets/dd5ce13a-abdc-4083-81a1-ff0a83521104

---

## ❄️ Data Warehousing & Ingestion (Snowflake)

### Technical Architecture & Storage Integration
* *Data Context:* Database: MEMPOOL_DB | Staging Schema: MEMPOOL_DBT.
* *Zero-Egress Storage Integration:* Direct integration via Azure Service Principal (azure_adls_snowflake_int) using Tenant ID authentication, eliminating static credential storage inside Snowflake scripts.
* *Serverless Ingestion Engine:* Automated ingestion task (load_mempool_azure_task) executing on a 360-minute schedule using Snowflake COPY INTO state tracking to prevent duplicate file reads.
* *Scalability Proof:* Ingested and indexed over *50,119,413 active rows* (~1 GB compressed micro-partitions) with sub-second aggregate lookup latency.

### Why This Architecture? (Engineering Rationale)
* *Decoupled Compute & Storage:* Snowflake micro-partitioning allows massive data scanning while keeping compute costs low by scaling virtual warehouses down when idle.
* *Service Principal Security:* Eliminates hardcoded SAS tokens and account keys in database code, aligning with SOC2 compliance standards.

### Business Value
* *High-Throughput Warehousing:* Handles tens of millions of records seamlessly, giving BI analysts instant access to historical mempool trends.
* *Cost Management:* Serverless auto-suspend warehouses ensure infrastructure costs scale down during quiet network periods.

### VIDEO:
https://github.com/user-attachments/assets/5243c5b2-4deb-4f7c-bb97-69212c0671f4

---

## 💎 Data Modeling & Analytics Engineering (dbt Core + Snowflake)

[![dbt CI/CD Pipeline](https://github.com/mr-Anas-tech/mempool/actions/workflows/ci_cd_dbt.yml/badge.svg)](https://github.com/mr-Anas-tech/mempool/actions)
![dbt Core](https://img.shields.io/badge/dbt--core-1.7.0-orange?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?logo=githubactions)

### dbt Layering Strategy
1. *Bronze (stg_mempool.sql):* Incremental materialization strategy using dynamic watermark filters (enqueued_time > (SELECT MAX(enqueued_time) FROM {{ this }})) and surrogate keys generated via dbt_utils.generate_surrogate_key.
2. *Silver (int_mempool.sql):* Deduplicates records using window functions (ROW_NUMBER() OVER (PARTITION BY mempool_id ORDER BY enqueued_time DESC)). Computes Fee Volatility Spreads ($P_{99} - P_{10}$), Fee Skewness Ratios, and Block Compression Ratios.
3. *Gold (fct_mempool_daily, fct_mempool_hourly, fct_mempool_minutely):*
   * fct_mempool_daily: Clustered by ['mempool_date'] for long-term historical trends.
   * fct_mempool_hourly: Clustered by ['time_hour', 'network_congestion_status'] for macro network monitoring.
   * fct_mempool_minutely: Unclustered high-frequency table optimized for live anomaly detection.

### DevOps & CI/CD Pipeline
Automated via GitHub Actions (.github/workflows/ci_cd_dbt.yml) on every pull request and push to main. It dynamically injects repository secrets (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.) into ~/.dbt/profiles.yml to run dbt deps, dbt debug, and dbt build with automated schema testing.

### Why This Architecture? (Engineering Rationale)
* *Modular Data Modeling:* Isolates staging, transformation, and dimensional reporting layers, making data pipelines easier to maintain and audit.
* *Cost-Efficient Clustering:* Selective clustering on high-cardinality query dimensions (mempool_date, time_hour) avoids unnecessary Snowflake re-clustering fees.

### Business Value
* *Standardized Business Logic:* Centralizes key metrics (e.g., Congestion Status, Fee Volatility) directly in code rather than duplicating logic across BI tools.
* *Continuous Integration Quality:* Prevents broken models or malformed schemas from deploying to production environments.

### VIDEO:
https://github.com/user-attachments/assets/fffe2aa9-9820-4e24-a422-93d6badbcdb9

### VIDEO:
https://github.com/user-attachments/assets/9ac51e63-3cc3-449d-a6cd-2916b67526dc

---


## ⚡ Streamlit Real-Time Analytics & Gemini AI Agent

---

## ✨ Key Features & Technical Capabilities

1. **Dual Time-Horizon Analytics:**
   - **Hourly Aggregations (`FCT_MEMPOOL_HOURS`):** Long-term network trends, fee percentiles (P10 to P99), block size economics, fee skewness, and total block fees collected.
   - **Minute Real-time Spikes (`FCT_MEMPOOL_MINUTES`):** High-frequency monitoring, rapid fee spikes, real-time whale transaction detection, and micro congestion alerts.

2. **Resilient Dual-Source Fallback System:**
   - Queries live data directly from **Snowflake DB**.
   - If database connection fails or times out, seamlessly falls back to cached **Local Parquet Files** (`hourly_mempool_backup.parquet` & `minutely_mempool_backup.parquet`), guaranteeing high dashboard availability.

3. **In-App AI Conversational Agent (Google Gemini):**
   - Embedded sidebar chat powered by `gemini-3.1-flash-lite`.
   - Automatically feeds real-time context (latest network congestion, median sat/vB fees, P99 spikes, whale alerts) directly to the AI for accurate, context-aware user responses.

4. **Modern UI/UX Architecture:**
   - Custom CSS dark theme, responsive grid structures, interactive Plotly charts, dynamic KPI metric alert boxes (Status-aware color formatting).

---

## 📊 Analytics Dashboard Metrics Breakdown

### 1. Hourly Aggregations Horizon (`FCT_MEMPOOL_HOURS`)
* **Network Status & Alerts:** Network Congestion Status, Hourly Fee Alert, Hourly Skewness Alert.
* **Block Throughput & Size Economics:** Total Transactions Processed, Total Blocks Mined, Avg/Max Tx per Block, Total Block Size (MB), Avg Block VSize.
* **Sat/vB Fee Percentile Spread:** P10, P25, P50 (Median), P75, P90, P95, and P99 Fee Spikes.
* **Whale Outliers & Miner Revenue:** Avg Fee Spread, Interquartile Fee (IQR), Fee Skewness Ratio, Miner Revenue Ratio, Block Compactness.
* **Visual Distributions:** Hourly Fee Trends Line Chart, Throughput Bar Chart, Total Fees Area Chart.

### 2. Minute Real-time Horizon (`FCT_MEMPOOL_MINUTES`)
* **Real-time System Alerts:** Fee Spike Alert, Whale Outlier Alert, Congestion Alert, Network Congestion Status.
* **Minute Level Fee Percentiles:** P10, P50 (Median), P90, P99, and Peak Fee (P99).
* **High-Frequency Visualizations:** Minute-by-Minute Fee Spike Trends (p99 Level) & Scatter Plots for Whale Transaction Detection.

---

## 🛠 Technology Stack & Dependencies

* **Frontend Dashboard:** `streamlit`, `plotly`
* **Data Ingestion & Pipeline:** Azure Event Hub, Azure Databricks (PySpark), dbt Cloud & Snowflake
* * **Data Storage & Warehousing:** Snowflake, `snowflake-connector-python`, `pandas`, `pyarrow` (Parquet)
* **Generative AI:** `google-genai` (`gemini-3.1-flash-lite`)
* **Deployment:** Azure App Service / Streamlit Community Cloud

## Video:



https://github.com/user-attachments/assets/691c9d56-34ae-472d-a841-ba9593485881

### Pics:

<img width="1562" height="756" alt="Screenshot 2026-10-06 154231" src="https://github.com/user-attachments/assets/bf54140b-01fa-406f-a635-200a7de071f1" />
<img width="1911" height="833" alt="Screenshot 2026-10-06 154201" src="https://github.com/user-attachments/assets/dcc08f32-fd54-42a9-9a7d-f23e9c75e9b9" />
<img width="1907" height="868" alt="Screenshot 2026-10-06 154134" src="https://github.com/user-attachments/assets/e2b1f6ea-0afb-4836-ba3a-7fe973a8d621" />
<img width="1916" height="902" alt="Screenshot 2026-10-06 154057" src="https://github.com/user-attachments/assets/ea44965e-0d66-44a0-9ada-bdc12ff854e2" />
<img width="1907" height="870" alt="Screenshot 2026-10-06 154029" src="https://github.com/user-attachments/assets/1b79a1ca-bfca-44e1-977c-1a8bd90a7315" />





---

## 📊 Power BI Executive Dashboard

An executive-level interactive reporting solution built for macro monitoring of network congestion and miner economics.

### Power BI Dashboard Overview:

## Key KPI Metrics & Dashboard Components

### 1. High-Level Executive KPIs
* **Total Daily Transactions:** Tracks aggregated volume of processed mempool transactions (e.g., **2.62bn** across full timeline, filtering dynamically down to daily volumes like **242.03M** - **370.60M**).
* **Total Daily Miner Revenue:** Measures total daily transaction fees harvested by network miners.
* **Average Block Size (MB):** Highlights block capacity utilization (averaging **1.59 MB – 1.73 MB** per block).
* **Daily Overall Median Fee (Sat/vB):** Tracks network baseline transaction costs across time slices (ranging from **0.43 Sat/vB to 1.20 Sat/vB**).

---

### 2. Analytical Visualizations Breakdown

* **Fee Spread & Volatile Percentiles (Line Chart):**
  - Displays fee distribution spreads comparing `DAILY_MIN_P10_FEE`, `DAILY_MAX_P99_FEE`, and `AVG_DAILY_MEDIAN_FEE`.
  - Captures high-frequency fee spikes and whale transaction anomalies visually.

* **Daily Revenue & Fee Trends (Combo Bar Chart):**
  - Correlates total daily miner revenue against median network fee patterns across daily date ranges (`MEMPOOL_DATE`).

* **Block Efficiency & Skewness Ratio (Scatter Plot):**
  - Maps `Daily Avg Compression Ratio` against `Daily Avg Fee Skewness` to analyze block space efficiency vs. fee inequality within mined blocks.

* **Network Congestion Breakdown (Donut Chart):**
  - Categorizes network activity into congestion tiers:
    - 🟢 **Low Congestion (`Low_cong`):** Baseline network execution (typically ~92% – 95% of traffic).
    - 🟠 **Moderate Congestion (`Moderate_cong`):** Intermediate mempool pressure (~3% – 6%).
    - 🔴 **High Congestion (`High_cong`):** Severe transaction queueing & spike conditions (~0.1% – 1.5%).

* **Date & Slicer Controls:**
  - Interactive multi-select date slicer (`17/09/2026` to `25/09/2026`) enabling day-by-day filter drilling across all metrics.
 
## Video:



https://github.com/user-attachments/assets/d355eb03-b342-48c5-9c24-bf111c69c560


### Pics:

<img width="1562" height="756" alt="Screenshot 2026-10-06 154231" src="https://github.com/user-attachments/assets/aa397c21-5a4a-446c-903a-17d0b4da50ef" />




---

## 🛠 Setup & Local Deployment Guide

### 1. Prerequisites
* Python 3.10+
* Git installed

### 2. Installation
```bash
git clone https://github.com/mr-Anas-tech/-Bitcoin-Mempool-Real-Time-Analytics-Enterprise-AI-Platform.git
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
 * Deployment Platforms: Azure App Service / azure event hub/ Databricks / Snowflake / dbt/ Streamlit / Power BI






