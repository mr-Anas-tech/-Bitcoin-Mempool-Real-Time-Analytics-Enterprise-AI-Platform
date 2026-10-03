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
 * Deployment Platforms: Azure App Service / Databricks / Snowflake / Streamlit / Power BI






