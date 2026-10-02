# mempool

## AZURE-DEPLOYED DATA INGESTION & EVENT HUB CAPTURE ARCHITECTURE

### CLOUD INFRASTRUCTURE OVERVIEW
The ingestion subsystem is hosted on Azure App Service (`mempoolwebservice`, Python 3.12 runtime) 
in the West US 3 region. It maintains a persistent WebSocket connection to Mempool.space, streams 
multi-entity blockchain transactions in real time, and ingests them into Azure Event Hubs 
(`mempoolspace-Events / mempool_realtime_streamig`). The Event Hubs Capture feature automatically 
persists the incoming high-throughput binary event stream directly into Azure Blob Storage Containers 
using the highly efficient Apache Avro serialization format.
```
----------------------------------------------------------------------------------------------------
1. END-TO-END CLOUD DATA PIPELINE & DEPLOYMENT ARCHITECTURE
----------------------------------------------------------------------------------------------------

┌─────────────────────────────────┐
│ Mempool.space WebSocket Endpoint│
│     wss://mempool.space/ws      │
└────────────────┬────────────────┘
                 │
                 │ (Live Streams: blocks, mempool-blocks, stats, live-2xx)
                 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AZURE APP SERVICE PAAS ENVIRONMENT (`mempoolwebservice` | West US 3 | Python 3.12)              │
│                                                                                                  │
│   ┌──────────────────────────────────┐            ┌──────────────────────────────────────────┐   │
│   │  Flask HTTP Probe Server         │            │  WebSocket Streaming Engine Thread       │   │
│   │  (Port 8100 Health Status)       │            │  (24/7 Self-Healing Event Daemon)        │   │
│   └──────────────────────────────────┘            └────────────────────┬─────────────────────┘   │
│                                                                        │                         │
│                                                                        ▼                         │
│                                                   ┌──────────────────────────────────────────┐   │
│                                                   │ In-Memory Content Demultiplexer          │   │
│                                                   └────────────────────┬─────────────────────┘   │
│                                                                        │                         │
│                                       ┌────────────────────────────────┴─────────────────────┐   │
│                                       │                                                      │   │
│                           Single Event Envelope                                Multi-Tx Stream   │   │
│                                       ▼                                                      ▼   │
│                              ┌─────────────────┐                                   ┌─────────────────┐   │
│                              │ Schema Envelope │                                   │ Byte-Aware 1MB  │   │
│                              │ Wrapper         │                                   │ Batch Engine    │   │
│                              └────────┬────────┘                                   └────────┬────────┘   │
└───────────────────────────────────────┼─────────────────────────────────────────────────────┼────┘
                                        │                                                     │
                                        ▼ (AMQP / TLS 1.3)                                    ▼ (AMQP / TLS 1.3)
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AZURE EVENT HUBS INSTANCE (`mempoolspace-Events / mempool_realtime_streamig`)                    │
│                                                                                                  │
│   • Ingress Throughput: 124.2K+ Messages Processed / 68.7 MB Data Captured                        │
│   • Protocol: Native AMQP with Byte-Aware Batching & Retries                                     │
│   • Live Monitoring: Log stream verified (block_event, mempool_blocks_event, mempool_tx batches) │
└───────────────────────────────────────┬──────────────────────────────────────────────────────────┘
                                        │
                                        │ (Azure Native Auto-Capture Enabled)
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ AZURE STORAGE CONTAINER SINK                                                                     │
│                                                                                                  │
│   • Target Storage: Azure Blob Storage Containers                                                │
│   • File Format: Apache Avro Binary Serialization (Optimized for Lakehouse / Databricks)         │
│   • Partition Strategy: {Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘

----------------------------------------------------------------------------------------------------
2. DEEP-DIVE TECHNICAL "WHY" & CLOUD ARCHITECTURE JUSTIFICATIONS
----------------------------------------------------------------------------------------------------

```

###1. DEPLOYMENT TARGET: AZURE APP SERVICE (`mempoolwebservice`)
• PROBLEM: Running long-lived streaming producers on temporary local environments or bare VMs leads 
  to high maintenance overhead, lack of native TLS cert management, and unmanaged uptime.
• IMPLEMENTATION: Deployed to Azure App Service (`mempoolwebservice`, Region: West US 3) running 
  Python 3.12 stack. Built-in system logging stream tracks live execution.
• ARCHITECTURAL IMPACT: Ensures enterprise PaaS management, automated cloud container health checks, 
  and continuous uptime without manual server administration.

###2. EVENT STREAMING BUS: AZURE EVENT HUBS (`mempool_realtime_streamig`)
• PROBLEM: Ingesting high-concurrency WebSocket transaction bursts directly into standard SQL 
  databases or files causes thread blocking, write lock contention, and network bottlenecks.
• IMPLEMENTATION: Event Hubs acts as a massive-scale distributed buffer (`mempoolspace-Events` 
  namespace). It decouples edge ingestion from downstream analytical processing (Databricks / Snowflake).
• ARCHITECTURAL IMPACT: Handles hundreds of thousands of incoming events seamlessly with zero 
  producer throttling or dropped messages.

###3. NATIVE EVENT HUBS CAPTURE TO AVRO FORMAT IN BLOB CONTAINERS
• PROBLEM: Writing continuous streaming data directly to disk using plain JSON format causes 
  excessive disk I/O, large storage footprints, and slow parsing times in big-data engines.
• IMPLEMENTATION: Enabled Azure Event Hubs native Capture engine. It streams incoming partition 
  data directly into Azure Blob Storage Containers as compact binary **Apache Avro** files.
• ARCHITECTURAL IMPACT: 
  - Zero Code Overhead: Storage persistence happens automatically at the Azure infrastructure layer.
  - Schema Preservation: Avro embeds raw record schemas directly in binary headers.
  - Databricks/Lakehouse Ready: Avro files are natively optimized for high-speed parallel reading 
    by Apache Spark in PySpark streaming pipelines.

###4. IN-MEMORY BATCHING & EXCEPTION RECOVERY (1MB BOUNDARY)
• PROBLEM: Azure Event Hubs imposes a strict 1MB size limit per AMQP batch. Burst transaction 
  payloads during network volatility spikes can exceed this boundary and throw exceptions.
• IMPLEMENTATION: The Python client maintains client-side batching (`producer.create_batch()`). 
  When a batch hits the 1MB payload cap, the engine intercepts the `ValueError`, dispatches the 
  saturated batch immediately, instantiates a fresh batch container, and inserts the overflow record.
• ARCHITECTURAL IMPACT: Guarantees 100% ingestion reliability with zero data loss during high-traffic 
  blockchain fee spikes.

----------------------------------------------------------------------------------------------------
3. DEPLOYMENT & INFRASTRUCTURE MATRIX
----------------------------------------------------------------------------------------------------
```
-----------------------------------+------------------------------------+---------------------------------------+
| ARCHITECTURE COMPONENT            | AZURE RESOURCE SPECIFICATION       | PRODUCTION & TECHNICAL ROLE           |
-----------------------------------+------------------------------------+---------------------------------------+
| PaaS Host Engine                  | Azure App Service                  | Hosts Python 3.12 producer daemon     |
|                                   | Name: `mempoolwebservice`          | with Flask health probe on port 8100. |
|                                   | Region: West US 3                  |                                       |
-----------------------------------+------------------------------------+---------------------------------------+
| Distributed Ingestion Bus         | Azure Event Hubs                   | Real-time AMQP message ingestion bus  |
|                                   | Namespace: `mempoolspace-Events`   | handling high-concurrency WebSocket   |
|                                   | Hub: `mempool_realtime_streamig`   | transaction streams.                  |
-----------------------------------+------------------------------------+---------------------------------------+
| Automated Data Lake Sink          | Azure Event Hubs Capture           | Persists binary Apache Avro files into|
|                                   | Target: Azure Storage Containers   | Blob storage for PySpark lakehouse    |
|                                   | Format: Apache Avro Binary         | consumption.                          |
-----------------------------------+------------------------------------+---------------------------------------+
| Resilience & Self-Healing         | WebSocket Ping Heartbeat +         | Automatically recovers connection drops|
|                                   | Exponential 5s Backoff             | without container crashes or bans.    |
-----------------------------------+------------------------------------+---------------------------------------+
```



### AZURE-DEPLOYED DATA INGESTION & EVENT HUB CAPTURE ARCHITECTURE VIDEO:

https://github.com/user-attachments/assets/2c2a1a35-b9f2-4d07-baac-54a59f7104d7


  ##           PYSPARK STREAM & BATCH PROCESSING LAYER (AZURE DATABRICKS)

[MEDALLION ARCHITECTURE PIPELINE]
The PySpark processing engine executes on Azure Databricks, orchestrating a structured 3-stage 
data transformation model that ingests binary Avro events and outputs query-optimized Parquet datasets.

----------------------------------------------------------------------------------------------------
1. TECHNICAL EXECUTION BREAKDOWN (CELL-BY-CELL REFINEMENT)
----------------------------------------------------------------------------------------------------
```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: SECURE CONNECTION & AVRO INGESTION (BRONZE LAYER)                                       │
│                                                                                                  │
│   • Secret Management: Azure Key Vault scope integration via Databricks Secrets Utility          │
│     `dbutils.secrets.get(scope='vaultmempool', key='mempool-access-key')`                        │
│   • Binary Reader: Reads raw Apache Avro event payloads from ADLS Gen2 container:                │
│     `abfss://bronze@mempoolspaceacc.dfs.core.windows.net/mempoolspace-events/`                   │
│   • Bronze Persistence: Saves raw binary streams directly to Bronze Delta format.                │
└────────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                         │
                                         │ (Dynamic JSON Schema Parsing)
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: DESERIALIZATION & STRUCTURED EXPLODING (SILVER LAYER)                                   │
│                                                                                                  │
│   • Payload Decoding: Casts binary payload to string and infers JSON schema dynamically using    │
│     `schema_of_json()` and `from_json()`.                                                        │
│   • Relational Unnesting: Uses `explode_outer(col("data"))` to flatten nested JSON array         │
│     structures into individual block and transaction records without data loss.                  │
└────────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                         │
                                         │ (Fee Percentile Extraction & Aggregation)
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: METRIC EXTRACTION & PARQUET STORAGE (GOLD LAYER)                                        │
│                                                                                                  │
│   • Fee Percentile Indexing: Extracts raw array fee ranges into granular analytical metrics:     │
│     - P10, P25, P50 (Median), P75, P90, P95, P99 Fee Ranges (`feeRange[0]` to `feeRange[6]`).    │
│   • Target Output: Writes clean, highly compressed analytical tables to ADLS Gen2:               │
│     `abfss://bronze@mempoolspaceacc.dfs.core.windows.net/Mempool_parsing/` (Parquet Format)     │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```
----------------------------------------------------------------------------------------------------
2. DATABRICKS JOB ORCHESTRATION & RESILIENCY
----------------------------------------------------------------------------------------------------
### Automated Workflow Trigger: Executed via Databricks Automated Workflows scheduled every 5 hours 
  (`mempool_parsing` job pipeline).
###  Production Uptime: 100% success rate with zero pipeline failures across multi-day continuous runs.
### Key Vault Hardening: Zero hardcoded storage secrets in codebase, maintaining enterprise cloud security standards.

## Video :


https://github.com/user-attachments/assets/7a635d85-0249-4a0a-a4fb-96eee0ed5770



##            SNOWFLAKE DATA WAREHOUSING & AUTOMATED INGESTION TASK


###ENTERPRISE WAREHOUSE ARCHITECTURE
Snowflake serves as the centralized Cloud Data Warehouse, securely integrating with Azure Data Lake 
Storage (ADLS Gen2) via native Azure Storage Integrations. It automatically parses incoming Parquet 
files and loads them into analytical staging tables via scheduled background tasks.

----------------------------------------------------------------------------------------------------
1. SNOWFLAKE SETUP & DATA PIPELINE STEPS
----------------------------------------------------------------------------------------------------
```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: DATABASE & STORAGE INTEGRATION                                                           │
│                                                                                                  │
│   • Database Context: `mempool_DB`                                                               │
│   • Integration: `azure_adls_snowflake_int` (External Stage Integration using Azure Tenant ID)   │
│   • Target ADLS Container: `azure://mempoolspaceacc.blob.core.windows.net/bronze/Mempool_parsing`│
└────────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                         │
                                         │ (Parquet File Format & External Stage Definition)
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: FILE FORMAT & EXTERNAL STAGING (`adls_api_stage`)                                        │
│                                                                                                  │
│   • File Format: `parquet_format` (TYPE = PARQUET)                                               │
│   • External Stage: `adls_api_stage` pointing to ADLS Gen2 parquet outputs from Databricks.     │
└────────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                         │
                                         │ (Schema Parsing & Automated Task Scheduled Load)
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: AUTOMATED TASK & STAGING TABLE (`STG_MEMPOOL_PARSED`)                                    │
│                                                                                                  │
│   • Target Table: `STG_MEMPOOL_PARSED` containing block metrics & full fee percentiles           │
│     (P10, P25, P50, P75, P90, P95, P99).                                                         │
│   • Automated Serverless Task: `load_mempool_azure_task` running on a 360-minute schedule:      │
│     `CREATE OR REPLACE TASK load_mempool_azure_task WAREHOUSE = 'MEMPOOL' SCHEDULE = '360 MINUTE'` │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```
----------------------------------------------------------------------------------------------------
2. WAREHOUSE METRICS & DATA VOLUME PROOF
----------------------------------------------------------------------------------------------------
*  Total Active Records Ingested: 50,119,413 Records (50M+ parsed time-series transactions).
*  Compressed Lakehouse Storage: 1019.7 MB (~1 GB optimized Parquet/Snowflake Micro-Partitions).
*  Query Latency: Sub-second lookup speed across 50M+ historical rows.
*  Zero-Egress Security: Direct Azure Tenant integration via Service Principals with zero persistent credentials stored.

Video:


https://github.com/user-attachments/assets/bba0e81e-31a8-4cd2-8b01-b140c9f69660



<img width="1817" height="657" alt="Screenshot 2026-09-29 175754" src="https://github.com/user-attachments/assets/2fa47ae7-c058-4ff7-ab39-dfb77f25fd36" />


====================================================================================================




## Bitcoin Mempool Real-Time Data Transformation & Analytics Pipeline (dbt + Snowflake)

[![dbt CI/CD Pipeline](https://github.com/mr-Anas-tech/mempool/actions/workflows/ci_cd_dbt.yml/badge.svg)](https://github.com/mr-Anas-tech/mempool/actions)
![dbt Core](https://img.shields.io/badge/dbt--core-1.7.0-orange?logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI%2FCD-2088FF?logo=githubactions)

This repository also contains the enterprise-grade **dbt (Data Build Tool)** transformation layer for processing real-time **Bitcoin Mempool & Block streaming data** at scale. The pipeline transforms raw streaming records ingested into **Snowflake** into highly optimized, aggregated analytical data marts with automated network congestion alerting, fee volatility risk indicators, and performance-tuned clustering.


## 🏛 Architecture & Data Flow
```

+-----------------------------------------------------------------------------------+
|                            STREAMING INGESTION LAYER                              |
|   Bitcoin Mempool WebSocket ---> Azure Event Hubs ---> Databricks PySpark (Avro)  |
+-----------------------------------------------------------------------------------+
|
v
+-----------------------------------------------------------------------------------+
|                       SNOWFLAKE RAW DATA WAREHOUSE (STG)                          |
|                       Source Table: STG_MEMPOOL_PARSED                            |
+-----------------------------------------------------------------------------------+
|
v
+-----------------------------------------------------------------------------------+
|                            dbt MEDALLION ARCHITECTURE                             |
|                                                                                   |
|  [BRONZE LAYER]                                                                   |
|   └── stg_mempool (Incremental Model, Watermark Filter, Surrogate Key)             |
|            |                                                                      |
|            v                                                                      |
|  [SILVER LAYER]                                                                   |
|   └── int_mempool (Deduplicated via ROW_NUMBER, Core Feature Engineering)         |
|            |                                                                      |
|            +-----------------------+-----------------------+                      |
|            |                       |                       |                      |
|            v                       v                       v                      |
|  [GOLD LAYER - MARTS]                                                             |
|   ├── fct_mempool_daily   ├── fct_mempool_hourly  └── fct_mempool_minutely         |
|   │   (Clustered: Date)   │   (Clustered: Hour)       (Unclustered / High-Freq)   |
+-----------------------------------------------------------------------------------+
|
v
+-----------------------------------------------------------------------------------+
|                        ANALYTICS & OPERATIONAL MONITORING                         |
|             Automated Risk Alerts | Fee Spike Warnings | BI Dashboards            |
+-----------------------------------------------------------------------------------+

```

---

## 💎 dbt Transformation Layers

### 1. Bronze Layer — Staging (`stg_mempool.sql`)
* **Materialization Strategy:** `incremental` (Unique Key: `mempool_id`).
* **Surrogate Key Generation:** Uses `dbt_utils.generate_surrogate_key(['sequence_number', 'enqueued_time'])` to establish a deterministic primary key for every streaming snapshot.
* **Watermark Incremental Load:** Enforces `WHERE enqueued_time > (SELECT MAX(enqueued_time) FROM {{ this }})` to process only new records, saving compute costs on 50M+ row tables.
* **Type Casting & Standardization:** Converts raw strings/variants into strict numeric and timestamp data types (`BIGINT`, `TIMESTAMP`, `DOUBLE PRECISION`).

### 2. Silver Layer — Intermediate (`int_mempool.sql`)
* **Purpose:** Serves as the standardized, deduplicated **Single Source of Truth** for downstream analytics.
* **Window Deduplication:** Executes `ROW_NUMBER() OVER (PARTITION BY mempool_id ORDER BY enqueued_time DESC)` to guarantee zero duplicate event records (`WHERE rn = 1`).
* **Advanced Feature Engineering:**
  * **Fee Volatility Spreads:** `fee_spread_range` ($p_{99} - p_{10}$) & Interquartile Fee Range ($p_{75} - p_{25}$).
  * **Fee Skewness Ratio:** Evaluates whale transaction fee anomalies ($p_{99} / p_{10}$).
  * **Block Efficiency Metrics:** `block_compression_ratio` ($\text{vSize} / \text{Size}$) and `miner_revenue_per_byte`.
  * **Network Congestion Status:** Categorizes blocks into `High Congestion` (>3 sat/vB), `Moderate Congestion` (1-3 sat/vB), and `Low Congestion` (<1 sat/vB).

---

## 📊 Gold Layer — Analytical Data Marts & Optimization Strategy

To deliver optimal performance at massive scale without incurring high Snowflake compute credits, data aggregations are split across three distinct temporal data marts:

| Data Mart Model | Materialization | Clustering Key | Optimization Strategy & Architectural Rationale |
| :--- | :--- | :--- | :--- |
| **`fct_mempool_daily`** | `table` | `['mempool_date']` | Optimized for long-term trends, financial reporting, and daily network stress alerts. |
| **`fct_mempool_hourly`** | `table` | `['time_hour', 'network_congestion_status']` | Multi-column clustering prunes micro-partitions during hourly fee volatility and congestion analysis. |
| **`fct_mempool_minutely`** | `table` | **None (Unclustered)** | **Cost Efficiency Choice:** High-frequency minute-level data creates smaller, frequent partitions. Applying clustering at the minute level would induce heavy background re-clustering overhead, leading to micro-partition fragmentation and unnecessary Snowflake costs. |

### 🚨 Real-time Operational Alerts
Every data mart computes automated conditional alerts directly within SQL transformations:
* **`daily_network_stress_alert`**: Flags days with $>20$ high-congestion blocks.
* **`hourly_fee_alert`**: Emits `CRITICAL: Hourly Network Congestion` when median fees surpass operational thresholds.
* **`fee_spike_alert` & `fee_skewness_alert`**: Minute-level flags to identify real-time fee spikes ($p_{99} > 100$) and whale transaction fee anomalies ($>100x$ skewness).

---

## ⚙️ DevOps & Continuous Integration (CI/CD)

The project employs an automated **GitHub Actions CI/CD workflow** (`.github/workflows/ci_cd_dbt.yml`) that triggers on all pull requests and pushes to `main`.

### CI/CD Workflow Features:
1. **Automated Environment Setup:** Configures Python 3.10 and installs `dbt-snowflake`.
2. **Zero-Trust Security Management:** Inject credentials dynamically into `~/.dbt/profiles.yml` using encrypted **GitHub Repository Secrets** at runtime.
3. **Automated Verification Pipeline:** Executes `dbt deps`, `dbt debug` (connectivity validation), and `dbt build` (compiles models and runs schema/data tests).

### GitHub Workflow Configuration (`ci_cd_dbt.yml`):

```yaml
name: dbt Snowflake CI/CD Pipeline

on:
  push:
    branches: [ main, mr-Anas-tech-patch-1 ]
  pull_request:
    branches: [ main ]

jobs:
  dbt_transformation_and_test:
    runs-on: ubuntu-latest

    env:
      DBT_ENV_SECRET_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      DBT_ENV_SECRET_USER: ${{ secrets.SNOWFLAKE_USER }}
      DBT_ENV_SECRET_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
      DBT_ENV_SECRET_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
      DBT_ENV_SECRET_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}

    steps:
      - name: Checkout Repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dbt-snowflake
        run: |
          python -m pip install --upgrade pip
          pip install dbt-snowflake

      - name: Create dbt Profiles Directory & File
        run: |
          mkdir -p ~/.dbt
          cat <<EOF> ~/.dbt/profiles.yml
          default:
            target: dev
            outputs:
              dev:
                type: snowflake
                account: "${DBT_ENV_SECRET_ACCOUNT}"
                user: "${DBT_ENV_SECRET_USER}"
                password: "${DBT_ENV_SECRET_PASSWORD}"
                role: "${DBT_ENV_SECRET_ROLE}"
                warehouse: "${DBT_ENV_SECRET_WAREHOUSE}"
                database: MEMPOOL_DB
                schema: mempool_dbt
                threads: 4
                client_session_keep_alive: False
          EOF

      - name: Install dbt Dependencies
        run: dbt deps

      - name: Run dbt Debug
        run: dbt debug

      - name: Execute dbt Build (Models & Tests)
        run: dbt build
```

Security & Secret Governance
 * Zero Plain-Text Credentials: Database user credentials, roles, passwords, and warehouse configurations are strictly prohibited from being committed to source control.
 * Runtime Dynamic Profile Injection: Profiles are dynamically generated in ephemeral GitHub Action runners during execution, leveraging encrypted secret store values.
🚀 Local Execution Setup
To run this dbt project locally:
 * Clone the repository:
   git clone [https://github.com/mr-Anas-tech/mempool.git](https://github.com/mr-Anas-tech/mempool.git)
cd mempool

 * Set up virtual environment & install dependencies:
   python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install dbt-snowflake

 * Configure environment variables:
   Set DBT_ENV_SECRET_ACCOUNT, DBT_ENV_SECRET_USER, DBT_ENV_SECRET_PASSWORD, DBT_ENV_SECRET_ROLE, and DBT_ENV_SECRET_WAREHOUSE in your local environment.
 * Run dbt pipeline:
   dbt deps
dbt debug
dbt build


### DBT_VIDEO:


https://github.com/user-attachments/assets/5097e2f8-3bb9-48f7-b13f-f5d2a5489c5e


### DBT_DEVOPS_VIDEO:


https://github.com/user-attachments/assets/19cbcc83-d33d-4a18-8f1d-1bdaee9bc9de





