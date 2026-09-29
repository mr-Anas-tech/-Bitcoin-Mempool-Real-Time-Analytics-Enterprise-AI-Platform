# mempool

## AZURE-DEPLOYED DATA INGESTION & EVENT HUB CAPTURE ARCHITECTURE

[CLOUD INFRASTRUCTURE OVERVIEW]
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

[1. DEPLOYMENT TARGET: AZURE APP SERVICE (`mempoolwebservice`)]
• PROBLEM: Running long-lived streaming producers on temporary local environments or bare VMs leads 
  to high maintenance overhead, lack of native TLS cert management, and unmanaged uptime.
• IMPLEMENTATION: Deployed to Azure App Service (`mempoolwebservice`, Region: West US 3) running 
  Python 3.12 stack. Built-in system logging stream tracks live execution.
• ARCHITECTURAL IMPACT: Ensures enterprise PaaS management, automated cloud container health checks, 
  and continuous uptime without manual server administration.

[2. EVENT STREAMING BUS: AZURE EVENT HUBS (`mempool_realtime_streamig`)]
• PROBLEM: Ingesting high-concurrency WebSocket transaction bursts directly into standard SQL 
  databases or files causes thread blocking, write lock contention, and network bottlenecks.
• IMPLEMENTATION: Event Hubs acts as a massive-scale distributed buffer (`mempoolspace-Events` 
  namespace). It decouples edge ingestion from downstream analytical processing (Databricks / Snowflake).
• ARCHITECTURAL IMPACT: Handles hundreds of thousands of incoming events seamlessly with zero 
  producer throttling or dropped messages.

[3. NATIVE EVENT HUBS CAPTURE TO AVRO FORMAT IN BLOB CONTAINERS]
• PROBLEM: Writing continuous streaming data directly to disk using plain JSON format causes 
  excessive disk I/O, large storage footprints, and slow parsing times in big-data engines.
• IMPLEMENTATION: Enabled Azure Event Hubs native Capture engine. It streams incoming partition 
  data directly into Azure Blob Storage Containers as compact binary **Apache Avro** files.
• ARCHITECTURAL IMPACT: 
  - Zero Code Overhead: Storage persistence happens automatically at the Azure infrastructure layer.
  - Schema Preservation: Avro embeds raw record schemas directly in binary headers.
  - Databricks/Lakehouse Ready: Avro files are natively optimized for high-speed parallel reading 
    by Apache Spark in PySpark streaming pipelines.

[4. IN-MEMORY BATCHING & EXCEPTION RECOVERY (1MB BOUNDARY)]
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


====================================================================================================
  #             PYSPARK STREAM & BATCH PROCESSING LAYER (AZURE DATABRICKS)
====================================================================================================

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
====================================================================================================




