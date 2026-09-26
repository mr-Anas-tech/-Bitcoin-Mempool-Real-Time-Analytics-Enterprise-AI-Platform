{{
    config(
        materialized='table'
    )
}}

with int_data as (
    select * from {{ ref('int_mempool') }}
)

select
    -- Time Bucket
    date_trunc('minute', enqueued_time) as time_minute,
    network_congestion_status,

    -- Count Metrics
    count(distinct sequence_number) as total_blocks,
    sum(number_of_transactions) as total_transactions,
    avg(number_of_transactions) as avg_transactions_per_block,
    max(number_of_transactions) as max_transactions_in_block,

    -- Block Metrics
    sum(block_size) as total_block_bytes,
    avg(block_size) as avg_block_size,
    sum(block_vsize) as total_block_vsize,
    avg(block_vsize) as avg_block_vsize,

    -- Fee & Percentile Aggregations
    sum(total_fees) as total_fees,
    avg(total_fees) as avg_block_fees,
    max(total_fees) as max_block_fees,
    
    avg(median_fee) as avg_median_fee,
    max(median_fee) as max_median_fee,
    avg(fee_p10) as avg_fee_p10,
    avg(fee_p25) as avg_fee_p25,
    avg(fee_p50) as avg_fee_p50,
    avg(fee_p75) as avg_fee_p75,
    avg(fee_p90) as avg_fee_p90,
    avg(fee_p95) as avg_fee_p95,
    avg(fee_p99) as avg_fee_p99,
    max(fee_p99) as peak_fee_p99,

    -- Advanced Computed Indicators
    avg(fee_spread_range) as avg_fee_spread_range,
    max(fee_spread_range) as max_fee_spread_range,
    avg(interquartile_fee_range) as avg_interquartile_fee_range,
    avg(fee_skewness_ratio) as avg_fee_skewness_ratio,
    max(fee_skewness_ratio) as max_fee_skewness_ratio,

    avg(block_compression_ratio) as avg_block_compression_ratio,
    avg(miner_revenue_per_byte) as avg_miner_revenue_per_byte,
    avg(avg_fee_per_tx) as avg_fee_per_tx,
    max(avg_fee_per_tx) as max_fee_per_tx,

    -- 🚨 Real-time Minute Alerts
    case 
        when max(fee_p99) > 100 then 'CRITICAL: High Fee Spike'
        when max(fee_p99) > 50 then 'WARNING: Moderate Fee Spike'
        else 'NORMAL'
    end as fee_spike_alert,

    case 
        when max(fee_skewness_ratio) > 100 then 'ALERT: Whale Fee Outlier'
        else 'NORMAL'
    end as fee_skewness_alert,

    case 
        when sum(number_of_transactions) > 150000 then 'ALERT: High Mempool Congestion'
        else 'NORMAL'
    end as congestion_alert

from int_data
group by 1, 2