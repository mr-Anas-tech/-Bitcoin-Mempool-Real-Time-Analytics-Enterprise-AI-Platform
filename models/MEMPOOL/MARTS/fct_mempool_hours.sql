{{
    config(
        materialized='table',
        cluster_by=['time_hour', 'network_congestion_status']
    )
}}

with int_data as (
    select * from {{ ref('int_mempool') }}
)

select
    -- Time Bucket
    date_trunc('hour', enqueued_time) as time_hour,
    network_congestion_status,

    -- Counts & Volumes
    count(distinct sequence_number) as total_blocks,
    sum(number_of_transactions) as total_transactions,
    avg(number_of_transactions) as avg_transactions_per_block,
    max(number_of_transactions) as max_transactions_in_block,

    -- Block Sizes
    sum(block_size) as total_block_size,
    avg(block_size) as avg_block_size,
    sum(block_vsize) as total_block_vsize,
    avg(block_vsize) as avg_block_vsize,

    -- Fees & Percentiles
    sum(total_fees) as total_fees_collected,
    avg(total_fees) as avg_fees_per_block,
    max(total_fees) as peak_block_fee,

    avg(median_fee) as avg_median_fee,
    max(median_fee) as max_median_fee,
    avg(fee_p10) as avg_fee_p10,
    avg(fee_p25) as avg_fee_p25,
    avg(fee_p50) as avg_fee_p50,
    avg(fee_p75) as avg_fee_p75,
    avg(fee_p90) as avg_fee_p90,
    avg(fee_p95) as avg_fee_p95,
    avg(fee_p99) as avg_fee_p99,
    max(fee_p99) as max_fee_p99_hourly,

    -- Spreads, Efficiency & Revenue
    avg(fee_spread_range) as avg_fee_spread,
    max(fee_spread_range) as max_fee_spread,
    avg(interquartile_fee_range) as avg_iqr_fee,
    avg(fee_skewness_ratio) as avg_fee_skewness,
    max(fee_skewness_ratio) as max_fee_skewness,

    avg(block_compression_ratio) as avg_block_compression,
    avg(miner_revenue_per_byte) as avg_miner_revenue_per_byte,
    avg(avg_fee_per_tx) as avg_fee_per_tx,
    max(avg_fee_per_tx) as max_fee_per_tx,

    -- 🚨 Hourly Alerts
    case 
        when avg(median_fee) > 50 then 'CRITICAL: Hourly Network Congestion'
        when avg(median_fee) between 20 and 50 then 'WARNING: Elevated Fees'
        else 'HEALTHY'
    end as hourly_fee_alert,

    case 
        when max(fee_skewness_ratio) > 35 then 'ALERT: Extreme Fee Distortion'
        else 'STABLE'
    end as hourly_skewness_alert

from int_data
group by 1, 2