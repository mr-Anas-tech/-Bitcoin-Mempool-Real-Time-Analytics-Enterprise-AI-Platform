{{
    config(
        materialized='table',
        cluster_by=['mempool_date']
    )
}}

with int_data as (
    select * from {{ ref('int_mempool') }}
)

select
    -- Date Bucket
    date_trunc('day', enqueued_time) as mempool_date,

    -- Global Aggregations
    count(distinct sequence_number) as total_blocks_processed,
    sum(number_of_transactions) as total_daily_transactions,
    avg(number_of_transactions) as avg_tx_per_block,
    max(number_of_transactions) as max_tx_in_single_block,

    -- Size Aggregations
    sum(block_size) as total_daily_bytes,
    avg(block_size) as avg_daily_block_size,
    sum(block_vsize) as total_daily_vbytes,

    -- Total Revenue & Fee Distributions
    sum(total_fees) as daily_total_miner_revenue,
    avg(total_fees) as daily_avg_block_fees,
    max(total_fees) as max_single_block_fee,

    avg(median_fee) as daily_avg_median_fee,
    median(median_fee) as daily_overall_median_fee,
    min(fee_p10) as daily_min_p10_fee,
    max(fee_p99) as daily_max_p99_fee,

    -- Calculated Risk & Ratio Metrics
    avg(fee_spread_range) as daily_avg_fee_spread,
    avg(fee_skewness_ratio) as daily_avg_fee_skewness,
    avg(block_compression_ratio) as daily_avg_compression_ratio,
    avg(miner_revenue_per_byte) as daily_avg_revenue_per_byte,
    avg(avg_fee_per_tx) as daily_avg_fee_per_tx,

    -- Congestion Breakdown Counters
    count(case when network_congestion_status = 'High Congestion' then 1 end) as high_congestion_blocks,
    count(case when network_congestion_status = 'Moderate Congestion' then 1 end) as moderate_congestion_blocks,
    count(case when network_congestion_status = 'Low Congestion' then 1 end) as low_congestion_blocks,

    -- 🚨 Daily Alerts
    case 
        when count(case when network_congestion_status = 'High Congestion' then 1 end) > 20 
            then 'ALERT: Network Stress Day'
        else 'NORMAL'
    end as daily_network_stress_alert,

    case 
        when max(fee_p99) > 300 then 'ALERT: Severe Fee Volatility Day'
        else 'STABLE'
    end as daily_fee_volatility_alert

from int_data
group by 1