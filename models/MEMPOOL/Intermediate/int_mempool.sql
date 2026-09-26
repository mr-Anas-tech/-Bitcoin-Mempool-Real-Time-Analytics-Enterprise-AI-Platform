with staging as (
    select * from {{ ref('stg_mempool') }}
)

select
    mempool_id,
    sequence_number,
    enqueued_time,

    block_size,
    block_vsize,
    number_of_transactions,
    total_fees,
    
    median_fee,
    fee_p10,
    fee_p25,
    fee_p50,
    fee_p75,
    fee_p90,
    fee_p95,
    fee_p99,
    
    -- 1. Fee Spreads & Volatility Indicators
    (fee_p99 - fee_p10) as fee_spread_range,
    (fee_p75 - fee_p25) as interquartile_fee_range,
    
    -- 2. Fee Pressure & Skewness Ratios
    case 
        when fee_p10 > 0 then (fee_p99 / fee_p10) 
        else null 
    end as fee_skewness_ratio,

    -- 3. Block Efficiency & Density Metrics
    case 
        when block_size > 0 then (cast(block_vsize as float) / block_size) 
        else 0 
    end as block_compression_ratio,
    
    case 
        when block_size > 0 then (cast(total_fees as float) / block_size) 
        else 0 
    end as miner_revenue_per_byte,
    
    -- 4. Average Fee per Transaction
    case 
        when number_of_transactions > 0 then (cast(total_fees as float) / number_of_transactions) 
        else 0 
    end as avg_fee_per_tx,

    -- 5. Network Congestion Categorization Flag
    case 
        when median_fee > 50 then 'High Congestion'
        when median_fee between 20 and 50 then 'Moderate Congestion'
        else 'Low Congestion'
    end as network_congestion_status

from staging