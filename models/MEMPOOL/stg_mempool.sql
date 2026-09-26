{{
    config(
        materialized='incremental',
        unique_key='mempool_id'
    )
}}

with mempool_stg as (
    select * from {{ source('raw_source', 'STG_MEMPOOL_PARSED') }}
    
    {% if is_incremental() %}
    where enqueued_time > (select max(enqueued_time) from {{ this }})
    {% endif %}
)


select
    -- Unique surrogate key generate
    {{ dbt_utils.generate_surrogate_key(['sequence_number', 'enqueued_time']) }} as mempool_id,
    cast(sequence_number as bigint) as sequence_number,
    cast(enqueued_time as timestamp) as enqueued_time,
    cast(block_size as bigint) as block_size,
    cast(block_vsize as bigint) as block_vsize,
    cast(median_fee as double precision) as median_fee,
    cast(n_tx as integer) as number_of_transactions,
    cast(total_fees as bigint) as total_fees,
    cast(fee_p10 as double precision) as fee_p10,
    cast(fee_p25 as double precision) as fee_p25,
    cast(fee_p50 as double precision) as fee_p50,
    cast(fee_p75 as double precision) as fee_p75,
    cast(fee_p90 as double precision) as fee_p90,
    cast(fee_p95 as double precision) as fee_p95,
    cast(fee_p99 as double precision) as fee_p99

from mempool_stg