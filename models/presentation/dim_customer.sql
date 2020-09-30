
{{
    config(
        materialized='view',
    )
}}

-- Only show the latest date partition in the view
with max_dp as (
    select
        max(date_partition) max_date_partition
    from {{ ref('dim_customer_hist') }}
)

select
    * except (max_date_partition)
from {{ ref('dim_customer_hist') }}
join max_dp
    on date_partition = max_dp.max_date_partition
