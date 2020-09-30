
{{
  config(
    materialized='ephemeral'
  )
}}

-- Only need the latest file from GCS
with max_file_date as (
  select
    max(cast(substr(_file, 0, 10) as date)) as dt
  from source_gcs.customer_emails
)

select
  user_id,
  email
from source_gcs.customer_emails
where cast(substr(_file, 0, 10) as date) = (select dt from max_file_date)
