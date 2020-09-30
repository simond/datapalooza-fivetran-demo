
{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}

{{
  config(
    materialized='incremental',
    partition_by={
      "field": "date_partition",
      "data_type": "date"
    },
    incremental_strategy = 'insert_overwrite',
    partitions = partitions_to_replace
  )
}}


select
  id,
  username,
  first_name,
  last_name,
  birthday,
  gender,
  email_address,
  ARRAY_AGG(IFNULL(cust_emails.email, 'No Email')) as other_emails,
  telephone_number,
  valid_telephone_number,
  state,
  state_full,
  city,
  company,
  occupation,
  lat_long,
  latitude,
  longitude,
  ad_mob,
  ad_words,
  adsense,
  browser_user_agent,
  browser_user_agent_group,
  country_full,
  double_click_bid_campaign_manager,
  double_click_publishers,
  google_play_store,
  submit_date,
  you_tube,
  description,
  deleted,
  DATE_ADD(current_date(), INTERVAL -1 day) AS date_partition
from
  {{ ref('customers') }} cust
left join {{ ref ('customer_emails') }} cust_emails
     on cust.id = cust_emails.user_id
group by 1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32

