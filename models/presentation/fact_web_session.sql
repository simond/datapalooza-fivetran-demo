
{{
  config(
    materialized='incremental',
    partition_by={
      "field": "visitTimestamp",
      "data_type": "timestamp"
    },
    incremental_strategy = 'merge',
  )
}}


select
    IFNULL(dim_customer.id, 0) as customerid,
    STRUCT(
      dim_customer.id as customerid,
      dim_customer.username as username,
      dim_customer.first_name as first_name,
      dim_customer.last_name as last_name,
      dim_customer.birthday as birthday,
      dim_customer.gender as gender,
      dim_customer.email_address as email_address,
      dim_customer.other_emails as other_emails,
      dim_customer.telephone_number as telephone_number,
      dim_customer.state as state,
      dim_customer.state_full as state_full,
      dim_customer.city as city,
      dim_customer.company as company,
      dim_customer.occupation as occupation,
      dim_customer.lat_long as lat_long,
      safe_cast(dim_customer.latitude as numeric) as latitude,
      safe_cast(dim_customer.longitude as numeric) as longitude,
      dim_customer.ad_mob as ad_mob,
      dim_customer.ad_words as ad_words,
      dim_customer.adsense as adsense,
      dim_customer.browser_user_agent as browser_user_agent,
      dim_customer.browser_user_agent_group as browser_user_agent_group,
      dim_customer.country_full as country_full,
      dim_customer.double_click_bid_campaign_manager as double_click_bid_campaign_manager,
      dim_customer.double_click_publishers as double_click_publishers,
      dim_customer.google_play_store as google_play_store,
      dim_customer.submit_date as submit_date,
      dim_customer.you_tube as you_tube,
      dim_customer.date_partition as date_partition
    ) as customer,
    sessions.channelGrouping,
    sessions.fullVisitorId,
    sessions.customDimensions,
    sessions.socialEngagementType,
    sessions.trafficSource,
    sessions.device,
    sessions.hits,
    sessions.date,
    sessions.visitId,
    sessions.visitStartTime,
    sessions.visitNumber,
    sessions.visitTime,
    sessions.geoNetgeoNetwork,
    sessions.visitTimestamp
from
  {{ ref('sessions') }} as sessions
left join {{ ref('dim_customer') }} as dim_customer
  on IFNULL(sessions.userid, 0) = dim_customer.id

  -- If we're not doinga a full load
  {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where visitTimestamp > (select max(visitTimestamp) from {{ this }})

  {% endif %}
