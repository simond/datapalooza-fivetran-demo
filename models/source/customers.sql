{{
    config(
        materialized='ephemeral'
    )
}}

-- The "undefined" customer
select
    0 as id,
    'undefined' as username,
    'undefined' as first_name,
    'undefined' as last_name,
    null as birthday,
    'undefined' as gender,
    'undefined' as email_address,
    'undefined' as telephone_number,
    false as valid_telephone_number,
    'undefined' as state,
    'undefined' as state_full,
    'undefined' as city,
    'undefined' as company,
    'undefined' as occupation,
    'undefined' as lat_long,
    null as latitude,
    null as longitude,
    false as ad_mob,
    false as ad_words,
    false as adsense,
    'undefined' as browser_user_agent,
    'undefined' as browser_user_agent_group,
    'undefined' as country_full,
    false as double_click_bid_campaign_manager,
    false as double_click_publishers,
    false as google_play_store,
    null as submit_date,
    false as you_tube,
    'undefined' as description,
    false as deleted

union all

-- The actual customers
select
    id,
    username,
    first_name,
    last_name,
    birthday,
    gender,
    email_address,
    telephone_number,
    valid_telephone_number,
    state,
    state_full,
    city,
    company,
    occupation,
    lat_long,
    safe_cast(latitude as numeric) as latitude,
    safe_cast(longitude as numeric) as longitude,
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
    _fivetran_deleted as deleted
from
    source_postgres_public.customers
