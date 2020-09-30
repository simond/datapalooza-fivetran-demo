{{
    config(
        materialized='ephemeral'
    )
}}

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
