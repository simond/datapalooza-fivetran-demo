
{{
    config(
        materialized='view',
    )
}}

WITH hits AS(
    SELECT
      fullVisitorId,
      visitNumber,
      visitStartTime,
      hits.hitNumber AS hitNumber,
      hits.time AS time,
      hits.latencyTracking.domainLookupTime AS domainLookupTime,
      hits.latencyTracking.domContentLoadedTime AS domContentLoadedTime,
      hits.latencyTracking.domInteractiveTime AS domInteractiveTime,
      hits.latencyTracking.speedMetricsSample AS speedMetricsSample,
      hits.latencyTracking.serverResponseTime AS serverResponseTime,
      hits.latencyTracking.redirectionTime AS redirectionTime,
      hits.latencyTracking.pageLoadTime AS pageLoadTime,
      hits.latencyTracking.serverConnectionTime AS serverConnectionTime,
      hits.latencyTracking.domLatencyMetricsSample AS domLatencyMetricsSample,
      hits.latencyTracking.pageDownloadTime AS pageDownloadTime,
      hits.latencyTracking.pageLoadSample AS pageLoadSample,
      hits.contentGroup.contentGroupUniqueViews3 AS contentGroupUniqueViews3,
      hits.contentGroup.contentGroupUniqueViews2 AS contentGroupUniqueViews2,
      hits.contentGroup.contentGroupUniqueViews1 AS contentGroupUniqueViews1,
      hits.eCommerceAction.action_type AS action_type,
      --strings
      hits.contentGroup.previousContentGroup2 AS previousContentGroup2,
      hits.contentGroup.contentGroup4 AS contentGroup4,
      hits.contentGroup.previousContentGroup5 AS previousContentGroup5,
      hits.contentGroup.contentGroup3 AS contentGroup3,
      hits.contentGroup.previousContentGroup3 AS previousContentGroup3,
      hits.contentGroup.contentGroup1 AS contentGroup1,
      hits.contentGroup.contentGroup2 AS contentGroup2,
      hits.contentGroup.previousContentGroup1 AS previousContentGroup1,
      hits.social.socialNetwork AS socialNetwork,
      hits.type AS type,
      hits.page.pagePathLevel4 AS pagePathLevel4,
      hits.page.pagePathLevel3 AS pagePathLevel3,
      hits.page.pagePathLevel2 AS pagePathLevel2,
      hits.page.pagePathLevel1 AS pagePathLevel1,
      hits.page.pageTitle AS pageTitle,
      hits.page.pagePath AS pagePath,
      hits.appInfo.exitScreenName AS exitScreenName,
      hits.appInfo.landingScreenName AS landingScreenName,
      hits.appInfo.screenName AS screenName,
      --bool
      IFNULL(hits.social.hasSocialSourceReferral, 'false') AS hasSocialSourceReferral,
      --new features
      LEAD(hits.time, 1) OVER (PARTITION BY CAST(fullVisitorId AS STRING), visitNumber ORDER BY hits.hitNumber ASC ) - hits.time AS hitDuration, -- DO NOT IFNULL -- COULD USE IMPROVEMENT IF TIME ALLOWS
      EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime + hits.time)) AS hour_of_day,
      EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(visitStartTime + hits.time)) AS day_of_week,
      EXTRACT(DAY FROM TIMESTAMP_SECONDS(visitStartTime + hits.time)) AS day_of_month
    FROM
      {{ ref('fact_web_sessions') }},
      UNNEST(hits) AS hits 
  ),

  hits_agg AS(
    SELECT
      fullVisitorId,
      MAX(hitNumber) AS hitNumber,
      MAX(time) AS visitDuration,
      IFNULL(ARRAY_LENGTH(ARRAY_AGG(action_type)), 0) AS action_type_count,
      IFNULL(ARRAY_LENGTH(ARRAY_AGG(DISTINCT action_type)), 0) AS action_type_distinct_count,
      IFNULL(ARRAY_TO_STRING(ARRAY_AGG(CAST(action_type AS STRING) ORDER BY CAST(action_type AS STRING)), '-'), "null_string") AS action_type_pattern,
      IFNULL(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(action_type AS STRING) ORDER BY CAST(action_type AS STRING)), '-'), "null_string") AS action_type_distinct_pattern,
      --int64
      IFNULL(MIN(domainLookupTime), 0) AS min_domainLookupTime,
      IFNULL(MIN(domContentLoadedTime), 0) AS min_domContentLoadedTime,
      IFNULL(MIN(domInteractiveTime), 0) AS min_domInteractiveTime,
      IFNULL(MIN(speedMetricsSample), 0) AS min_speedMetricsSample,
      IFNULL(MIN(serverResponseTime), 0) AS min_serverResponseTime,
      IFNULL(MIN(redirectionTime), 0) AS min_redirectionTime,
      IFNULL(MIN(pageLoadTime), 0) AS min_pageLoadTime,
      IFNULL(MIN(serverConnectionTime), 0) AS min_serverConnectionTime,
      IFNULL(MIN(domLatencyMetricsSample), 0) AS min_domLatencyMetricsSample,
      IFNULL(MIN(pageDownloadTime), 0) AS min_pageDownloadTime,
      IFNULL(MIN(pageLoadSample), 0) AS min_pageLoadSample,
      IFNULL(MIN(contentGroupUniqueViews3), 0) AS min_contentGroupUniqueViews3,
      IFNULL(MIN(contentGroupUniqueViews2), 0) AS min_contentGroupUniqueViews2,
      IFNULL(MIN(contentGroupUniqueViews1), 0) AS min_contentGroupUniqueViews1,
      IFNULL(MIN(hitDuration), 0) AS min_HitDuration,
      MIN(hour_of_day) AS min_hour_of_day,
      MIN(day_of_week) AS min_day_of_week,
      MIN(day_of_month) AS min_day_of_month,
      IFNULL(AVG(domainLookupTime), 0) AS avg_domainLookupTime,
      IFNULL(AVG(domContentLoadedTime), 0) AS avg_domContentLoadedTime,
      IFNULL(AVG(domInteractiveTime), 0) AS avg_domInteractiveTime,
      IFNULL(AVG(speedMetricsSample), 0) AS avg_speedMetricsSample,
      IFNULL(AVG(serverResponseTime), 0) AS avg_serverResponseTime,
      IFNULL(AVG(redirectionTime), 0) AS avg_redirectionTime,
      IFNULL(AVG(pageLoadTime), 0) AS avg_pageLoadTime,
      IFNULL(AVG(serverConnectionTime), 0) AS avg_serverConnectionTime,
      IFNULL(AVG(domLatencyMetricsSample), 0) AS avg_domLatencyMetricsSample,
      IFNULL(AVG(pageDownloadTime), 0) AS avg_pageDownloadTime,
      IFNULL(AVG(pageLoadSample), 0) AS avg_pageLoadSample,
      IFNULL(AVG(contentGroupUniqueViews3), 0) AS avg_contentGroupUniqueViews3,
      IFNULL(AVG(contentGroupUniqueViews2), 0) AS avg_contentGroupUniqueViews2,
      IFNULL(AVG(contentGroupUniqueViews1), 0) AS avg_contentGroupUniqueViews1,
      IFNULL(AVG(hitDuration), 0) AS avg_HitDuration,
      AVG(hour_of_day) AS avg_hour_of_day, AVG(day_of_week) AS avg_day_of_week,
      AVG(day_of_month) AS avg_day_of_month, IFNULL(MAX(domainLookupTime), 0) AS max_domainLookupTime,
      IFNULL(MAX(domContentLoadedTime), 0) AS max_domContentLoadedTime,
      IFNULL(MAX(domInteractiveTime), 0) AS max_domInteractiveTime,
      IFNULL(MAX(speedMetricsSample), 0) AS max_speedMetricsSample,
      IFNULL(MAX(serverResponseTime), 0) AS max_serverResponseTime,
      IFNULL(MAX(redirectionTime), 0) AS max_redirectionTime,
      IFNULL(MAX(pageLoadTime), 0) AS max_pageLoadTime,
      IFNULL(MAX(serverConnectionTime), 0) AS max_serverConnectionTime,
      IFNULL(MAX(domLatencyMetricsSample), 0) AS max_domLatencyMetricsSample,
      IFNULL(MAX(pageDownloadTime), 0) AS max_pageDownloadTime,
      IFNULL(MAX(pageLoadSample), 0) AS max_pageLoadSample,
      IFNULL(MAX(contentGroupUniqueViews3), 0) AS max_contentGroupUniqueViews3,
      IFNULL(MAX(contentGroupUniqueViews2), 0) AS max_contentGroupUniqueViews2,
      IFNULL(MAX(contentGroupUniqueViews1), 0) AS max_contentGroupUniqueViews1,
      IFNULL(MAX(hitDuration), 0) AS max_HitDuration,
      MAX(hour_of_day) AS max_hour_of_day,
      MAX(day_of_week) AS max_day_of_week,
      MAX(day_of_month) AS max_day_of_month,
      IFNULL(STDDEV(domainLookupTime), 0) AS std_domainLookupTime,
      IFNULL(STDDEV(domContentLoadedTime), 0) AS std_domContentLoadedTime,
      IFNULL(STDDEV(domInteractiveTime), 0) AS std_domInteractiveTime,
      IFNULL(STDDEV(speedMetricsSample), 0) AS std_speedMetricsSample,
      IFNULL(STDDEV(serverResponseTime), 0) AS std_serverResponseTime,
      IFNULL(STDDEV(redirectionTime), 0) AS std_redirectionTime,
      IFNULL(STDDEV(pageLoadTime), 0) AS std_pageLoadTime,
      IFNULL(STDDEV(serverConnectionTime), 0) AS std_serverConnectionTime,
      IFNULL(STDDEV(domLatencyMetricsSample), 0) AS std_domLatencyMetricsSample,
      IFNULL(STDDEV(pageDownloadTime), 0) AS std_pageDownloadTime,
      IFNULL(STDDEV(pageLoadSample), 0) AS std_pageLoadSample,
      IFNULL(STDDEV(contentGroupUniqueViews3), 0) AS std_contentGroupUniqueViews3,
      IFNULL(STDDEV(contentGroupUniqueViews2), 0) AS std_contentGroupUniqueViews2,
      IFNULL(STDDEV(contentGroupUniqueViews1), 0) AS std_contentGroupUniqueViews1,
      IFNULL(STDDEV(hitDuration), 0) AS std_HitDuration,
      --strings
      ARRAY_LENGTH(ARRAY_AGG(previousContentGroup1)) AS previousContentGroup1_count,
      ARRAY_LENGTH(ARRAY_AGG(previousContentGroup2)) AS previousContentGroup2_count,
      ARRAY_LENGTH(ARRAY_AGG(previousContentGroup3)) AS previousContentGroup3_count,
      ARRAY_LENGTH(ARRAY_AGG(previousContentGroup5)) AS previousContentGroup5_count,
      ARRAY_LENGTH(ARRAY_AGG(contentGroup1)) AS contentGroup1_count,
      ARRAY_LENGTH(ARRAY_AGG(contentGroup2)) AS contentGroup2_count,
      ARRAY_LENGTH(ARRAY_AGG(contentGroup3)) AS contentGroup3_count,
      ARRAY_LENGTH(ARRAY_AGG(contentGroup4)) AS contentGroup4_count,
      ARRAY_LENGTH(ARRAY_AGG(socialNetwork)) AS socialNetwork_count,
      ARRAY_LENGTH(ARRAY_AGG(type)) AS type_count,
      ARRAY_LENGTH(ARRAY_AGG(pagePathLevel1)) AS pagePathLevel1_count,
      ARRAY_LENGTH(ARRAY_AGG(pagePathLevel2)) AS pagePathLevel2_count,
      ARRAY_LENGTH(ARRAY_AGG(pagePathLevel3)) AS pagePathLevel3_count,
      ARRAY_LENGTH(ARRAY_AGG(pagePathLevel4)) AS pagePathLevel4_count,
      ARRAY_LENGTH(ARRAY_AGG(pageTitle)) AS pageTitle_count,
      ARRAY_LENGTH(ARRAY_AGG(pagePath)) AS pagePath_count,
      ARRAY_LENGTH(ARRAY_AGG(exitScreenName)) AS exitScreenName_count,
      ARRAY_LENGTH(ARRAY_AGG(landingScreenName)) AS landingScreenName_count,
      ARRAY_LENGTH(ARRAY_AGG(ScreenName)) AS ScreenName_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT previousContentGroup1)) AS previousContentGroup1_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT previousContentGroup2)) AS previousContentGroup2_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT previousContentGroup3)) AS previousContentGroup3_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT previousContentGroup5)) AS previousContentGroup5_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT contentGroup1)) AS contentGroup1_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT contentGroup2)) AS contentGroup2_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT contentGroup3)) AS contentGroup3_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT contentGroup4)) AS contentGroup4_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT socialNetwork)) AS socialNetwork_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT type)) AS type_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pagePathLevel1)) AS pagePathLevel1_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pagePathLevel2)) AS pagePathLevel2_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pagePathLevel3)) AS pagePathLevel3_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pagePathLevel4)) AS pagePathLevel4_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pageTitle)) AS pageTitle_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT pagePath)) AS pagePath_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT exitScreenName)) AS exitScreenName_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT landingScreenName)) AS landingScreenName_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT ScreenName)) AS ScreenName_distinct_count,
      ARRAY_TO_STRING(ARRAY_AGG(previousContentGroup1 ORDER BY previousContentGroup1), '-') AS previousContentGroup1_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(previousContentGroup2 ORDER BY previousContentGroup2), '-') AS previousContentGroup2_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(previousContentGroup3 ORDER BY previousContentGroup3), '-') AS previousContentGroup3_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(previousContentGroup5 ORDER BY previousContentGroup5), '-') AS previousContentGroup5_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(contentGroup1 ORDER BY contentGroup1), '-') AS contentGroup1_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(contentGroup2 ORDER BY contentGroup2), '-') AS contentGroup2_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(contentGroup3 ORDER BY contentGroup3), '-') AS contentGroup3_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(contentGroup4 ORDER BY contentGroup4), '-') AS contentGroup4_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(socialNetwork ORDER BY socialNetwork), '-') AS socialNetwork_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(type ORDER BY type), '-') AS type_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pagePathLevel1 ORDER BY pagePathLevel1), '-') AS pagePathLevel1_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pagePathLevel2 ORDER BY pagePathLevel2), '-') AS pagePathLevel2_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pagePathLevel3 ORDER BY pagePathLevel3), '-') AS pagePathLevel3_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pagePathLevel4 ORDER BY pagePathLevel4), '-') AS pagePathLevel4_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pageTitle ORDER BY pageTitle), '-') AS pageTitle_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(pagePath ORDER BY pagePath), '-') AS pagePath_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(exitScreenName ORDER BY exitScreenName), '-') AS exitScreenName_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(landingScreenName ORDER BY landingScreenName), '-') AS landingScreenName_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(ScreenName ORDER BY ScreenName), '-') AS ScreenName_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT previousContentGroup1 ORDER BY previousContentGroup1), '-') AS previousContentGroup1_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT previousContentGroup2 ORDER BY previousContentGroup2), '-') AS previousContentGroup2_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT previousContentGroup3 ORDER BY previousContentGroup3), '-') AS previousContentGroup3_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT previousContentGroup5 ORDER BY previousContentGroup5), '-') AS previousContentGroup5_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT contentGroup1 ORDER BY contentGroup1), '-') AS contentGroup1_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT contentGroup2 ORDER BY contentGroup2), '-') AS contentGroup2_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT contentGroup3 ORDER BY contentGroup3), '-') AS contentGroup3_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT contentGroup4 ORDER BY contentGroup4), '-') AS contentGroup4_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT socialNetwork ORDER BY socialNetwork), '-') AS socialNetwork_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT type ORDER BY type), '-') AS type_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pagePathLevel1 ORDER BY pagePathLevel1), '-') AS pagePathLevel1_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pagePathLevel2 ORDER BY pagePathLevel2), '-') AS pagePathLevel2_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pagePathLevel3 ORDER BY pagePathLevel3), '-') AS pagePathLevel3_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pagePathLevel4 ORDER BY pagePathLevel4), '-') AS pagePathLevel4_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pageTitle ORDER BY pageTitle), '-') AS pageTitle_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT pagePath ORDER BY pagePath), '-') AS pagePath_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT exitScreenName ORDER BY exitScreenName), '-') AS exitScreenName_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT landingScreenName ORDER BY landingScreenName), '-') AS landingScreenName_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ScreenName ORDER BY ScreenName), '-') AS ScreenName_distinct_pattern,
      --bool
      ARRAY_LENGTH(ARRAY_AGG(hasSocialSourceReferral)) AS hasSocialSourceReferral_count,
      ARRAY_TO_STRING(ARRAY_AGG(CAST(hasSocialSourceReferral AS STRING) ORDER BY CAST(hasSocialSourceReferral AS STRING)), '-') AS hasSocialSourceReferral_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(hasSocialSourceReferral AS STRING) ORDER BY CAST(hasSocialSourceReferral AS STRING)), '-') AS hasSocialSourceReferral_distinct_pattern
    FROM
      hits
    GROUP BY
      fullVisitorId 
  ),

  layer_one AS(
    SELECT
      fullVisitorId,
      -- important but unused
      --, visitStartTime
      --keep for agg
      visitNumber,
      --strings
      customer.username as customer_username, -- Simon Added 2020-09-02
      customer.first_name as customer_first_name, -- Simon Added 2020-09-02
      customer.last_name as customer_last_name, -- Simon Added 2020-09-02
      customer.gender as customer_gender, -- Simon Added 2020-09-02
      customer.email_address as customer_email_address, -- Simon Added 2020-09-02
      customer.telephone_number as customer_telephone_number, -- Simon Added 2020-09-02
      customer.state as customer_state, -- Simon Added 2020-09-02
      customer.city as customer_city, -- Simon Added 2020-09-02
      customer.company as customer_company, -- Simon Added 2020-09-02
      customer.occupation as customer_occupation, -- Simon Added 2020-09-02
      customer.lat_long as customer_lat_long, -- Simon Added 2020-09-02
      customer.browser_user_agent as customer_browser_user_agent, -- Simon Added 2020-09-02
      customer.browser_user_agent_group as customer_browser_user_agent_group, -- Simon Added 2020-09-02
      channelGrouping AS channelGrouping,
      trafficSource.source AS trafficSource_source,
      trafficSource.campaign AS trafficSource_campaign,
      trafficSource.medium AS trafficSource_medium,
      device.deviceCategory AS device_deviceCategory,
      device.operatingSystem AS device_operatingSystem,
      device.browser AS device_browser,
      geoNetgeoNetwork.city AS geoNetgeoNetwork_city,
      geoNetgeoNetwork.metro AS geoNetgeoNetwork_metro,
      geoNetgeoNetwork.networkDomain AS geoNetgeoNetwork_networkDomain,
      geoNetgeoNetwork.subContinent AS geoNetgeoNetwork_subContinent,
      geoNetgeoNetwork.country AS geoNetgeoNetwork_country,
      geoNetgeoNetwork.continent AS geoNetgeoNetwork_continent,
      geoNetgeoNetwork.region AS geoNetgeoNetwork_region,
      customDimensions[SAFE_OFFSET(0)].value AS customDimensionsRegion,
      --bool
      IFNULL(customer.ad_mob, FALSE) as customer_ad_mob, -- Simon Added 2020-09-02
      IFNULL(customer.ad_words, FALSE) as customer_ad_words, -- Simon Added 2020-09-02
      IFNULL(customer.adsense, FALSE) as customer_adsense, -- Simon Added 2020-09-02
      IFNULL(customer.double_click_bid_campaign_manager, FALSE) as customer_double_click_bid_campaign_manager, -- Simon Added 2020-09-02
      IFNULL(customer.double_click_publishers, FALSE) as customer_double_click_publishers, -- Simon Added 2020-09-02
      IFNULL(customer.google_play_store, FALSE) as customer_google_play_store, -- Simon Added 2020-09-02
      IFNULL(customer.you_tube, FALSE) as customer_you_tube, -- Simon Added 2020-09-02
      IFNULL(device.isMobile, FALSE) AS device_isMobile,
      IFNULL(trafficSource.isTrueDirect, FALSE) AS isTrueDirect,
      --new feature
      ARRAY_LENGTH(hits) AS hit_count
    FROM
      {{ ref('fact_web_sessions') }}
  ),

  layer_one_agg AS (
    SELECT
      fullVisitorId,
      --unique cases
      MAX(visitNumber) AS total_visits,
      --int64
      IFNULL(MIN(hit_count), 0) AS min_hit_count,
      IFNULL(AVG(hit_count), 0) AS avg_hit_count,
      IFNULL(MAX(hit_count), 0) AS max_hit_count,
      IFNULL(STDDEV(hit_count), 0) AS std_hit_count,
      --strings
      ARRAY_LENGTH(ARRAY_AGG(customer_username)) as customer_username_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_first_name)) as customer_first_name_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_last_name)) as customer_last_name_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_gender)) as customer_gender_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_email_address)) as customer_email_address_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_telephone_number)) as customer_telephone_number_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_state)) as customer_state_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_city)) as customer_city_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_company)) as customer_company_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_occupation)) as customer_occupation_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_lat_long)) as customer_lat_long_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_browser_user_agent)) as customer_browser_user_agent_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(customer_browser_user_agent_group)) as customer_browser_user_agent_group_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(channelGrouping)) AS channelGrouping_count,
      ARRAY_LENGTH(ARRAY_AGG(trafficSource_source)) AS trafficSource_source_count,
      ARRAY_LENGTH(ARRAY_AGG(trafficSource_campaign)) AS trafficSource_campaign_count,
      ARRAY_LENGTH(ARRAY_AGG(trafficSource_medium)) AS trafficSource_medium_count,
      ARRAY_LENGTH(ARRAY_AGG(device_deviceCategory)) AS device_deviceCategory_count,
      ARRAY_LENGTH(ARRAY_AGG(device_operatingSystem)) AS device_operatingSystem_count,
      ARRAY_LENGTH(ARRAY_AGG(device_browser)) AS device_browser_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_city)) AS geoNetgeoNetwork_city_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_metro)) AS geoNetgeoNetwork_metro_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_networkDomain)) AS geoNetgeoNetwork_networkDomain_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_subContinent)) AS geoNetgeoNetwork_subContinent_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_country)) AS geoNetgeoNetwork_country_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_continent)) AS geoNetgeoNetwork_continent_count,
      ARRAY_LENGTH(ARRAY_AGG(geoNetgeoNetwork_region)) AS geoNetgeoNetwork_region_count,
      ARRAY_LENGTH(ARRAY_AGG(customDimensionsRegion)) AS customDimensionsRegion_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_username)) as customer_username_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_first_name)) as customer_first_name_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_last_name)) as customer_last_name_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_gender)) as customer_gender_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_email_address)) as customer_email_address_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_telephone_number)) as customer_telephone_number_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_state)) as customer_state_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_city)) as customer_city_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_company)) as customer_company_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_occupation)) as customer_occupation_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_lat_long)) as customer_lat_long_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_browser_user_agent)) as customer_browser_user_agent_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customer_browser_user_agent_group)) as customer_browser_user_agent_group_distinct_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT channelGrouping)) AS channelGrouping_distinct_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT trafficSource_source)) AS trafficSource_source_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT trafficSource_campaign)) AS trafficSource_campaign_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT trafficSource_medium)) AS trafficSource_medium_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT device_deviceCategory)) AS device_deviceCategory_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT device_operatingSystem)) AS device_operatingSystem_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT device_browser)) AS device_browser_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_city)) AS geoNetgeoNetwork_city_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_metro)) AS geoNetgeoNetwork_metro_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_networkDomain)) AS geoNetgeoNetwork_networkDomain_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_subContinent)) AS geoNetgeoNetwork_subContinent_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_country)) AS geoNetgeoNetwork_country_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_continent)) AS geoNetgeoNetwork_continent_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT geoNetgeoNetwork_region)) AS geoNetgeoNetwork_region_distinct_count,
      ARRAY_LENGTH(ARRAY_AGG(DISTINCT customDimensionsRegion)) AS customDimensionsRegion_distinct_count,
      ARRAY_TO_STRING(ARRAY_AGG(customer_username ORDER BY customer_username), '-') as customer_username_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_first_name ORDER BY customer_first_name), '-') as customer_first_name_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_last_name ORDER BY customer_last_name), '-') as customer_last_name_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_gender ORDER BY customer_gender), '-') as customer_gender_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_email_address ORDER BY customer_email_address), '-') as customer_email_address_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_telephone_number ORDER BY customer_telephone_number), '-') as customer_telephone_number_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_state ORDER BY customer_state), '-') as customer_state_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_city ORDER BY customer_city), '-') as customer_city_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_company ORDER BY customer_company), '-') as customer_company_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_occupation ORDER BY customer_occupation), '-') as customer_occupation_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_lat_long ORDER BY customer_lat_long), '-') as customer_lat_long_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_browser_user_agent ORDER BY customer_browser_user_agent), '-') as customer_browser_user_agent_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(customer_browser_user_agent_group ORDER BY customer_browser_user_agent_group), '-') as customer_browser_user_agent_group_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(channelGrouping ORDER BY channelGrouping), '-') AS channelGrouping_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(trafficSource_source ORDER BY trafficSource_source), '-') AS trafficSource_source_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(trafficSource_campaign ORDER BY trafficSource_campaign), '-') AS trafficSource_campaign_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(trafficSource_medium ORDER BY trafficSource_medium), '-') AS trafficSource_medium_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(device_deviceCategory ORDER BY device_deviceCategory), '-') AS device_deviceCategory_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(device_operatingSystem ORDER BY device_operatingSystem), '-') AS device_operatingSystem_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(device_browser ORDER BY device_browser), '-') AS device_browser_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_city ORDER BY geoNetgeoNetwork_city), '-') AS geoNetgeoNetwork_city_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_metro ORDER BY geoNetgeoNetwork_metro), '-') AS geoNetgeoNetwork_metro_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_networkDomain ORDER BY geoNetgeoNetwork_networkDomain), '-') AS geoNetgeoNetwork_networkDomain_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_subContinent ORDER BY geoNetgeoNetwork_subContinent), '-') AS geoNetgeoNetwork_subContinent_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_country ORDER BY geoNetgeoNetwork_country), '-') AS geoNetgeoNetwork_country_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_continent ORDER BY geoNetgeoNetwork_continent), '-') AS geoNetgeoNetwork_continent_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(geoNetgeoNetwork_region ORDER BY geoNetgeoNetwork_region), '-') AS geoNetgeoNetwork_region_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(customDimensionsRegion ORDER BY customDimensionsRegion), '-') AS customDimensionsRegion_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_username ORDER BY customer_username), '-') as customer_username_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_first_name ORDER BY customer_first_name), '-') as customer_first_name_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_last_name ORDER BY customer_last_name), '-') as customer_last_name_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_gender ORDER BY customer_gender), '-') as customer_gender_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_email_address ORDER BY customer_email_address), '-') as customer_email_address_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_telephone_number ORDER BY customer_telephone_number), '-') as customer_telephone_number_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_state ORDER BY customer_state), '-') as customer_state_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_city ORDER BY customer_city), '-') as customer_city_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_company ORDER BY customer_company), '-') as customer_company_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_occupation ORDER BY customer_occupation), '-') as customer_occupation_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_lat_long ORDER BY customer_lat_long), '-') as customer_lat_long_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_browser_user_agent ORDER BY customer_browser_user_agent), '-') as customer_browser_user_agent_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_browser_user_agent_group ORDER BY customer_browser_user_agent_group), '-') as customer_browser_user_agent_group_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT channelGrouping ORDER BY channelGrouping), '-') AS channelGrouping_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT trafficSource_source ORDER BY trafficSource_source), '-') AS trafficSource_source_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT trafficSource_campaign ORDER BY trafficSource_campaign), '-') AS trafficSource_campaign_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT trafficSource_medium ORDER BY trafficSource_medium), '-') AS trafficSource_medium_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT device_deviceCategory ORDER BY device_deviceCategory), '-') AS device_deviceCategory_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT device_operatingSystem ORDER BY device_operatingSystem), '-') AS device_operatingSystem_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT device_browser ORDER BY device_browser), '-') AS device_browser_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_city ORDER BY geoNetgeoNetwork_city), '-') AS geoNetgeoNetwork_city_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_metro ORDER BY geoNetgeoNetwork_metro), '-') AS geoNetgeoNetwork_metro_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_networkDomain ORDER BY geoNetgeoNetwork_networkDomain), '-') AS geoNetgeoNetwork_networkDomain_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_subContinent ORDER BY geoNetgeoNetwork_subContinent), '-') AS geoNetgeoNetwork_subContinent_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_country ORDER BY geoNetgeoNetwork_country), '-') AS geoNetgeoNetwork_country_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_continent ORDER BY geoNetgeoNetwork_continent), '-') AS geoNetgeoNetwork_continent_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT geoNetgeoNetwork_region ORDER BY geoNetgeoNetwork_region), '-') AS geoNetgeoNetwork_region_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customDimensionsRegion ORDER BY customDimensionsRegion), '-') AS customDimensionsRegion_distinct_pattern,
      --bool
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_ad_mob, FALSE))) as customer_ad_mob_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_ad_words, FALSE))) as customer_ad_words_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_adsense, FALSE))) as customer_adsense_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_double_click_bid_campaign_manager, FALSE))) as customer_double_click_bid_campaign_manager_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_double_click_publishers, FALSE))) as customer_double_click_publishers_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_google_play_store, FALSE))) as customer_google_play_store_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(customer_you_tube, FALSE))) as customer_you_tube_count, -- Simon Added 2020-09-02
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(device_isMobile, FALSE))) AS device_isMobile_count,
      ARRAY_LENGTH(ARRAY_AGG(IFNULL(isTrueDirect, FALSE))) isTrueDirect_count,
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_ad_mob, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_ad_mob, FALSE) AS STRING)), '-') AS customer_ad_mob_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_ad_words, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_ad_words, FALSE) AS STRING)), '-') AS customer_ad_words_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_adsense, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_adsense, FALSE) AS STRING)), '-') AS customer_adsense_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_double_click_bid_campaign_manager, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_double_click_bid_campaign_manager, FALSE) AS STRING)), '-') AS customer_double_click_bid_campaign_manager_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_double_click_publishers, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_double_click_publishers, FALSE) AS STRING)), '-') AS customer_double_click_publishers_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_google_play_store, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_google_play_store, FALSE) AS STRING)), '-') AS customer_google_play_store_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(customer_you_tube, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_you_tube, FALSE) AS STRING)), '-') AS customer_you_tube_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(device_isMobile, FALSE) AS STRING) ORDER BY CAST(IFNULL(device_isMobile, FALSE) AS STRING)), '-') AS device_isMobile_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(CAST(IFNULL(isTrueDirect, FALSE) AS STRING) ORDER BY CAST(IFNULL(isTrueDirect, FALSE) AS STRING)), '-') isTrueDirect_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_ad_mob, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_ad_mob, FALSE) AS STRING)), '-') AS customer_ad_mob_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_ad_words, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_ad_words, FALSE) AS STRING)), '-') AS customer_ad_words_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_adsense, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_adsense, FALSE) AS STRING)), '-') AS customer_adsense_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_double_click_bid_campaign_manager, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_double_click_bid_campaign_manager, FALSE) AS STRING)), '-') AS customer_double_click_bid_campaign_manager_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_double_click_publishers, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_double_click_publishers, FALSE) AS STRING)), '-') AS customer_double_click_publishers_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_google_play_store, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_google_play_store, FALSE) AS STRING)), '-') AS customer_google_play_store_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(customer_you_tube, FALSE) AS STRING) ORDER BY CAST(IFNULL(customer_you_tube, FALSE) AS STRING)), '-') AS customer_you_tube_distinct_pattern, -- Simon Added 2020-09-02
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(device_isMobile, FALSE) AS STRING) ORDER BY CAST(IFNULL(device_isMobile, FALSE) AS STRING)), '-') AS device_isMobile_distinct_distinct_pattern,
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(IFNULL(isTrueDirect, FALSE) AS STRING) ORDER BY CAST(IFNULL(isTrueDirect, FALSE) AS STRING)), '-') isTrueDirect_distinct_pattern
    FROM
      layer_one
    GROUP BY
      fullVisitorId 
  )

  -- End of CTE
  SELECT
    loa.*,
    ha.* EXCEPT(fullVisitorId)
  FROM
    layer_one_agg AS loa
  INNER JOIN
    hits_agg AS ha
  ON
    loa.fullVisitorId = ha.fullVisitorId