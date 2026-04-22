{{
  config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["event_name", "platform"]
  )
}}

WITH raw_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    platform,
    app_info.version AS app_version,
    device.mobile_marketing_name,
    device.mobile_model_name,
    device.operating_system_version,
    device.advertising_id,
    device.language,
    geo.country,
    geo.city,
    geo.continent,
    event_params,
    user_properties,
    event_value_in_usd
  -- Sử dụng source để dbt tự điền Project ID và Dataset
  FROM {{ source('firebase_raw_bm2', 'events_intraday_all') }}
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 1 DAY))
),

pivoted_metrics AS (
  SELECT 
    * EXCEPT(event_params, user_properties),
    (SELECT AS STRUCT 
      MAX(IF(key = 'ga_session_id', value.int_value, NULL)) as ga_session_id,
      MAX(IF(key = 'ga_session_number', value.int_value, NULL)) as ga_session_number,
      MAX(IF(key = 'level', value.int_value, NULL)) as level,
      MAX(IF(key = 'play_type', value.string_value, NULL)) as start_type,
      MAX(IF(key = 'play_index', value.int_value, NULL)) as play_index,
      MAX(IF(key = 'play_time', value.double_value, NULL)) as play_duration,
      MAX(IF(key = 'lose_by', value.string_value, NULL)) as lose_by,
      MAX(IF(key = 'result', value.string_value, NULL)) as result_by,
      MAX(IF(key = 'ad_format', value.string_value, NULL)) as ad_format,
      MAX(IF(key = 'ad_network', value.string_value, NULL)) as ad_network,
      MAX(IF(key = 'network_name', value.string_value, NULL)) as ad_platform,
      MAX(IF(key = 'ad_duration', value.int_value, NULL)) as ad_duration,
      MAX(IF(key = 'value', value.double_value, NULL)) as value,
      MAX(IF(key = 'currency', value.string_value, NULL)) as currency,
      MAX(IF(key = 'placement', value.string_value, NULL)) as placement,
      MAX(IF(key = 'current_screen', value.string_value, NULL)) as current_screen,
      MAX(IF(key = 'feature_placement', value.string_value, NULL)) as feature_placement,
      MAX(IF(key = 'feature_name', value.string_value, NULL)) as feature_name,
      MAX(IF(key = 'feature_duration', value.int_value, NULL)) as feature_duration,
      MAX(IF(key = 'button_name', value.string_value, NULL)) as button_name,
      MAX(IF(key = 'button_placement', value.string_value, NULL)) as button_placement,
      MAX(IF(key = 'ad_type', value.string_value, NULL)) as ad_type,
      MAX(IF(key = 'ad_unit_name', value.string_value, NULL)) as ad_unit_name,
      MAX(IF(key = 'reason', value.string_value, NULL)) as reason,
      MAX(IF(key = 'resource_type', value.string_value, NULL)) as resource_type,
      MAX(IF(key = 'engagement_time_msec', value.int_value, NULL)) as engagement_time_msec,
      MAX(IF(key = 'resource_amount', value.int_value, NULL)) as resource_amount,
      STRING_AGG(IF(key LIKE 'action_seq_%', value.string_value, NULL), '|' ORDER BY key) as action_sequence
    FROM UNNEST(event_params)) as ep,
    (SELECT AS STRUCT
      MAX(IF(key = 'current_level', SAFE_CAST(value.string_value AS INT64), NULL)) as current_level,
      MAX(IF(key = 'first_open_time', DATE(TIMESTAMP_MILLIS(value.int_value), "Asia/Ho_Chi_Minh"), NULL)) as first_open_date,
      MAX(IF(key = 'campaign', value.string_value, NULL)) as campaign,
      MAX(IF(key = 'adset', value.string_value, NULL)) as adset,
      MAX(IF(key LIKE 'firebase_exp_%', value.string_value, NULL)) as firebase_exp
    FROM UNNEST(user_properties)) as up,
    ARRAY(
      SELECT AS STRUCT key, value.string_value, value.int_value, value.float_value, value.double_value
      FROM UNNEST(event_params)
    ) as event_params_json
  FROM raw_data
)

SELECT
  event_date, event_timestamp, event_name, user_pseudo_id, platform, 
  app_version, mobile_marketing_name, mobile_model_name, 
  operating_system_version, advertising_id, language, country, 
  city, continent, up.current_level, up.first_open_date, up.campaign, up.adset,
  CASE 
    WHEN LOWER(up.firebase_exp) IN ('1','true','t') THEN 'B'
    WHEN LOWER(up.firebase_exp) IN ('0','false','f') THEN 'A'
    ELSE up.firebase_exp 
  END as firebase_exp,
  ep.ga_session_id, ep.ga_session_number, ep.level, ep.start_type, 
  ep.play_index, SAFE_CAST(ROUND(ep.play_duration) AS INT64) as play_duration, 
  ep.lose_by, ep.action_sequence, ep.result_by, ep.ad_format, 
  ep.ad_network, ep.ad_platform, ep.ad_duration, ep.value, 
  ep.currency, event_value_in_usd, ep.placement, ep.current_screen, 
  ep.feature_placement, ep.feature_name, ep.feature_duration, 
  ep.button_name, ep.button_placement, ep.ad_type, ep.ad_unit_name, 
  ep.reason, ep.resource_type, ep.engagement_time_msec, 
  ep.resource_amount, event_params_json
FROM pivoted_metrics
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_pseudo_id, event_name, event_timestamp
    ORDER BY event_timestamp
) = 1