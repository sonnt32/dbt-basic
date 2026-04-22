{{
  config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

SELECT
    event_name,
    event_timestamp,
    event_date,  
    user_pseudo_id,
    ga_session_number,
    ga_session_id,
    app_version,
    mobile_marketing_name,
    mobile_model_name,
    operating_system_version,
    advertising_id,
    language,
    country,
    city,
    continent,
    first_open_date,
    current_level,
    campaign,
    adset,
    current_screen,
    firebase_exp
-- Sử dụng ref để kết nối logic giữa các model
FROM {{ ref('brain-master-2-event_flatten_raw') }}
-- dbt sẽ tự động lấy dữ liệu từ kết quả của model event_flatten_raw vừa chạy