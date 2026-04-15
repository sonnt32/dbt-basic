{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["event_name", "user_pseudo_id"],
    unique_key='surrogate_key'
) }}

WITH source_flatten AS (
    /* Sử dụng hàm ref để tạo mối quan hệ tự động. 
       dbt sẽ tự hiểu bảng này phải chạy SAU bảng event_flatten_raw.
    */
    SELECT * FROM {{ ref('event_flatten_raw') }}
    
    WHERE 1=1
    {% if is_incremental() %}
      -- Chỉ lấy dữ liệu 2 ngày gần nhất từ bảng flatten để tối ưu chi phí
      AND event_date >= DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 2 DAY)
    {% endif %}
)

SELECT
    surrogate_key,
    event_name,
    event_timestamp,
    event_date,
    user_pseudo_id,
    ga_session_number,
    ga_session_id,
    app_version,
    mobile_marketing_name AS device_mobile_marketing_name,
    mobile_model_name AS device_mobile_model_name,
    operating_system_version AS device_operating_system_version,
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
FROM source_flatten

{% if is_incremental() %}
  -- Đảm bảo không chèn trùng các bản ghi đã tồn tại trong bảng base
  WHERE surrogate_key NOT IN (SELECT surrogate_key FROM {{ this }})
{% endif %}