{{ config(materialized='table') }}

with ambient_weather_unioned as (
    select
        data_source,
        CAST(date AS date) as date,
        CAST(extraction_date AS date) as extraction_date,
        tsp_pcode,
        township,
        CAST(latitude AS double precision) AS latitude,
        CAST(longitude AS double precision) AS longitude,
        weather_summary,
        ROUND(CAST(temperature_min AS numeric), 3) AS temperature_min_value,
        'celsius' as temperature_min_unit,
        ROUND(CAST(temperature_max AS numeric), 3) AS temperature_max_value,
        'celsius' as temperature_max_unit,
        CAST(precipitation_probability AS double precision) AS precipitation_probability,
        ROUND(CAST(precipitation_intensity AS numeric), 3) AS precipitation_intensity_value,
        precipitation_intensity_unit,
        ROUND(CAST(precipitation_accumulation AS numeric), 3) AS precipitation_total_value,
        CASE 
            WHEN precipitation_accumulation_unit = 'mm' THEN 'millimeters' 
            ELSE precipitation_accumulation_unit 
        END AS precipitation_total_unit, 
        ROUND(CAST(wind_speed AS numeric), 3) AS wind_speed_value,
        CASE 
            WHEN wind_speed_unit = 'km/h' THEN 'kph' 
            ELSE wind_speed_unit 
        END AS wind_speed_unit,
        ROUND(CAST(wind_gust AS numeric), 3) AS wind_gust_value,
        CASE 
            WHEN wind_gust_unit = 'km/h' THEN 'kph' 
            ELSE wind_gust_unit 
        END AS wind_gust_unit,
        CAST(wind_bearing AS double precision) AS wind_direction_value,
        CASE 
            WHEN wind_bearing_unit = '°' THEN 'degree' 
            WHEN wind_bearing_unit = 'degrees' THEN 'degree' 
            ELSE wind_bearing_unit 
        END AS wind_direction_unit, 
        weather_icon,

        -- Columns not common or less common, filled with NULLs for UNION compatibility
        -- Casting NULL to a type is valid and good for type consistency in UNIONs
        CAST(null AS double precision) as temperature_mean_value,
        'celsius' as temperature_mean_unit,
        CAST(null AS double precision) as relative_humidity_mean_value,        
        'percent' as relative_humidity_mean_unit,
        CAST(null AS double precision) as uv_index_value,

    from {{ ref('stg_ambientweather_forecast') }}
),

meteoblue_unioned as (
    select
        data_source,
        CAST(date AS date) as date,
        CAST(extraction_date AS date) as extraction_date,
        tsp_pcode,
        township,
        CAST(latitude AS double precision) AS latitude, -- Added cast
        CAST(longitude AS double precision) AS longitude, -- Added cast
        null as weather_summary,
        CAST(temperature_min AS double precision) AS temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max AS double precision) AS temperature_max_value,
        'celsius' as temperature_max_unit,
        CAST(precipitation_probability AS double precision) AS precipitation_probability, -- Added cast
        CAST(null AS double precision) as precipitation_intensity_value, -- Cast NULL for type consistency
        'mm/hr' as precipitation_intensity_unit,
        CAST(precipitation AS double precision) AS precipitation_total_value, -- Already present, confirmed
        CASE 
            WHEN precipitation_unit = 'mm' THEN 'millimeters' 
            ELSE precipitation_unit 
        END AS precipitation_total_unit, 
        ROUND(CAST((wind_speed_mean * 3.6) AS numeric), 3) AS wind_speed_value,
        'kph' as wind_speed_unit, -- Assuming kph for consistency
        CAST(null AS double precision) as wind_gust_value, -- Cast NULL for type consistency
        'kph' as wind_gust_unit,
        CAST(wind_direction AS double precision) AS wind_direction_value,
        CASE 
            WHEN wind_direction_unit = '°' THEN 'degree' 
            WHEN wind_direction_unit = 'degrees' THEN 'degree' 
            ELSE wind_direction_unit 
        END AS wind_direction_unit, 
        null as weather_icon,

        CAST(temperature_mean AS double precision) AS temperature_mean_value,
        'celsius' as temperature_mean_unit,
        CAST(relative_humidity_mean AS double precision) AS relative_humidity_mean_value,        
        CASE 
            WHEN relative_humidity_mean_unit = '%' THEN 'percent' 
            ELSE relative_humidity_mean_unit 
        END AS relative_humidity_mean_unit, 
        CAST(uv_index AS double precision) AS uv_index_value,

    from {{ ref('stg_meteoblue_forecast') }}
),

openmeteo_unioned as (
    select
        data_source,
        CAST(date AS date) as date,
        CAST(extraction_date AS date) as extraction_date,
        tsp_pcode,
        township,
        CAST(latitude AS double precision) AS latitude, -- Added cast
        CAST(longitude AS double precision) AS longitude, -- Added cast
        weather_description as weather_summary,
        CAST(temperature_min AS double precision) AS temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max AS double precision) AS temperature_max_value,
        'celsius' as temperature_max_unit,
        CAST(precipitation_probability AS double precision) AS precipitation_probability, -- Added cast
        CAST(null AS double precision) as precipitation_intensity_value, -- Cast NULL for type consistency
        'mm/hr' as precipitation_intensity_unit,
        CAST(precipitation AS double precision) AS precipitation_total_value, -- Already present, confirmed
        CASE 
            WHEN precipitation_units = 'mm' THEN 'millimeters' 
            ELSE precipitation_units 
        END AS precipitation_total_unit,
        CAST(wind_speed AS double precision) AS wind_speed_value,
        CASE 
            WHEN wind_speed_units = 'km/h' THEN 'kph' 
            ELSE wind_speed_units 
        END AS wind_speed_unit,
        CAST(wind_gusts AS double precision) AS wind_gust_value,
        CASE 
            WHEN wind_gusts_units = 'km/h' THEN 'kph' 
            ELSE wind_gusts_units 
        END AS wind_gust_unit,
        CAST(wind_direction AS double precision) AS wind_direction_value,
        CASE 
            WHEN wind_direction_units = '°' THEN 'degree' 
            WHEN wind_direction_units = 'degrees' THEN 'degree' 
            ELSE wind_direction_units 
        END AS wind_direction_unit, 
        null as weather_icon,

        CAST(null AS double precision) as temperature_mean_value, -- Cast NULL for type consistency
        'celsius' as temperature_mean_unit,
        CAST(null AS double precision) as relative_humidity_mean_value, -- Cast NULL for type consistency
        'percent' as relative_humidity_mean_unit,
        CAST(uv_index_max AS double precision) AS uv_index_value,

    from {{ ref('stg_openmeteo_forecast') }}
),

weatherapi_unioned as (
    select
        data_source,
        CAST(date AS date) as date,
        CAST(extraction_date AS date) as extraction_date,
        tsp_pcode,
        township,
        CAST(latitude AS double precision) AS latitude, -- Added cast
        CAST(longitude AS double precision) AS longitude, -- Added cast
        weather_summary,
        CAST(temperature_min AS double precision) AS temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max AS double precision) AS temperature_max_value,
        'celsius' as temperature_max_unit,
        CAST(null AS double precision) as precipitation_probability, -- Cast NULL for type consistency
        CAST(null AS double precision) as precipitation_intensity_value, -- Cast NULL for type consistency
        'mm/hr' as precipitation_intensity_unit,
        CAST(precipitation AS double precision) AS precipitation_total_value,
        CASE 
            WHEN precipitation_units = 'mm' THEN 'millimeters' 
            ELSE precipitation_units 
        END AS precipitation_total_unit,
        CAST(wind_speed AS double precision) AS wind_speed_value,
        CASE 
            WHEN wind_speed_units = 'km/h' THEN 'kph' 
            ELSE wind_speed_units 
        END AS wind_speed_unit,
        CAST(null AS double precision) as wind_gust_value, -- Cast NULL for type consistency
        'kph' as wind_gust_unit,
        CAST(null AS double precision) as wind_direction_value, -- Cast NULL for type consistency
        'degree' as wind_direction_unit,
        weather_icon,

        CAST(temperature_avg AS double precision) AS temperature_mean_value,
        'celsius' as temperature_mean_unit,
        CAST(humidity AS double precision) AS relative_humidity_mean_value,
        'percent' as relative_humidity_mean_unit,
        CAST(uv_index AS double precision) AS uv_index_value,

    from {{ ref('stg_weatherapi_forecast') }}
),

all_forecasts_unioned as (
    select * from ambient_weather_unioned
    union all
    select * from meteoblue_unioned
    union all
    select * from openmeteo_unioned
    union all
    select * from weatherapi_unioned
)

select
    -- Core Identifiers
    data_source,
    date,
    extraction_date,
    tsp_pcode,
    township,
    latitude,
    longitude,
    -- Common Weather Text/Icon
    weather_summary,
    weather_icon,

    -- Temperature (Values & Units)
    temperature_min_value,
    temperature_min_unit,
    temperature_max_value,
    temperature_max_unit,
    temperature_mean_value,
    temperature_mean_unit,

    -- Precipitation (Values & Units)
    precipitation_probability, -- Unit for probability can vary (0-1 or %), not always explicit
    precipitation_intensity_value,
    precipitation_intensity_unit,
    precipitation_total_value,
    precipitation_total_unit,

    -- Wind (Values & Units)
    wind_speed_value,
    wind_speed_unit,
    wind_gust_value,
    wind_gust_unit,
    wind_direction_value,
    wind_direction_unit,

    -- Other Common Metrics
    relative_humidity_mean_value,
    relative_humidity_mean_unit,
    uv_index_value,
    uv_index_unit

from all_forecasts_unioned
order by
    date,
    tsp_pcode,
    extraction_date,
    data_source