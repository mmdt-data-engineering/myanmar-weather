{{ config(materialized='table') }}

with ambient_weather_temp as (
    select
        data_source,
        CAST(date as date) as date,
        CAST(extraction_date as date) as extraction_date,
        tsp_pcode,
        township,

        latitude as latitude,
        longitude as longitude,

        ROUND(CAST(temperature_min as numeric), 3) as temperature_min_value,
        'celsius' as temperature_min_unit,
        ROUND(CAST(temperature_max as numeric), 3) as temperature_max_value,
        'celsius' as temperature_max_unit

    from {{ ref('stg_ambientweather_forecast') }}
),

meteoblue_temp as (
    select
        data_source,
        CAST(date as date) as date,
        CAST(extraction_date as date) as extraction_date,
        tsp_pcode,
        township,

        latitude as latitude,
        longitude as longitude,
        
        CAST(temperature_min as double precision) as temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max as double precision) as temperature_max_value,
        'celsius' as temperature_max_unit

    from {{ ref('stg_meteoblue_forecast') }}
),

openmeteo_temp as (
    select
        data_source,
        CAST(date as date) as date,
        CAST(extraction_date as date) as extraction_date,
        tsp_pcode,
        township,

        latitude as latitude,
        longitude as longitude,

        CAST(temperature_min as double precision) as temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max as double precision) as temperature_max_value,
        'celsius' as temperature_max_unit

    from {{ ref('stg_openmeteo_forecast') }}
),

weatherapi_temp as (
    select
        data_source,
        CAST(date as date) as date,
        CAST(extraction_date as date) as extraction_date,
        tsp_pcode,
        township,

        latitude as latitude,
        longitude as longitude,

        CAST(temperature_min as double precision) as temperature_min_value,
        'celsius' as temperature_min_unit,
        CAST(temperature_max as double precision) as temperature_max_value,
        'celsius' as temperature_max_unit

    from {{ ref('stg_weatherapi_forecast') }}
),

all_forecasts_temperature as (
    select * from ambient_weather_temp
    union all
    select * from meteoblue_temp
    union all
    select * from openmeteo_temp
    union all
    select * from weatherapi_temp
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

    -- Temperature (Values & Units)
    temperature_min_value,
    temperature_min_unit,
    temperature_max_value,
    temperature_max_unit

from all_forecasts_temperature
order by
    date,
    tsp_pcode,
    extraction_date,
    data_source