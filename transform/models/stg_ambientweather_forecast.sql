{{ config(materialized='table') }}

with ambientweather_forecast as (

    select 
        data_source as data_source,
        date as date,
        extraction_date as extraction_date,
        state as state,
        district as district,
        township as township,
        latitude as latitude,
        longitude as longitude,
        timezone as timezone,
        summary as weather_summary,

        precipitation_probability as precipitation_probability,

        precipitation_intensity as precipitation_intensity,
        precipitation_intensity_unit as precipitation_intensity_unit,

        precipitation_accumulation as precipitation_accumulation,
        precipitation_accumulation_unit as precipitation_accumulation_unit,

        wind_speed as wind_speed,
        wind_speed_unit as wind_speed_unit,

        wind_gust as wind_gust,
        wind_gust_unit as wind_gust_unit,

        wind_bearing as wind_bearing,
        wind_bearing_unit as wind_bearing_unit,

        temperature_min as temperature_min,
        temperature_min_unit as temperature_min_unit,

        temperature_max as temperature_max,
        temperature_max_unit as temperature_max_unit,

        icon as weather_icon

    from public.ambientweather_forecast

)

select *
from ambientweather_forecast

