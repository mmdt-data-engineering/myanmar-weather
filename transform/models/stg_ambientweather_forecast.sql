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
        precipitation_intensity_inches_per_hour as precipitation_intensity,
        precipitation_accumulation_inches as precipitation_accumulation,
        wind_speed_miles_per_hour as wind_speed,
        icon as weather_icon,
        wind_bearing_degrees as wind_bearing,
        wind_gust_miles_per_hour as wind_gust,
        temperature_min_fahrenheit as temperature_min,
        temperature_max_fahrenheit as temperature_max

    from public.ambientweather_forecast

)

select *
from ambientweather_forecast
