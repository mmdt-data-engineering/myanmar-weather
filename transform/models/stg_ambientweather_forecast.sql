{{ config(materialized='table') }}

with ambientweather_forecast as (

    select 
        data_source as data_source,
        date as date,
        extraction_date as extraction_date,
        tsp_pcode as tsp_pcode,
        township as township,
        latitude as latitude,
        longitude as longitude,
        timezone as timezone,
        summary as weather_summary,

        precipitation_probability as precipitation_probability,

         -- ✅ Convert inches/hour → mm/hour
        precipitation_intensity * 25.4 as precipitation_intensity,
        'mm/hr' as precipitation_intensity_unit,

        -- ✅ Convert inches → mm
        precipitation_accumulation * 25.4 as precipitation_accumulation,
        'mm' as precipitation_accumulation_unit,

        -- ✅ Convert mph → kph
        wind_speed * 1.60934 as wind_speed,
        'kph' as wind_speed_unit,

        wind_gust * 1.60934 as wind_gust,
        'kph' as wind_gust_unit,

        wind_bearing as wind_bearing,
        'degrees' as wind_bearing_unit,

        -- ✅ Convert °F → °C
        (temperature_min - 32) * 5.0 / 9.0 as temperature_min,
        'C' as temperature_min_unit,

        (temperature_max - 32) * 5.0 / 9.0 as temperature_max,
        'C' as temperature_max_unit,

        icon as weather_icon

    from {{ source('myanmar_weather', 'ambient_forecast')}}

)

select *
from ambientweather_forecast

