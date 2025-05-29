{{ config(materialized='table') }}

with openmeteo_current as (

    select data_source as data_source,
            date as date,
            extraction_date as extraction_date,
            state as state,
            district as district,
            township as township,
            latitude as latitude,
            longitude as longitude,
            weather_description as weather_description,
            temperature_2m as temperature,
            temperature_2m_units as temperature_units,
            apparent_temperature as apparent_temperature,
            apparent_temperature_units as apparent_temperature_units,
            relative_humidity_2m as humidity, 
            relative_humidity_2m_units as humidity_units,
            wind_speed_10m as wind_speed,
            wind_speed_10m_units as wind_speed_units,
            wind_direction_10m as wind_direction,
            wind_direction_10m_units as wind_direction_units,
            wind_gusts_10m as wind_gusts,
            wind_gusts_10m_units as wind_gusts_units,
            precipitation as precipitation,
            precipitation_units as precipitation_units,
            showers as showers,
            showers_units as showers_units,
            snowfall as snowfall,
            snowfall_units as snowfall_units,
            rain as rain,
            rain_units as rain_units,
            cloud_cover as cloud_cover,
            cloud_cover_units as cloud_cover_units,
            pressure_msl as pressure,
            pressure_msl_units as pressure_units,
            surface_pressure as surface_pressure,
            surface_pressure_units as surface_pressure_units
            
    from public.openmeteo_current
    

)

select *
from openmeteo_current