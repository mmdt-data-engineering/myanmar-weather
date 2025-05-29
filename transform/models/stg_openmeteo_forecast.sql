{{ config(materialized='table') }}

with openmeteo_forecast as (

    select data_source as data_source,
        date as date,
        extraction_date as extraction_date,
        tsp_pcode as tsp_pcode,
        township as township,
        latitude as latitude,
        longitude as longitude,
        weather_description as weather_description, 
        temperature_2m_max as temperature_max,
        temperature_2m_max_units as temperature_max_units,
        temperature_2m_min as temperature_min,
        temperature_2m_min_units as temperature_min_units,
        apparent_temperature_max as apparent_temperature_max,
        apparent_temperature_max_units as apparent_temperature_max_units,
        apparent_temperature_min as apparent_temperature_min,
        apparent_temperature_min_units as apparent_temperature_min_units,
        sunrise as sunrise,
        sunrise_units as sunrise_units,
        sunset as sunset,
        sunset_units as sunset_units,
        daylight_duration as daylight_duration,
        daylight_duration_units as daylight_duration_units,
        sunshine_duration as sunshine_duration,
        sunshine_duration_units as sunshine_duration_units,
        uv_index_max as uv_index_max,
        uv_index_max_units as uv_index_max_units,
        uv_index_clear_sky_max as uv_index_clear_sky_max,
        uv_index_clear_sky_max_units as uv_index_clear_sky_max_units,
        rain_sum as rain,
        rain_sum_units as rain_units,
        showers_sum as showers,
        showers_sum_units as showers_units,
        snowfall_sum as snowfall,
        snowfall_sum_units as snowfall_units,
        precipitation_sum as precipitation,
        precipitation_sum_units as precipitation_units,
        precipitation_hours as precipitation_hours,
        precipitation_hours_units as precipitation_hours_units,
        precipitation_probability_max as precipitation_probability,
        precipitation_probability_max_units as precipitation_probability_units,
        wind_speed_10m_max as wind_speed,
        wind_speed_10m_max_units as wind_speed_units,
        wind_direction_10m_dominant as wind_direction,
        wind_direction_10m_dominant_units as wind_direction_units,
        wind_gusts_10m_max as wind_gusts,
        wind_gusts_10m_max_units as wind_gusts_units,
        shortwave_radiation_sum as shortwave_radiation,
        shortwave_radiation_sum_units as shortwave_radiation_units,
        et0_fao_evapotranspiration as evapotranspiration,
        et0_fao_evapotranspiration_units as evapotranspiration_units

           
    from {{ source('myanmar_weather', 'openmeteo_forecast') }}
)

select *
from openmeteo_forecast