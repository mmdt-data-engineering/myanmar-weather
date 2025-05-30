{{ config(materialized='table') }}

with meteoblue_forecast as (

    select data_source as data_source,
        date as date,
        date_unit as date_unit,
        extraction_date as extraction_date,
        tsp_pcode as tsp_pcode,
        township as township,
        latitude as latitude,
        longitude as longitude,
        temperature_instant as temperature_instant,
        temperature_instant_unit as temperature_instant_unit,
        temperature_min as temperature_min,
        temperature_min_unit as temperature_min_unit,
        temperature_max as temperature_max,
        temperature_max_unit as temperature_max_unit,
        temperature_mean as temperature_mean,
        temperature_mean_unit as temperature_mean_unit,
        sealevelpressure_min as sea_level_pressure_min,
        sealevelpressure_min_unit as sea_level_pressure_min_unit,
        sealevelpressure_max as sea_level_pressure_max,
        sealevelpressure_max_unit as sea_level_pressure_max_unit,
        sealevelpressure_mean as sea_level_pressure_mean,
        sealevelpressure_mean_unit as sea_level_pressure_mean_unit,
        windspeed_min as wind_speed_min,
        windspeed_min_unit as wind_speed_min_unit,
        windspeed_max as wind_speed_max,
        windspeed_max_unit as wind_speed_max_unit,
        windspeed_mean as wind_speed_mean,
        windspeed_mean_unit as wind_speed_mean_unit,
        winddirection as wind_direction,
        winddirection_unit as wind_direction_unit,
        humiditygreater90_hours as humidity_greater_90_hours,
        humiditygreater90_hours_unit as humidity_greater_90_hours_unit,
        relativehumidity_min as relative_humidity_min,
        relativehumidity_min_unit as relative_humidity_min_unit,
        relativehumidity_max as relative_humidity_max,
        relativehumidity_max_unit as relative_humidity_max_unit,
        relativehumidity_mean as relative_humidity_mean,
        relativehumidity_mean_unit as relative_humidity_mean_unit,
        precipitation as precipitation,
        precipitation_unit as precipitation_unit,
        precipitation_probability as precipitation_probability,
        precipitation_probability_unit as precipitation_probability_unit,
        predictability as predictability,
        predictability_unit as predictability_unit,
        convective_precipitation as convective_precipitation,
        convective_precipitation_unit as convective_precipitation_unit,
        uvindex as uv_index,
        rainspot as rain_spot,
        predictability_class as predictability_class

    from public.meteoblue_forecast
)

select *
from meteoblue_forecast