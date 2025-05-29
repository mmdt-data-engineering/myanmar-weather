{{ config(materialized='table') }}

with weatherapi_forecast as (

    select source as data_source,
            date as date,
            extraction_date as extraction_date,
            tsp_pcode as tsp_pcode,
            township as township,
            latitude as latitude,
            longitude as longitude,
            temperature_min_c as temperature_min,
            temperature_min_c_unit as temperature_min_units,
            temperature_max_c as temperature_max,
            temperature_max_c_unit as temperature_max_units,
            temperature_avg_c as temperature_avg,
            temperature_avg_c_unit as temperature_avg_units,
            wind_max_kph as wind_speed,
            wind_max_kph_unit as wind_speed_units,
            precipitation_total_mm as precipitation,
            precipitation_total_mm_unit as precipitation_units,
            snow_total_cm as snowfall,
            snow_total_cm_unit as snowfall_units,
            visibility_avg_km as visibility,
            visibility_avg_km_unit as visibility_units,
            humidity_avg as humidity,
            uv_index as uv_index,
            condition_text as weather_summary,
            condition_icon as weather_icon,
            condition_code as weather_code
            
    from public.weatherapi_forecast
    

)

select *
from weatherapi_forecast