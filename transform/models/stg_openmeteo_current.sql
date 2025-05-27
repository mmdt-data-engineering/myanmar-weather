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
           weather_description as weather_description
           
    from public.openmeteo_current
    

)

select *
from openmeteo_current