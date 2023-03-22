{{ config (materialized='table') }}

with zone_lookup as (
    select 
    "LocationID" as locationid,
    "Borough" as borough,
    "Zone" as zone,
    replace(service_zone,'Boro','Green') as service_zone
    from {{ ref ('taxi_zone_lookup') }}
)

select * from zone_lookup