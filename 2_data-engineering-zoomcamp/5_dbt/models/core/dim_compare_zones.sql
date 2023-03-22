{{ config(materialized='table') }}

with revenue_data as (
    select * from {{ ref('dim_monthly_zone_revenue') }}
),

final as(
    select 
    -- Reveneue grouping 
    revenue_zone,
    service_type, 

    -- Revenue calculation 
    avg(revenue_monthly_fare) as revenue_monthly_fare,
    avg(revenue_monthly_extra) as revenue_monthly_extra,
    avg(revenue_monthly_mta_tax) as revenue_monthly_mta_tax,
    avg(revenue_monthly_tip_amount) as revenue_monthly_tip_amount,
    avg(revenue_monthly_tolls_amount) as revenue_monthly_tolls_amount,
    avg(revenue_monthly_ehail_fee) as revenue_monthly_ehail_fee,
    avg(revenue_monthly_improvement_surcharge) as revenue_monthly_improvement_surcharge,
    avg(revenue_monthly_total_amount) as revenue_monthly_total_amount,
    avg(revenue_monthly_congestion_surcharge) as revenue_monthly_congestion_surcharge,

    -- Additional calculations
    avg(total_monthly_trips) as total_monthly_trips,
    avg(avg_montly_passenger_count) as avg_montly_passenger_count,
    avg(avg_montly_trip_distance) as avg_montly_trip_distance

    from revenue_data
    group by 1,2
)
SELECT * from final