with green_source as (
    SELECT
        *,row_number() over(PARTITION by "VendorID",lpep_pickup_datetime) as rn
    FROM
        {{ source('staging_sources', 'green_taxi_data') }}
    where "VendorID" is not null
),

green_data as (
    SELECT
        --ids
        CAST("VendorID" as numeric) as vendorid,
        CAST("RatecodeID" as numeric) as ratecodeid,
        CAST("PULocationID" as numeric) as pickup_locationid,
        CAST("DOLocationID" as numeric) as dropoff_locationid,
        {{ dbt_utils.generate_surrogate_key(['"VendorID"', 'lpep_pickup_datetime']) }} as trip_id,
        --timestamps
        CAST(lpep_pickup_datetime as timestamp) as pickup_datetime,
        CAST(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
        --tripinfo
        store_and_fwd_flag,
        {{ convert_numeric_to_null('passenger_count') }} as passenger_count,
        trip_distance,
        --payment info
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CAST(ehail_fee as float8) as ehail_fee,
        improvement_surcharge,
        total_amount,
        {{ convert_numeric_to_null('payment_type') }} as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        CAST(trip_type as numeric) as trip_type,
        congestion_surcharge
    FROM
        green_source
        where rn=1
        {% if var('is_test_run', default=true) %}
            limit 1000
        {% endif %}
)
SELECT
    *
FROM
    green_data