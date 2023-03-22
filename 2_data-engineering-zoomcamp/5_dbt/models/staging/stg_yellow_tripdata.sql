with yellow_source as (
    SELECT
        -- how to use row_numer() and partition by, search internet
        *,row_number() over(PARTITION by "VendorID",tpep_pickup_datetime) as rn
    FROM
        {{ source('staging_sources', 'yellow_taxi_data') }}
        where "VendorID" is not null
),
yellow_data as (
    SELECT
        --ids
        CAST("VendorID" as numeric) as vendorid,
        CAST("RatecodeID" as numeric) as ratecodeid,
        CAST("PULocationID" as numeric) as pickup_locationid,
        CAST("DOLocationID" as numeric) as dropoff_locationid,
        {{ dbt_utils.generate_surrogate_key(['"VendorID"', 'tpep_pickup_datetime']) }} as trip_id,
        --timestamps
        CAST(tpep_pickup_datetime as timestamp) as pickup_datetime,
        CAST(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        --tripinfo
        store_and_fwd_flag,
        CAST(passenger_count as numeric) as passenger_count,
        trip_distance,
        --payment info
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        0 as ehail_fee,
        improvement_surcharge,
        total_amount,
        5 as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        1 as trip_type,
        congestion_surcharge
    FROM
        yellow_source
        where rn=1
        {% if var('is_test_run', default=true) %}
            limit 1000
        {% endif %}
)
SELECT
    *
FROM
    yellow_data