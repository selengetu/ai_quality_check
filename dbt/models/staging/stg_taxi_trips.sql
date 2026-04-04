-- stg_taxi_trips.sql
-- Cleans and casts the raw NYC Taxi dataset.
--
-- Transformations applied:
--   - Rename vendor/TLC columns to snake_case
--   - Cast all timestamps, numerics, and flags to correct types
--   - Filter rows missing critical fields (pickup_datetime, dropoff_datetime,
--     passenger_count, trip_distance, fare_amount)
--   - Filter out obviously corrupt fares (fare_amount <= 0)
--   - Add dq_loaded_at audit column

WITH source AS (
    SELECT * FROM {{ source('raw', 'taxi_trips') }}
),

cast_and_rename AS (
    SELECT
        -- identifiers
        CAST(VendorID          AS INTEGER)           AS vendor_id,
        CAST(PULocationID      AS INTEGER)           AS pickup_location_id,
        CAST(DOLocationID      AS INTEGER)           AS dropoff_location_id,
        CAST(RatecodeID        AS INTEGER)           AS rate_code_id,
        CAST(payment_type      AS INTEGER)           AS payment_type,

        -- timestamps
        CAST(tpep_pickup_datetime  AS TIMESTAMP)     AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP)     AS dropoff_datetime,

        -- measures
        CAST(passenger_count       AS INTEGER)       AS passenger_count,
        CAST(trip_distance         AS DOUBLE)        AS trip_distance,
        CAST(fare_amount           AS DOUBLE)        AS fare_amount,
        CAST(extra                 AS DOUBLE)        AS extra,
        CAST(mta_tax               AS DOUBLE)        AS mta_tax,
        CAST(tip_amount            AS DOUBLE)        AS tip_amount,
        CAST(tolls_amount          AS DOUBLE)        AS tolls_amount,
        CAST(improvement_surcharge AS DOUBLE)        AS improvement_surcharge,
        CAST(total_amount          AS DOUBLE)        AS total_amount,
        CAST(congestion_surcharge  AS DOUBLE)        AS congestion_surcharge,
        CAST(airport_fee           AS DOUBLE)        AS airport_fee,

        -- flags
        CAST(store_and_fwd_flag    AS VARCHAR)       AS store_and_fwd_flag,

        -- audit
        _run_id,
        _loaded_at,
        NOW()                                        AS dq_loaded_at
    FROM source
),

filtered AS (
    SELECT *
    FROM cast_and_rename
    WHERE
        -- critical fields must be non-null
        pickup_datetime   IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND passenger_count IS NOT NULL
        AND trip_distance   IS NOT NULL
        AND fare_amount     IS NOT NULL
        -- basic sanity: fare must be positive
        AND fare_amount > 0
        -- trip must go forward in time
        AND dropoff_datetime > pickup_datetime
)

SELECT * FROM filtered
