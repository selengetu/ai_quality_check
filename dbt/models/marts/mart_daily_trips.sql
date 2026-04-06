-- mart_daily_trips.sql
-- Daily aggregate of cleaned taxi trips with embedded DQ metrics.
--
-- DQ signals surfaced per day:
--   null_fare_pct, null_distance_pct  — detect upstream nullability regressions
--   p95_fare, p95_distance            — detect outlier / fare-meter anomalies
--
-- Grain: one row per calendar date (pickup date).

WITH trips AS (
    SELECT * FROM {{ ref('stg_taxi_trips') }}
),

-- Re-join against raw to compute null rates on the *unfiltered* source
-- so the DQ metrics reflect the full raw population, not just clean rows.
raw_counts AS (
    SELECT
        CAST(tpep_pickup_datetime AS DATE)          AS trip_date,
        COUNT(*)                                    AS raw_total,
        SUM(CASE WHEN fare_amount   IS NULL THEN 1 ELSE 0 END) AS null_fare_count,
        SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS null_distance_count
    FROM (
        {% if target.name == 'prod' %}
            SELECT * FROM {{ source('raw', 'taxi_trips') }}
        {% else %}
            SELECT * FROM read_parquet(
                'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
            )
        {% endif %}
    )
    GROUP BY 1
),

daily AS (
    SELECT
        CAST(pickup_datetime AS DATE)               AS trip_date,

        -- volume
        COUNT(*)                                    AS total_trips,

        -- fare metrics
        AVG(fare_amount)                            AS avg_fare,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount)
                                                    AS p95_fare,

        -- distance metrics
        AVG(trip_distance)                          AS avg_distance,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY trip_distance)
                                                    AS p95_distance,

        -- passenger metrics
        AVG(passenger_count)                        AS avg_passengers,

        -- revenue
        SUM(total_amount)                           AS total_revenue,
        AVG(total_amount)                           AS avg_total_amount,

        -- tip rate (trips with any tip / total trips)
        AVG(CASE WHEN tip_amount > 0 THEN 1.0 ELSE 0.0 END)
                                                    AS tip_rate

    FROM trips
    GROUP BY 1
)

SELECT
    d.trip_date,
    d.total_trips,

    -- fare
    ROUND(d.avg_fare, 2)                            AS avg_fare,
    ROUND(d.p95_fare, 2)                            AS p95_fare,

    -- distance
    ROUND(d.avg_distance, 2)                        AS avg_distance,
    ROUND(d.p95_distance, 2)                        AS p95_distance,

    -- passengers
    ROUND(d.avg_passengers, 2)                      AS avg_passengers,

    -- revenue
    ROUND(d.total_revenue, 2)                       AS total_revenue,
    ROUND(d.avg_total_amount, 2)                    AS avg_total_amount,

    -- tip
    ROUND(d.tip_rate, 4)                            AS tip_rate,

    -- DQ metrics (from raw, to catch nulls that staging filters out)
    ROUND(
        COALESCE(rc.null_fare_count, 0) * 1.0 / NULLIF(rc.raw_total, 0),
        4
    )                                               AS null_fare_pct,
    ROUND(
        COALESCE(rc.null_distance_count, 0) * 1.0 / NULLIF(rc.raw_total, 0),
        4
    )                                               AS null_distance_pct,

    NOW()                                           AS dbt_updated_at

FROM daily d
LEFT JOIN raw_counts rc ON d.trip_date = rc.trip_date
ORDER BY d.trip_date
