-- Generic test: positive_fare_amount
-- Fails if any row in the tested model has fare_amount <= 0.
-- Usage in schema.yml:
--   tests:
--     - positive_fare_amount
--
-- Returns the offending rows (dbt treats any rows returned as a test failure).

{% test positive_fare_amount(model, column_name) %}

SELECT
    {{ column_name }}   AS fare_amount,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    vendor_id
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} <= 0

{% endtest %}
