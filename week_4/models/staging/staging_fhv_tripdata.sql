{{ config(materialized='view') }}
 
WITH Tripdata AS (
    SELECT     *
    FROM       {{ source('staging','fhv_tripdata') }}
)
SELECT     -- identifiers
           CAST(dispatching_base_num AS STRING) AS dispatching_base_num
           , CAST(Affiliated_base_number AS STRING) AS affiliated_base_number
           , CAST(pulocationid AS INTEGER) AS pickup_locationid
           , CAST(dolocationid AS INTEGER) AS dropoff_locationid
    
           -- timestamps
           , CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime
           , CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime
    
           -- trip info
           , CAST(SR_Flag AS STRING) AS store_and_fwd_flag
FROM       Tripdata

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}