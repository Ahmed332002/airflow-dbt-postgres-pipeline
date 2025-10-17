
WITH base AS (
    SELECT DISTINCT
        customer_id,
        country
    FROM {{ ref('stg_online_retail') }}
),

dim AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_pk,  
        customer_id,
        country AS customer_country
    FROM base
)

SELECT *
FROM dim
