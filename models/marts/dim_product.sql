
WITH base AS (
    SELECT DISTINCT
        stock_code,
        description
    FROM {{ ref('stg_online_retail') }}
),

dim AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY stock_code) AS product_pk, 
        stock_code,
        description AS product_description
    FROM base
)

SELECT *
FROM dim
