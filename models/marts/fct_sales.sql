{{ config(
    materialized='incremental',
    unique_key='invoice_no' 
) }}

WITH base AS (
    SELECT
        s.invoice_no,
        d.date_pk AS date_fk,
        p.product_pk AS product_fk,
        c.customer_pk AS customer_fk,
        s.quantity,
        s.unit_price,
        s.quantity * s.unit_price AS total_price
    FROM {{ ref('stg_online_retail') }} s
    LEFT JOIN {{ ref('dim_product') }} p
        ON s.stock_code = p.stock_code
    LEFT JOIN {{ ref('dim_customer') }} c
        ON s.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_date') }} d
        ON s.invoice_date = d.invoice_date
)

{% if is_incremental() %}

WHERE invoice_no NOT IN (SELECT invoice_no FROM {{ this }})
{% endif %}

SELECT *
FROM base
WHERE date_fk IS NOT NULL
