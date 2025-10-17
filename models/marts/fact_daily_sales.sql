
SELECT
    date_fk,
    product_fk,
    SUM(quantity) AS total_quantity,
    SUM(total_price) AS total_revenue
FROM {{ ref('fct_sales') }}
GROUP BY date_fk, product_fk
