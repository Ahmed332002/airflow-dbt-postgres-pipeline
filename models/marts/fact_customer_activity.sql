
SELECT
    customer_fk,
    COUNT(DISTINCT invoice_no) AS num_invoices,
    SUM(total_price) AS total_spent
FROM {{ ref('fct_sales') }}
GROUP BY customer_fk
