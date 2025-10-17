-- models/marts/dim_date.sql
WITH dates AS (
    SELECT DISTINCT invoice_date
    FROM {{ ref('stg_online_retail') }}
    WHERE invoice_date IS NOT NULL
),

dim AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY invoice_date) AS date_pk,
        invoice_date,
        EXTRACT(YEAR FROM invoice_date) AS year,
        EXTRACT(MONTH FROM invoice_date) AS month,
        EXTRACT(DAY FROM invoice_date) AS day,
        EXTRACT(QUARTER FROM invoice_date) AS quarter,
        TO_CHAR(invoice_date, 'Day') AS weekday
    FROM dates
)

SELECT *
FROM dim
