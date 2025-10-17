WITH source AS (
    SELECT *
    FROM {{ source('raw', 'online_retail_raw') }}
),

cleaned AS (
    SELECT
        invoiceno AS invoice_no,
        stockcode AS stock_code,
        TRIM(description) AS description,
        quantity,
        CASE
            WHEN invoicedate IS NOT NULL THEN
                TO_TIMESTAMP(invoicedate, 'MM/DD/YYYY HH24:MI')
            ELSE NULL
        END AS invoice_date,
        unitprice AS unit_price,
        customerid AS customer_id,
        TRIM(country) AS country
    FROM source
    WHERE customerid IS NOT NULL
      AND quantity > 0
      AND unitprice > 0
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY invoice_no, stock_code, customer_id, invoice_date
                   ORDER BY invoice_date DESC
               ) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),

final AS (
    SELECT *
    FROM deduped
    WHERE quantity < 1000
      AND unit_price < 10000
)

SELECT *
FROM final
