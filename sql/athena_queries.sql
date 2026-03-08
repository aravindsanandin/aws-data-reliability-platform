SELECT *
FROM gold_sales_summary
ORDER BY total_revenue DESC;

SELECT
    country,
    total_revenue,
    total_transactions,
    ROUND(avg_transaction_value,2) AS avg_transaction_value
FROM gold_sales_summary
ORDER BY total_revenue DESC;

SELECT AVG(avg_transaction_value) AS overall_avg_transaction
FROM gold_sales_summary;

