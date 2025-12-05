WITH sales_2023 AS (
    SELECT 
        st.transaction_id,
        st.staff_id,
        st.total_amount,
        st.transaction_date
    FROM sales_transactions st
    WHERE EXTRACT(YEAR FROM st.transaction_date) = 2023
) SELECT
    sr.staff_id,
    b.branch_name,
    COUNT(s2023.transaction_id) AS total_transactions,
    SUM(s2023.total_amount) AS total_sales_amount,
    AVG(s2023.total_amount) AS avg_transaction_value,
    sr.performance_score
FROM staff_records sr
JOIN branch_details b 
    ON b.branch_code = sr.branch_code
LEFT JOIN sales_2023 s2023
    ON s2023.staff_id = sr.staff_id
WHERE sr.resignation_date IS NULL  -- active staff only
GROUP BY 
    sr.staff_id,
    b.branch_name,
    sr.performance_score
ORDER BY total_sales_amount DESC NULLS LAST;