-- =========================================
-- Q4
-- Changes for Snowflake:
-- 1) Cast string literal to DATE via TO_DATE('YYYY-MM-DD') instead of comparing DATE to a string.
-- 2) Replace DATE '...' + INTERVAL 'n' MONTH with DATEADD(month, n, TO_DATE(...)).
-- =========================================
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
WHERE
      o_orderdate >= TO_DATE('1993-07-01')                       -- CHANGED: explicit TO_DATE(...)
  AND o_orderdate < DATEADD(month, 3, TO_DATE('1993-07-01'))     -- CHANGED: DATE + INTERVAL → DATEADD
  AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
              l_orderkey = o_orderkey
          AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;