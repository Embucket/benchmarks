-- =========================================
-- Q15
-- Changes for Snowflake:
-- 1) CREATE VIEW → CREATE OR REPLACE TEMP VIEW (convenient for repeated runs in one session).
-- 2) Replace DATE '...' + INTERVAL '3' MONTH with DATEADD(month, 3, TO_DATE(...)).
-- 3) Use TO_DATE('YYYY-MM-DD') for date literals.
-- 4) DROP VIEW → DROP VIEW IF EXISTS (safe drop across repeats).
-- =========================================
CREATE OR REPLACE TEMP VIEW revenue0 (supplier_no, total_revenue) AS
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount))
    FROM
        lineitem
    WHERE
          l_shipdate >= TO_DATE('1996-01-01')                      -- CHANGED: explicit TO_DATE(...)
      AND l_shipdate <  DATEADD(month, 3, TO_DATE('1996-01-01'))   -- CHANGED: DATE + INTERVAL → DATEADD
    GROUP BY
        l_suppkey;

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue0
WHERE
      s_suppkey = supplier_no
  AND total_revenue = (
        SELECT MAX(total_revenue) FROM revenue0
    )
ORDER BY
    s_suppkey;

DROP VIEW IF EXISTS revenue0;   -- CHANGED: safe drop (won't error if the view is missing)