-- =========================================
-- Q20
-- Changes for Snowflake:
-- 1) All date comparisons use TO_DATE('YYYY-MM-DD').
-- 2) Replace +/- INTERVAL with DATEADD(unit, n, TO_DATE(...)).
-- =========================================
SELECT
    s_name,
    s_address
FROM
    supplier,
    nation
WHERE
      s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
              ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                      p_name LIKE 'forest%'
            )
          AND ps_availqty > (
                SELECT
                    0.5 * SUM(l_quantity)
                FROM
                    lineitem
                WHERE
                      l_partkey = ps_partkey
                  AND l_suppkey = ps_suppkey
                  AND l_shipdate >= TO_DATE('1994-01-01')                 -- CHANGED: explicit TO_DATE(...)
                  AND l_shipdate <  DATEADD(year, 1, TO_DATE('1994-01-01')) -- CHANGED: DATE + INTERVAL → DATEADD
            )
    )
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY
    s_name;