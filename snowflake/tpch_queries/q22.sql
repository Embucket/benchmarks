-- =========================================
-- Q22
-- Changes for Snowflake:
-- 1) substring(c_phone from 1 for 2) → SUBSTR(c_phone, 1, 2)
--    (use positional SUBSTR/SUBSTRING(expr, start, length) form).
-- =========================================
SELECT
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
(
    SELECT
        SUBSTR(c_phone, 1, 2) AS cntrycode,  -- CHANGED: SUBSTR(...) syntax
        c_acctbal
    FROM
        customer
    WHERE
          SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')  -- CHANGED
      AND c_acctbal > (
            SELECT
                AVG(c_acctbal)
            FROM
                customer
            WHERE
                  c_acctbal > 0.00
              AND SUBSTR(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') -- CHANGED
        )
      AND NOT EXISTS (
            SELECT
                *
            FROM
                orders
            WHERE
                o_custkey = c_custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;