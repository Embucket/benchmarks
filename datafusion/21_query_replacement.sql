WITH failed_orders AS (

    SELECT o_orderkey
    FROM orders
    WHERE o_orderstatus = 'F'
),
order_supplier_counts AS (

    SELECT
        l.l_orderkey,
        COUNT(DISTINCT l.l_suppkey) AS num_suppliers
    FROM
        lineitem l
        INNER JOIN failed_orders fo ON l.l_orderkey = fo.o_orderkey
    GROUP BY
        l.l_orderkey
    HAVING
        COUNT(DISTINCT l.l_suppkey) > 1
),
late_receipts AS (

    SELECT
        l.l_orderkey,
        l.l_suppkey
    FROM
        lineitem l
        INNER JOIN failed_orders fo ON l.l_orderkey = fo.o_orderkey
    WHERE
        l.l_receiptdate > l.l_commitdate
),
saudi_late_receipts AS (

    SELECT
        lr.l_orderkey,
        lr.l_suppkey,
        s.s_name
    FROM
        late_receipts lr
        INNER JOIN supplier s ON lr.l_suppkey = s.s_suppkey
        INNER JOIN nation n ON s.s_nationkey = n.n_nationkey
    WHERE
        n.n_name = 'SAUDI ARABIA'
)

SELECT
    slr.s_name AS s_name,
    COUNT(*) AS numwait
FROM
    saudi_late_receipts slr
    INNER JOIN order_supplier_counts osc
        ON slr.l_orderkey = osc.l_orderkey
WHERE


    NOT EXISTS (
        SELECT 1
        FROM late_receipts lr2
        WHERE lr2.l_orderkey = slr.l_orderkey
          AND lr2.l_suppkey <> slr.l_suppkey
    )
GROUP BY
    slr.s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;


