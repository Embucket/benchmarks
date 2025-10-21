WITH order_totals AS (
      SELECT
          l_orderkey,
          SUM(l_quantity) AS sum_qty
      FROM lineitem
      GROUP BY l_orderkey
      HAVING SUM(l_quantity) > 300
  )
  SELECT
      c.c_name,
      c.c_custkey,
      o.o_orderkey,
      o.o_orderdate,
      o.o_totalprice,
      ot.sum_qty
  FROM order_totals ot
  JOIN orders o
    ON o.o_orderkey = ot.l_orderkey
  JOIN customer c
    ON c.c_custkey = o.o_custkey
  ORDER BY o.o_totalprice DESC, o.o_orderdate
  LIMIT 100;