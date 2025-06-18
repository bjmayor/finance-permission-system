-- =====================================================
-- Step 3: Source-Query Refactor (Set-Based Generation)
-- =====================================================
-- This query creates three CTE blocks for finance permissions
-- Expected result cardinality: ~6.6M rows
-- =====================================================

WITH handle_perm AS (
  SELECT f.id     AS finance_id,
         e.supervisor_id,
         'handle' AS permission_type
  FROM finance f
  JOIN employee e ON f.handled_by = e.employee_id
),
order_perm AS (
  SELECT f.id, e.supervisor_id, 'order'
  FROM finance f
  JOIN orders o     ON f.order_id = o.id
  JOIN employee e   ON o.owner_id = e.employee_id
),
customer_perm AS (
  SELECT f.id, e.supervisor_id, 'customer'
  FROM finance f
  JOIN customer c   ON f.customer_id = c.id
  JOIN employee e   ON c.owner_id = e.employee_id
)
SELECT * FROM handle_perm
UNION ALL
SELECT * FROM order_perm
UNION ALL
SELECT * FROM customer_perm;

