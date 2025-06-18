
#!/bin/bash
# refresh_materialized_view.sh
# 此脚本用于定期刷新物化视图

MYSQL_CMD="mysql -h127.0.0.1 -P3306 -uroot -p123456 finance"

# 清空并重新填充物化视图
$MYSQL_CMD << EOF
TRUNCATE TABLE mv_supervisor_financial;

INSERT INTO mv_supervisor_financial
    (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount)
(
    -- 根据 handle_by 权限
    SELECT
        h.user_id AS supervisor_id,
        f.fund_id,
        f.handle_by,
        u.name AS handler_name,
        u.department,
        f.order_id,
        f.customer_id,
        f.amount
    FROM user_hierarchy h
    JOIN financial_funds f ON h.subordinate_id = f.handle_by
    JOIN users u ON f.handle_by = u.id
    WHERE h.depth >= 0
)
UNION
(
    -- 根据 order_id 权限
    SELECT
        h.user_id AS supervisor_id,
        f.fund_id,
        f.handle_by,
        u.name AS handler_name,
        u.department,
        f.order_id,
        f.customer_id,
        f.amount
    FROM user_hierarchy h
    JOIN orders o ON h.subordinate_id = o.user_id
    JOIN financial_funds f ON o.order_id = f.order_id
    JOIN users u ON f.handle_by = u.id
    WHERE h.depth >= 0
)
UNION
(
    -- 根据 customer_id 权限
    SELECT
        h.user_id AS supervisor_id,
        f.fund_id,
        f.handle_by,
        u.name AS handler_name,
        u.department,
        f.order_id,
        f.customer_id,
        f.amount
    FROM user_hierarchy h
    JOIN customers c ON h.subordinate_id = c.admin_user_id
    JOIN financial_funds f ON c.customer_id = f.customer_id
    JOIN users u ON f.handle_by = u.id
    WHERE h.depth >= 0
)

UPDATE mv_supervisor_financial SET last_updated = NOW();

SELECT COUNT(*) FROM mv_supervisor_financial;
EOF

echo "物化视图刷新完成，时间: $(date)"
