-- =====================================================
-- Materialized View Redesign - Step 2 Schema
-- =====================================================
-- This script creates the redesigned materialized view
-- with the following specifications:
-- • Single MV (finance_permission_mv) with permission_type ENUM
-- • Surrogate BIGINT PK (mv_id) for maintenance
-- • Partitioned by permission_type for independent processing
-- • No indexes during load for optimal performance
-- =====================================================

-- Drop existing materialized views if they exist
DROP TABLE IF EXISTS finance_permission_mv;
DROP TABLE IF EXISTS mv_supervisor_financial;

-- Check if partitioning is supported
-- (This query will help determine if we can use partitioning)
SELECT 
    PLUGIN_NAME, 
    PLUGIN_STATUS 
FROM INFORMATION_SCHEMA.PLUGINS 
WHERE PLUGIN_NAME = 'partition';

-- =====================================================
-- Create the redesigned materialized view with partitioning
-- =====================================================
CREATE TABLE finance_permission_mv (
    -- Surrogate key for maintenance operations (BIGINT for large datasets)
    mv_id BIGINT NOT NULL AUTO_INCREMENT,
    
    -- Permission relationship fields
    supervisor_id INT NOT NULL COMMENT 'ID of the supervisor who has permission',
    fund_id INT NOT NULL COMMENT 'ID of the financial fund record',
    handle_by INT NOT NULL COMMENT 'ID of the user who handled the transaction',
    
    -- Denormalized user information for query performance
    handler_name VARCHAR(255) COMMENT 'Name of the handler (denormalized from users table)',
    department VARCHAR(100) COMMENT 'Department of the handler',
    
    -- Financial record details
    order_id INT COMMENT 'Associated order ID (if applicable)',
    customer_id INT COMMENT 'Associated customer ID (if applicable)',
    amount DECIMAL(15, 2) COMMENT 'Transaction amount',
    
    -- NEW: Permission type dimension
    permission_type ENUM('handle','order','customer') NOT NULL 
        COMMENT 'Type of permission: handle=direct subordinate, order=order owner, customer=customer admin',
    
    -- Maintenance fields
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        COMMENT 'Timestamp of last update for incremental refresh',
    
    -- Primary key includes permission_type for partitioning
    PRIMARY KEY (mv_id, permission_type)
) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_general_ci
ROW_FORMAT=COMPRESSED  -- Space efficiency
COMMENT='Redesigned materialized view for finance permissions with three dimensions'
-- Partition by permission_type for independent loading and querying
PARTITION BY LIST COLUMNS(permission_type) (
    PARTITION p_handle VALUES IN ('handle') 
        COMMENT 'Direct subordinate handling permissions',
    PARTITION p_order VALUES IN ('order') 
        COMMENT 'Order ownership permissions', 
    PARTITION p_customer VALUES IN ('customer') 
        COMMENT 'Customer administration permissions'
);

-- =====================================================
-- Alternative schema without partitioning (fallback)
-- =====================================================
-- Use this if partitioning is not supported:
/*
CREATE TABLE finance_permission_mv (
    mv_id BIGINT NOT NULL AUTO_INCREMENT,
    supervisor_id INT NOT NULL,
    fund_id INT NOT NULL,
    handle_by INT NOT NULL,
    handler_name VARCHAR(255),
    department VARCHAR(100),
    order_id INT,
    customer_id INT,
    amount DECIMAL(15, 2),
    permission_type ENUM('handle','order','customer') NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (mv_id)
) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_general_ci
ROW_FORMAT=COMPRESSED;
*/

-- =====================================================
-- Data Population Queries (executed without indexes)
-- =====================================================

-- 1. Load HANDLE dimension data
-- This covers permissions where supervisor manages the person who handled the transaction
INSERT INTO finance_permission_mv 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type)
SELECT 
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'handle' as permission_type
FROM user_hierarchy h
JOIN financial_funds f ON h.subordinate_id = f.handle_by
JOIN users u ON f.handle_by = u.id;

-- 2. Load ORDER dimension data  
-- This covers permissions where supervisor manages the person who owns the order
INSERT INTO finance_permission_mv 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type)
SELECT DISTINCT
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'order' as permission_type
FROM user_hierarchy h
JOIN orders o ON h.subordinate_id = o.user_id
JOIN financial_funds f ON o.order_id = f.order_id
LEFT JOIN users u ON f.handle_by = u.id
WHERE f.order_id IS NOT NULL;

-- 3. Load CUSTOMER dimension data
-- This covers permissions where supervisor manages the customer administrator
INSERT INTO finance_permission_mv 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type)
SELECT DISTINCT
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'customer' as permission_type
FROM user_hierarchy h
JOIN customers c ON h.subordinate_id = c.admin_user_id
JOIN financial_funds f ON c.customer_id = f.customer_id
LEFT JOIN users u ON f.handle_by = u.id
WHERE f.customer_id IS NOT NULL;

-- Update all timestamps
UPDATE finance_permission_mv SET last_updated = NOW();

-- =====================================================
-- Post-Load Index Creation (for query optimization)
-- =====================================================
-- These indexes are created AFTER data loading for optimal performance

-- Primary query pattern: supervisor + permission type
CREATE INDEX idx_supervisor_type ON finance_permission_mv (supervisor_id, permission_type);

-- Secondary query patterns
CREATE INDEX idx_supervisor_fund ON finance_permission_mv (supervisor_id, fund_id);
CREATE INDEX idx_permission_type ON finance_permission_mv (permission_type);
CREATE INDEX idx_supervisor_amount ON finance_permission_mv (supervisor_id, amount DESC);
CREATE INDEX idx_last_updated ON finance_permission_mv (last_updated);

-- =====================================================
-- Verification Queries
-- =====================================================

-- Check data distribution across partitions/permission types
SELECT 
    permission_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT supervisor_id) as unique_supervisors,
    COUNT(DISTINCT fund_id) as unique_funds
FROM finance_permission_mv
GROUP BY permission_type
ORDER BY permission_type;

-- Check partitioning information (if partitioning is enabled)
SELECT 
    PARTITION_NAME,
    PARTITION_EXPRESSION,
    PARTITION_DESCRIPTION,
    TABLE_ROWS
FROM INFORMATION_SCHEMA.PARTITIONS 
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
    AND PARTITION_NAME IS NOT NULL;

-- Verify indexes
SELECT 
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
    NON_UNIQUE,
    INDEX_TYPE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
ORDER BY INDEX_NAME;

-- Performance test query
SELECT 
    supervisor_id,
    permission_type,
    COUNT(*) as accessible_records,
    SUM(amount) as total_amount
FROM finance_permission_mv 
WHERE supervisor_id = 70  -- Replace with actual supervisor ID
GROUP BY supervisor_id, permission_type;

-- =====================================================
-- Usage Examples
-- =====================================================

-- Query 1: Get all financial records a supervisor can access
/*
SELECT 
    fp.fund_id,
    fp.permission_type,
    fp.handler_name,
    fp.amount,
    fp.order_id,
    fp.customer_id
FROM finance_permission_mv fp
WHERE fp.supervisor_id = ?
ORDER BY fp.amount DESC;
*/

-- Query 2: Get records by specific permission type
/*
SELECT 
    fp.fund_id,
    fp.handler_name,
    fp.amount
FROM finance_permission_mv fp
WHERE fp.supervisor_id = ? 
    AND fp.permission_type = 'handle'
ORDER BY fp.amount DESC;
*/

-- Query 3: Aggregate permissions by type
/*
SELECT 
    fp.permission_type,
    COUNT(*) as record_count,
    SUM(fp.amount) as total_amount,
    AVG(fp.amount) as avg_amount
FROM finance_permission_mv fp
WHERE fp.supervisor_id = ?
GROUP BY fp.permission_type;
*/

-- =====================================================
-- Maintenance Procedures
-- =====================================================

-- Incremental refresh procedure (for future implementation)
/*
DELIMITER //
CREATE PROCEDURE RefreshFinancePermissionMV()
BEGIN
    -- Truncate and reload (full refresh)
    TRUNCATE TABLE finance_permission_mv;
    
    -- Reload all three dimensions
    -- (Insert statements from above)
    
    -- Update timestamps
    UPDATE finance_permission_mv SET last_updated = NOW();
END //
DELIMITER ;
*/

-- Drop procedure if exists
-- DROP PROCEDURE IF EXISTS RefreshFinancePermissionMV;

