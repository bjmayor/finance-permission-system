-- =====================================================
-- Step 4: High-Speed Bulk Load Pipeline - Approach A
-- Single SQL with Materialized View CONCURRENTLY
-- =====================================================

-- Approach A: Single SQL implementation with CONCURRENT operations
-- This approach uses PostgreSQL-style MATERIALIZED VIEW CONCURRENTLY
-- For MySQL, we'll implement equivalent functionality

-- Drop existing materialized view if it exists
DROP TABLE IF EXISTS finance_permission_mv;

-- Create the materialized view structure
CREATE TABLE finance_permission_mv (
    mv_id BIGINT NOT NULL AUTO_INCREMENT,
    supervisor_id INT NOT NULL COMMENT 'ID of the supervisor user',
    fund_id INT NOT NULL COMMENT 'ID of the financial fund record',
    handle_by INT NOT NULL COMMENT 'ID of the user who handled the transaction',
    
    -- Denormalized user information for query performance
    handler_name VARCHAR(255) COMMENT 'Name of the handler (denormalized from users table)',
    department VARCHAR(100) COMMENT 'Department of the handler',
    
    -- Financial record details
    order_id INT COMMENT 'Associated order ID (if applicable)',
    customer_id INT COMMENT 'Associated customer ID (if applicable)',
    amount DECIMAL(15, 2) COMMENT 'Transaction amount',
    
    -- Permission type dimension
    permission_type ENUM('handle','order','customer') NOT NULL 
        COMMENT 'Type of permission: handle=direct subordinate, order=order owner, customer=customer admin',
    
    -- Maintenance fields
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        COMMENT 'Timestamp of last update for incremental refresh',
    
    -- Primary key for maintenance
    PRIMARY KEY (mv_id)
) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_general_ci
ROW_FORMAT=COMPRESSED
COMMENT='Finance permission materialized view for high-speed bulk loading';

-- =====================================================
-- Single UNION ALL Query for Bulk Load
-- =====================================================

-- Load all three permission dimensions in one statement
INSERT INTO finance_permission_mv 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type)

-- HANDLE dimension: Direct subordinate handling permissions
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
JOIN users u ON f.handle_by = u.id

UNION ALL

-- ORDER dimension: Order ownership permissions
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
WHERE f.order_id IS NOT NULL

UNION ALL

-- CUSTOMER dimension: Customer administration permissions
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
-- Post-Load Index Creation (MySQL equivalent of CONCURRENTLY)
-- =====================================================

-- Create indexes after data load for optimal performance
-- MySQL doesn't have CONCURRENTLY, but we can create indexes efficiently

-- Primary query pattern: supervisor + permission type
CREATE INDEX idx_supervisor_type ON finance_permission_mv (supervisor_id, permission_type);

-- Secondary query patterns
CREATE INDEX idx_supervisor_fund ON finance_permission_mv (supervisor_id, fund_id);
CREATE INDEX idx_permission_type ON finance_permission_mv (permission_type);
CREATE INDEX idx_supervisor_amount ON finance_permission_mv (supervisor_id, amount DESC);
CREATE INDEX idx_last_updated ON finance_permission_mv (last_updated);

-- =====================================================
-- Verification and Statistics
-- =====================================================

-- Check data distribution
SELECT 
    permission_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT supervisor_id) as unique_supervisors,
    COUNT(DISTINCT fund_id) as unique_funds,
    ROUND(AVG(amount), 2) as avg_amount
FROM finance_permission_mv
GROUP BY permission_type
ORDER BY permission_type;

-- Check total statistics
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT supervisor_id) as total_supervisors,
    COUNT(DISTINCT fund_id) as total_funds,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM finance_permission_mv;

-- Show index information
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

-- =====================================================
-- Performance Notes for Approach A
-- =====================================================
/*
Approach A Characteristics:

✅ Advantages:
• Single atomic operation - all or nothing
• Simpler logic and fewer steps
• Uses standard SQL UNION ALL pattern
• Automatic transaction consistency
• Less risk of partial failures

⚠️ Considerations:
• Large memory usage for complex UNION ALL
• Longer execution time due to single transaction
• Cannot parallelize individual dimensions
• May require larger temp space
• Harder to restart if interrupted

📊 Expected Performance:
• Execution time: 2-5 minutes for ~6M records
• Memory usage: High during UNION ALL processing
• Disk I/O: Sequential writes, good for HDDs
• CPU usage: Moderate, limited parallelism

🎯 Best For:
• Smaller datasets (< 10M records)
• Systems with plenty of memory
• When simplicity is preferred over speed
• When atomic consistency is critical
*/

