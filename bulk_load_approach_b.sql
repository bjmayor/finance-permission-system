-- =====================================================
-- Step 4: High-Speed Bulk Load Pipeline - Approach B
-- Staging for Parallelism with Optimized Loading
-- =====================================================

-- Approach B: Multi-stage parallel loading with optimized performance
-- This approach uses staging tables and parallel processing

-- =====================================================
-- Step 1: Create UNLOGGED staging table
-- =====================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS finance_permission_stage;
DROP TABLE IF EXISTS finance_permission_mv;

-- Create staging table (UNLOGGED for faster writes)
-- Note: MySQL doesn't have UNLOGGED, but we can optimize with ENGINE=MEMORY or disable logging
CREATE TABLE finance_permission_stage (
    stage_id BIGINT NOT NULL AUTO_INCREMENT,
    supervisor_id INT NOT NULL,
    fund_id INT NOT NULL,
    handle_by INT NOT NULL,
    handler_name VARCHAR(255),
    department VARCHAR(100),
    order_id INT,
    customer_id INT,
    amount DECIMAL(15, 2),
    permission_type ENUM('handle','order','customer') NOT NULL,
    load_batch INT DEFAULT 1 COMMENT 'Batch number for parallel loading',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (stage_id),
    KEY idx_temp_batch (load_batch, permission_type)
) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4 
COLLATE=utf8mb4_general_ci
COMMENT='Staging table for parallel bulk loading';

-- =====================================================
-- Step 2: Parallel INSERT with MySQL session settings
-- =====================================================

-- Optimize MySQL for bulk operations
SET SESSION sql_log_bin = 0;  -- Disable binary logging for this session
SET SESSION foreign_key_checks = 0;  -- Disable FK checks for speed
SET SESSION unique_checks = 0;  -- Disable unique checks during load
SET SESSION autocommit = 0;  -- Use explicit transactions

-- Enable parallel processing (MySQL 8.0+)
SET SESSION max_parallel_workers_per_gather = 4;
SET SESSION innodb_parallel_read_threads = 4;

-- Batch 1: Load HANDLE dimension data
INSERT /*+ PARALLEL(4) */ INTO finance_permission_stage 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type, load_batch)
SELECT 
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'handle' as permission_type,
    1 as load_batch
FROM user_hierarchy h
JOIN financial_funds f ON h.subordinate_id = f.handle_by
JOIN users u ON f.handle_by = u.id;

COMMIT;

-- Batch 2: Load ORDER dimension data
INSERT /*+ PARALLEL(4) */ INTO finance_permission_stage 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type, load_batch)
SELECT DISTINCT
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'order' as permission_type,
    2 as load_batch
FROM user_hierarchy h
JOIN orders o ON h.subordinate_id = o.user_id
JOIN financial_funds f ON o.order_id = f.order_id
LEFT JOIN users u ON f.handle_by = u.id
WHERE f.order_id IS NOT NULL;

COMMIT;

-- Batch 3: Load CUSTOMER dimension data
INSERT /*+ PARALLEL(4) */ INTO finance_permission_stage 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type, load_batch)
SELECT DISTINCT
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount,
    'customer' as permission_type,
    3 as load_batch
FROM user_hierarchy h
JOIN customers c ON h.subordinate_id = c.admin_user_id
JOIN financial_funds f ON c.customer_id = f.customer_id
LEFT JOIN users u ON f.handle_by = u.id
WHERE f.customer_id IS NOT NULL;

COMMIT;

-- =====================================================
-- Step 3: Create indexes CONCURRENTLY on staging table
-- =====================================================

-- MySQL doesn't have CONCURRENTLY, but we can create indexes efficiently
-- Create essential indexes for the final transfer

-- Index for supervisor-based queries
CREATE INDEX idx_stage_supervisor_type ON finance_permission_stage (supervisor_id, permission_type);

-- Index for fund-based operations
CREATE INDEX idx_stage_fund ON finance_permission_stage (fund_id);

-- Index for batch processing
CREATE INDEX idx_stage_batch ON finance_permission_stage (load_batch);

-- Index for deduplication
CREATE INDEX idx_stage_unique ON finance_permission_stage (supervisor_id, fund_id, permission_type);

-- =====================================================
-- Step 4: Convert to logged and optimize
-- =====================================================

-- Re-enable logging and constraints
SET SESSION sql_log_bin = 1;
SET SESSION foreign_key_checks = 1;
SET SESSION unique_checks = 1;
SET SESSION autocommit = 1;

-- Analyze the staging table for better query plans
ANALYZE TABLE finance_permission_stage;

-- =====================================================
-- Step 5: Create final materialized view from staging
-- =====================================================

-- Create the final materialized view table
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
ROW_FORMAT=COMPRESSED
COMMENT='Final materialized view for finance permissions';

-- Transfer data from staging to final table with deduplication
INSERT INTO finance_permission_mv 
    (supervisor_id, fund_id, handle_by, handler_name, department, 
     order_id, customer_id, amount, permission_type)
SELECT DISTINCT
    supervisor_id,
    fund_id,
    handle_by,
    handler_name,
    department,
    order_id,
    customer_id,
    amount,
    permission_type
FROM finance_permission_stage
ORDER BY supervisor_id, permission_type, fund_id;

-- Update timestamps
UPDATE finance_permission_mv SET last_updated = NOW();

-- =====================================================
-- Step 6: Create production indexes on final table
-- =====================================================

-- Primary query pattern: supervisor + permission type
CREATE INDEX idx_supervisor_type ON finance_permission_mv (supervisor_id, permission_type);

-- Secondary query patterns
CREATE INDEX idx_supervisor_fund ON finance_permission_mv (supervisor_id, fund_id);
CREATE INDEX idx_permission_type ON finance_permission_mv (permission_type);
CREATE INDEX idx_supervisor_amount ON finance_permission_mv (supervisor_id, amount DESC);
CREATE INDEX idx_last_updated ON finance_permission_mv (last_updated);

-- =====================================================
-- Step 7: Cleanup and verification
-- =====================================================

-- Verify data integrity
SELECT 
    'Staging Table' as source,
    permission_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT supervisor_id) as unique_supervisors
FROM finance_permission_stage
GROUP BY permission_type

UNION ALL

SELECT 
    'Final Table' as source,
    permission_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT supervisor_id) as unique_supervisors
FROM finance_permission_mv
GROUP BY permission_type
ORDER BY source, permission_type;

-- Check for data consistency
SELECT 
    COUNT(DISTINCT CONCAT(supervisor_id, '-', fund_id, '-', permission_type)) as unique_combinations_stage
FROM finance_permission_stage;

SELECT 
    COUNT(DISTINCT CONCAT(supervisor_id, '-', fund_id, '-', permission_type)) as unique_combinations_final
FROM finance_permission_mv;

-- Optional: Drop staging table after verification
-- DROP TABLE finance_permission_stage;

-- =====================================================
-- Performance Monitoring Queries
-- =====================================================

-- Show table sizes
SELECT 
    table_name,
    table_rows,
    ROUND((data_length + index_length) / (1024 * 1024), 2) as size_mb,
    ROUND(data_length / (1024 * 1024), 2) as data_mb,
    ROUND(index_length / (1024 * 1024), 2) as index_mb
FROM information_schema.tables
WHERE table_schema = DATABASE() 
    AND table_name IN ('finance_permission_stage', 'finance_permission_mv')
ORDER BY table_name;

-- Show index efficiency
SELECT 
    table_name,
    index_name,
    GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
    non_unique,
    cardinality
FROM information_schema.statistics
WHERE table_schema = DATABASE() 
    AND table_name = 'finance_permission_mv'
GROUP BY table_name, index_name, non_unique
ORDER BY table_name, index_name;

-- =====================================================
-- Performance Notes for Approach B
-- =====================================================
/*
Approach B Characteristics:

✅ Advantages:
• Parallel processing for faster loading
• Batched operations reduce memory pressure
• Staging table allows for validation before final load
• Better error recovery (can restart individual batches)
• Optimized indexing strategy (create after load)
• More granular monitoring and progress tracking

⚠️ Considerations:
• More complex implementation with multiple steps
• Requires more disk space (staging + final tables)
• Need to manage staging table lifecycle
• Potential for partial failures requiring cleanup
• More monitoring and error handling needed

📊 Expected Performance:
• Execution time: 1-3 minutes for ~6M records
• Memory usage: Lower, distributed across batches
• Disk I/O: Higher due to staging, but more parallel
• CPU usage: Higher due to parallelism
• Scalability: Better for large datasets

🎯 Best For:
• Large datasets (> 10M records)
• Systems with multiple CPU cores
• When maximum speed is critical
• When partial recovery is important
• Production environments with monitoring

🔧 Tuning Parameters:
• Batch size: Adjust based on available memory
• Parallel workers: Match to CPU core count
• Index creation: Consider partitioned indexes for very large tables
• Buffer pool: Increase innodb_buffer_pool_size for better performance
*/

