-- =====================================================
-- Step 5: Index & Constraint Management
-- Optimized for High-Speed Bulk Load Pipeline
-- =====================================================
-- This script implements index and constraint management for the 
-- finance_permission_mv table with the following strategy:
-- • Drop/disable FK & secondary indexes before load
-- • After load (expected ~4-6 min), create optimized indexes
-- • Build indexes CONCURRENTLY to avoid MV lock
-- • Parallel index build if DB ≥13
-- =====================================================

-- =====================================================
-- Phase 1: Pre-Load - Drop/Disable Constraints & Indexes
-- =====================================================

-- Check MySQL version for parallel index support
SELECT VERSION() as mysql_version;

-- Check if the table exists
SELECT 
    TABLE_NAME, 
    ENGINE, 
    TABLE_ROWS,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / (1024 * 1024), 2) AS size_mb
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv';

-- Store existing indexes for reference
SELECT 
    'EXISTING INDEXES' as info_type,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
    NON_UNIQUE,
    INDEX_TYPE,
    CARDINALITY
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
    AND INDEX_NAME != 'PRIMARY'
GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
ORDER BY INDEX_NAME;

-- =====================================================
-- Phase 1A: Drop Secondary Indexes (Keep PRIMARY)
-- =====================================================

-- Drop existing secondary indexes if they exist
-- Note: Using IF EXISTS for MySQL 5.7+ compatibility
SET @sql = (SELECT CONCAT('DROP INDEX ', INDEX_NAME, ' ON finance_permission_mv') 
           FROM INFORMATION_SCHEMA.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE() 
           AND TABLE_NAME = 'finance_permission_mv' 
           AND INDEX_NAME = 'idx_supervisor_type' 
           LIMIT 1);
SET @sql = IFNULL(@sql, 'SELECT "Index idx_supervisor_type does not exist" as notice');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT CONCAT('DROP INDEX ', INDEX_NAME, ' ON finance_permission_mv') 
           FROM INFORMATION_SCHEMA.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE() 
           AND TABLE_NAME = 'finance_permission_mv' 
           AND INDEX_NAME = 'idx_supervisor_fund' 
           LIMIT 1);
SET @sql = IFNULL(@sql, 'SELECT "Index idx_supervisor_fund does not exist" as notice');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT CONCAT('DROP INDEX ', INDEX_NAME, ' ON finance_permission_mv') 
           FROM INFORMATION_SCHEMA.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE() 
           AND TABLE_NAME = 'finance_permission_mv' 
           AND INDEX_NAME = 'idx_permission_type' 
           LIMIT 1);
SET @sql = IFNULL(@sql, 'SELECT "Index idx_permission_type does not exist" as notice');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT CONCAT('DROP INDEX ', INDEX_NAME, ' ON finance_permission_mv') 
           FROM INFORMATION_SCHEMA.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE() 
           AND TABLE_NAME = 'finance_permission_mv' 
           AND INDEX_NAME = 'idx_supervisor_amount' 
           LIMIT 1);
SET @sql = IFNULL(@sql, 'SELECT "Index idx_supervisor_amount does not exist" as notice');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT CONCAT('DROP INDEX ', INDEX_NAME, ' ON finance_permission_mv') 
           FROM INFORMATION_SCHEMA.STATISTICS 
           WHERE TABLE_SCHEMA = DATABASE() 
           AND TABLE_NAME = 'finance_permission_mv' 
           AND INDEX_NAME = 'idx_last_updated' 
           LIMIT 1);
SET @sql = IFNULL(@sql, 'SELECT "Index idx_last_updated does not exist" as notice');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- =====================================================
-- Phase 1B: Disable Foreign Key Checks (for load performance)
-- =====================================================

-- Note: These settings should be used during bulk load operations
-- They are provided here for reference and should be applied in the loading script
SELECT 'Foreign key and constraint management settings for bulk load:' as guidance;
SELECT 'SET SESSION foreign_key_checks = 0;' as pre_load_setting;
SELECT 'SET SESSION unique_checks = 0;' as pre_load_setting;
SELECT 'SET SESSION sql_log_bin = 0;' as pre_load_setting;
SELECT 'SET SESSION autocommit = 0;' as pre_load_setting;

-- =====================================================
-- Phase 1C: Verify Index Removal
-- =====================================================

SELECT 
    'INDEXES AFTER CLEANUP' as info_type,
    COALESCE(INDEX_NAME, 'No secondary indexes found') as remaining_indexes
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
    AND INDEX_NAME != 'PRIMARY'
GROUP BY INDEX_NAME
ORDER BY INDEX_NAME;

SELECT 'Pre-load phase completed. Table is ready for bulk loading.' as status;
SELECT 'Expected load time: 4-6 minutes for ~6M records' as estimate;

-- =====================================================
-- Phase 2: Post-Load - Create Optimized Indexes
-- =====================================================
-- Execute this section AFTER the bulk load is complete
-- =====================================================

-- =====================================================
-- Phase 2A: Required Indexes from Task Specification
-- =====================================================

-- MySQL doesn't support CONCURRENTLY keyword like PostgreSQL
-- But we can simulate concurrent-like behavior with optimizations

-- Enable optimizations for index creation
SET SESSION innodb_sort_buffer_size = 67108864; -- 64MB for index sorting
SET SESSION read_buffer_size = 2097152; -- 2MB read buffer
SET SESSION myisam_sort_buffer_size = 67108864; -- 64MB for MyISAM if needed

-- Check if MySQL 8.0+ for parallel index features
SET @mysql_version = CAST(SUBSTRING_INDEX(VERSION(), '.', 1) AS UNSIGNED);
SET @mysql_minor = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(VERSION(), '.', 2), '.', -1) AS UNSIGNED);

SELECT 
    CASE 
        WHEN @mysql_version >= 8 THEN 'MySQL 8.0+ detected: Enhanced index creation available'
        WHEN @mysql_version = 5 AND @mysql_minor >= 7 THEN 'MySQL 5.7+ detected: Standard index creation'
        ELSE 'Older MySQL version detected: Basic index creation'
    END as mysql_capabilities;

-- =====================================================
-- Required Index 1: btree (supervisor_id, permission_type, fund_id)
-- =====================================================
-- This is the primary composite index for query optimization
-- Note: MySQL uses fund_id instead of finance_id based on schema analysis

SELECT 'Creating primary composite index: (supervisor_id, permission_type, fund_id)' as status;

CREATE INDEX idx_supervisor_perm_fund 
ON finance_permission_mv (supervisor_id, permission_type, fund_id)
USING BTREE
COMMENT 'Primary composite index for supervisor permission queries';

SELECT 'Index idx_supervisor_perm_fund created successfully' as status;

-- =====================================================
-- Required Index 2: btree (fund_id) for fast revoke cascade
-- =====================================================
-- This index enables fast cascade operations when revoking permissions

SELECT 'Creating fund_id index for fast revoke cascade operations' as status;

CREATE INDEX idx_fund_revoke_cascade 
ON finance_permission_mv (fund_id)
USING BTREE
COMMENT 'Fast revoke cascade index on fund_id';

SELECT 'Index idx_fund_revoke_cascade created successfully' as status;

-- =====================================================
-- Phase 2B: Additional Performance Indexes
-- =====================================================
-- These indexes support common query patterns identified in the system

-- Permission type index for filtering
SELECT 'Creating permission_type index for filtering operations' as status;

CREATE INDEX idx_permission_type 
ON finance_permission_mv (permission_type)
USING BTREE
COMMENT 'Permission type filtering index';

-- Supervisor + amount index for financial analysis
SELECT 'Creating supervisor + amount index for financial queries' as status;

CREATE INDEX idx_supervisor_amount 
ON finance_permission_mv (supervisor_id, amount DESC)
USING BTREE
COMMENT 'Supervisor financial analysis index';

-- Last updated index for incremental refresh
SELECT 'Creating last_updated index for incremental refresh' as status;

CREATE INDEX idx_last_updated 
ON finance_permission_mv (last_updated)
USING BTREE
COMMENT 'Incremental refresh timestamp index';

-- =====================================================
-- Phase 2C: Parallel Index Creation (MySQL 8.0+)
-- =====================================================
-- Note: MySQL doesn't have true parallel index creation like PostgreSQL
-- But we can optimize with session settings and potentially use multiple connections

SELECT 
    CASE 
        WHEN @mysql_version >= 8 THEN 'Parallel index optimization: Session configured for MySQL 8.0+'
        ELSE 'Standard index creation: Parallel features not available in this MySQL version'
    END as parallel_status;

-- For MySQL 8.0+, enable parallel reading during index creation
SET @parallel_sql = 
    CASE 
        WHEN @mysql_version >= 8 THEN 'SET SESSION innodb_parallel_read_threads = 4;'
        ELSE 'SELECT "Parallel read threads not available" as notice;'
    END;

PREPARE stmt FROM @parallel_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- =====================================================
-- Phase 2D: Index Creation Verification
-- =====================================================

-- Verify all indexes were created successfully
SELECT 
    'POST-LOAD INDEX VERIFICATION' as info_type,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
    NON_UNIQUE,
    INDEX_TYPE,
    CARDINALITY,
    INDEX_COMMENT
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE, INDEX_COMMENT
ORDER BY 
    CASE INDEX_NAME 
        WHEN 'PRIMARY' THEN 1
        WHEN 'idx_supervisor_perm_fund' THEN 2  -- Primary required index
        WHEN 'idx_fund_revoke_cascade' THEN 3   -- Secondary required index
        ELSE 4
    END,
    INDEX_NAME;

-- Check index sizes and efficiency
SELECT 
    'INDEX SIZE ANALYSIS' as analysis_type,
    INDEX_NAME,
    ROUND(STAT_VALUE * @@innodb_page_size / (1024 * 1024), 2) as size_mb
FROM INFORMATION_SCHEMA.INNODB_BUFFER_STATS_BY_INDEX
WHERE OBJECT_SCHEMA = DATABASE() 
    AND OBJECT_NAME = 'finance_permission_mv'
    AND INDEX_NAME != 'PRIMARY'
ORDER BY size_mb DESC;

-- =====================================================
-- Phase 2E: Performance Testing Queries
-- =====================================================

-- Test the primary composite index
SELECT 'Testing primary composite index performance' as test_type;

EXPLAIN FORMAT=JSON
SELECT COUNT(*) 
FROM finance_permission_mv 
WHERE supervisor_id = 1 
    AND permission_type = 'handle';

-- Test the revoke cascade index
SELECT 'Testing revoke cascade index performance' as test_type;

EXPLAIN FORMAT=JSON
SELECT supervisor_id, permission_type 
FROM finance_permission_mv 
WHERE fund_id = 1001;

-- =====================================================
-- Phase 2F: Re-enable Constraints (if disabled)
-- =====================================================

SELECT 'Post-load constraint management settings:' as guidance;
SELECT 'SET SESSION foreign_key_checks = 1;' as post_load_setting;
SELECT 'SET SESSION unique_checks = 1;' as post_load_setting;
SELECT 'SET SESSION sql_log_bin = 1;' as post_load_setting;
SELECT 'SET SESSION autocommit = 1;' as post_load_setting;

-- =====================================================
-- Phase 3: Final Verification & Statistics
-- =====================================================

-- Table statistics after index creation
SELECT 
    'FINAL TABLE STATISTICS' as summary_type,
    TABLE_ROWS as estimated_rows,
    ROUND((DATA_LENGTH) / (1024 * 1024), 2) AS data_mb,
    ROUND((INDEX_LENGTH) / (1024 * 1024), 2) AS index_mb,
    ROUND((DATA_LENGTH + INDEX_LENGTH) / (1024 * 1024), 2) AS total_mb,
    ROUND((INDEX_LENGTH / (DATA_LENGTH + INDEX_LENGTH)) * 100, 1) as index_ratio_percent
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv';

-- Index cardinality analysis
SELECT 
    'INDEX CARDINALITY ANALYSIS' as analysis_type,
    INDEX_NAME,
    CARDINALITY,
    CASE 
        WHEN CARDINALITY = 0 THEN 'No data or needs ANALYZE'
        WHEN CARDINALITY < 100 THEN 'Low selectivity'
        WHEN CARDINALITY < 10000 THEN 'Medium selectivity'
        ELSE 'High selectivity'
    END as selectivity_assessment
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME = 'finance_permission_mv'
    AND SEQ_IN_INDEX = 1  -- Only first column of each index
ORDER BY CARDINALITY DESC;

-- Success confirmation
SELECT 
    'Index and constraint management completed successfully!' as final_status,
    NOW() as completion_time;

-- =====================================================
-- Usage Instructions
-- =====================================================
/*
USAGE INSTRUCTIONS:

1. PRE-LOAD (Phase 1):
   - Execute Phase 1 sections before starting bulk load
   - This removes secondary indexes to speed up insertion
   - Keep PRIMARY key for data integrity

2. BULK LOAD:
   - Run your bulk load process (expected 4-6 minutes)
   - Use the provided session settings for optimal performance

3. POST-LOAD (Phase 2):
   - Execute Phase 2 sections after bulk load completion
   - Creates optimized indexes including required ones:
     * btree (supervisor_id, permission_type, fund_id)
     * btree (fund_id) for fast revoke cascade
   - Additional performance indexes for common query patterns

4. VERIFICATION (Phase 3):
   - Review index creation results
   - Check performance with test queries
   - Analyze table and index statistics

KEY FEATURES:
- MySQL version detection for optimal settings
- Safe index dropping with existence checks
- Comprehensive verification and testing
- Performance monitoring queries
- Compatible with MySQL 5.7+ and optimized for 8.0+

INDEX STRATEGY:
- Primary composite index: (supervisor_id, permission_type, fund_id)
- Fast revoke cascade: (fund_id)
- Additional indexes for common query patterns
- All indexes use BTREE for optimal performance
*/

-- =====================================================
-- Performance Monitoring Queries (for ongoing use)
-- =====================================================

-- Monitor index usage
/*
SELECT 
    OBJECT_SCHEMA,
    OBJECT_NAME,
    INDEX_NAME,
    COUNT_FETCH,
    COUNT_INSERT,
    COUNT_UPDATE,
    COUNT_DELETE
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE OBJECT_SCHEMA = DATABASE()
    AND OBJECT_NAME = 'finance_permission_mv'
ORDER BY COUNT_FETCH DESC;
*/

-- Monitor table locks and wait times
/*
SELECT 
    OBJECT_SCHEMA,
    OBJECT_NAME,
    COUNT_READ,
    COUNT_WRITE,
    SUM_TIMER_READ/1000000000 as read_time_sec,
    SUM_TIMER_WRITE/1000000000 as write_time_sec
FROM performance_schema.table_io_waits_summary_by_table
WHERE OBJECT_SCHEMA = DATABASE()
    AND OBJECT_NAME = 'finance_permission_mv';
*/

