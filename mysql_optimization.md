# MySQL Optimization Recommendations for Finance Permission System

## Database Configuration Optimizations

### Server Configuration (`my.cnf`)

```sql
# InnoDB specific settings
innodb_buffer_pool_size = 4G          # Allocate up to 70-80% of server memory
innodb_log_file_size = 512M           # Larger log files for better performance
innodb_flush_log_at_trx_commit = 2    # Slightly less durable but much faster
innodb_flush_method = O_DIRECT        # Avoid double buffering
innodb_file_per_table = 1             # Separate tablespace files
innodb_stats_on_metadata = 0          # Don't update statistics during metadata operations

# Query cache (disable for high write workloads)
query_cache_type = 0
query_cache_size = 0

# Connection settings
max_connections = 500
thread_cache_size = 32

# Temporary tables
tmp_table_size = 64M
max_heap_table_size = 64M

# Sort buffer
sort_buffer_size = 4M
join_buffer_size = 4M
read_rnd_buffer_size = 8M

# Logging for slow queries
slow_query_log = 1
slow_query_log_file = /var/log/mysql/mysql-slow.log
long_query_time = 2
```

### Session Settings for Bulk Operations

```sql
-- Before bulk operations
SET SESSION sql_log_bin = 0;            -- Disable binary logging
SET foreign_key_checks = 0;             -- Disable foreign key checks
SET unique_checks = 0;                  -- Disable unique checks
SET autocommit = 0;                     -- Disable autocommit
SET SESSION transaction_isolation = 'READ-COMMITTED';  -- Use less strict isolation

-- After bulk operations
SET foreign_key_checks = 1;
SET unique_checks = 1;
SET autocommit = 1;
SET SESSION sql_log_bin = 1;            -- Re-enable binary logging
COMMIT;
```

## Schema Optimizations

### Indexes

```sql
-- Compound indexes for common access patterns
CREATE INDEX idx_funds_handle_order ON financial_funds(handle_by, order_id);
CREATE INDEX idx_funds_order_customer ON financial_funds(order_id, customer_id);
CREATE INDEX idx_orders_user_order ON orders(user_id, order_id);
CREATE INDEX idx_customers_admin_customer ON customers(admin_user_id, customer_id);

-- Consider using a covering index for common queries
CREATE INDEX idx_funds_covering ON financial_funds(handle_by, order_id, customer_id, amount);

-- Add index hints to queries when needed
SELECT * FROM financial_funds FORCE INDEX (idx_funds_handle_by) WHERE handle_by = 123;
```

### Partitioning

```sql
-- Partition financial_funds by fund_id range for better pruning
ALTER TABLE financial_funds
PARTITION BY RANGE (fund_id) (
    PARTITION p0 VALUES LESS THAN (100000),
    PARTITION p1 VALUES LESS THAN (200000),
    PARTITION p2 VALUES LESS THAN (300000),
    PARTITION p3 VALUES LESS THAN (400000),
    PARTITION p4 VALUES LESS THAN (500000),
    PARTITION p5 VALUES LESS THAN (600000),
    PARTITION p6 VALUES LESS THAN (700000),
    PARTITION p7 VALUES LESS THAN (800000),
    PARTITION p8 VALUES LESS THAN (900000),
    PARTITION p9 VALUES LESS THAN (1000000),
    PARTITION p10 VALUES LESS THAN MAXVALUE
);
```

### Denormalization and Materialized Views

```sql
-- Create a user hierarchy materialized table
CREATE TABLE user_hierarchy (
    user_id INT NOT NULL,
    subordinate_id INT NOT NULL,
    depth INT NOT NULL,
    PRIMARY KEY (user_id, subordinate_id),
    KEY idx_subordinate (subordinate_id)
);

-- Create a materialized access table for supervisors
CREATE TABLE supervisor_access_scope (
    supervisor_id INT NOT NULL,
    order_id INT,
    customer_id INT,
    fund_id INT,
    PRIMARY KEY (supervisor_id, fund_id),
    KEY idx_supervisor_order (supervisor_id, order_id),
    KEY idx_supervisor_customer (supervisor_id, customer_id)
);

-- Refresh script to run periodically
DELIMITER //
CREATE PROCEDURE refresh_supervisor_access()
BEGIN
    TRUNCATE TABLE supervisor_access_scope;
    
    INSERT INTO supervisor_access_scope (supervisor_id, order_id, customer_id, fund_id)
    SELECT DISTINCT 
        h.user_id AS supervisor_id,
        o.order_id,
        c.customer_id,
        f.fund_id
    FROM user_hierarchy h
    JOIN users u ON h.subordinate_id = u.id
    LEFT JOIN orders o ON u.id = o.user_id
    LEFT JOIN customers c ON u.id = c.admin_user_id
    LEFT JOIN financial_funds f ON f.handle_by = h.subordinate_id OR f.order_id = o.order_id OR f.customer_id = c.customer_id
    WHERE u.role = 'supervisor';
END //
DELIMITER ;
```

## Query Optimizations

### Supervisor Funds Query

Replace complex joins with pre-computed data:

```sql
-- Before: Complex nested query with multiple joins
SELECT f.* 
FROM financial_funds f
WHERE f.handle_by IN (
    SELECT subordinate_id FROM user_hierarchy WHERE user_id = ? AND depth <= 3
)
OR f.order_id IN (
    SELECT o.order_id FROM orders o
    JOIN user_hierarchy h ON o.user_id = h.subordinate_id
    WHERE h.user_id = ? AND h.depth <= 3
)
OR f.customer_id IN (
    SELECT c.customer_id FROM customers c
    JOIN user_hierarchy h ON c.admin_user_id = h.subordinate_id
    WHERE h.user_id = ? AND h.depth <= 3
);

-- After: Using pre-computed access scope
SELECT f.*
FROM financial_funds f
JOIN supervisor_access_scope s ON f.fund_id = s.fund_id
WHERE s.supervisor_id = ?
LIMIT 1000;
```

### Batch Processing

When working with large result sets, use batched queries:

```sql
-- Process large result sets in batches
SELECT f.* FROM financial_funds f
WHERE f.fund_id > ? AND f.fund_id <= ?
ORDER BY f.fund_id
LIMIT 10000;
```

## Monitoring and Profiling

### Identify Slow Queries

```sql
-- Enable the slow query log
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL slow_query_log_file = '/var/log/mysql/mysql-slow.log';
SET GLOBAL long_query_time = 1;

-- Query the slow query log
SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT 10;

-- Analyze a specific query
EXPLAIN SELECT f.* FROM financial_funds f WHERE f.handle_by = 123;
EXPLAIN ANALYZE SELECT f.* FROM financial_funds f WHERE f.handle_by = 123;
```

### Monitor Index Usage

```sql
-- Check index usage
SELECT 
    table_name, index_name, 
    index_type, non_unique, 
    cardinality
FROM information_schema.statistics 
WHERE table_schema = 'finance'
ORDER BY table_name, index_name;

-- Identify unused indexes
SELECT * FROM sys.schema_unused_indexes;

-- Identify redundant indexes
SELECT * FROM sys.schema_redundant_indexes;
```

## Replication and Scalability

### Read-Write Splitting

```
Primary Server (Writes):
- Handle all write operations
- Handle critical read operations that need absolute consistency

Replica Servers (Reads):
- Handle most read operations
- Scale horizontally by adding more replicas
```

### Connection Pooling

Implement connection pooling in the application:

```python
# Using a connection pool
from mysql.connector.pooling import MySQLConnectionPool

pool = MySQLConnectionPool(
    pool_name="finance_pool",
    pool_size=10,
    **config
)

# Get connection from pool
conn = pool.get_connection()
try:
    # Use connection
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
finally:
    conn.close()  # Return to pool, not close
```

## Performance Testing

### Load Testing

```bash
# Using mysqlslap for load testing
mysqlslap --concurrency=50 --iterations=10 --query="SELECT * FROM financial_funds LIMIT 1000" --create-schema=finance

# Using sysbench for more comprehensive testing
sysbench --db-driver=mysql --mysql-db=finance --mysql-user=root --mysql-password=123456 oltp_read_only prepare
sysbench --db-driver=mysql --mysql-db=finance --mysql-user=root --mysql-password=123456 oltp_read_only run
```

## Summary of Recommendations

1. **Immediate Improvements**:
   - Add compound indexes on frequently joined columns
   - Use proper database configuration settings
   - Implement user hierarchy table for faster permission checks

2. **Medium-term Improvements**:
   - Implement materialized views for supervisor access
   - Add data partitioning for large tables
   - Use connection pooling in the application

3. **Long-term Improvements**:
   - Implement read-write splitting with replication
   - Consider sharding for horizontal scalability
   - Set up automated monitoring and performance tuning

By implementing these optimizations, the system should be able to efficiently handle queries even with millions of records per table.