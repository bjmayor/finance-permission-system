# Database Optimization Recommendations for Finance Permission System

## Current Performance Analysis

With 1 million records per table, we're seeing the following performance:

- Admin queries: ~5.8ms (retrieving 1000 funds)
- Staff queries: ~1.7ms (retrieving 4 funds)
- Supervisor queries: ~2.7s (retrieving 1000 funds)

The supervisor queries are the most expensive due to the hierarchical nature of permissions and the complex joins required.

## Short-term Optimizations

### 1. Additional Indexes

```sql
-- Create composite indexes for common query patterns
CREATE INDEX idx_funds_composite_1 ON financial_funds(handle_by, order_id);
CREATE INDEX idx_funds_composite_2 ON financial_funds(order_id, customer_id);
CREATE INDEX idx_users_role_id ON users(role, id);
CREATE INDEX idx_orders_user_id_order_id ON orders(user_id, order_id);
CREATE INDEX idx_customers_admin_id_customer_id ON customers(admin_user_id, customer_id);
```

### 2. Denormalized Permission Table

Create a denormalized table for hierarchical relationships to avoid recursive queries:

```sql
CREATE TABLE user_hierarchy (
    user_id INTEGER NOT NULL,
    subordinate_id INTEGER NOT NULL,
    depth INTEGER NOT NULL,
    PRIMARY KEY (user_id, subordinate_id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (subordinate_id) REFERENCES users(id)
);

CREATE INDEX idx_hierarchy_user_id ON user_hierarchy(user_id);
CREATE INDEX idx_hierarchy_subordinate_id ON user_hierarchy(subordinate_id);
```

Populate this table with a one-time operation that calculates all subordinate relationships:

```sql
INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
WITH RECURSIVE subordinates(supervisor_id, subordinate_id, depth) AS (
    -- Direct subordinates (depth 1)
    SELECT u1.id, u2.id, 1
    FROM users u1
    JOIN users u2 ON u2.parent_id = u1.id
    UNION ALL
    -- Recursive subordinates (depth > 1)
    SELECT s.supervisor_id, u.id, s.depth + 1
    FROM subordinates s
    JOIN users u ON u.parent_id = s.subordinate_id
)
SELECT supervisor_id, subordinate_id, depth FROM subordinates
UNION
-- Everyone is their own subordinate at depth 0
SELECT id, id, 0 FROM users;
```

### 3. Materialized Access Views

Create materialized views (tables that are periodically refreshed) for common access patterns:

```sql
CREATE TABLE supervisor_accessible_orders (
    supervisor_id INTEGER NOT NULL,
    order_id INTEGER NOT NULL,
    PRIMARY KEY (supervisor_id, order_id),
    FOREIGN KEY (supervisor_id) REFERENCES users(id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE supervisor_accessible_customers (
    supervisor_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    PRIMARY KEY (supervisor_id, customer_id),
    FOREIGN KEY (supervisor_id) REFERENCES users(id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

### 4. Database Configuration Optimization

```sql
-- Increase cache size to handle more queries in memory
PRAGMA cache_size = -102400;  -- 100MB

-- Use WAL mode for better concurrency
PRAGMA journal_mode = WAL;

-- Memory-mapped I/O for faster reads
PRAGMA mmap_size = 8589934592;  -- 8GB

-- Optimize for read-heavy workloads
PRAGMA synchronous = NORMAL;

-- Increase page size for better read efficiency
PRAGMA page_size = 16384;  -- 16KB
```

## Long-term Architecture Improvements

### 1. Database Partitioning

For multi-million record tables, consider partitioning:

- Partition `financial_funds` by date ranges if temporal data is available
- Shard data by department or geographical region

### 2. Read Replicas

- Set up read replicas for reporting and analysis queries
- Direct all read queries to replicas, reserving the primary database for writes

### 3. Caching Layer

Implement a Redis or Memcached layer for:

- Caching permission scope results for each user
- Storing frequently accessed fund lists
- Caching subordinate hierarchies for supervisors

### 4. Batch Processing

For operations that don't need real-time results:

- Create periodic background jobs to pre-compute access scopes
- Build materialized views during off-peak hours
- Implement incremental updates for hierarchical data

### 5. Database Migration to PostgreSQL

For very large datasets, consider migrating to PostgreSQL which offers:

- Better support for complex hierarchical queries using recursive CTEs
- More efficient query planner for complex joins
- Advanced indexing options like GIN and BRIN indexes
- Better concurrency model for high throughput

## Query Optimization Patterns

### Supervisor Fund Access Query

Replace the current query with:

```sql
WITH supervisor_subordinates AS (
    SELECT subordinate_id 
    FROM user_hierarchy 
    WHERE user_id = ? AND depth <= 3
)
SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount
FROM financial_funds f
WHERE f.handle_by IN (SELECT subordinate_id FROM supervisor_subordinates)
   OR f.order_id IN (
      SELECT o.order_id 
      FROM orders o
      WHERE o.user_id IN (SELECT subordinate_id FROM supervisor_subordinates)
   )
   OR f.customer_id IN (
      SELECT c.customer_id 
      FROM customers c
      WHERE c.admin_user_id IN (SELECT subordinate_id FROM supervisor_subordinates)
   )
LIMIT 1000;
```

### Monitoring and Profiling

Add query monitoring to identify bottlenecks:

```sql
-- Turn on query timing
EXPLAIN QUERY PLAN SELECT ...;

-- Analyze table statistics
ANALYZE users;
ANALYZE orders;
ANALYZE customers;
ANALYZE financial_funds;

-- Check index usage
SELECT * FROM sqlite_stat1;
```

## Data Volume Growth Strategy

As the system grows beyond 1 million records per table:

1. Implement data archiving for older records
2. Consider time-based partitioning (e.g., funds by year/quarter)
3. Implement a data retention policy to manage database size
4. Use appropriate data types to minimize storage requirements
5. Consider columnar storage for analytical queries

By implementing these recommendations, the system should be able to handle 10+ million records per table while maintaining acceptable query performance.