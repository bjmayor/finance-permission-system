# Finance Permission System

财务权限设计demo (Financial Permission System Demo)

[快速开始指南](QUICK_START.md) | [性能测试结果](performance_test_results.md)

## Overview

This project demonstrates a hierarchical permission system for financial data with comprehensive performance testing and optimization for complex permission queries in a multi-million record database. It implements different data storage options and multiple pagination strategies:

1. In-memory data structures (original implementation)
2. SQLite database implementation
3. MySQL database implementation with million-record dataset
4. **Advanced Pagination Solutions** - Multiple approaches tested for high-concurrency scenarios
5. **Permission Query Optimization** - From materialized views to real-time query strategies

The system models users with different roles (admin, supervisor, staff) and enforces permissions on financial data access based on these roles and organizational hierarchy. It includes comprehensive performance testing for various query patterns, including recursive permission queries, pagination, sorting, and permission-filtered queries.

## Key Findings & Recommendations

### **Final Recommendation: Temporary Table Approach**

After extensive testing of multiple pagination strategies, **the temporary table approach is recommended for production financial systems** due to:

- ✅ **100% Data Accuracy** - Critical for financial systems
- ✅ **Real-time Consistency** - Always returns current data
- ✅ **Stable Performance** - 0.3-0.5s response time, scales to 100+ concurrent users
- ✅ **Complete Pagination Support** - Full support for frontend pagination requirements
- ✅ **Predictable Resource Usage** - No connection pool exhaustion issues

### **Performance Comparison Summary**

| Approach | Single User | 20 Concurrent Users | Data Accuracy | Recommendation |
|----------|-------------|-------------------|---------------|----------------|
| **Materialized View** | 28+ minutes | N/A (too slow) | ✅ Accurate | ❌ Not viable |
| **4-Step Query** | 1.39s | Failed with errors | ✅ Accurate | ❌ Concurrency issues |
| **Parallel Query** | 0.79s | 26.3s avg, many failures | ❌ Potential inconsistency | ❌ Unstable |
| **Temporary Table** | 0.4s | 1.7s avg, 100% success | ✅ 100% Accurate | ✅ **Recommended** |

## Requirements

- Python 3.8+
- MySQL Server 5.7+ or 8.0+
- Required packages (install with `pip install -r requirements.txt` or `uv pip install -r requirements.txt`):
  - mysql-connector-python
  - python-dotenv
  - matplotlib (optional, for visualization)
  - numpy (optional)
  - prettytable (for formatted test output)

## Configuration

### MySQL Configuration

Create a `.env` file with your MySQL connection details:

```
DB_HOST_V2=127.0.0.1
DB_PORT_V2=3306
DB_USER_V2=root
DB_PASSWORD_V2=123456
DB_NAME_V2=finance
```

## Usage

### Running the In-memory Implementation

```bash
python main.py
```

### Using the SQLite Implementation

1. Initialize the database (creates database with 1M records per table):
```bash
python run_database.py --init
```

2. Initialize with custom record count:
```bash
python run_database.py --init --records 10000
```

3. Run performance tests:
```bash
python run_database.py
```

### Using the MySQL Implementation

1. Initialize the MySQL database:
```bash
python run_mysql.py --init
```

2. Initialize with custom record count:
```bash
python run_mysql.py --init --records 10000
```

3. Run performance tests:
```bash
python run_mysql.py
```

4. Show database statistics:
```bash
python run_mysql.py --stats
```

### Benchmarking

Compare performance across implementations:

```bash
python compare_benchmark.py
```

Options:
- `--iterations 5`: Set number of test iterations
- `--memory-only`: Test only in-memory implementation
- `--sqlite-only`: Test only SQLite implementation
- `--mysql-only`: Test only MySQL implementation

## Creating Million-Record Test Dataset

To generate a test database with million-record tables:

```bash
python create_million.py
```

This script will:
1. Create database tables (users, orders, customers, financial_funds, user_hierarchy)
2. Insert basic seed data
3. Generate 1,000,000 records in each table
4. Create meaningful relationships between records
5. Properly set up hierarchy relationships for testing permission models

Options:
- `--records 500000`: Create a smaller dataset with 500,000 records per table
- `--skip-setup`: Skip database initialization and only add records

## Performance Testing Tools

### 1. Basic Pagination Testing

Test basic pagination performance with different filter conditions:

```bash
python pagination_examples.py --query users --page 1 --page_size 20
python pagination_examples.py --query funds --min_amount 800000 --page_size 10
python pagination_examples.py --query complex --department 华东区 --page_size 15
```

Available query types:
- `users`: User list with role/department filtering
- `funds`: Financial funds with amount filtering
- `customer_orders`: Join query for customers and their orders
- `complex`: Complex report joining multiple tables
- `subordinates`: Query subordinates with hierarchy depth

### 2. Performance Testing for Supervisor Queries

Test performance for supervisor-level queries with permissions:

```bash
# List supervisors in the system
python run_performance_test.py --list

# Test specific supervisor query performance
python run_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC
```

Options:
- `--page_size 20`: Change records per page
- `--page 2`: Test different page numbers
- `--iterations 5`: Run multiple iterations for better averaging

### 3. Recursive CTE Permission Query Testing

Test complex recursive permission model performance:

```bash
# Test standard recursive CTE query
python recursive_cte_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC

# Test optimized non-recursive query
python recursive_cte_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC --optimized
```

## Comprehensive Permission Query Research & Testing

### **Research Journey: From Materialized Views to Real-time Queries**

This project evolved through extensive research and testing of different approaches to solve complex financial permission queries:

#### **Phase 1: Materialized View Investigation**
- **Initial Goal**: Use pre-computed materialized views for fast permission queries
- **Challenge Discovered**: Complex OR-based permission logic (handle_by OR order_id OR customer_id)
- **Result**: 28+ minute refresh times made materialized views impractical
- **Conclusion**: Real-time querying approach needed

#### **Phase 2: 4-Step Permission Query Strategy**
Implemented a "divide and conquer" approach:
1. Query supervisor's subordinate user IDs
2. Query authorized order IDs from orders table  
3. Query authorized customer IDs from customers table
4. Query financial_funds with OR-based IN clauses

**Performance Results**:
- Single user: 1.39 seconds (1000x faster than materialized view!)
- Batch processing implemented to handle large ID lists (>50k IDs)
- **Critical Issue**: High concurrency caused connection pool exhaustion

#### **Phase 3: Advanced Pagination Strategy Comparison**

Tested multiple approaches for high-concurrency pagination:

**A. Temporary Table Approach**
```sql
CREATE TEMPORARY TABLE temp_permissions_xxx (...)
INSERT INTO temp_permissions_xxx SELECT ... WHERE handle_by IN (...)
UNION 
INSERT INTO temp_permissions_xxx SELECT ... WHERE order_id IN (...)
UNION
INSERT INTO temp_permissions_xxx SELECT ... WHERE customer_id IN (...)
```
- **Performance**: 0.4s single user, 1.7s average with 20 concurrent users
- **Accuracy**: 100% accurate, real-time data
- **Concurrency**: Stable up to 100+ users
- **✅ RECOMMENDED for production**

**B. Parallel Query Approach**
```python
with ThreadPoolExecutor(max_workers=3) as executor:
    # Parallel execution of 3 permission dimensions
```
- **Performance**: 0.79s single user, but 26s+ with concurrency
- **Issues**: Massive connection pool pressure, query timeouts
- **❌ Not suitable for production**

**C. Memory-based Sorting Approach**
- Load all permission data into memory, sort and paginate in application
- **Issues**: Memory pressure, data consistency concerns
- **❌ Not suitable for financial systems**

### **Performance Test Results Summary**

The performance testing revealed several key insights. For detailed analysis and results, see [Performance Test Results](performance_test_results.md).

### **Core Permission Model Performance**
1. **Permission Resolution Speed**:
   - Small-scale supervisors (10-12 subordinates): 1.6-1.9ms
   - Large-scale supervisors (1000+ subordinates): 17-19ms
   - Performance scales linearly with subordinate count

2. **Query Optimization Impact**:
   - Proper indexing: 10-100x performance improvement
   - Recursive CTE: 3-7 seconds
   - Non-recursive optimized: 0.9-1.1 seconds (3-8x faster)

3. **Pagination Performance**:
   - Simple paginated queries: 2-100ms even with million-record tables
   - COUNT(*) queries often more expensive than data retrieval
   - Complex permission filtering adds 0.1-0.5s overhead

### **Critical Performance Bottlenecks Identified**
1. **Materialized View Refresh**: 28+ minutes (completely impractical)
2. **Large IN clause operations**: >50k IDs cause MySQL limits and performance issues
3. **Concurrent connection management**: Connection pool exhaustion at 50+ concurrent users
4. **Memory pressure**: Loading millions of records into application memory

## Production Implementation Guide

### **Recommended Architecture**

```python
# Production-ready pagination service
def financial_pagination_service(supervisor_id: int, page: int = 1, 
                               page_size: int = 20, sort_by: str = "fund_id", 
                               sort_order: str = "ASC") -> Dict[str, Any]:
    """
    Production implementation using temporary table approach
    - 100% data accuracy guaranteed
    - Handles 100+ concurrent users
    - 0.3-0.5s response time
    """
```

### **Key Implementation Files**

- **`accurate_pagination.py`** - Production-ready temporary table implementation
- **`alternative_permission_test.py`** - 4-step permission query approach  
- **`final_concurrent_pagination.py`** - High-concurrency testing framework
- **`refresh_materialized_view.sh`** - Materialized view approach (not recommended)

### **Optimization Recommendations**

- For SQLite optimizations, see `optimization_recommendations.md`
- For MySQL optimizations, see `mysql_optimization.md`
- For detailed performance test results and analysis, see `performance_test_results.md`
- For quick setup and testing, see `QUICK_START.md`

### **Production Deployment Recommendations**

1. **Database Configuration**:
   - MySQL connection pool: 50-100 connections minimum
   - `tmp_table_size` and `max_heap_table_size`: 256MB+
   - Ensure proper indexing on financial_funds (handle_by, order_id, customer_id)

2. **Application Configuration**:
   - Use temporary table approach (`accurate_pagination.py`)
   - Implement permission caching (5-minute TTL recommended)
   - Add user-friendly loading indicators ("Calculating permissions...")

3. **Monitoring & Alerts**:
   - Monitor temporary table creation rate
   - Alert on response times >2 seconds
   - Track concurrent user count vs. performance

4. **Scaling Considerations**:
   - Read replicas for permission queries
   - Consider departmental data partitioning for >10k users
   - Implement connection pooling with circuit breakers

### **Alternative Approaches Analysis**

1. **Materialized View Approach** (`refresh_materialized_view.sh`):
   - ❌ 28+ minute refresh time
   - ❌ Not suitable for real-time financial data
   - ✅ Would be fast for queries (if refresh was feasible)

2. **4-Step Query Approach** (`alternative_permission_test.py`):
   - ✅ 1.39s single-user performance
   - ❌ Connection pool issues with concurrency
   - ✅ 100% data accuracy

3. **Parallel Query Approach** (`final_concurrent_pagination.py`):
   - ✅ 0.79s single-user performance
   - ❌ Fails catastrophically under load
   - ❌ Potential data consistency issues

4. **Temporary Table Approach** (`accurate_pagination.py`):
   - ✅ 0.4s single-user, 1.7s under load
   - ✅ Handles 100+ concurrent users
   - ✅ 100% data accuracy guaranteed
   - ✅ **RECOMMENDED FOR PRODUCTION**

## How to Reproduce Our Research & Tests

See [QUICK_START.md](QUICK_START.md) for a simplified guide to get started quickly.

### **1. Setup Test Environment**
```bash
# Create million-record test database
python create_million.py

# Verify database setup
python test_mysql_conn.py
```

### **2. Test Different Pagination Approaches**

**A. Test Temporary Table Approach (Recommended)**:
```bash
# Single user test
python accurate_pagination.py

# Concurrent load test (5 users, 2 requests each)
# This will show 100% success rate with ~1.7s average response time
```

**B. Test 4-Step Permission Query**:
```bash
# Fast single-user performance (1.39s)
python alternative_permission_test.py

# Shows the divide-and-conquer approach performance
```

**C. Test Parallel Query Approach**:
```bash
# Shows good single-user performance but concurrency issues
python final_concurrent_pagination.py
```

**D. Test Materialized View Approach** (Educational only):
```bash
# WARNING: This takes 28+ minutes to refresh
bash refresh_materialized_view.sh
```

### **3. Comprehensive Performance Testing**

**Traditional Performance Tests**:
```bash
# Basic pagination performance
python pagination_examples.py --query funds --min_amount 500000 --page 1 --page_size 20
python pagination_examples.py --query complex --department 华东区 --page_size 15

# Supervisor permission queries
python run_performance_test.py --list
python run_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC --iterations 3

# Recursive vs non-recursive comparison
python recursive_cte_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC
python recursive_cte_performance_test.py --supervisor_id 2 --optimized
```

**Advanced Concurrent Testing**:
```bash
# Test high-concurrency scenarios
python simple_concurrent_pagination.py  # Memory-based approach
python final_concurrent_pagination.py   # Parallel query approach (will show failures)
python accurate_pagination.py           # Temporary table approach (stable)
```

### **4. Analyze Different Query Strategies**

**Permission Resolution Analysis**:
```bash
# Analyze the 4-step approach performance breakdown
python alternative_permission_test.py
# Shows: Step 1 (0.004s), Step 2 (0.068s), Step 3 (0.068s), Step 4 (1.25s)

# Test batch processing for large ID lists
python simple_pagination_test.py
# Demonstrates batch processing to avoid MySQL IN clause limits
```

**Database Performance Analysis**:
```bash
# Monitor database performance during tests
python monitor_performance.py

# Deep analysis of materialized view performance issues
python analyze_mv_inconsistency.py
```

### **5. Reproduce Our Key Findings**

**Finding 1: Materialized View Performance Issue**:
```bash
# This will demonstrate the 28+ minute refresh time
time bash refresh_materialized_view.sh
```

**Finding 2: 4-Step Query Success**:
```bash
# Shows 1000x performance improvement over materialized view
python alternative_permission_test.py
# Expected output: "Total execution time: 1.3913s" vs 28+ minutes
```

**Finding 3: Concurrency Issues with Parallel Approach**:
```bash
# Shows connection pool exhaustion and query failures
python final_concurrent_pagination.py
# Look for "并行查询异常" (parallel query exceptions)
```

**Finding 4: Temporary Table Stability**:
```bash
# Shows 100% success rate under load
python accurate_pagination.py
# Expected: "成功请求: 40/40" (40/40 successful requests)
```

### **6. Create Custom Test Scenarios**

```bash
# Create a supervisor with many subordinates
mysql -h 127.0.0.1 -P 3306 -u root -p123456 -e "
USE finance; 
INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth) 
SELECT 500, id, 1 FROM users WHERE id BETWEEN 1000 AND 3000;"

# Test the performance impact
python alternative_permission_test.py  # Modify supervisor_id to 500
python accurate_pagination.py          # Test with large permission scope
```

### **7. Performance Comparison Script**

```bash
# Run comprehensive comparison of all approaches
python compare_benchmark.py --mysql-only
# This will show the performance differences between all tested approaches
```
