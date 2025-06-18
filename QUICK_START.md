# Finance Permission System - Quick Start Guide

This guide will help you quickly get started with the Finance Permission System for testing performance with complex permission models.

## Prerequisites

- Python 3.8+
- MySQL Server 5.7+ or 8.0+
- Git (for cloning the repository)

## 1. Setup Database Connection

Create a `.env` file in the project root with your MySQL connection details:

```
DB_HOST_V2=127.0.0.1
DB_PORT_V2=3306
DB_USER_V2=root
DB_PASSWORD_V2=123456
DB_NAME_V2=finance
```

## 2. Install Dependencies

```bash
# Using pip
pip install mysql-connector-python python-dotenv prettytable

# OR using uv (faster)
uv pip install mysql-connector-python python-dotenv prettytable
```

## 3. Create Test Database

Generate a million-record test database:

```bash
python create_million.py
```

This will create database tables and populate them with:
- 1,000,000 user records
- 1,000,000 order records
- 1,000,000 customer records
- 1,000,000 financial fund records
- User hierarchy relationships

The process takes approximately 1-2 minutes depending on your system.

## 4. Run Basic Tests

### Test Pagination

```bash
# Test user pagination
python pagination_examples.py --query users --page 1 --page_size 20

# Test financial funds with filtering
python pagination_examples.py --query funds --min_amount 500000 --page 1 --page_size 10
```

### Test Supervisor Queries

```bash
# List available supervisors
python run_performance_test.py --list

# Run performance test for a specific supervisor
python run_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC
```

### Test Recursive Permission Model

```bash
# Test recursive CTE permission model
python recursive_cte_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC

# Test optimized non-recursive permission model
python recursive_cte_performance_test.py --supervisor_id 2 --sort_by amount --sort_order DESC --optimized
```

## 5. Create Custom Test Scenarios

### Create a Super-Supervisor

Create a supervisor with many subordinates for testing:

```bash
mysql -h 127.0.0.1 -P 3306 -u root -p123456 -e "USE finance; INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth) SELECT 500, id, 1 FROM users WHERE id BETWEEN 1000 AND 2000;"
```

Then test the performance:

```bash
python run_performance_test.py --supervisor_id 500 --iterations 3
```

### Test Different Data Volumes

For smaller datasets:

```bash
# Clean existing database
mysql -h 127.0.0.1 -P 3306 -u root -p123456 -e "DROP DATABASE IF EXISTS finance;"

# Create smaller dataset (100,000 records per table)
python create_million.py --records 100000
```

## 6. Interpret Results

The most important metrics to look for:

1. **Total query time**: Should be under 100ms for standard queries
2. **Count query time**: Often the most expensive operation
3. **Data query time**: Shows efficiency of indices and joins
4. **Query execution plan**: Check for table scans vs. index usage

## 7. Next Steps

- Check detailed analysis in `performance_test_results.md`
- Explore optimization recommendations
- Try implementing your own permission models
- Test with different database engines
- Add your own test cases

For detailed information about the project and all available tests, see the full `README.md`.