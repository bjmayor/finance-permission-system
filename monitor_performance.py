import sqlite3
import time
import os
import argparse
from database import DatabaseApiGateway

def measure_query_performance(db_path, query, params=None, iterations=5):
    """Measure the performance of a specific query"""
    if params is None:
        params = []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable EXPLAIN QUERY PLAN for analysis
    cursor.execute("EXPLAIN QUERY PLAN " + query, params)
    plan = cursor.fetchall()
    print("\nQuery execution plan:")
    for step in plan:
        print(f"  {step}")
    
    # Run the query multiple times and measure performance
    execution_times = []
    for i in range(iterations):
        start_time = time.time()
        cursor.execute(query, params)
        cursor.fetchall()
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convert to ms
        execution_times.append(execution_time)
        print(f"Iteration {i+1}: {execution_time:.2f} ms")
    
    avg_time = sum(execution_times) / len(execution_times)
    min_time = min(execution_times)
    max_time = max(execution_times)
    
    # Calculate standard deviation manually
    variance = sum((t - avg_time) ** 2 for t in execution_times) / len(execution_times)
    std_dev = variance ** 0.5
    
    print(f"\nAverage execution time: {avg_time:.2f} ms")
    print(f"Min execution time: {min_time:.2f} ms")
    print(f"Max execution time: {max_time:.2f} ms")
    print(f"Standard deviation: {std_dev:.2f} ms")
    
    conn.close()
    return avg_time, min_time, max_time, std_dev

def analyze_database(db_path):
    """Analyze database structure and size"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get database size
    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"Database file size: {db_size_mb:.2f} MB")
    
    # Get table counts
    tables = ["users", "orders", "customers", "financial_funds"]
    table_counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        table_counts[table] = count
        print(f"Table '{table}' contains {count:,} records")
    
    # Check indexes
    print("\nIndexes in the database:")
    cursor.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index'")
    for idx in cursor.fetchall():
        index_name, table_name = idx
        print(f"  {index_name} on table {table_name}")
    
    # Check for missing indexes on frequently joined columns
    print("\nChecking for potentially missing indexes:")
    key_columns = {
        "users": ["id", "parent_id"],
        "orders": ["user_id"],
        "customers": ["admin_user_id"],
        "financial_funds": ["handle_by", "order_id", "customer_id"]
    }
    
    for table, columns in key_columns.items():
        for column in columns:
            cursor.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='{table}' AND sql LIKE '%{column}%'")
            if cursor.fetchone()[0] == 0:
                print(f"  Missing index on {table}.{column}")
    
    conn.close()
    
    # Return table counts for visualization
    return table_counts, db_size_mb

def run_role_based_queries(db_path):
    """Run and measure performance of role-based queries"""
    gateway = DatabaseApiGateway(db_path)
    
    results = {}
    roles = ["admin", "supervisor", "staff"]
    
    for role in roles:
        print(f"\n=== Testing {role.upper()} role ===")
        start_time = time.time()
        gateway.authenticate(role)
        auth_time = time.time() - start_time
        print(f"Authentication time: {auth_time:.4f} seconds")
        
        # Measure funds retrieval performance
        start_time = time.time()
        funds = gateway.get_funds()
        query_time = time.time() - start_time
        print(f"Retrieved {len(funds):,} funds in {query_time:.4f} seconds")
        
        results[role] = {
            "auth_time": auth_time,
            "query_time": query_time,
            "record_count": len(funds)
        }
    
    return results

def visualize_performance(role_results, table_counts):
    """Display performance metrics in text format"""
    print("\n=== Performance Analysis ===")
    
    # Query performance by role
    print("\nQuery Performance by Role:")
    roles = list(role_results.keys())
    for role in roles:
        print(f"  {role}: {role_results[role]['query_time']:.4f} seconds")
    
    # Records retrieved by role
    print("\nRecords Retrieved by Role:")
    for role in roles:
        print(f"  {role}: {role_results[role]['record_count']} records")
    
    # Table sizes
    print("\nRecords per Table:")
    tables = list(table_counts.keys())
    for table in tables:
        print(f"  {table}: {table_counts[table]} records")
    
    # Time per record (efficiency)
    print("\nTime per Record (Lower is Better):")
    for role in roles:
        efficiency = role_results[role]["query_time"] / max(role_results[role]["record_count"], 1) * 1000
        print(f"  {role}: {efficiency:.4f} ms/record")

def run_specific_queries(db_path):
    """Run specific queries to measure performance"""
    print("\n=== Testing specific query performance ===")
    
    # Query 1: Get all users with supervisor role
    query1 = "SELECT id, name FROM users WHERE role = ?"
    print("\nQuery 1: Get all users with specific role")
    measure_query_performance(db_path, query1, ["supervisor"])
    
    # Query 2: Get orders for a specific user
    user_id = 3  # Existing user ID
    query2 = "SELECT order_id FROM orders WHERE user_id = ?"
    print("\nQuery 2: Get orders for a specific user")
    measure_query_performance(db_path, query2, [user_id])
    
    # Query 3: Get financial funds with complex joins
    query3 = """
    SELECT f.fund_id, f.amount, u.name as handler_name
    FROM financial_funds f
    JOIN users u ON f.handle_by = u.id
    JOIN orders o ON f.order_id = o.order_id
    WHERE u.role = ? 
    LIMIT 1000
    """
    print("\nQuery 3: Get funds with complex joins")
    measure_query_performance(db_path, query3, ["staff"])
    
    # Query 4: Subordinates query (most complex operation)
    user_id = 2  # A supervisor user ID
    query4 = """
    WITH RECURSIVE subordinates(id) AS (
        VALUES(?)
        UNION
        SELECT u.id FROM users u, subordinates s
        WHERE u.parent_id = s.id
    )
    SELECT group_concat(id) FROM subordinates
    """
    print("\nQuery 4: Get all subordinates recursively")
    measure_query_performance(db_path, query4, [user_id])

def main():
    parser = argparse.ArgumentParser(description="Finance Permission System Performance Monitor")
    parser.add_argument("--db", type=str, default="finance_system.db", help="Database file path")
    parser.add_argument("--analyze", action="store_true", help="Analyze database structure")
    parser.add_argument("--roles", action="store_true", help="Test role-based query performance")
    parser.add_argument("--queries", action="store_true", help="Test specific query performance")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Database file {args.db} does not exist.")
        return
    
    # Run selected tests
    if args.all or args.analyze:
        print("\n=== Database Analysis ===")
        table_counts, db_size = analyze_database(args.db)
    else:
        table_counts = None
    
    if args.all or args.queries:
        run_specific_queries(args.db)
    
    if args.all or args.roles:
        role_results = run_role_based_queries(args.db)
        if table_counts:
            visualize_performance(role_results, table_counts)

if __name__ == "__main__":
    main()