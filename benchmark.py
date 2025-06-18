import time
import argparse
import os
import sqlite3
from main import User, FinancialFund, Order, Customer, PermissionService, FinancialService, ApiGateway
from database import DatabasePermissionService, DatabaseFinancialService, DatabaseApiGateway

def format_time(seconds):
    """Format time in appropriate units"""
    if seconds < 0.001:
        return f"{seconds * 1000000:.2f} μs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.4f} sec"

def run_in_memory_benchmark(iterations=5):
    """Run benchmark with in-memory implementation"""
    print("\n=== In-Memory Implementation Benchmark ===")
    
    # Initialize the gateway
    gateway = ApiGateway()
    
    # Test admin role
    print("\n--- Admin Role Performance ---")
    admin_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("admin")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        admin_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_admin_time = sum(admin_times) / len(admin_times)
    print(f"Average execution time: {format_time(avg_admin_time)}")
    
    # Test supervisor role
    print("\n--- Supervisor Role Performance ---")
    supervisor_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("supervisor")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        supervisor_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_supervisor_time = sum(supervisor_times) / len(supervisor_times)
    print(f"Average execution time: {format_time(avg_supervisor_time)}")
    
    # Test staff role
    print("\n--- Staff Role Performance ---")
    staff_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("staff")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        staff_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_staff_time = sum(staff_times) / len(staff_times)
    print(f"Average execution time: {format_time(avg_staff_time)}")
    
    return {
        "admin": avg_admin_time,
        "supervisor": avg_supervisor_time, 
        "staff": avg_staff_time
    }

def run_database_benchmark(db_path, iterations=5):
    """Run benchmark with database implementation"""
    if not os.path.exists(db_path):
        print(f"Database file {db_path} does not exist.")
        return None
    
    print(f"\n=== Database Implementation Benchmark ({db_path}) ===")
    
    # Get database statistics
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get table counts
    tables = ["users", "orders", "customers", "financial_funds"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table '{table}' contains {count:,} records")
    
    conn.close()
    
    # Initialize the gateway
    gateway = DatabaseApiGateway(db_path)
    
    # Test admin role
    print("\n--- Admin Role Performance ---")
    admin_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("admin")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        admin_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_admin_time = sum(admin_times) / len(admin_times)
    print(f"Average execution time: {format_time(avg_admin_time)}")
    
    # Test supervisor role
    print("\n--- Supervisor Role Performance ---")
    supervisor_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("supervisor")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        supervisor_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_supervisor_time = sum(supervisor_times) / len(supervisor_times)
    print(f"Average execution time: {format_time(avg_supervisor_time)}")
    
    # Test staff role
    print("\n--- Staff Role Performance ---")
    staff_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("staff")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        staff_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {format_time(execution_time)}")
    
    avg_staff_time = sum(staff_times) / len(staff_times)
    print(f"Average execution time: {format_time(avg_staff_time)}")
    
    return {
        "admin": avg_admin_time,
        "supervisor": avg_supervisor_time, 
        "staff": avg_staff_time
    }

def compare_results(in_memory_results, db_results):
    """Compare and display benchmark results"""
    if not db_results:
        print("\nCouldn't compare results because database benchmark failed.")
        return
    
    print("\n=== Performance Comparison (Database vs In-Memory) ===")
    print(f"{'Role':<12} {'In-Memory':<15} {'Database':<15} {'Ratio (DB/Memory)':<20}")
    print("-" * 65)
    
    for role in ["admin", "supervisor", "staff"]:
        in_mem_time = in_memory_results[role]
        db_time = db_results[role]
        ratio = db_time / in_mem_time if in_mem_time > 0 else float('inf')
        
        print(f"{role.capitalize():<12} {format_time(in_mem_time):<15} {format_time(db_time):<15} {ratio:.2f}x")

def main():
    parser = argparse.ArgumentParser(description="Finance Permission System Benchmark")
    parser.add_argument("--db", type=str, default="finance_system.db", help="Database file path")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iterations for each benchmark")
    parser.add_argument("--memory-only", action="store_true", help="Run only in-memory benchmark")
    parser.add_argument("--db-only", action="store_true", help="Run only database benchmark")
    
    args = parser.parse_args()
    
    # Run selected benchmarks
    in_memory_results = None
    db_results = None
    
    if not args.db_only:
        in_memory_results = run_in_memory_benchmark(args.iterations)
    
    if not args.memory_only and os.path.exists(args.db):
        db_results = run_database_benchmark(args.db, args.iterations)
    
    # Compare results if both benchmarks were run
    if in_memory_results and db_results:
        compare_results(in_memory_results, db_results)

if __name__ == "__main__":
    main()