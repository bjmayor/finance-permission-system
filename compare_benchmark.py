#!/usr/bin/env python3
import time
import os
import argparse
from dotenv import load_dotenv
from main import User, FinancialFund, Order, Customer, PermissionService, FinancialService, ApiGateway
from database import DatabaseApiGateway
from mysql_database import MySQLApiGateway

def format_time(seconds):
    """Format time in appropriate units"""
    if seconds < 0.001:
        return f"{seconds * 1000000:.2f} μs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.4f} sec"

def run_in_memory_benchmark(iterations=3):
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

def run_sqlite_benchmark(db_path, iterations=3):
    """Run benchmark with SQLite implementation"""
    if not os.path.exists(db_path):
        print(f"SQLite database file {db_path} does not exist.")
        return None
    
    print(f"\n=== SQLite Implementation Benchmark ({db_path}) ===")
    
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

def run_mysql_benchmark(iterations=3):
    """Run benchmark with MySQL implementation"""
    # Load environment variables
    load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456'),
        'database': os.getenv('DB_NAME_V2', 'finance')
    }
    
    print("\n=== MySQL Implementation Benchmark ===")
    
    try:
        # Initialize the gateway
        gateway = MySQLApiGateway(config)
        
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
    
    except Exception as e:
        print(f"Error running MySQL benchmark: {e}")
        return None

def compare_results(in_memory_results, sqlite_results, mysql_results):
    """Compare and display benchmark results"""
    print("\n=== Performance Comparison ===")
    print(f"{'Role':<12} {'In-Memory':<15} {'SQLite':<15} {'MySQL':<15} {'SQLite/Memory':<15} {'MySQL/Memory':<15} {'MySQL/SQLite':<15}")
    print("-" * 100)
    
    for role in ["admin", "supervisor", "staff"]:
        in_mem_time = in_memory_results.get(role, float('nan')) if in_memory_results else float('nan')
        sqlite_time = sqlite_results.get(role, float('nan')) if sqlite_results else float('nan')
        mysql_time = mysql_results.get(role, float('nan')) if mysql_results else float('nan')
        
        sqlite_ratio = sqlite_time / in_mem_time if in_mem_time > 0 and sqlite_time > 0 else float('nan')
        mysql_ratio = mysql_time / in_mem_time if in_mem_time > 0 and mysql_time > 0 else float('nan')
        mysql_sqlite_ratio = mysql_time / sqlite_time if sqlite_time > 0 and mysql_time > 0 else float('nan')
        
        print(f"{role.capitalize():<12} {format_time(in_mem_time):<15} {format_time(sqlite_time):<15} {format_time(mysql_time):<15} {sqlite_ratio:.2f}x{' '*10} {mysql_ratio:.2f}x{' '*10} {mysql_sqlite_ratio:.2f}x{' '*10}")

def main():
    parser = argparse.ArgumentParser(description="Finance Permission System Benchmark Comparison")
    parser.add_argument("--sqlite", type=str, default="finance_system.db", help="SQLite database file path")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations for each benchmark")
    parser.add_argument("--memory-only", action="store_true", help="Run only in-memory benchmark")
    parser.add_argument("--sqlite-only", action="store_true", help="Run only SQLite benchmark")
    parser.add_argument("--mysql-only", action="store_true", help="Run only MySQL benchmark")
    
    args = parser.parse_args()
    
    # Run selected benchmarks
    in_memory_results = None
    sqlite_results = None
    mysql_results = None
    
    if not args.sqlite_only and not args.mysql_only:
        in_memory_results = run_in_memory_benchmark(args.iterations)
    
    if not args.memory_only and not args.mysql_only and os.path.exists(args.sqlite):
        sqlite_results = run_sqlite_benchmark(args.sqlite, args.iterations)
    
    if not args.memory_only and not args.sqlite_only:
        mysql_results = run_mysql_benchmark(args.iterations)
    
    # Compare results if multiple benchmarks were run
    if sum(1 for x in [in_memory_results, sqlite_results, mysql_results] if x is not None) > 1:
        compare_results(in_memory_results, sqlite_results, mysql_results)

if __name__ == "__main__":
    main()