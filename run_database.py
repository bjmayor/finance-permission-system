import argparse
import os
import time
import sqlite3
from main import User, FinancialFund, Order, Customer
from database import DatabasePermissionService, DatabaseApiGateway

def check_database_size(db_path):
    """Check the size of the database file and tables"""
    if not os.path.exists(db_path):
        print(f"Database file {db_path} does not exist.")
        return False
    
    size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"Database file size: {size_mb:.2f} MB")
    
    # Count records in each table
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    tables = ["users", "orders", "customers", "financial_funds"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table '{table}' contains {count:,} records")
    
    conn.close()
    return True

def initialize_database(db_path, num_records):
    """Initialize the database with the specified number of records"""
    print(f"Initializing database with {num_records:,} records per table...")
    
    # Delete existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Deleted existing database: {db_path}")
    
    # Create and populate the database
    db_svc = DatabasePermissionService(db_path)
    start_time = time.time()
    db_svc.populate_test_data(num_records)
    end_time = time.time()
    
    print(f"Database initialization completed in {end_time - start_time:.2f} seconds")
    check_database_size(db_path)

def run_performance_test(db_path):
    """Run performance tests with the database"""
    print("\nRunning performance tests...")
    
    gateway = DatabaseApiGateway(db_path)
    
    # Test admin view
    print("\n=== 超管视角 ===")
    start_time = time.time()
    gateway.authenticate("admin")
    funds = gateway.get_funds()
    end_time = time.time()
    
    print(f"Retrieved {len(funds):,} funds for admin in {end_time - start_time:.4f} seconds")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"超管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
    
    # Test supervisor view
    print("\n=== 主管视角 ===")
    start_time = time.time()
    gateway.authenticate("supervisor")
    funds = gateway.get_funds()
    end_time = time.time()
    
    print(f"Retrieved {len(funds):,} funds for supervisor in {end_time - start_time:.4f} seconds")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"主管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
    
    # Test staff view
    print("\n=== 员工视角 ===")
    start_time = time.time()
    gateway.authenticate("staff")
    funds = gateway.get_funds()
    end_time = time.time()
    
    print(f"Retrieved {len(funds):,} funds for staff in {end_time - start_time:.4f} seconds")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"员工查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")

def main():
    parser = argparse.ArgumentParser(description="Finance Permission System Database Runner")
    parser.add_argument("--init", action="store_true", help="Initialize the database")
    parser.add_argument("--records", type=int, default=1000000, help="Number of records per table (default: 1,000,000)")
    parser.add_argument("--db", type=str, default="finance_system.db", help="Database file path")
    
    args = parser.parse_args()
    
    if args.init:
        initialize_database(args.db, args.records)
    else:
        if check_database_size(args.db):
            run_performance_test(args.db)
        else:
            print(f"Database not found. Run with --init to create it first.")

if __name__ == "__main__":
    main()