import argparse
import os
import time
import sys
import mysql.connector
from mysql_database import MySQLPermissionService, MySQLApiGateway, get_database_stats
from main import User, FinancialFund, Order, Customer
from dotenv import load_dotenv

def check_database_exists():
    """Check if the MySQL database exists and has the required tables"""
    # Load environment variables
    load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456')
    }
    
    db_name = os.getenv('DB_NAME_V2', 'finance')
    
    try:
        # Connect to MySQL server (without database)
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return False
            
        # Connect to the database to check if tables exist
        conn.close()
        conn = mysql.connector.connect(**config, database=db_name)
        cursor = conn.cursor()
        
        # Check for required tables
        required_tables = ['users', 'orders', 'customers', 'financial_funds']
        for table in required_tables:
            cursor.execute(f"SHOW TABLES LIKE '{table}'")
            if not cursor.fetchone():
                conn.close()
                return False
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return False

def initialize_database(num_records):
    """Initialize the database with the specified number of records"""
    print(f"Initializing database with {num_records:,} records per table...")
    
    # Load environment variables
    load_dotenv()
    
    # First connect without database to create it if needed
    base_config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456')
    }
    
    db_name = os.getenv('DB_NAME_V2', 'finance')
    
    # Create the database if it doesn't exist
    try:
        conn = mysql.connector.connect(**base_config)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        return
    
    # Now connect with the database
    config = {**base_config, 'database': db_name}
    
    # Create database and populate with test data
    try:
        db_svc = MySQLPermissionService(config)
        start_time = time.time()
        db_svc.populate_test_data(num_records)
        end_time = time.time()
        
        print(f"Database initialization completed in {end_time - start_time:.2f} seconds")
        get_database_stats(config)
    except Exception as e:
        print(f"Error initializing database: {e}")

def run_performance_test():
    """Run performance tests with the database"""
    print("\nRunning performance tests...")
    
    # Load environment variables
    load_dotenv()
    
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456'),
        'database': os.getenv('DB_NAME_V2', 'finance')
    }
    
    # 直接执行查询测试
    print("\n=== 直接执行SQL查询测试 ===")
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 测试查询用户
        print("\n--- 查询所有用户 ---")
        start_time = time.time()
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
        end_time = time.time()
        print(f"查询到 {len(users)} 个用户，耗时 {end_time - start_time:.4f} 秒")
        print(f"显示前5个用户:")
        for user in users[:5]:
            print(f"用户ID: {user[0]}, 姓名: {user[1]}, 角色: {user[2]}, 部门: {user[3]}")
        
        # 测试查询财务资金
        print("\n--- 查询所有财务资金 ---")
        start_time = time.time()
        cursor.execute("SELECT * FROM financial_funds")
        funds = cursor.fetchall()
        end_time = time.time()
        print(f"查询到 {len(funds)} 条财务记录，耗时 {end_time - start_time:.4f} 秒")
        print(f"显示前5条财务记录:")
        for fund in funds[:5]:
            print(f"资金ID: {fund[0]}, 处理人: {fund[1]}, 订单: {fund[2]}, 客户: {fund[3]}, 金额: {fund[4]}")
        
        # 测试复杂查询 - 获取主管可见的财务资金
        print("\n--- 主管可见的财务资金 ---")
        supervisor_id = 2  # 财务主管ID
        start_time = time.time()
        cursor.execute("""
        WITH RECURSIVE subordinates AS (
            SELECT id FROM users WHERE id = %s
            UNION ALL
            SELECT u.id FROM users u JOIN subordinates s ON u.parent_id = s.id
        )
        SELECT f.* 
        FROM financial_funds f
        WHERE f.handle_by IN (SELECT id FROM subordinates)
        OR f.order_id IN (SELECT o.order_id FROM orders o WHERE o.user_id IN (SELECT id FROM subordinates))
        OR f.customer_id IN (SELECT c.customer_id FROM customers c WHERE c.admin_user_id IN (SELECT id FROM subordinates))
        """, (supervisor_id,))
        supervisor_funds = cursor.fetchall()
        end_time = time.time()
        print(f"主管可见 {len(supervisor_funds)} 条财务记录，耗时 {end_time - start_time:.4f} 秒")
        print("显示前5条记录:")
        for fund in supervisor_funds[:5]:
            print(f"资金ID: {fund[0]}, 处理人: {fund[1]}, 订单: {fund[2]}, 客户: {fund[3]}, 金额: {fund[4]}")
        
        conn.close()
    except mysql.connector.Error as e:
        print(f"MySQL查询出错: {e}")
    
    # 使用API网关测试
    print("\n=== 使用API网关测试 ===")
    try:
        gateway = MySQLApiGateway(config)
        
        # Test admin view
        print("\n=== 超管视角 ===")
        start_time = time.time()
        gateway.authenticate("admin")
        funds = gateway.get_funds()
        end_time = time.time()
        
        print(f"超管可见 {len(funds)} 条财务记录，耗时 {end_time - start_time:.4f} 秒")
        
        # Display first 5 funds only
        for fund in funds[:5]:
            print(f"超管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
        
        # Test supervisor view
        print("\n=== 主管视角 ===")
        start_time = time.time()
        gateway.authenticate("supervisor")
        funds = gateway.get_funds()
        end_time = time.time()
        
        print(f"主管可见 {len(funds)} 条财务记录，耗时 {end_time - start_time:.4f} 秒")
        
        # Display first 5 funds only
        for fund in funds[:5]:
            print(f"主管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
        
        # Test staff view
        print("\n=== 员工视角 ===")
        start_time = time.time()
        gateway.authenticate("staff")
        funds = gateway.get_funds()
        end_time = time.time()
        
        print(f"员工可见 {len(funds)} 条财务记录，耗时 {end_time - start_time:.4f} 秒")
        
        # Display first 5 funds only
        for fund in funds[:5]:
            print(f"员工查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
    except Exception as e:
        print(f"API网关测试出错: {e}")

def main():
    parser = argparse.ArgumentParser(description="Finance Permission System MySQL Runner")
    parser.add_argument("--init", action="store_true", help="Initialize the database")
    parser.add_argument("--force", action="store_true", help="Force database recreation even if it exists")
    parser.add_argument("--records", type=int, default=1000000, help="Number of records per table (default: 1,000,000)")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--test", action="store_true", help="Run test queries")
    
    args = parser.parse_args()
    
    if args.init:
        if args.force or not check_database_exists():
            initialize_database(args.records)
        else:
            print("Database already exists. Use --force to recreate it.")
            response = input("Do you want to continue and recreate the database? (y/n): ").strip().lower()
            if response == 'y' or response == 'yes':
                initialize_database(args.records)
            else:
                print("Database initialization cancelled.")
    elif args.stats:
        get_database_stats()
    elif args.test or not args.init:
        if check_database_exists():
            run_performance_test()
        else:
            print(f"Database not found. Run with --init to create it first.")

if __name__ == "__main__":
    main()