#!/usr/bin/env python3
import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库连接配置
config = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456'),
    'database': os.getenv('DB_NAME_V2', 'finance')
}

def run_million_record_test():
    """测试百万级数据性能"""
    print("开始百万级数据性能测试...")
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 测试用户表数量
        start_time = time.time()
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        end_time = time.time()
        print(f"用户表共有 {user_count:,} 条记录，查询耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 测试财务资金表数量
        start_time = time.time()
        cursor.execute("SELECT COUNT(*) FROM financial_funds")
        fund_count = cursor.fetchone()[0]
        end_time = time.time()
        print(f"财务资金表共有 {fund_count:,} 条记录，查询耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 测试复杂查询 - 主管查询
        print("\n测试主管查询性能 (带递归查询):")
        supervisor_id = 2  # 财务主管ID
        
        start_time = time.time()
        cursor.execute("""
        WITH RECURSIVE subordinates AS (
            SELECT id FROM users WHERE id = %s
            UNION ALL
            SELECT u.id FROM users u JOIN subordinates s ON u.parent_id = s.id
        )
        SELECT COUNT(*) FROM financial_funds f
        WHERE f.handle_by IN (SELECT id FROM subordinates)
        OR f.order_id IN (SELECT o.order_id FROM orders o WHERE o.user_id IN (SELECT id FROM subordinates))
        OR f.customer_id IN (SELECT c.customer_id FROM customers c WHERE c.admin_user_id IN (SELECT id FROM subordinates))
        """, (supervisor_id,))
        
        count = cursor.fetchone()[0]
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"主管可见 {count:,} 条财务记录，查询耗时: {execution_time*1000:.2f} ms")
        
        # 测试单表简单查询
        print("\n测试简单查询性能:")
        start_time = time.time()
        cursor.execute("SELECT * FROM users WHERE role = 'supervisor' LIMIT 100")
        supervisors = cursor.fetchall()
        end_time = time.time()
        print(f"查询100名主管用户，耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 测试JOIN查询性能
        print("\n测试JOIN查询性能:")
        start_time = time.time()
        cursor.execute("""
        SELECT u.id, u.name, COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.role = 'staff'
        GROUP BY u.id, u.name
        LIMIT 100
        """)
        staff_orders = cursor.fetchall()
        end_time = time.time()
        print(f"查询员工订单统计，耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 测试索引查询性能
        print("\n测试索引查询性能:")
        start_time = time.time()
        cursor.execute("""
        SELECT * FROM financial_funds 
        WHERE handle_by = %s
        LIMIT 100
        """, (3,))  # 查询ID为3的员工处理的资金
        funds_by_staff = cursor.fetchall()
        end_time = time.time()
        print(f"通过索引字段查询财务资金，耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 测试范围查询性能
        print("\n测试范围查询性能:")
        start_time = time.time()
        cursor.execute("""
        SELECT * FROM financial_funds 
        WHERE amount BETWEEN 500000 AND 1000000
        LIMIT 100
        """)
        large_funds = cursor.fetchall()
        end_time = time.time()
        print(f"查询大额资金，耗时: {(end_time - start_time)*1000:.2f} ms")
        
        # 关闭连接
        conn.close()
        
    except mysql.connector.Error as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="百万级数据性能测试")
    parser.add_argument("--verbose", action="store_true", help="显示详细测试结果")
    
    args = parser.parse_args()
    
    run_million_record_test()