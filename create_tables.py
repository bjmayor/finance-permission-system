#!/usr/bin/env python3
import os
import mysql.connector
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库连接配置
config = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456')
}

db_name = os.getenv('DB_NAME_V2', 'finance')

def create_database():
    """创建数据库"""
    print(f"创建数据库 {db_name}...")
    
    try:
        # 连接到MySQL服务器
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"数据库 {db_name} 创建成功或已存在")
        
        # 关闭连接
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"创建数据库时出错: {e}")
        return False

def create_tables():
    """创建表结构"""
    # 连接到指定数据库
    db_config = {**config, 'database': db_name}
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 禁用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 创建用户表
        print("创建用户表...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            department VARCHAR(100) NOT NULL,
            parent_id INT,
            INDEX idx_users_role (role),
            INDEX idx_users_parent_id (parent_id)
        )
        ''')
        
        # 创建订单表
        print("创建订单表...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            user_id INT NOT NULL,
            INDEX idx_orders_user_id (user_id)
        )
        ''')
        
        # 创建客户表
        print("创建客户表...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            admin_user_id INT NOT NULL,
            INDEX idx_customers_admin_user_id (admin_user_id)
        )
        ''')
        
        # 创建财务资金表
        print("创建财务资金表...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_funds (
            fund_id INT PRIMARY KEY,
            handle_by INT NOT NULL,
            order_id INT NOT NULL,
            customer_id INT NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            INDEX idx_funds_handle_by (handle_by),
            INDEX idx_funds_order_id (order_id),
            INDEX idx_funds_customer_id (customer_id)
        )
        ''')
        
        # 创建用户层级关系表
        print("创建用户层级关系表...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_hierarchy (
            user_id INT NOT NULL,
            subordinate_id INT NOT NULL,
            depth INT NOT NULL,
            PRIMARY KEY (user_id, subordinate_id),
            INDEX idx_hierarchy_user_id (user_id),
            INDEX idx_hierarchy_subordinate_id (subordinate_id)
        )
        ''')
        
        # 重新启用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        print("所有表创建成功")
        
        # 显示所有表
        cursor.execute("SHOW TABLES")
        print("\n数据库中的表:")
        for table in cursor:
            print(f"- {table[0]}")
        
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"创建表时出错: {e}")
        return False

def insert_test_data():
    """插入测试数据"""
    db_config = {**config, 'database': db_name}
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # 禁用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 插入基础用户数据
        print("插入测试用户数据...")
        base_users = [
            (1, "超级管理员", "admin", "总部", None),
            (2, "财务主管", "supervisor", "华东区", 1),
            (3, "财务专员", "staff", "华东区", 2),
            (4, "财务专员", "staff", "华南区", 1)
        ]
        
        cursor.executemany(
            "INSERT INTO users (id, name, role, department, parent_id) VALUES (%s, %s, %s, %s, %s)",
            base_users
        )
        
        # 插入测试订单数据
        print("插入测试订单数据...")
        test_orders = [
            (2001, 3),
            (2002, 2),
            (2003, 3)
        ]
        
        cursor.executemany(
            "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
            test_orders
        )
        
        # 插入测试客户数据
        print("插入测试客户数据...")
        test_customers = [
            (3001, 3),
            (3002, 2),
            (3003, 3)
        ]
        
        cursor.executemany(
            "INSERT INTO customers (customer_id, admin_user_id) VALUES (%s, %s)",
            test_customers
        )
        
        # 插入测试财务资金数据
        print("插入测试财务资金数据...")
        test_funds = [
            (1001, 3, 2001, 3001, 50000),
            (1002, 2, 2002, 3002, 80000),
            (1003, 3, 2003, 3003, 60000)
        ]
        
        cursor.executemany(
            "INSERT INTO financial_funds (fund_id, handle_by, order_id, customer_id, amount) VALUES (%s, %s, %s, %s, %s)",
            test_funds
        )
        
        # 插入用户层级关系
        print("插入用户层级关系数据...")
        
        # 每个用户都是自己的下属 (深度为0)
        cursor.execute("""
        INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT id, id, 0 FROM users
        """)
        
        # 直接下属关系 (深度为1)
        cursor.execute("""
        INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT u1.id, u2.id, 1
        FROM users u1
        JOIN users u2 ON u2.parent_id = u1.id
        """)
        
        # 重新启用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        print("测试数据插入成功")
        
        # 检查数据
        tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"表 '{table}' 包含 {count} 条记录")
        
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"插入测试数据时出错: {e}")
        return False

if __name__ == "__main__":
    print("开始创建财务权限系统数据库...")
    
    if create_database():
        if create_tables():
            insert_test_data()
    
    print("完成！")