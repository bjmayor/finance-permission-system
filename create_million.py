#!/usr/bin/env python3
import os
import time
import random
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

def create_database():
    """创建数据库"""
    try:
        # 连接MySQL（不指定数据库）
        base_config = {k: v for k, v in config.items() if k != 'database'}
        conn = mysql.connector.connect(**base_config)
        cursor = conn.cursor()
        
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
        print(f"数据库 {config['database']} 创建成功或已存在")
        
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"创建数据库时出错: {e}")
        return False

def setup_tables():
    """创建表结构"""
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 禁用外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 删除现有表（如果存在）
        cursor.execute("DROP TABLE IF EXISTS user_hierarchy")
        cursor.execute("DROP TABLE IF EXISTS financial_funds")
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS users")
        
        # 创建用户表
        print("创建用户表...")
        cursor.execute('''
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            department VARCHAR(100) NOT NULL,
            parent_id INT,
            INDEX idx_users_role (role),
            INDEX idx_users_parent_id (parent_id)
        ) ENGINE=InnoDB
        ''')
        
        # 创建订单表
        print("创建订单表...")
        cursor.execute('''
        CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            user_id INT NOT NULL,
            INDEX idx_orders_user_id (user_id)
        ) ENGINE=InnoDB
        ''')
        
        # 创建客户表
        print("创建客户表...")
        cursor.execute('''
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            admin_user_id INT NOT NULL,
            INDEX idx_customers_admin_user_id (admin_user_id)
        ) ENGINE=InnoDB
        ''')
        
        # 创建财务资金表
        print("创建财务资金表...")
        cursor.execute('''
        CREATE TABLE financial_funds (
            fund_id INT PRIMARY KEY,
            handle_by INT NOT NULL,
            order_id INT NOT NULL,
            customer_id INT NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            INDEX idx_funds_handle_by (handle_by),
            INDEX idx_funds_order_id (order_id),
            INDEX idx_funds_customer_id (customer_id)
        ) ENGINE=InnoDB
        ''')
        
        # 创建用户层级关系表
        print("创建用户层级关系表...")
        cursor.execute('''
        CREATE TABLE user_hierarchy (
            user_id INT NOT NULL,
            subordinate_id INT NOT NULL,
            depth INT NOT NULL,
            PRIMARY KEY (user_id, subordinate_id),
            INDEX idx_hierarchy_user_id (user_id),
            INDEX idx_hierarchy_subordinate_id (subordinate_id)
        ) ENGINE=InnoDB
        ''')
        
        # 恢复外键检查
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        conn.close()
        print("表结构创建完成")
        return True
    except mysql.connector.Error as e:
        print(f"创建表时出错: {e}")
        return False

def insert_base_data():
    """插入基础数据"""
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 插入基础用户数据
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
        
        # 插入基础订单数据
        base_orders = [
            (2001, 3),
            (2002, 2),
            (2003, 3)
        ]
        
        cursor.executemany(
            "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
            base_orders
        )
        
        # 插入基础客户数据
        base_customers = [
            (3001, 3),
            (3002, 2),
            (3003, 3)
        ]
        
        cursor.executemany(
            "INSERT INTO customers (customer_id, admin_user_id) VALUES (%s, %s)",
            base_customers
        )
        
        # 插入基础财务资金数据
        base_funds = [
            (1001, 3, 2001, 3001, 50000),
            (1002, 2, 2002, 3002, 80000),
            (1003, 3, 2003, 3003, 60000)
        ]
        
        cursor.executemany(
            "INSERT INTO financial_funds (fund_id, handle_by, order_id, customer_id, amount) VALUES (%s, %s, %s, %s, %s)",
            base_funds
        )
        
        # 插入基础用户层级关系
        base_hierarchy = [
            (1, 1, 0),  # 超管是自己的下属（深度0）
            (1, 2, 1),  # 超管 -> 主管（深度1）
            (1, 3, 2),  # 超管 -> 主管 -> 员工（深度2）
            (1, 4, 1),  # 超管 -> 员工（深度1）
            (2, 2, 0),  # 主管是自己的下属（深度0）
            (2, 3, 1),  # 主管 -> 员工（深度1）
            (3, 3, 0),  # 员工是自己的下属（深度0）
            (4, 4, 0)   # 员工是自己的下属（深度0）
        ]
        
        cursor.executemany(
            "INSERT INTO user_hierarchy (user_id, subordinate_id, depth) VALUES (%s, %s, %s)",
            base_hierarchy
        )
        
        conn.commit()
        conn.close()
        print("基础数据插入完成")
        return True
    except mysql.connector.Error as e:
        print(f"插入基础数据时出错: {e}")
        return False

def insert_bulk_data(num_records=1000000):
    """插入大量数据"""
    print(f"开始插入百万级数据，每个表 {num_records:,} 条记录...")
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 优化批量插入设置
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        cursor.execute("SET autocommit = 0")
        
        # 配置MySQL会话
        # 注意：innodb_flush_log_at_trx_commit是全局变量，需要管理员权限
        try:
            cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 0")
        except mysql.connector.Error as e:
            print(f"警告: 无法设置全局变量 innodb_flush_log_at_trx_commit: {e}")
            print("继续执行，但性能可能受到影响")
        cursor.execute("SET SESSION sql_log_bin = 0")
        
        # 用户数据生成
        print("生成用户数据...")
        roles = ["staff"] * 80 + ["supervisor"] * 15 + ["admin"] * 5  # 分布：80% 员工，15% 主管，5% 管理员
        departments = ["华东区", "华南区", "华北区", "西南区", "东北区", "西北区"]
        
        start_user_id = 5  # 起始ID为5（1-4已经使用）
        batch_size = 10000
        progress_step = max(1, num_records // 20)  # 5%进度显示间隔
        
        for i in range(0, num_records, batch_size):
            batch_size = min(batch_size, num_records - i)
            user_batch = []
            
            for j in range(batch_size):
                user_id = start_user_id + i + j
                name = f"用户{user_id}"
                role = random.choice(roles)
                department = random.choice(departments)
                # 下级用户的parent_id可能指向已有用户
                if role != "admin":
                    parent_id = random.randint(1, min(user_id - 1, 10000))  # 限制parent_id范围，避免树过深
                else:
                    parent_id = None
                
                user_batch.append((user_id, name, role, department, parent_id))
            
            cursor.executemany(
                "INSERT INTO users (id, name, role, department, parent_id) VALUES (%s, %s, %s, %s, %s)",
                user_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"已插入 {i + batch_size:,}/{num_records:,} 个用户 ({((i + batch_size) / num_records * 100):.1f}%)")
        
        max_user_id = start_user_id + num_records - 1
        
        # 订单数据生成
        print("\n生成订单数据...")
        start_order_id = 2004  # 起始ID为2004（2001-2003已经使用）
        
        for i in range(0, num_records, batch_size):
            batch_size = min(batch_size, num_records - i)
            order_batch = []
            
            for j in range(batch_size):
                order_id = start_order_id + i + j
                user_id = random.randint(1, max_user_id)
                
                order_batch.append((order_id, user_id))
            
            cursor.executemany(
                "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
                order_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"已插入 {i + batch_size:,}/{num_records:,} 个订单 ({((i + batch_size) / num_records * 100):.1f}%)")
        
        max_order_id = start_order_id + num_records - 1
        
        # 客户数据生成
        print("\n生成客户数据...")
        start_customer_id = 3004  # 起始ID为3004（3001-3003已经使用）
        
        for i in range(0, num_records, batch_size):
            batch_size = min(batch_size, num_records - i)
            customer_batch = []
            
            for j in range(batch_size):
                customer_id = start_customer_id + i + j
                admin_user_id = random.randint(1, max_user_id)
                
                customer_batch.append((customer_id, admin_user_id))
            
            cursor.executemany(
                "INSERT INTO customers (customer_id, admin_user_id) VALUES (%s, %s)",
                customer_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"已插入 {i + batch_size:,}/{num_records:,} 个客户 ({((i + batch_size) / num_records * 100):.1f}%)")
        
        max_customer_id = start_customer_id + num_records - 1
        
        # 财务资金数据生成
        print("\n生成财务资金数据...")
        start_fund_id = 1004  # 起始ID为1004（1001-1003已经使用）
        
        for i in range(0, num_records, batch_size):
            batch_size = min(batch_size, num_records - i)
            fund_batch = []
            
            for j in range(batch_size):
                fund_id = start_fund_id + i + j
                handle_by = random.randint(1, max_user_id)
                order_id = random.randint(2001, max_order_id)
                customer_id = random.randint(3001, max_customer_id)
                amount = round(random.uniform(1000, 1000000), 2)
                
                fund_batch.append((fund_id, handle_by, order_id, customer_id, amount))
            
            cursor.executemany(
                "INSERT INTO financial_funds (fund_id, handle_by, order_id, customer_id, amount) VALUES (%s, %s, %s, %s, %s)",
                fund_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"已插入 {i + batch_size:,}/{num_records:,} 条财务记录 ({((i + batch_size) / num_records * 100):.1f}%)")
        
        # 为部分主管和员工生成层级关系（只针对前10000个用户，避免数据量过大）
        print("\n生成用户层级关系数据...")
        max_user_for_hierarchy = min(10000, max_user_id)
        
        # 为每个主管找出其直接下属，先检查是否存在重复记录
        cursor.execute("""
        INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT DISTINCT u1.id, u2.id, 1
        FROM users u1
        JOIN users u2 ON u2.parent_id = u1.id
        WHERE u1.id <= %s AND u2.id <= %s AND u1.id != u2.id
        """, (max_user_for_hierarchy, max_user_for_hierarchy))
        
        # 每个用户是自己的下属（深度0），先检查是否存在重复记录
        cursor.execute("""
        INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT id, id, 0
        FROM users
        WHERE id <= %s AND id NOT IN (SELECT user_id FROM user_hierarchy WHERE depth = 0)
        """, (max_user_for_hierarchy,))
        
        conn.commit()
        
        # 恢复正常设置
        cursor.execute("SET foreign_key_checks = 1")
        cursor.execute("SET unique_checks = 1")
        cursor.execute("SET autocommit = 1")
        try:
            cursor.execute("SET GLOBAL innodb_flush_log_at_trx_commit = 1")
        except mysql.connector.Error as e:
            print(f"警告: 无法恢复全局变量 innodb_flush_log_at_trx_commit: {e}")
        cursor.execute("SET SESSION sql_log_bin = 1")
        
        # 分析表优化性能
        print("\n分析表以优化性能...")
        tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
        for table in tables:
            print(f"分析表 {table}...")
            cursor.execute(f"ANALYZE TABLE {table}")
            cursor.fetchall()  # 消耗结果
        
        # 输出表统计信息
        print("\n数据库统计信息:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"表 '{table}' 包含 {count:,} 条记录")
        
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"插入大量数据时出错: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="创建百万级数据的财务权限系统")
    parser.add_argument("--records", type=int, default=1000000, help="每个表要插入的记录数量 (默认: 1,000,000)")
    parser.add_argument("--skip-setup", action="store_true", help="跳过数据库和表的创建")
    args = parser.parse_args()
    
    start_time = time.time()
    
    if not args.skip_setup:
        if create_database() and setup_tables() and insert_base_data():
            print("数据库初始化完成")
        else:
            print("数据库初始化失败，终止操作")
            exit(1)
    
    if insert_bulk_data(args.records):
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n百万级数据生成完成！总耗时: {total_time:.2f} 秒 ({total_time/60:.2f} 分钟)")
    else:
        print("百万级数据生成失败")