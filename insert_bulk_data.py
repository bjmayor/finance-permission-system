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

def insert_bulk_data(num_records=100000):
    """向数据库中插入大量测试数据"""
    print(f"开始插入{num_records:,}条记录到每个表...")
    start_time = time.time()
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # 设置批量插入优化参数
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        cursor.execute("SET autocommit = 0")
        
        # 保留原有数据，只添加新数据
        
        # 用户数据生成
        print("生成用户数据...")
        roles = ["staff"] * 80 + ["supervisor"] * 15 + ["admin"] * 5  # 分布：80% 员工，15% 主管，5% 管理员
        departments = ["华东区", "华南区", "华北区", "西南区", "东北区", "西北区"]
        
        # 确定起始ID (找到当前最大ID)
        cursor.execute("SELECT MAX(id) FROM users")
        max_user_id = cursor.fetchone()[0] or 0
        start_user_id = max_user_id + 1
        
        # 批量插入用户
        batch_size = 10000
        progress_step = max(1, num_records // 10)  # 10%进度显示间隔
        
        for i in range(0, num_records, batch_size):
            batch_size = min(batch_size, num_records - i)
            user_batch = []
            
            for j in range(batch_size):
                user_id = start_user_id + i + j
                name = f"用户{user_id}"
                role = random.choice(roles)
                department = random.choice(departments)
                # 下级用户的parent_id可能指向已有用户或本批次前面的用户
                if role != "admin" and user_id > 4:
                    if random.random() < 0.7:  # 70%的概率指向1-4号用户
                        parent_id = random.randint(1, 4)
                    else:  # 30%的概率指向其他用户
                        parent_id = random.randint(5, max(5, user_id - 1))
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
        
        # 更新最大用户ID
        max_user_id = start_user_id + num_records - 1
        
        # 订单数据生成
        print("\n生成订单数据...")
        cursor.execute("SELECT MAX(order_id) FROM orders")
        max_order_id = cursor.fetchone()[0] or 2000
        start_order_id = max_order_id + 1
        
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
        
        # 更新最大订单ID
        max_order_id = start_order_id + num_records - 1
        
        # 客户数据生成
        print("\n生成客户数据...")
        cursor.execute("SELECT MAX(customer_id) FROM customers")
        max_customer_id = cursor.fetchone()[0] or 3000
        start_customer_id = max_customer_id + 1
        
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
        
        # 更新最大客户ID
        max_customer_id = start_customer_id + num_records - 1
        
        # 财务资金数据生成
        print("\n生成财务资金数据...")
        cursor.execute("SELECT MAX(fund_id) FROM financial_funds")
        max_fund_id = cursor.fetchone()[0] or 1000
        start_fund_id = max_fund_id + 1
        
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
        
        # 为一部分主管和管理员生成用户层级关系
        # 这里只生成一部分，避免数据量过大
        print("\n生成用户层级关系数据...")
        
        # 找出所有主管和管理员
        cursor.execute("SELECT id FROM users WHERE role IN ('supervisor', 'admin') LIMIT 100")
        supervisors = [row[0] for row in cursor.fetchall()]
        
        for supervisor_id in supervisors:
            # 每个用户是自己的下属(深度0)
            cursor.execute(
                "INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth) VALUES (%s, %s, %s)",
                (supervisor_id, supervisor_id, 0)
            )
            
            # 找出直接下属(深度1)
            cursor.execute("SELECT id FROM users WHERE parent_id = %s LIMIT 1000", (supervisor_id,))
            direct_subordinates = [row[0] for row in cursor.fetchall()]
            
            for sub_id in direct_subordinates:
                cursor.execute(
                    "INSERT IGNORE INTO user_hierarchy (user_id, subordinate_id, depth) VALUES (%s, %s, %s)",
                    (supervisor_id, sub_id, 1)
                )
        
        conn.commit()
        print("用户层级关系数据生成完成")
        
        # 恢复正常设置
        cursor.execute("SET foreign_key_checks = 1")
        cursor.execute("SET unique_checks = 1")
        cursor.execute("SET autocommit = 1")
        
        # 提交所有更改
        conn.commit()
        
        # 显示统计信息
        tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
        print("\n数据库统计信息:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"表 '{table}' 现在包含 {count:,} 条记录")
        
        # 关闭连接
        conn.close()
        
        end_time = time.time()
        print(f"\n数据生成完成，总耗时: {end_time - start_time:.2f} 秒")
        
    except mysql.connector.Error as e:
        print(f"错误: {e}")

def analyze_tables():
    """分析表以优化性能"""
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
        
        print("\n开始分析表以优化性能...")
        for table in tables:
            print(f"分析表 {table}...")
            cursor.execute(f"ANALYZE TABLE {table}")
            cursor.fetchall()  # 消耗结果
        
        print("表分析完成")
        conn.close()
    except mysql.connector.Error as e:
        print(f"分析表时出错: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="向财务权限系统数据库中插入大量测试数据")
    parser.add_argument("--records", type=int, default=100000, help="要插入的记录数量 (默认: 100,000)")
    parser.add_argument("--analyze", action="store_true", help="在插入数据后分析表以优化性能")
    
    args = parser.parse_args()
    
    insert_bulk_data(args.records)
    
    if args.analyze:
        analyze_tables()