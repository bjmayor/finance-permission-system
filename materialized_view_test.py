#!/usr/bin/env python3
import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv
from prettytable import PrettyTable

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

def connect_db():
    """连接数据库"""
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def create_materialized_view():
    """创建主管财务列表的物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # 1. 创建物化视图表
        print("创建物化视图表...")
        
        # 先删除已存在的表(如果有)
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial")
        
        # 创建物化视图表结构
        cursor.execute("""
        CREATE TABLE mv_supervisor_financial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            supervisor_id INT NOT NULL,
            fund_id INT NOT NULL,
            handle_by INT NOT NULL,
            handler_name VARCHAR(255),
            department VARCHAR(100),
            order_id INT,
            customer_id INT,
            amount DECIMAL(15, 2),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_supervisor_fund (supervisor_id, fund_id),
            INDEX idx_supervisor_amount (supervisor_id, amount),
            INDEX idx_supervisor_id (supervisor_id),
            INDEX idx_last_updated (last_updated)
        ) ENGINE=InnoDB
        """)
        
        conn.commit()
        print("物化视图表创建完成")
        
        # 2. 手动执行第一次数据填充
        print("开始填充物化视图数据...")
        
        # 插入直接处理的资金记录
        insert_query = """
        INSERT INTO mv_supervisor_financial 
            (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount)
        SELECT 
            h.user_id AS supervisor_id,
            f.fund_id,
            f.handle_by,
            u.name AS handler_name,
            u.department,
            f.order_id,
            f.customer_id,
            f.amount
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        JOIN users u ON f.handle_by = u.id
        WHERE h.depth >= 0
        """
        cursor.execute(insert_query)
        conn.commit()
        print(f"已插入直接处理的资金记录，影响行数: {cursor.rowcount}")
        
        # 查询当前视图中的记录数
        cursor.execute("SELECT COUNT(*) as total FROM mv_supervisor_financial")
        count_result = cursor.fetchone()
        print(f"物化视图已填充，当前记录数: {count_result[0] if count_result else 0}")
        
        # 创建一个简单的刷新脚本（而不是存储过程）
        print("创建刷新脚本示例...")
        refresh_script = """
#!/bin/bash
# refresh_materialized_view.sh
# 此脚本用于定期刷新物化视图

MYSQL_CMD="mysql -h127.0.0.1 -P3306 -uroot -p123456 finance"

# 清空并重新填充物化视图
$MYSQL_CMD << EOF
TRUNCATE TABLE mv_supervisor_financial;

INSERT INTO mv_supervisor_financial 
    (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount)
SELECT 
    h.user_id AS supervisor_id,
    f.fund_id,
    f.handle_by,
    u.name AS handler_name,
    u.department,
    f.order_id,
    f.customer_id,
    f.amount
FROM user_hierarchy h
JOIN financial_funds f ON h.subordinate_id = f.handle_by
JOIN users u ON f.handle_by = u.id
WHERE h.depth >= 0;

UPDATE mv_supervisor_financial SET last_updated = NOW();

SELECT COUNT(*) FROM mv_supervisor_financial;
EOF

echo "物化视图刷新完成，时间: $(date)"
"""
        with open("refresh_materialized_view.sh", "w") as f:
            f.write(refresh_script)
        
        print("刷新脚本已创建，您可以使用crontab定期执行")
        print("示例crontab配置: 0 * * * * /path/to/refresh_materialized_view.sh")
        
        print("物化视图设置完成")
        return True
    
    except mysql.connector.Error as e:
        print(f"创建物化视图时出错: {e}")
        conn.rollback()
        return False
    
    finally:
        cursor.close()
        conn.close()

def test_query_performance(supervisor_id, page=1, page_size=10, sort_by="fund_id", sort_order="ASC", iterations=3):
    """测试原始查询和物化视图查询的性能差异"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    
    # 处理排序
    valid_sort_fields = ["fund_id", "amount", "handle_by", "order_id", "customer_id"]
    valid_sort_orders = ["ASC", "DESC"]
    
    if sort_by not in valid_sort_fields:
        sort_by = "fund_id"
    
    if sort_order not in valid_sort_orders:
        sort_order = "ASC"
    
    print(f"\n=== 测试主管(ID={supervisor_id})的查询性能 ===")
    print(f"页码: {page}, 每页记录数: {page_size}")
    print(f"排序: {sort_by} {sort_order}")
    print(f"重复次数: {iterations}")
    
    # 测试原始递归CTE查询
    original_times = []
    original_count = 0
    
    print("\n1. 测试原始递归CTE查询...")
    
    for i in range(iterations):
        print(f"\n迭代 {i+1}:")
        
        # 获取总记录数
        start_time = time.time()
        
        count_query = """
        WITH RECURSIVE subordinates AS (
            SELECT id FROM users WHERE id = %s
            UNION ALL
            SELECT u.id FROM users u JOIN subordinates s ON u.parent_id = s.id
        )
        SELECT COUNT(*) as total 
        FROM financial_funds f
        WHERE f.handle_by IN (SELECT id FROM subordinates)
        OR f.order_id IN (SELECT o.order_id FROM orders o WHERE o.user_id IN (SELECT id FROM subordinates))
        OR f.customer_id IN (SELECT c.customer_id FROM customers c WHERE c.admin_user_id IN (SELECT id FROM subordinates))
        """
        
        cursor.execute(count_query, (supervisor_id,))
        result = cursor.fetchone()
        original_count = result['total']
        
        count_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 获取分页数据
        start_time = time.time()
        offset = (page - 1) * page_size
        
        data_query = f"""
        WITH RECURSIVE subordinates AS (
            SELECT id FROM users WHERE id = %s
            UNION ALL
            SELECT u.id FROM users u JOIN subordinates s ON u.parent_id = s.id
        )
        SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
               u.name as handler_name, u.department
        FROM financial_funds f
        JOIN users u ON f.handle_by = u.id
        WHERE f.handle_by IN (SELECT id FROM subordinates)
        OR f.order_id IN (SELECT o.order_id FROM orders o WHERE o.user_id IN (SELECT id FROM subordinates))
        OR f.customer_id IN (SELECT c.customer_id FROM customers c WHERE c.admin_user_id IN (SELECT id FROM subordinates))
        ORDER BY f.{sort_by} {sort_order}
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(data_query, (supervisor_id, page_size, offset))
        data = cursor.fetchall()
        
        data_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        total_time = count_time + data_time
        original_times.append(total_time)
        
        print(f"获取总数用时: {count_time:.2f}ms")
        print(f"获取数据用时: {data_time:.2f}ms")
        print(f"总执行时间: {total_time:.2f}ms")
        print(f"总记录数: {original_count}")
        print(f"返回记录数: {len(data)}")
    
    # 测试物化视图查询
    mv_times = []
    mv_count = 0
    
    print("\n2. 测试物化视图查询...")
    
    for i in range(iterations):
        print(f"\n迭代 {i+1}:")
        
        # 获取总记录数
        start_time = time.time()
        
        count_query = """
        SELECT COUNT(*) as total 
        FROM mv_supervisor_financial
        WHERE supervisor_id = %s
        """
        
        cursor.execute(count_query, (supervisor_id,))
        result = cursor.fetchone()
        mv_count = result['total']
        
        count_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 获取分页数据
        start_time = time.time()
        offset = (page - 1) * page_size
        
        data_query = f"""
        SELECT fund_id, handle_by, order_id, customer_id, amount,
               handler_name, department
        FROM mv_supervisor_financial
        WHERE supervisor_id = %s
        ORDER BY {sort_by} {sort_order}
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(data_query, (supervisor_id, page_size, offset))
        data = cursor.fetchall()
        
        data_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        total_time = count_time + data_time
        mv_times.append(total_time)
        
        print(f"获取总数用时: {count_time:.2f}ms")
        print(f"获取数据用时: {data_time:.2f}ms")
        print(f"总执行时间: {total_time:.2f}ms")
        print(f"总记录数: {mv_count}")
        print(f"返回记录数: {len(data)}")
    
    # 计算平均时间
    avg_original_time = sum(original_times) / len(original_times)
    avg_mv_time = sum(mv_times) / len(mv_times)
    
    # 显示对比结果
    print("\n=== 查询性能对比 ===")
    table = PrettyTable(["查询类型", "平均执行时间(ms)", "总记录数", "性能提升"])
    table.add_row(["原始递归CTE", f"{avg_original_time:.2f}", original_count, "-"])
    table.add_row(["物化视图", f"{avg_mv_time:.2f}", mv_count, f"{(avg_original_time / avg_mv_time):.2f}x"])
    
    print(table)
    
    # 计算性能提升百分比
    improvement = ((avg_original_time - avg_mv_time) / avg_original_time) * 100
    print(f"\n物化视图提供了 {improvement:.2f}% 的性能提升")
    
    # 检查结果一致性
    consistency = "一致" if original_count == mv_count else "不一致"
    print(f"查询结果: {consistency} (原始查询: {original_count}, 物化视图: {mv_count})")
    
    cursor.close()
    conn.close()

def manual_refresh_mv():
    """手动刷新物化视图"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        print("手动刷新物化视图...")
        start_time = time.time()
        
        # 记录旧记录数
        cursor.execute("SELECT COUNT(*) as total FROM mv_supervisor_financial")
        count_result = cursor.fetchone()
        old_count = count_result[0] if count_result else 0
        
        # 使用直接SQL语句而不是存储过程
        print("清空物化视图...")
        cursor.execute("TRUNCATE TABLE mv_supervisor_financial")
        
        print("重新填充物化视图...")
        cursor.execute("""
        INSERT INTO mv_supervisor_financial 
            (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount)
        SELECT 
            h.user_id AS supervisor_id,
            f.fund_id,
            f.handle_by,
            u.name AS handler_name,
            u.department,
            f.order_id,
            f.customer_id,
            f.amount
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        JOIN users u ON f.handle_by = u.id
        WHERE h.depth >= 0
        """)
        
        # 更新时间戳
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        end_time = time.time()
        
        # 查询当前视图中的记录数
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        count_result = cursor.fetchone()
        new_count = count_result[0] if count_result else 0
        
        print(f"物化视图刷新完成，总记录数: {new_count}，耗时: {end_time - start_time:.2f} 秒")
        
        conn.commit()
    except mysql.connector.Error as e:
        print(f"刷新物化视图时出错: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_mv_stats():
    """获取物化视图统计信息"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        print("\n=== 物化视图统计信息 ===")
        
        # 获取物化视图总记录数
        cursor.execute("SELECT COUNT(*) as total FROM mv_supervisor_financial")
        total = cursor.fetchone()['total']
        print(f"总记录数: {total:,}")
        
        # 获取物化视图大小
        cursor.execute("""
        SELECT 
            table_name, 
            ROUND((data_length + index_length) / (1024 * 1024), 2) as size_mb
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = 'mv_supervisor_financial'
        """)
        size_info = cursor.fetchone()
        if size_info:
            print(f"表大小: {size_info['size_mb']} MB")
        
        # 获取每个主管的记录数
        cursor.execute("""
        SELECT 
            supervisor_id,
            COUNT(*) as record_count
        FROM mv_supervisor_financial
        GROUP BY supervisor_id
        ORDER BY record_count DESC
        LIMIT 10
        """)
        supervisor_stats = cursor.fetchall()
        
        print("\n主管记录数前10:")
        stats_table = PrettyTable(["主管ID", "记录数"])
        for stat in supervisor_stats:
            stats_table.add_row([stat['supervisor_id'], stat['record_count']])
        print(stats_table)
        
        # 获取最后更新时间
        cursor.execute("SELECT MAX(last_updated) as last_update FROM mv_supervisor_financial")
        last_update = cursor.fetchone()['last_update']
        print(f"\n最后更新时间: {last_update}")
        
        # 获取索引信息
        cursor.execute("""
        SELECT 
            INDEX_NAME as index_name,
            COLUMN_NAME as column_name,
            SEQ_IN_INDEX as seq_in_index
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
        AND table_name = 'mv_supervisor_financial'
        ORDER BY index_name, seq_in_index
        """)
        indices = cursor.fetchall()
        
        print("\n索引信息:")
        index_table = PrettyTable(["索引名", "列名", "序号"])
        for idx in indices:
            index_table.add_row([idx['index_name'], idx['column_name'], idx['seq_in_index']])
        print(index_table)
        
    except mysql.connector.Error as e:
        print(f"获取物化视图统计信息时出错: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="物化视图测试工具")
    parser.add_argument("--create", action="store_true", help="创建物化视图")
    parser.add_argument("--refresh", action="store_true", help="手动刷新物化视图")
    parser.add_argument("--stats", action="store_true", help="显示物化视图统计信息")
    parser.add_argument("--test", action="store_true", help="测试查询性能")
    parser.add_argument("--supervisor_id", type=int, default=2, help="测试的主管ID")
    parser.add_argument("--page", type=int, default=1, help="页码")
    parser.add_argument("--page_size", type=int, default=10, help="每页记录数")
    parser.add_argument("--sort_by", type=str, default="fund_id", help="排序字段")
    parser.add_argument("--sort_order", type=str, default="ASC", choices=["ASC", "DESC"], help="排序方向")
    parser.add_argument("--iterations", type=int, default=3, help="测试迭代次数")
    
    args = parser.parse_args()
    
    if args.create:
        create_materialized_view()
    
    if args.refresh:
        manual_refresh_mv()
    
    if args.stats:
        get_mv_stats()
    
    if args.test:
        test_query_performance(
            args.supervisor_id,
            args.page,
            args.page_size,
            args.sort_by,
            args.sort_order,
            args.iterations
        )
    
    # 如果没有指定任何操作，显示帮助
    if not (args.create or args.refresh or args.stats or args.test):
        parser.print_help()

if __name__ == "__main__":
    main()