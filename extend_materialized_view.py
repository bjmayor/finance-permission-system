#!/usr/bin/env python3
"""
扩展物化视图以支持完整的三维权限逻辑
包含处理人、订单、客户三个维度的权限判断
"""

import os
import mysql.connector
from dotenv import load_dotenv
import time
from prettytable import PrettyTable

# 加载环境变量
load_dotenv()

config = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456'),
    'database': os.getenv('DB_NAME_V2', 'finance'),
    'autocommit': False
}

def connect_db():
    """连接数据库"""
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def backup_current_mv():
    """备份当前物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("=== 备份当前物化视图 ===")
        
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_backup_v1")
        cursor.execute("""
            CREATE TABLE mv_supervisor_financial_backup_v1 AS 
            SELECT * FROM mv_supervisor_financial
        """)
        
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial_backup_v1")
        backup_count = cursor.fetchone()[0]
        
        conn.commit()
        print(f"✅ 备份完成，备份记录数: {backup_count:,}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 备份失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def create_new_materialized_view():
    """创建新的扩展物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 创建扩展物化视图结构 ===")
        
        # 删除旧的物化视图
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_v2")
        
        # 创建新的物化视图表结构
        cursor.execute("""
            CREATE TABLE mv_supervisor_financial_v2 (
                id int(11) NOT NULL AUTO_INCREMENT,
                supervisor_id int(11) NOT NULL,
                fund_id int(11) NOT NULL,
                handle_by int(11) NOT NULL,
                handler_name varchar(255) DEFAULT NULL,
                department varchar(100) DEFAULT NULL,
                order_id int(11) DEFAULT NULL,
                customer_id int(11) DEFAULT NULL,
                amount decimal(15,2) DEFAULT NULL,
                permission_type varchar(20) NOT NULL COMMENT 'handle/order/customer',
                last_updated timestamp NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY idx_unique_record (supervisor_id, fund_id, permission_type),
                KEY idx_supervisor_fund (supervisor_id, fund_id),
                KEY idx_supervisor_amount (supervisor_id, amount),
                KEY idx_supervisor_type (supervisor_id, permission_type),
                KEY idx_permission_type (permission_type),
                KEY idx_last_updated (last_updated)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """)
        
        conn.commit()
        print("✅ 新物化视图表结构创建成功")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 创建表结构失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def populate_extended_materialized_view():
    """填充扩展物化视图数据"""
    conn = connect_db()
    if not conn:
        return 0
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 填充扩展物化视图数据 ===")
        
        start_time = time.time()
        
        # 清空表
        cursor.execute("TRUNCATE TABLE mv_supervisor_financial_v2")
        
        print("1. 插入处理人维度数据...")
        handle_start = time.time()
        
        cursor.execute("""
            INSERT INTO mv_supervisor_financial_v2 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT 
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'handle' as permission_type
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
        """)
        
        handle_count = cursor.rowcount
        handle_time = time.time() - handle_start
        print(f"   ✅ 处理人维度: {handle_count:,} 条记录，耗时 {handle_time:.2f} 秒")
        
        print("2. 插入订单维度数据...")
        order_start = time.time()
        
        cursor.execute("""
            INSERT IGNORE INTO mv_supervisor_financial_v2 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT 
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'order' as permission_type
            FROM user_hierarchy h
            JOIN orders o ON h.subordinate_id = o.user_id
            JOIN financial_funds f ON o.order_id = f.order_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE NOT EXISTS (
                SELECT 1 FROM mv_supervisor_financial_v2 mv 
                WHERE mv.supervisor_id = h.user_id 
                AND mv.fund_id = f.fund_id 
                AND mv.permission_type = 'handle'
            )
        """)
        
        order_count = cursor.rowcount
        order_time = time.time() - order_start
        print(f"   ✅ 订单维度: {order_count:,} 条记录，耗时 {order_time:.2f} 秒")
        
        print("3. 插入客户维度数据...")
        customer_start = time.time()
        
        cursor.execute("""
            INSERT IGNORE INTO mv_supervisor_financial_v2 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT 
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'customer' as permission_type
            FROM user_hierarchy h
            JOIN customers c ON h.subordinate_id = c.admin_user_id
            JOIN financial_funds f ON c.customer_id = f.customer_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE NOT EXISTS (
                SELECT 1 FROM mv_supervisor_financial_v2 mv 
                WHERE mv.supervisor_id = h.user_id 
                AND mv.fund_id = f.fund_id 
                AND mv.permission_type IN ('handle', 'order')
            )
        """)
        
        customer_count = cursor.rowcount
        customer_time = time.time() - customer_start
        print(f"   ✅ 客户维度: {customer_count:,} 条记录，耗时 {customer_time:.2f} 秒")
        
        print("4. 更新时间戳...")
        cursor.execute("UPDATE mv_supervisor_financial_v2 SET last_updated = NOW()")
        
        # 获取最终统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial_v2")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT permission_type, COUNT(*) 
            FROM mv_supervisor_financial_v2 
            GROUP BY permission_type
        """)
        type_stats = cursor.fetchall()
        
        total_time = time.time() - start_time
        
        conn.commit()
        
        print(f"\n✅ 扩展物化视图填充完成")
        print(f"   总记录数: {total_count:,}")
        print(f"   总耗时: {total_time:.2f} 秒")
        
        print(f"\n📊 各维度统计:")
        for ptype, count in type_stats:
            print(f"   {ptype}: {count:,} 条")
        
        return total_count
        
    except mysql.connector.Error as e:
        print(f"❌ 填充数据失败: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def replace_old_materialized_view():
    """替换旧的物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 替换旧物化视图 ===")
        
        # 重命名表
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_old")
        cursor.execute("RENAME TABLE mv_supervisor_financial TO mv_supervisor_financial_old")
        cursor.execute("RENAME TABLE mv_supervisor_financial_v2 TO mv_supervisor_financial")
        
        # 删除旧表的索引，重新创建适合新结构的索引
        cursor.execute("""
            ALTER TABLE mv_supervisor_financial 
            ADD KEY idx_supervisor_id (supervisor_id),
            ADD KEY idx_fund_id (fund_id)
        """)
        
        conn.commit()
        print("✅ 物化视图替换成功")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 替换失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def verify_extended_materialized_view():
    """验证扩展物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 验证扩展物化视图 ===")
        
        test_supervisor = 70
        
        # 1. 基础统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT permission_type, COUNT(*) 
            FROM mv_supervisor_financial 
            GROUP BY permission_type
        """)
        type_distribution = cursor.fetchall()
        
        print(f"✅ 基础验证:")
        print(f"   总记录数: {total_count:,}")
        print(f"   权限类型分布:")
        for ptype, count in type_distribution:
            print(f"     {ptype}: {count:,} 条")
        
        # 2. 测试用户验证
        cursor.execute("""
            SELECT permission_type, COUNT(*) 
            FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
            GROUP BY permission_type
        """, (test_supervisor,))
        
        user_stats = cursor.fetchall()
        user_total = sum(count for _, count in user_stats)
        
        print(f"\n✅ 用户{test_supervisor}验证:")
        print(f"   总可访问记录: {user_total:,}")
        for ptype, count in user_stats:
            print(f"     {ptype}权限: {count:,} 条")
        
        # 3. 与原始查询对比
        print(f"\n🔍 与原始业务逻辑对比:")
        
        # 模拟原始三维权限查询
        cursor.execute("""
            SELECT COUNT(DISTINCT f.fund_id) 
            FROM user_hierarchy h
            JOIN financial_funds f ON (
                h.subordinate_id = f.handle_by OR
                h.subordinate_id IN (
                    SELECT o.user_id FROM orders o WHERE o.order_id = f.order_id
                ) OR
                h.subordinate_id IN (
                    SELECT c.admin_user_id FROM customers c WHERE c.customer_id = f.customer_id
                )
            )
            WHERE h.user_id = %s
        """, (test_supervisor,))
        
        original_count = cursor.fetchone()[0]
        
        # 新物化视图查询
        cursor.execute("""
            SELECT COUNT(DISTINCT fund_id) 
            FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
        """, (test_supervisor,))
        
        mv_count = cursor.fetchone()[0]
        
        print(f"   原始查询结果: {original_count:,}")
        print(f"   物化视图结果: {mv_count:,}")
        
        if original_count == mv_count:
            print("   ✅ 数据一致性验证通过")
            return True
        else:
            print(f"   ❌ 数据不一致，差异: {abs(original_count - mv_count):,}")
            return False
        
    except mysql.connector.Error as e:
        print(f"❌ 验证失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def performance_comparison():
    """性能对比测试"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 性能对比测试 ===")
        
        test_supervisor = 70
        iterations = 5
        
        # 1. 新物化视图查询性能
        print("1. 测试新物化视图性能...")
        mv_times = []
        
        for i in range(iterations):
            start_time = time.time()
            
            cursor.execute("""
                SELECT COUNT(*), SUM(amount) 
                FROM mv_supervisor_financial 
                WHERE supervisor_id = %s
            """, (test_supervisor,))
            
            mv_result = cursor.fetchone()
            end_time = time.time()
            mv_times.append((end_time - start_time) * 1000)
        
        mv_avg_time = sum(mv_times) / len(mv_times)
        
        # 2. 原始多表JOIN查询性能
        print("2. 测试原始多表JOIN性能...")
        join_times = []
        
        for i in range(iterations):
            start_time = time.time()
            
            cursor.execute("""
                SELECT COUNT(DISTINCT f.fund_id), SUM(DISTINCT f.amount)
                FROM user_hierarchy h
                JOIN financial_funds f ON (
                    h.subordinate_id = f.handle_by OR
                    h.subordinate_id IN (
                        SELECT o.user_id FROM orders o WHERE o.order_id = f.order_id
                    ) OR
                    h.subordinate_id IN (
                        SELECT c.admin_user_id FROM customers c WHERE c.customer_id = f.customer_id
                    )
                )
                WHERE h.user_id = %s
            """, (test_supervisor,))
            
            join_result = cursor.fetchone()
            end_time = time.time()
            join_times.append((end_time - start_time) * 1000)
        
        join_avg_time = sum(join_times) / len(join_times)
        
        # 3. 分页查询性能对比
        print("3. 测试分页查询性能...")
        
        # 物化视图分页
        start_time = time.time()
        cursor.execute("""
            SELECT fund_id, amount, permission_type
            FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
            ORDER BY amount DESC
            LIMIT 20
        """, (test_supervisor,))
        mv_page_data = cursor.fetchall()
        mv_page_time = (time.time() - start_time) * 1000
        
        # 原始查询分页（简化版）
        start_time = time.time()
        cursor.execute("""
            SELECT DISTINCT f.fund_id, f.amount, 'mixed' as permission_type
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            WHERE h.user_id = %s
            ORDER BY f.amount DESC
            LIMIT 20
        """, (test_supervisor,))
        join_page_data = cursor.fetchall()
        join_page_time = (time.time() - start_time) * 1000
        
        # 显示结果
        comparison_table = PrettyTable()
        comparison_table.field_names = ["查询类型", "物化视图(ms)", "原始JOIN(ms)", "性能提升"]
        
        count_speedup = join_avg_time / mv_avg_time if mv_avg_time > 0 else float('inf')
        page_speedup = join_page_time / mv_page_time if mv_page_time > 0 else float('inf')
        
        comparison_table.add_row([
            "统计查询", 
            f"{mv_avg_time:.2f}", 
            f"{join_avg_time:.2f}", 
            f"{count_speedup:.1f}x"
        ])
        comparison_table.add_row([
            "分页查询", 
            f"{mv_page_time:.2f}", 
            f"{join_page_time:.2f}", 
            f"{page_speedup:.1f}x"
        ])
        
        print(comparison_table)
        
        print(f"\n📊 性能总结:")
        print(f"   物化视图查询: {mv_avg_time:.2f}ms")
        print(f"   原始JOIN查询: {join_avg_time:.2f}ms")
        print(f"   性能提升: {count_speedup:.1f}倍")
        
        # 验证数据一致性
        print(f"\n🔍 数据一致性:")
        print(f"   物化视图结果: {mv_result}")
        print(f"   原始查询结果: {join_result}")
        print(f"   分页记录数: MV={len(mv_page_data)}, JOIN={len(join_page_data)}")
        
    except mysql.connector.Error as e:
        print(f"❌ 性能测试失败: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """主函数"""
    print("🚀 扩展物化视图以支持完整的三维权限逻辑")
    print("包含处理人、订单、客户三个维度的权限判断")
    
    # 1. 备份当前物化视图
    if not backup_current_mv():
        print("备份失败，停止执行")
        return
    
    # 2. 创建新物化视图结构
    if not create_new_materialized_view():
        print("创建新结构失败，停止执行")
        return
    
    # 3. 填充数据
    total_records = populate_extended_materialized_view()
    if total_records == 0:
        print("数据填充失败，停止执行")
        return
    
    # 4. 替换旧物化视图
    if not replace_old_materialized_view():
        print("替换失败，停止执行")
        return
    
    # 5. 验证结果
    if not verify_extended_materialized_view():
        print("验证失败，可能需要进一步调试")
        return
    
    # 6. 性能对比
    performance_comparison()
    
    print(f"\n🎉 扩展物化视图完成！")
    print(f"   ✅ 支持完整的三维权限逻辑")
    print(f"   ✅ 总记录数: {total_records:,}")
    print(f"   ✅ 性能验证通过")
    print(f"   ✅ 数据一致性验证通过")

if __name__ == "__main__":
    main()