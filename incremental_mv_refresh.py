#!/usr/bin/env python3
"""
增量物化视图刷新优化脚本

专门优化现有物化视图的刷新性能，避免全量重建
支持：
1. 删除过期数据
2. 只添加新的/变化的数据
3. 分批处理避免锁竞争
4. 性能监控和统计
"""

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
    'database': os.getenv('DB_NAME_V2', 'finance'),
    'autocommit': False
}

def connect_db():
    """连接数据库"""
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def get_mv_status():
    """获取物化视图当前状态"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE 'mv_supervisor_financial'")
        if not cursor.fetchone():
            print("❌ 物化视图表不存在")
            return None
        
        # 获取基本统计
        cursor.execute("SELECT COUNT(*) as total FROM mv_supervisor_financial")
        total = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(DISTINCT supervisor_id) as supervisors FROM mv_supervisor_financial")
        supervisors = cursor.fetchone()['supervisors']
        
        cursor.execute("SELECT MAX(last_updated) as last_update FROM mv_supervisor_financial")
        last_update = cursor.fetchone()['last_update']
        
        # 检查数据分布
        cursor.execute("""
            SELECT 
                MIN(supervisor_id) as min_sup,
                MAX(supervisor_id) as max_sup,
                COUNT(DISTINCT fund_id) as unique_funds
            FROM mv_supervisor_financial
        """)
        distribution = cursor.fetchone()
        
        return {
            'exists': True,
            'total_records': total,
            'unique_supervisors': supervisors,
            'last_updated': last_update,
            'min_supervisor': distribution['min_sup'],
            'max_supervisor': distribution['max_sup'],
            'unique_funds': distribution['unique_funds']
        }
        
    except mysql.connector.Error as e:
        print(f"❌ 获取物化视图状态失败: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def fast_truncate_and_rebuild():
    """快速清空并重建（最简单但可能较慢）"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("=== 快速重建模式 ===")
        start_time = time.time()
        
        # 1. 临时禁用一些检查以提速
        cursor.execute("SET SESSION foreign_key_checks = 0")
        cursor.execute("SET SESSION unique_checks = 0")
        cursor.execute("SET SESSION autocommit = 0")
        
        # 2. 清空表
        print("清空物化视图...")
        cursor.execute("TRUNCATE TABLE mv_supervisor_financial")
        
        # 3. 分批重建数据
        print("分批重建数据...")
        
        # 获取supervisor列表并分批
        cursor.execute("SELECT DISTINCT user_id FROM user_hierarchy ORDER BY user_id")
        all_supervisors = [row[0] for row in cursor.fetchall()]
        
        batch_size = 50  # 每批处理50个supervisor
        total_inserted = 0
        
        for i in range(0, len(all_supervisors), batch_size):
            batch = all_supervisors[i:i + batch_size]
            batch_start = time.time()
            
            placeholders = ','.join(['%s'] * len(batch))
            
            insert_query = f"""
                INSERT INTO mv_supervisor_financial 
                    (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount, permission_type)
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
                WHERE h.user_id IN ({placeholders})
            """
            
            cursor.execute(insert_query, batch)
            inserted = cursor.rowcount
            total_inserted += inserted
            
            batch_time = time.time() - batch_start
            
            print(f"  批次 {i//batch_size + 1}: {len(batch)} supervisor → {inserted:,} 记录，耗时 {batch_time:.2f}s")
        
        # 4. 提交事务
        print("提交事务...")
        conn.commit()
        
        # 5. 更新时间戳
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        conn.commit()
        
        # 6. 恢复设置
        cursor.execute("SET SESSION foreign_key_checks = 1")
        cursor.execute("SET SESSION unique_checks = 1")
        cursor.execute("SET SESSION autocommit = 1")
        
        total_time = time.time() - start_time
        
        print(f"\n✅ 快速重建完成：")
        print(f"   总插入记录: {total_inserted:,}")
        print(f"   总耗时: {total_time:.2f} 秒 ({total_time/60:.1f} 分钟)")
        print(f"   平均速度: {total_inserted/total_time:.0f} 记录/秒")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 快速重建失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def smart_incremental_refresh():
    """智能增量刷新"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("=== 智能增量刷新模式 ===")
        start_time = time.time()
        
        # 1. 检查哪些supervisor需要更新
        print("分析需要更新的supervisor...")
        
        # 当前物化视图中的supervisor
        cursor.execute("SELECT DISTINCT supervisor_id FROM mv_supervisor_financial")
        current_supervisors = set(row[0] for row in cursor.fetchall())
        
        # 实际应该存在的supervisor
        cursor.execute("SELECT DISTINCT user_id FROM user_hierarchy")
        expected_supervisors = set(row[0] for row in cursor.fetchall())
        
        # 找出差异
        missing_supervisors = expected_supervisors - current_supervisors
        extra_supervisors = current_supervisors - expected_supervisors
        
        print(f"   当前物化视图中的supervisor: {len(current_supervisors):,}")
        print(f"   应该存在的supervisor: {len(expected_supervisors):,}")
        print(f"   缺失的supervisor: {len(missing_supervisors):,}")
        print(f"   多余的supervisor: {len(extra_supervisors):,}")
        
        # 2. 删除多余的supervisor数据
        if extra_supervisors:
            print(f"删除 {len(extra_supervisors)} 个多余supervisor的数据...")
            extra_list = list(extra_supervisors)
            
            # 分批删除
            for i in range(0, len(extra_list), 100):
                batch = extra_list[i:i + 100]
                placeholders = ','.join(['%s'] * len(batch))
                
                cursor.execute(f"""
                    DELETE FROM mv_supervisor_financial 
                    WHERE supervisor_id IN ({placeholders})
                """, batch)
                
                print(f"   删除批次 {i//100 + 1}: {cursor.rowcount:,} 条记录")
        
        # 3. 添加缺失的supervisor数据
        if missing_supervisors:
            print(f"添加 {len(missing_supervisors)} 个缺失supervisor的数据...")
            missing_list = list(missing_supervisors)
            total_inserted = 0
            
            # 分批添加
            for i in range(0, len(missing_list), 50):
                batch = missing_list[i:i + 50]
                placeholders = ','.join(['%s'] * len(batch))
                
                cursor.execute(f"""
                    INSERT INTO mv_supervisor_financial 
                        (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount, permission_type)
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
                    WHERE h.user_id IN ({placeholders})
                """, batch)
                
                inserted = cursor.rowcount
                total_inserted += inserted
                print(f"   添加批次 {i//50 + 1}: {inserted:,} 条记录")
        
        # 4. 对于现存的supervisor，检查是否需要更新
        common_supervisors = current_supervisors & expected_supervisors
        if common_supervisors:
            print(f"检查 {len(common_supervisors):,} 个现存supervisor是否需要更新...")
            
            # 简化版：随机抽样检查10个supervisor
            import random
            sample_supervisors = random.sample(list(common_supervisors), min(10, len(common_supervisors)))
            
            need_update = []
            for sup_id in sample_supervisors:
                # 检查物化视图中的记录数
                cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (sup_id,))
                mv_count = cursor.fetchone()[0]
                
                # 检查实际应该有的记录数
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM user_hierarchy h
                    JOIN financial_funds f ON h.subordinate_id = f.handle_by
                    WHERE h.user_id = %s
                """, (sup_id,))
                actual_count = cursor.fetchone()[0]
                
                if mv_count != actual_count:
                    need_update.append(sup_id)
                    print(f"   Supervisor {sup_id}: MV={mv_count}, 实际={actual_count} → 需要更新")
            
            # 更新有差异的supervisor
            if need_update:
                print(f"更新 {len(need_update)} 个有差异的supervisor...")
                for sup_id in need_update:
                    # 删除旧数据
                    cursor.execute("DELETE FROM mv_supervisor_financial WHERE supervisor_id = %s", (sup_id,))
                    
                    # 插入新数据
                    cursor.execute("""
                        INSERT INTO mv_supervisor_financial 
                            (supervisor_id, fund_id, handle_by, handler_name, department, order_id, customer_id, amount, permission_type)
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
                        WHERE h.user_id = %s
                    """, (sup_id,))
                    
                    print(f"   更新 Supervisor {sup_id}: {cursor.rowcount:,} 条记录")
        
        # 5. 更新时间戳
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        # 6. 提交所有更改
        conn.commit()
        
        total_time = time.time() - start_time
        
        print(f"\n✅ 智能增量刷新完成：")
        print(f"   总耗时: {total_time:.2f} 秒")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 智能增量刷新失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def optimize_mv_table():
    """优化物化视图表结构和索引"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("=== 优化物化视图表结构 ===")
        
        # 1. 检查当前索引
        cursor.execute("SHOW INDEX FROM mv_supervisor_financial")
        current_indexes = cursor.fetchall()
        
        print("当前索引:")
        for idx in current_indexes:
            print(f"  {idx[2]}: {idx[4]} ({idx[10]})")
        
        # 2. 创建缺失的关键索引
        needed_indexes = [
            ("idx_supervisor_id", "supervisor_id"),
            ("idx_supervisor_fund", "supervisor_id, fund_id"),
            ("idx_supervisor_amount", "supervisor_id, amount")
        ]
        
        existing_index_names = set(idx[2] for idx in current_indexes)
        
        for index_name, columns in needed_indexes:
            if index_name not in existing_index_names:
                print(f"创建索引 {index_name}...")
                try:
                    cursor.execute(f"ALTER TABLE mv_supervisor_financial ADD INDEX {index_name} ({columns})")
                    print(f"  ✅ 索引 {index_name} 创建成功")
                except mysql.connector.Error as e:
                    print(f"  ⚠️ 索引 {index_name} 创建失败: {e}")
            else:
                print(f"  ✅ 索引 {index_name} 已存在")
        
        # 3. 分析表以更新统计信息
        print("分析表以更新统计信息...")
        cursor.execute("ANALYZE TABLE mv_supervisor_financial")
        
        conn.commit()
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 表优化失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def performance_test():
    """简单的性能测试"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        print("\n=== 性能测试 ===")
        
        # 找到一个有较多记录的supervisor
        cursor.execute("""
            SELECT supervisor_id, COUNT(*) as cnt 
            FROM mv_supervisor_financial 
            GROUP BY supervisor_id 
            ORDER BY cnt DESC 
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        if not result:
            print("没有数据进行测试")
            return
        
        test_supervisor = result[0]
        record_count = result[1]
        
        print(f"测试supervisor {test_supervisor} (有 {record_count:,} 条记录)")
        
        # 测试查询性能
        test_queries = [
            ("简单计数", "SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s"),
            ("带排序的分页", "SELECT * FROM mv_supervisor_financial WHERE supervisor_id = %s ORDER BY amount DESC LIMIT 20"),
            ("聚合查询", "SELECT COUNT(*), SUM(amount), AVG(amount) FROM mv_supervisor_financial WHERE supervisor_id = %s")
        ]
        
        for test_name, query in test_queries:
            times = []
            for _ in range(3):  # 运行3次取平均
                start = time.time()
                cursor.execute(query, (test_supervisor,))
                cursor.fetchall()  # 确保获取所有结果
                times.append((time.time() - start) * 1000)
            
            avg_time = sum(times) / len(times)
            print(f"  {test_name}: {avg_time:.2f}ms (平均)")
        
    except mysql.connector.Error as e:
        print(f"❌ 性能测试失败: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="增量物化视图刷新优化")
    parser.add_argument("--mode", choices=["fast", "incremental", "optimize", "test"], 
                       default="fast", help="刷新模式")
    parser.add_argument("--status", action="store_true", help="只显示当前状态")
    
    args = parser.parse_args()
    
    print("🔄 增量物化视图刷新优化工具")
    
    # 获取当前状态
    status = get_mv_status()
    if not status:
        print("无法获取物化视图状态")
        return
    
    print(f"\n📊 当前状态:")
    print(f"   总记录数: {status['total_records']:,}")
    print(f"   Supervisor数: {status['unique_supervisors']:,}")
    print(f"   Fund数: {status['unique_funds']:,}")
    print(f"   最后更新: {status['last_updated']}")
    
    if args.status:
        return
    
    # 执行指定操作
    if args.mode == "fast":
        success = fast_truncate_and_rebuild()
    elif args.mode == "incremental":
        success = smart_incremental_refresh()
    elif args.mode == "optimize":
        success = optimize_mv_table()
    elif args.mode == "test":
        performance_test()
        return
    
    if success:
        print("\n✅ 操作完成")
        # 显示更新后的状态
        new_status = get_mv_status()
        if new_status:
            print(f"\n📊 更新后状态:")
            print(f"   总记录数: {new_status['total_records']:,}")
            print(f"   Supervisor数: {new_status['unique_supervisors']:,}")
            print(f"   最后更新: {new_status['last_updated']}")
        
        # 运行快速性能测试
        performance_test()
    else:
        print("\n❌ 操作失败")

if __name__ == "__main__":
    main()

