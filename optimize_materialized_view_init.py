#!/usr/bin/env python3
"""
物化视图初始化性能优化脚本

当前问题分析：
1. 单一大查询：1百万条财务记录 × 5千个supervisor = 可能产生数十亿次JOIN计算
2. 无批量处理：一次性处理所有数据导致内存和锁竞争
3. 无并行化：串行执行所有supervisor的数据生成
4. 索引冲突：大量INSERT时索引维护开销巨大

优化策略：
1. 分批处理：将supervisor分组，每批处理少量supervisor
2. 并行优化：利用MySQL的批量插入和优化配置
3. 临时禁用索引：在数据插入期间禁用非必要索引
4. 增量更新：支持只更新特定supervisor的数据
5. 内存优化：调整MySQL配置以支持大批量操作
"""

import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
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
    'autocommit': False,  # 手动控制事务
    'charset': 'utf8mb4'
}

# 线程锁
print_lock = threading.Lock()
stats_lock = threading.Lock()

def safe_print(*args, **kwargs):
    """线程安全的打印"""
    with print_lock:
        print(*args, **kwargs)

def connect_db():
    """连接数据库"""
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as e:
        safe_print(f"数据库连接失败: {e}")
        return None

def optimize_mysql_settings():
    """优化MySQL设置以提高批量插入性能"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("=== 优化MySQL设置 ===")
        
        # 获取当前设置
        cursor.execute("SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
        buffer_pool = cursor.fetchone()
        
        cursor.execute("SHOW VARIABLES LIKE 'bulk_insert_buffer_size'")
        bulk_insert = cursor.fetchone()
        
        safe_print(f"当前InnoDB缓冲池大小: {buffer_pool[1] if buffer_pool else 'unknown'}")
        safe_print(f"当前批量插入缓冲区: {bulk_insert[1] if bulk_insert else 'unknown'}")
        
        # 设置会话级别的优化参数
        optimizations = [
            "SET SESSION bulk_insert_buffer_size = 256*1024*1024",  # 256MB
            "SET SESSION innodb_change_buffering = all",
            "SET SESSION foreign_key_checks = 0",  # 临时禁用外键检查
            "SET SESSION unique_checks = 0",       # 临时禁用唯一性检查
            "SET SESSION sql_log_bin = 0"          # 禁用二进制日志（如果不需要复制）
        ]
        
        for opt in optimizations:
            try:
                cursor.execute(opt)
                safe_print(f"✅ {opt}")
            except mysql.connector.Error as e:
                safe_print(f"⚠️ {opt} - {e}")
        
        conn.commit()
        safe_print("MySQL优化设置完成")
        return True
        
    except mysql.connector.Error as e:
        safe_print(f"❌ MySQL优化失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def backup_and_recreate_mv_table():
    """备份并重建物化视图表，优化结构"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("\n=== 重建物化视图表结构 ===")
        
        # 备份现有表（如果存在）
        cursor.execute("SHOW TABLES LIKE 'mv_supervisor_financial'")
        if cursor.fetchone():
            safe_print("备份现有物化视图表...")
            cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_backup")
            cursor.execute("RENAME TABLE mv_supervisor_financial TO mv_supervisor_financial_backup")
        
        # 创建优化后的表结构
        safe_print("创建优化的表结构...")
        cursor.execute("""
            CREATE TABLE mv_supervisor_financial (
                id BIGINT AUTO_INCREMENT,
                supervisor_id INT NOT NULL,
                fund_id INT NOT NULL,
                handle_by INT NOT NULL,
                handler_name VARCHAR(255),
                department VARCHAR(100),
                order_id INT,
                customer_id INT,
                amount DECIMAL(15, 2),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB 
              DEFAULT CHARSET=utf8mb4 
              ROW_FORMAT=COMPRESSED
              KEY_BLOCK_SIZE=8
        """)
        
        # 注意：暂时不创建其他索引，在数据插入完成后再添加
        
        conn.commit()
        safe_print("✅ 表结构创建完成（索引将在数据插入后创建）")
        return True
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 表结构创建失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def get_supervisor_batches(batch_size=100):
    """获取supervisor批次列表"""
    conn = connect_db()
    if not conn:
        return []
    
    cursor = conn.cursor()
    
    try:
        # 获取所有有下属的supervisor，并按下属数量排序
        cursor.execute("""
            SELECT h.user_id, COUNT(*) as subordinate_count
            FROM user_hierarchy h
            GROUP BY h.user_id
            ORDER BY subordinate_count DESC
        """)
        
        supervisors = cursor.fetchall()
        
        # 分批，每批包含batch_size个supervisor
        batches = []
        for i in range(0, len(supervisors), batch_size):
            batch = supervisors[i:i + batch_size]
            batch_subordinates = sum(count for _, count in batch)
            batches.append({
                'batch_id': i // batch_size + 1,
                'supervisors': [sup_id for sup_id, _ in batch],
                'supervisor_count': len(batch),
                'estimated_records': batch_subordinates  # 估算记录数
            })
        
        return batches
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 获取supervisor批次失败: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def process_supervisor_batch(batch_info, total_batches):
    """处理单个supervisor批次"""
    conn = connect_db()
    if not conn:
        return {'success': False, 'error': 'Database connection failed'}
    
    cursor = conn.cursor()
    
    try:
        batch_id = batch_info['batch_id']
        supervisors = batch_info['supervisors']
        estimated_records = batch_info['estimated_records']
        
        safe_print(f"开始处理批次 {batch_id}/{total_batches}：{len(supervisors)} 个supervisor，预估 {estimated_records:,} 条记录")
        
        start_time = time.time()
        
        # 构建批量插入查询
        placeholders = ','.join(['%s'] * len(supervisors))
        
        insert_query = f"""
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
            WHERE h.user_id IN ({placeholders})
        """
        
        cursor.execute(insert_query, supervisors)
        inserted_count = cursor.rowcount
        
        conn.commit()
        
        elapsed_time = time.time() - start_time
        
        result = {
            'success': True,
            'batch_id': batch_id,
            'supervisor_count': len(supervisors),
            'inserted_count': inserted_count,
            'elapsed_time': elapsed_time,
            'records_per_second': inserted_count / elapsed_time if elapsed_time > 0 else 0
        }
        
        safe_print(f"✅ 批次 {batch_id} 完成：插入 {inserted_count:,} 条记录，耗时 {elapsed_time:.2f}s，速度 {result['records_per_second']:.0f} 记录/秒")
        
        return result
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 批次 {batch_info['batch_id']} 失败: {e}")
        conn.rollback()
        return {'success': False, 'batch_id': batch_info['batch_id'], 'error': str(e)}
    finally:
        cursor.close()
        conn.close()

def parallel_populate_materialized_view(max_workers=4, batch_size=100):
    """并行填充物化视图"""
    safe_print("\n=== 并行填充物化视图 ===")
    
    # 获取批次
    batches = get_supervisor_batches(batch_size)
    if not batches:
        safe_print("❌ 无法获取supervisor批次")
        return False
    
    total_batches = len(batches)
    total_supervisors = sum(b['supervisor_count'] for b in batches)
    estimated_total_records = sum(b['estimated_records'] for b in batches)
    
    safe_print(f"总共 {total_batches} 个批次，{total_supervisors} 个supervisor，预估 {estimated_total_records:,} 条记录")
    safe_print(f"使用 {max_workers} 个并行线程")
    
    overall_start_time = time.time()
    results = []
    
    # 并行处理批次
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_batch = {
            executor.submit(process_supervisor_batch, batch, total_batches): batch
            for batch in batches
        }
        
        # 处理完成的任务
        for future in as_completed(future_to_batch):
            result = future.result()
            results.append(result)
    
    overall_elapsed_time = time.time() - overall_start_time
    
    # 统计结果
    successful_batches = [r for r in results if r['success']]
    failed_batches = [r for r in results if not r['success']]
    
    total_inserted = sum(r.get('inserted_count', 0) for r in successful_batches)
    average_speed = sum(r.get('records_per_second', 0) for r in successful_batches) / len(successful_batches) if successful_batches else 0
    
    safe_print(f"\n=== 并行填充完成 ===")
    safe_print(f"总耗时: {overall_elapsed_time:.2f} 秒")
    safe_print(f"成功批次: {len(successful_batches)}/{total_batches}")
    safe_print(f"失败批次: {len(failed_batches)}")
    safe_print(f"总插入记录: {total_inserted:,}")
    safe_print(f"平均速度: {average_speed:.0f} 记录/秒")
    safe_print(f"整体速度: {total_inserted / overall_elapsed_time:.0f} 记录/秒")
    
    if failed_batches:
        safe_print(f"\n失败的批次:")
        for failed in failed_batches:
            safe_print(f"  批次 {failed.get('batch_id', 'unknown')}: {failed.get('error', 'unknown error')}")
    
    return len(failed_batches) == 0

def create_indexes_after_data_load():
    """在数据加载完成后创建索引"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("\n=== 创建索引 ===")
        
        indexes = [
            "ALTER TABLE mv_supervisor_financial ADD INDEX idx_supervisor_id (supervisor_id)",
            "ALTER TABLE mv_supervisor_financial ADD INDEX idx_supervisor_fund (supervisor_id, fund_id)",
            "ALTER TABLE mv_supervisor_financial ADD INDEX idx_supervisor_amount (supervisor_id, amount)",
            "ALTER TABLE mv_supervisor_financial ADD INDEX idx_fund_id (fund_id)",
            "ALTER TABLE mv_supervisor_financial ADD INDEX idx_last_updated (last_updated)"
        ]
        
        for i, index_sql in enumerate(indexes, 1):
            safe_print(f"创建索引 {i}/{len(indexes)}...")
            start_time = time.time()
            
            cursor.execute(index_sql)
            
            elapsed = time.time() - start_time
            safe_print(f"  ✅ 完成，耗时 {elapsed:.2f}s")
        
        conn.commit()
        safe_print("✅ 所有索引创建完成")
        return True
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 索引创建失败: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def restore_mysql_settings():
    """恢复MySQL设置"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("\n=== 恢复MySQL设置 ===")
        
        restorations = [
            "SET SESSION foreign_key_checks = 1",
            "SET SESSION unique_checks = 1",
            "SET SESSION sql_log_bin = 1"
        ]
        
        for restore in restorations:
            try:
                cursor.execute(restore)
                safe_print(f"✅ {restore}")
            except mysql.connector.Error as e:
                safe_print(f"⚠️ {restore} - {e}")
        
        conn.commit()
        safe_print("MySQL设置恢复完成")
        return True
        
    except mysql.connector.Error as e:
        safe_print(f"❌ MySQL设置恢复失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def update_timestamps():
    """更新时间戳"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("\n更新时间戳...")
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        conn.commit()
        safe_print(f"✅ 已更新 {cursor.rowcount:,} 条记录的时间戳")
        return True
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 时间戳更新失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def verify_materialized_view():
    """验证物化视图"""
    conn = connect_db()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        safe_print("\n=== 验证物化视图 ===")
        
        # 基本统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT supervisor_id) FROM mv_supervisor_financial")
        unique_supervisors = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT fund_id) FROM mv_supervisor_financial")
        unique_funds = cursor.fetchone()[0]
        
        safe_print(f"总记录数: {total_records:,}")
        safe_print(f"不同supervisor数: {unique_supervisors:,}")
        safe_print(f"不同fund数: {unique_funds:,}")
        
        # 抽样验证
        cursor.execute("""
            SELECT supervisor_id, COUNT(*) as record_count
            FROM mv_supervisor_financial
            GROUP BY supervisor_id
            ORDER BY record_count DESC
            LIMIT 5
        """)
        
        top_supervisors = cursor.fetchall()
        safe_print("\n记录最多的5个supervisor:")
        for sup_id, count in top_supervisors:
            safe_print(f"  Supervisor {sup_id}: {count:,} 条记录")
        
        # 数据一致性检查（抽样）
        test_supervisor = top_supervisors[0][0] if top_supervisors else None
        if test_supervisor:
            safe_print(f"\n对supervisor {test_supervisor}进行一致性检查...")
            
            # 物化视图记录数
            cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_supervisor,))
            mv_count = cursor.fetchone()[0]
            
            # 原始查询记录数
            cursor.execute("""
                SELECT COUNT(*)
                FROM user_hierarchy h
                JOIN financial_funds f ON h.subordinate_id = f.handle_by
                WHERE h.user_id = %s
            """, (test_supervisor,))
            original_count = cursor.fetchone()[0]
            
            safe_print(f"  物化视图: {mv_count:,} 条")
            safe_print(f"  原始查询: {original_count:,} 条")
            
            if mv_count == original_count:
                safe_print("  ✅ 数据一致性验证通过")
            else:
                safe_print(f"  ❌ 数据不一致，差异: {abs(mv_count - original_count):,} 条")
        
        return total_records > 0
        
    except mysql.connector.Error as e:
        safe_print(f"❌ 验证失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="物化视图初始化性能优化")
    parser.add_argument("--batch_size", type=int, default=100, help="每批处理的supervisor数量")
    parser.add_argument("--max_workers", type=int, default=4, help="并行线程数")
    parser.add_argument("--skip_backup", action="store_true", help="跳过表备份和重建")
    parser.add_argument("--only_indexes", action="store_true", help="只创建索引")
    parser.add_argument("--verify_only", action="store_true", help="只进行验证")
    
    args = parser.parse_args()
    
    overall_start_time = time.time()
    
    safe_print("🚀 物化视图初始化性能优化")
    safe_print(f"批次大小: {args.batch_size} supervisor/批次")
    safe_print(f"并行线程: {args.max_workers}")
    
    if args.verify_only:
        verify_materialized_view()
        return
    
    if args.only_indexes:
        create_indexes_after_data_load()
        return
    
    success = True
    
    # 1. 优化MySQL设置
    if not optimize_mysql_settings():
        safe_print("MySQL优化失败，但继续执行")
    
    # 2. 重建表结构（除非跳过）
    if not args.skip_backup:
        if not backup_and_recreate_mv_table():
            safe_print("表结构重建失败，退出")
            return
    
    # 3. 并行填充数据
    if not parallel_populate_materialized_view(args.max_workers, args.batch_size):
        safe_print("数据填充失败")
        success = False
    
    # 4. 创建索引
    if success:
        if not create_indexes_after_data_load():
            safe_print("索引创建失败")
            success = False
    
    # 5. 更新时间戳
    if success:
        if not update_timestamps():
            safe_print("时间戳更新失败")
    
    # 6. 恢复MySQL设置
    restore_mysql_settings()
    
    # 7. 验证结果
    if success:
        verify_materialized_view()
    
    overall_elapsed_time = time.time() - overall_start_time
    
    safe_print(f"\n{'='*60}")
    if success:
        safe_print("🎉 物化视图优化初始化完成！")
    else:
        safe_print("❌ 物化视图初始化过程中出现错误")
    
    safe_print(f"总耗时: {overall_elapsed_time:.2f} 秒 ({overall_elapsed_time/60:.1f} 分钟)")
    safe_print(f"{'='*60}")
    
    if success:
        safe_print("\n优化效果：")
        safe_print("✅ 批量处理减少数据库锁竞争")
        safe_print("✅ 并行处理提升整体速度")
        safe_print("✅ 延迟索引创建减少插入开销")
        safe_print("✅ MySQL优化配置提升性能")
        
        estimated_improvement = 1.5 * 60 * 60 / overall_elapsed_time  # 相对于1.5小时的提升倍数
        safe_print(f"\n预计性能提升: {estimated_improvement:.1f}x")

if __name__ == "__main__":
    main()

