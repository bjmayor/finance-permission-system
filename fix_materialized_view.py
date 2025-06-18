import os
import mysql.connector
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

def get_db_connection():
    """获取数据库连接"""
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456'),
        'database': os.getenv('DB_NAME_V2', 'finance'),
        'autocommit': False
    }
    return mysql.connector.connect(**config)

def backup_current_mv():
    """备份当前物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 备份当前物化视图 ===")
        
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_backup")
        cursor.execute("""
            CREATE TABLE mv_supervisor_financial_backup AS 
            SELECT * FROM mv_supervisor_financial
        """)
        
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial_backup")
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

def analyze_expected_records():
    """分析预期的记录数"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 分析预期记录数 ===")
        
        # 计算预期的总记录数
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
        """)
        expected_total = cursor.fetchone()[0]
        
        # 当前物化视图记录数
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        current_total = cursor.fetchone()[0]
        
        # 差异分析
        difference = expected_total - current_total
        
        print(f"预期总记录数: {expected_total:,}")
        print(f"当前物化视图记录数: {current_total:,}")
        print(f"缺失记录数: {difference:,}")
        
        if difference > 0:
            print(f"⚠️ 物化视图缺失了 {difference:,} 条记录")
            return expected_total
        else:
            print("✅ 物化视图记录数正确")
            return current_total
            
    except mysql.connector.Error as e:
        print(f"❌ 分析失败: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def rebuild_materialized_view():
    """重建物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 重建物化视图 ===")
        
        # 1. 清空物化视图
        print("1. 清空物化视图...")
        cursor.execute("TRUNCATE TABLE mv_supervisor_financial")
        
        # 2. 重新构建数据
        print("2. 重新构建数据...")
        
        start_time = time.time()
        
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
        """)
        
        inserted_count = cursor.rowcount
        
        # 3. 更新时间戳
        print("3. 更新时间戳...")
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        # 4. 验证结果
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        final_count = cursor.fetchone()[0]
        
        end_time = time.time()
        duration = end_time - start_time
        
        conn.commit()
        
        print(f"✅ 重建完成")
        print(f"   插入记录数: {inserted_count:,}")
        print(f"   最终记录数: {final_count:,}")
        print(f"   耗时: {duration:.2f} 秒")
        
        return final_count
        
    except mysql.connector.Error as e:
        print(f"❌ 重建失败: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def verify_fix():
    """验证修复结果"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 验证修复结果 ===")
        
        test_user_id = 70
        
        # 1. 总记录数验证
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        mv_total = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
        """)
        expected_total = cursor.fetchone()[0]
        
        print(f"物化视图总记录数: {mv_total:,}")
        print(f"预期总记录数: {expected_total:,}")
        
        if mv_total == expected_total:
            print("✅ 总记录数一致")
        else:
            print(f"❌ 总记录数不一致，差异: {abs(mv_total - expected_total):,}")
            return False
        
        # 2. 测试用户验证
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_user_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (test_user_id,))
        expected_user_count = cursor.fetchone()[0]
        
        print(f"\n用户{test_user_id}记录数验证:")
        print(f"   物化视图: {mv_user_count:,}")
        print(f"   预期: {expected_user_count:,}")
        
        if mv_user_count == expected_user_count:
            print("   ✅ 用户记录数一致")
        else:
            print(f"   ❌ 用户记录数不一致，差异: {abs(mv_user_count - expected_user_count):,}")
            return False
        
        # 3. 数据完整性验证
        cursor.execute("""
            SELECT COUNT(DISTINCT supervisor_id) FROM mv_supervisor_financial
        """)
        unique_supervisors = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT user_id) FROM user_hierarchy
        """)
        expected_supervisors = cursor.fetchone()[0]
        
        print(f"\n数据完整性验证:")
        print(f"   物化视图中的supervisor数: {unique_supervisors:,}")
        print(f"   预期supervisor数: {expected_supervisors:,}")
        
        # 4. 索引和性能验证
        cursor.execute("""
            SELECT COUNT(*) FROM mv_supervisor_financial 
            WHERE supervisor_id = %s AND amount > 100000
        """, (test_user_id,))
        
        test_query_start = time.time()
        cursor.execute("""
            SELECT fund_id, amount FROM mv_supervisor_financial 
            WHERE supervisor_id = %s 
            ORDER BY amount DESC 
            LIMIT 10
        """, (test_user_id,))
        test_results = cursor.fetchall()
        test_query_end = time.time()
        
        query_time = (test_query_end - test_query_start) * 1000
        
        print(f"\n性能验证:")
        print(f"   测试查询耗时: {query_time:.2f}ms")
        print(f"   返回记录数: {len(test_results)}")
        
        if query_time < 100:  # 小于100ms
            print("   ✅ 查询性能良好")
        else:
            print("   ⚠️ 查询性能可能需要优化")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 验证失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def compare_methods_performance():
    """对比各方法性能"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 性能对比测试 ===")
        
        test_user_id = 70
        
        # 物化视图查询
        start_time = time.time()
        cursor.execute("""
            SELECT COUNT(*) FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
        """, (test_user_id,))
        mv_count = cursor.fetchone()[0]
        mv_time = (time.time() - start_time) * 1000
        
        # 直接JOIN查询
        start_time = time.time()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (test_user_id,))
        join_count = cursor.fetchone()[0]
        join_time = (time.time() - start_time) * 1000
        
        print(f"性能对比结果:")
        print(f"   物化视图: {mv_count:,} 条记录，耗时 {mv_time:.2f}ms")
        print(f"   直接JOIN: {join_count:,} 条记录，耗时 {join_time:.2f}ms")
        
        if mv_count == join_count:
            print("   ✅ 结果一致性验证通过")
            speedup = join_time / mv_time if mv_time > 0 else float('inf')
            print(f"   🚀 物化视图比直接JOIN快 {speedup:.1f}x")
            return True
        else:
            print(f"   ❌ 结果不一致，差异: {abs(mv_count - join_count):,}")
            return False
        
    except mysql.connector.Error as e:
        print(f"❌ 性能对比失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """主函数"""
    print("开始修复物化视图...")
    
    # 1. 备份当前物化视图
    if not backup_current_mv():
        print("备份失败，停止修复")
        return
    
    # 2. 分析预期记录数
    expected_count = analyze_expected_records()
    if expected_count == 0:
        print("分析失败，停止修复")
        return
    
    # 3. 重建物化视图
    actual_count = rebuild_materialized_view()
    if actual_count == 0:
        print("重建失败，停止修复")
        return
    
    # 4. 验证修复结果
    if not verify_fix():
        print("验证失败，可能需要进一步调试")
        return
    
    # 5. 性能对比测试
    if not compare_methods_performance():
        print("性能对比失败，但修复已完成")
        return
    
    print(f"\n🎉 物化视图修复成功！")
    print(f"   最终记录数: {actual_count:,}")
    print(f"   所有验证通过")

if __name__ == "__main__":
    main()