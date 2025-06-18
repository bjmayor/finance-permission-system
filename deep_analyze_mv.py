import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """获取数据库连接"""
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456'),
        'database': os.getenv('DB_NAME_V2', 'finance')
    }
    return mysql.connector.connect(**config)

def deep_analyze_mv_difference():
    """深度分析物化视图差异"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 深度分析物化视图差异 ===\n")
        
        test_user_id = 70
        
        # 1. 检查物化视图的实际构建SQL
        print("1. 检查物化视图表结构和索引:")
        cursor.execute("SHOW CREATE TABLE mv_supervisor_financial")
        table_def = cursor.fetchone()[1]
        print("   表定义:")
        print(f"   {table_def}")
        
        cursor.execute("SHOW INDEX FROM mv_supervisor_financial")
        indexes = cursor.fetchall()
        print("\n   索引:")
        for idx in indexes:
            print(f"   {idx}")
        
        # 2. 检查物化视图的数据完整性
        print(f"\n2. 物化视图数据完整性检查:")
        
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_mv = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT supervisor_id) FROM mv_supervisor_financial")
        unique_supervisors = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT fund_id) FROM mv_supervisor_financial") 
        unique_funds = cursor.fetchone()[0]
        
        print(f"   总记录数: {total_mv:,}")
        print(f"   不同supervisor数: {unique_supervisors:,}")
        print(f"   不同fund数: {unique_funds:,}")
        
        # 3. 对比物化视图构建SQL的实际执行结果
        print(f"\n3. 重新执行物化视图构建SQL:")
        
        mv_build_sql = """
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
        WHERE h.user_id = %s
        """
        
        cursor.execute(mv_build_sql, (test_user_id,))
        rebuild_results = cursor.fetchall()
        rebuild_count = len(rebuild_results)
        
        print(f"   重新构建SQL返回: {rebuild_count:,} 条记录")
        
        # 4. 检查物化视图中该用户的实际记录
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_user_count = cursor.fetchone()[0]
        
        print(f"   物化视图中该用户: {mv_user_count:,} 条记录")
        print(f"   差异: {abs(rebuild_count - mv_user_count):,} 条")
        
        # 5. 检查是否有重复记录
        print(f"\n4. 检查重复记录:")
        
        cursor.execute("""
            SELECT supervisor_id, fund_id, COUNT(*) as dup_count
            FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
            GROUP BY supervisor_id, fund_id
            HAVING COUNT(*) > 1
            LIMIT 10
        """, (test_user_id,))
        
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"   发现 {len(duplicates)} 组重复记录:")
            for sup_id, fund_id, dup_count in duplicates:
                print(f"     supervisor={sup_id}, fund={fund_id}: {dup_count} 次")
        else:
            print("   ✅ 无重复记录")
        
        # 6. 对比fund_id的分布
        print(f"\n5. fund_id分布对比:")
        
        # 物化视图中的fund_id
        cursor.execute("""
            SELECT fund_id FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
            ORDER BY fund_id
        """, (test_user_id,))
        mv_fund_ids = set(row[0] for row in cursor.fetchall())
        
        # 重新构建SQL的fund_id
        rebuild_fund_ids = set(row[1] for row in rebuild_results)
        
        print(f"   物化视图fund_id数量: {len(mv_fund_ids):,}")
        print(f"   重构SQL fund_id数量: {len(rebuild_fund_ids):,}")
        
        only_in_mv = mv_fund_ids - rebuild_fund_ids
        only_in_rebuild = rebuild_fund_ids - mv_fund_ids
        
        print(f"   只在物化视图中: {len(only_in_mv):,} 个fund_id")
        print(f"   只在重构SQL中: {len(only_in_rebuild):,} 个fund_id")
        
        if only_in_mv:
            print(f"   只在物化视图中的前10个: {sorted(list(only_in_mv))[:10]}")
        if only_in_rebuild:
            print(f"   只在重构SQL中的前10个: {sorted(list(only_in_rebuild))[:10]}")
        
        # 7. 检查物化视图的最后更新时间
        print(f"\n6. 物化视图更新时间检查:")
        
        cursor.execute("""
            SELECT 
                MIN(last_updated) as min_updated,
                MAX(last_updated) as max_updated,
                COUNT(DISTINCT last_updated) as unique_times
            FROM mv_supervisor_financial
            WHERE last_updated IS NOT NULL
        """)
        
        update_info = cursor.fetchone()
        if update_info[0]:
            print(f"   最早更新: {update_info[0]}")
            print(f"   最晚更新: {update_info[1]}")
            print(f"   不同更新时间: {update_info[2]}")
        else:
            print("   ⚠️ last_updated字段为空")
        
        # 8. 检查物化视图构建时是否有条件遗漏
        print(f"\n7. 详细SQL差异分析:")
        
        # 检查直接JOIN的完整SQL
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by  
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (test_user_id,))
        direct_join_count = cursor.fetchone()[0]
        
        # 检查是否还有其他JOIN条件
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            LEFT JOIN orders o ON f.order_id = o.order_id
            LEFT JOIN customers c ON f.customer_id = c.customer_id
            WHERE h.user_id = %s
        """, (test_user_id,))
        extended_join_count = cursor.fetchone()[0]
        
        print(f"   基础JOIN: {direct_join_count:,}")
        print(f"   扩展JOIN: {extended_join_count:,}")
        print(f"   物化视图: {mv_user_count:,}")
        
        # 9. 检查物化视图是否被部分更新
        print(f"\n8. 检查物化视图数据分布:")
        
        cursor.execute("""
            SELECT supervisor_id, COUNT(*) as record_count
            FROM mv_supervisor_financial
            GROUP BY supervisor_id
            ORDER BY record_count DESC
            LIMIT 10
        """)
        
        top_supervisors = cursor.fetchall()
        print("   记录最多的前10个supervisor:")
        for sup_id, count in top_supervisors:
            print(f"     supervisor {sup_id}: {count:,} 条记录")
        
        # 10. 尝试找出物化视图构建的问题
        print(f"\n9. 物化视图构建问题诊断:")
        
        # 检查物化视图构建时间
        cursor.execute("SELECT MAX(last_updated) FROM mv_supervisor_financial")
        last_refresh = cursor.fetchone()[0]
        
        # 检查financial_funds表的数据是否在物化视图刷新后有变化
        cursor.execute("SELECT COUNT(*) FROM financial_funds")
        current_funds = cursor.fetchone()[0]
        
        print(f"   物化视图最后刷新: {last_refresh}")
        print(f"   当前financial_funds记录数: {current_funds:,}")
        
        # 检查user_hierarchy表的数据
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        current_hierarchy = cursor.fetchone()[0]
        print(f"   当前user_hierarchy记录数: {current_hierarchy:,}")
        
        # 最终建议
        print(f"\n10. 问题诊断结果:")
        if rebuild_count == direct_join_count and mv_user_count != rebuild_count:
            print("   ❌ 物化视图数据不完整，需要重新刷新")
            print("   建议: 重新执行物化视图刷新脚本")
        elif rebuild_count != direct_join_count:
            print("   ❌ SQL逻辑不一致，需要检查查询条件")
        else:
            print("   🤔 需要进一步分析其他可能原因")
        
    except mysql.connector.Error as e:
        print(f"❌ 分析过程中出错: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    deep_analyze_mv_difference()