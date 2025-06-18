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

def fix_hierarchy_simple():
    """简化的层级关系重建"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 开始事务
        conn.start_transaction()
        
        print("=== 简化重建user_hierarchy表 ===")
        
        # 1. 备份并清空
        print("1. 备份原表...")
        cursor.execute("DROP TABLE IF EXISTS user_hierarchy_backup")
        cursor.execute("CREATE TABLE user_hierarchy_backup AS SELECT * FROM user_hierarchy")
        
        print("2. 清空user_hierarchy表...")
        cursor.execute("TRUNCATE TABLE user_hierarchy")
        
        # 2. 插入直接的父子关系（depth = 1）
        print("3. 插入直接父子关系...")
        cursor.execute("""
            INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
            SELECT DISTINCT p.parent_id, p.id, 1
            FROM users p
            WHERE p.parent_id IS NOT NULL
            AND p.parent_id IN (SELECT id FROM users)
        """)
        
        direct_count = cursor.rowcount
        print(f"   插入 {direct_count:,} 条直接关系")
        
        # 3. 插入间接关系（depth = 2）
        print("4. 插入间接关系（深度2）...")
        cursor.execute("""
            INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
            SELECT DISTINCT h1.user_id, h2.subordinate_id, 2
            FROM user_hierarchy h1
            JOIN user_hierarchy h2 ON h1.subordinate_id = h2.user_id
            WHERE h1.depth = 1 AND h2.depth = 1
            AND NOT EXISTS (
                SELECT 1 FROM user_hierarchy h3 
                WHERE h3.user_id = h1.user_id 
                AND h3.subordinate_id = h2.subordinate_id
            )
        """)
        
        indirect2_count = cursor.rowcount
        print(f"   插入 {indirect2_count:,} 条深度2关系")
        
        # 4. 插入间接关系（depth = 3）
        print("5. 插入间接关系（深度3）...")
        cursor.execute("""
            INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
            SELECT DISTINCT h1.user_id, h2.subordinate_id, 3
            FROM user_hierarchy h1
            JOIN user_hierarchy h2 ON h1.subordinate_id = h2.user_id
            WHERE h1.depth = 1 AND h2.depth = 2
            AND NOT EXISTS (
                SELECT 1 FROM user_hierarchy h3 
                WHERE h3.user_id = h1.user_id 
                AND h3.subordinate_id = h2.subordinate_id
            )
        """)
        
        indirect3_count = cursor.rowcount
        print(f"   插入 {indirect3_count:,} 条深度3关系")
        
        # 5. 检查结果
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT depth, COUNT(*) as count
            FROM user_hierarchy 
            GROUP BY depth 
            ORDER BY depth
        """)
        depth_stats = cursor.fetchall()
        
        print(f"\n6. 重建完成，总计 {total_count:,} 条关系:")
        for depth, count in depth_stats:
            print(f"   层级 {depth}: {count:,} 条")
        
        # 提交事务
        conn.commit()
        print("\n✅ 事务提交成功！")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 出错，回滚事务: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def refresh_materialized_view():
    """刷新物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        conn.start_transaction()
        
        print("\n=== 刷新物化视图 ===")
        
        cursor.execute("TRUNCATE TABLE mv_supervisor_financial")
        
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
            WHERE h.depth > 0
        """)
        
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        count = cursor.fetchone()[0]
        
        conn.commit()
        print(f"✅ 物化视图刷新完成，共 {count:,} 条记录")
        
        return count
        
    except mysql.connector.Error as e:
        print(f"❌ 刷新物化视图失败: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def final_comparison():
    """最终对比"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 最终对比 ===")
        
        test_user_id = 1
        
        # user_hierarchy方法
        cursor.execute("""
            SELECT COUNT(*) 
            FROM financial_funds f
            WHERE f.handle_by IN (
                SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
            )
        """, (test_user_id,))
        hierarchy_count = cursor.fetchone()[0]
        
        # 递归CTE方法
        cursor.execute("""
            WITH RECURSIVE subordinates AS (
                SELECT id FROM users WHERE id = %s
                UNION ALL
                SELECT u.id FROM users u 
                JOIN subordinates s ON u.parent_id = s.id
            )
            SELECT COUNT(*) 
            FROM financial_funds f
            WHERE f.handle_by IN (SELECT id FROM subordinates)
        """, (test_user_id,))
        cte_count = cursor.fetchone()[0]
        
        # 物化视图
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_count = cursor.fetchone()[0]
        
        print(f"用户 {test_user_id} 可访问的财务记录:")
        print(f"  user_hierarchy方法: {hierarchy_count:,}")
        print(f"  递归CTE方法: {cte_count:,}")
        print(f"  物化视图: {mv_count:,}")
        
        # 总体统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_mv = cursor.fetchone()[0]
        print(f"\n物化视图总记录数: {total_mv:,}")
        
        if hierarchy_count == cte_count == mv_count:
            print("\n✅ 所有方法结果完全一致！")
        else:
            print(f"\n差异分析:")
            print(f"  hierarchy vs CTE: {abs(hierarchy_count - cte_count):,}")
            print(f"  hierarchy vs MV: {abs(hierarchy_count - mv_count):,}")
            print(f"  CTE vs MV: {abs(cte_count - mv_count):,}")
        
    except mysql.connector.Error as e:
        print(f"❌ 对比失败: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 1. 修复层级关系
    if fix_hierarchy_simple():
        # 2. 刷新物化视图
        mv_count = refresh_materialized_view()
        if mv_count > 0:
            # 3. 最终对比
            final_comparison()
        else:
            print("物化视图刷新失败，跳过对比")
    else:
        print("层级关系修复失败")