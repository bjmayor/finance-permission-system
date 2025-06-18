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
        'autocommit': True
    }
    return mysql.connector.connect(**config)

def batch_insert_hierarchy():
    """分批插入层级关系"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 分批修复user_hierarchy表 ===")
        
        # 1. 清空表
        print("1. 清空user_hierarchy表...")
        cursor.execute("TRUNCATE TABLE user_hierarchy")
        print("   ✅ 清空完成")
        
        # 2. 分批插入直接关系
        print("2. 分批插入直接父子关系...")
        batch_size = 10000
        offset = 0
        total_inserted = 0
        
        while True:
            cursor.execute(f"""
                SELECT DISTINCT p.parent_id, p.id
                FROM users p
                WHERE p.parent_id IS NOT NULL
                  AND p.parent_id IN (SELECT id FROM users)
                LIMIT {batch_size} OFFSET {offset}
            """)
            
            batch_data = cursor.fetchall()
            if not batch_data:
                break
            
            # 批量插入
            insert_data = [(parent_id, child_id, 1) for parent_id, child_id in batch_data]
            cursor.executemany(
                "INSERT INTO user_hierarchy (user_id, subordinate_id, depth) VALUES (%s, %s, %s)",
                insert_data
            )
            
            total_inserted += len(insert_data)
            offset += batch_size
            print(f"   已插入 {total_inserted:,} 条直接关系...")
        
        print(f"✅ 直接关系插入完成，总计 {total_inserted:,} 条")
        
        # 3. 检查结果
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        final_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT depth, COUNT(*) as count
            FROM user_hierarchy 
            GROUP BY depth 
            ORDER BY depth
        """)
        depth_stats = cursor.fetchall()
        
        print(f"\n层级分布:")
        for depth, count in depth_stats:
            print(f"   层级 {depth}: {count:,} 条")
        
        print(f"\n✅ 修复完成，总计 {final_count:,} 条关系")
        
    except mysql.connector.Error as e:
        print(f"❌ 修复失败: {e}")
    finally:
        cursor.close()
        conn.close()

def refresh_materialized_view():
    """刷新物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
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
        
        print(f"✅ 物化视图刷新完成，共 {count:,} 条记录")
        return count
        
    except mysql.connector.Error as e:
        print(f"❌ 刷新物化视图失败: {e}")
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
            return True
        else:
            print(f"\n差异分析:")
            print(f"  hierarchy vs CTE: {abs(hierarchy_count - cte_count):,}")
            print(f"  hierarchy vs MV: {abs(hierarchy_count - mv_count):,}")
            print(f"  CTE vs MV: {abs(cte_count - mv_count):,}")
            return False
        
    except mysql.connector.Error as e:
        print(f"❌ 对比失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_time = time.time()
    
    # 1. 修复层级关系
    batch_insert_hierarchy()
    
    # 2. 刷新物化视图
    mv_count = refresh_materialized_view()
    
    if mv_count > 0:
        # 3. 最终对比
        success = final_comparison()
        if success:
            print("\n🎉 修复成功！所有方法结果一致！")
        else:
            print("\n⚠️ 修复完成，但结果仍有差异")
    else:
        print("物化视图刷新失败")
    
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")