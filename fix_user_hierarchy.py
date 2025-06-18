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
        'database': os.getenv('DB_NAME_V2', 'finance')
    }
    return mysql.connector.connect(**config)

def build_hierarchy_from_users():
    """从users表的parent_id重新构建user_hierarchy表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 开始修复user_hierarchy表 ===\n")
        
        # 1. 备份原有的user_hierarchy表
        print("1. 备份原有user_hierarchy表...")
        cursor.execute("DROP TABLE IF EXISTS user_hierarchy_backup")
        cursor.execute("""
            CREATE TABLE user_hierarchy_backup AS 
            SELECT * FROM user_hierarchy
        """)
        print("   ✅ 备份完成")
        
        # 2. 清空user_hierarchy表
        print("\n2. 清空user_hierarchy表...")
        cursor.execute("TRUNCATE TABLE user_hierarchy")
        print("   ✅ 清空完成")
        
        # 3. 使用递归查询重新构建层级关系
        print("\n3. 重新构建层级关系...")
        
        # 首先找到所有根节点（没有parent_id或parent_id不存在的用户）
        cursor.execute("""
            SELECT id FROM users 
            WHERE parent_id IS NULL 
            OR parent_id NOT IN (SELECT id FROM users)
        """)
        root_users = [row[0] for row in cursor.fetchall()]
        print(f"   发现 {len(root_users)} 个根用户: {root_users[:10]}...")
        
        # 为每个根用户构建完整的层级树
        total_relationships = 0
        for root_id in root_users:
            relationships = build_tree_for_user(cursor, root_id)
            total_relationships += relationships
            print(f"   用户 {root_id} 的层级树: {relationships} 条关系")
        
        print(f"\n   总共插入 {total_relationships} 条层级关系")
        
        # 4. 验证结果
        print("\n4. 验证修复结果...")
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        new_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy_backup")
        old_count = cursor.fetchone()[0]
        
        print(f"   原有记录数: {old_count:,}")
        print(f"   修复后记录数: {new_count:,}")
        print(f"   变化: {new_count - old_count:+,}")
        
        # 5. 显示层级分布
        print("\n5. 新的层级分布:")
        cursor.execute("""
            SELECT depth, COUNT(*) as count
            FROM user_hierarchy 
            GROUP BY depth 
            ORDER BY depth
        """)
        depth_stats = cursor.fetchall()
        for depth, count in depth_stats:
            print(f"   层级 {depth}: {count:,} 条关系")
        
        # 6. 提交更改
        conn.commit()
        print("\n✅ user_hierarchy表修复完成！")
        
    except mysql.connector.Error as e:
        print(f"❌ 修复过程中出错: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def build_tree_for_user(cursor, user_id, depth=0, visited=None):
    """为指定用户构建层级树（递归）"""
    if visited is None:
        visited = set()
    
    # 防止循环引用
    if user_id in visited:
        return 0
    
    visited.add(user_id)
    relationships_count = 0
    
    # 查找直接下属
    cursor.execute("SELECT id FROM users WHERE parent_id = %s", (user_id,))
    children = [row[0] for row in cursor.fetchall()]
    
    for child_id in children:
        # 插入层级关系
        cursor.execute("""
            INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
            VALUES (%s, %s, %s)
        """, (user_id, child_id, depth + 1))
        relationships_count += 1
        
        # 递归处理子节点
        child_relationships = build_tree_for_user(cursor, child_id, depth + 1, visited.copy())
        relationships_count += child_relationships
        
        # 为当前用户添加所有间接下属关系
        cursor.execute("""
            SELECT subordinate_id, depth FROM user_hierarchy
            WHERE user_id = %s AND depth > 0
        """, (child_id,))
        
        indirect_subordinates = cursor.fetchall()
        for indirect_sub_id, indirect_depth in indirect_subordinates:
            cursor.execute("""
                INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
                VALUES (%s, %s, %s)
            """, (user_id, indirect_sub_id, depth + 1 + indirect_depth))
            relationships_count += 1
    
    return relationships_count

def refresh_materialized_view():
    """刷新物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 刷新物化视图 ===")
        
        # 清空并重新填充物化视图
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
            LIMIT 10000000
        """)
        
        # 更新时间戳
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        # 获取记录数
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        count = cursor.fetchone()[0]
        
        conn.commit()
        print(f"✅ 物化视图刷新完成，共 {count:,} 条记录")
        
    except mysql.connector.Error as e:
        print(f"❌ 刷新物化视图失败: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def compare_results():
    """对比修复前后的结果"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 修复前后对比 ===")
        
        test_user_id = 1
        
        # 检查新的user_hierarchy方法
        cursor.execute("""
            SELECT COUNT(*) 
            FROM financial_funds f
            WHERE f.handle_by IN (
                SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
                UNION SELECT %s
            )
        """, (test_user_id, test_user_id))
        new_hierarchy_count = cursor.fetchone()[0]
        
        # 检查递归CTE方法
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
        
        # 检查物化视图
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_count = cursor.fetchone()[0]
        
        # 检查备份的旧数据
        cursor.execute("""
            SELECT COUNT(*) 
            FROM financial_funds f
            WHERE f.handle_by IN (
                SELECT subordinate_id FROM user_hierarchy_backup WHERE user_id = %s
                UNION SELECT %s
            )
        """, (test_user_id, test_user_id))
        old_hierarchy_count = cursor.fetchone()[0]
        
        print(f"用户 {test_user_id} 可访问的财务记录数:")
        print(f"  修复前user_hierarchy: {old_hierarchy_count:,}")
        print(f"  修复后user_hierarchy: {new_hierarchy_count:,}")
        print(f"  递归CTE方法: {cte_count:,}")
        print(f"  物化视图: {mv_count:,}")
        
        # 检查总体物化视图记录数
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_mv_count = cursor.fetchone()[0]
        print(f"\n物化视图总记录数: {total_mv_count:,}")
        
        # 一致性检查
        if new_hierarchy_count == cte_count == mv_count:
            print("\n✅ 所有方法结果一致！修复成功！")
        else:
            print(f"\n⚠️  结果仍有差异:")
            print(f"   user_hierarchy vs CTE: {abs(new_hierarchy_count - cte_count):,}")
            print(f"   user_hierarchy vs 物化视图: {abs(new_hierarchy_count - mv_count):,}")
            print(f"   CTE vs 物化视图: {abs(cte_count - mv_count):,}")
        
    except mysql.connector.Error as e:
        print(f"❌ 对比过程中出错: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_time = time.time()
    
    # 1. 修复user_hierarchy表
    build_hierarchy_from_users()
    
    # 2. 刷新物化视图
    refresh_materialized_view()
    
    # 3. 对比结果
    compare_results()
    
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")