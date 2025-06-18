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

def cleanup_users_table():
    """清理users表，只保留前1万个用户"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 清理users表，只保留1万个用户 ===")
        
        # 1. 备份原始表
        print("1. 备份原始users表...")
        cursor.execute("DROP TABLE IF EXISTS users_full_backup")
        cursor.execute("CREATE TABLE users_full_backup AS SELECT * FROM users")
        
        cursor.execute("SELECT COUNT(*) FROM users_full_backup")
        original_count = cursor.fetchone()[0]
        print(f"   原始用户数: {original_count:,}")
        
        # 2. 创建临时表存储要保留的用户
        print("2. 选择要保留的1万个用户...")
        cursor.execute("DROP TABLE IF EXISTS users_temp")
        cursor.execute("""
            CREATE TABLE users_temp AS 
            SELECT * FROM users 
            ORDER BY id 
            LIMIT 10000
        """)
        
        # 3. 更新parent_id，确保所有parent_id都在保留的用户范围内
        print("3. 修复parent_id关系...")
        cursor.execute("""
            UPDATE users_temp 
            SET parent_id = NULL 
            WHERE parent_id IS NOT NULL 
            AND parent_id NOT IN (SELECT id FROM (SELECT id FROM users_temp) t)
        """)
        
        # 4. 重新分配parent_id，构建合理的层级结构
        print("4. 重新构建层级结构...")
        
        # 设置前100个用户为根用户（没有parent_id）
        cursor.execute("UPDATE users_temp SET parent_id = NULL WHERE id <= 100")
        
        # 为其他用户分配parent_id，构建层级结构
        cursor.execute("""
            UPDATE users_temp 
            SET parent_id = CASE 
                WHEN id <= 1000 THEN (id % 100) + 1
                WHEN id <= 5000 THEN (id % 1000) + 1
                ELSE (id % 5000) + 1
            END
            WHERE id > 100
        """)
        
        # 5. 替换原始users表
        print("5. 替换原始users表...")
        cursor.execute("DROP TABLE users")
        cursor.execute("RENAME TABLE users_temp TO users")
        
        cursor.execute("SELECT COUNT(*) FROM users")
        new_count = cursor.fetchone()[0]
        print(f"   新用户数: {new_count:,}")
        
        # 6. 检查parent_id分布
        cursor.execute("SELECT COUNT(*) FROM users WHERE parent_id IS NULL")
        root_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM users WHERE parent_id IS NOT NULL")
        child_count = cursor.fetchone()[0]
        
        print(f"   根用户数: {root_count:,}")
        print(f"   有parent_id的用户数: {child_count:,}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 清理用户表失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def build_complete_hierarchy():
    """构建包含所有层级的user_hierarchy表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 构建完整层级的user_hierarchy表 ===")
        
        # 1. 备份并清空
        print("1. 备份并清空user_hierarchy表...")
        cursor.execute("DROP TABLE IF EXISTS user_hierarchy_old")
        cursor.execute("CREATE TABLE user_hierarchy_old AS SELECT * FROM user_hierarchy")
        cursor.execute("TRUNCATE TABLE user_hierarchy")
        
        # 2. 使用递归查询构建完整层级
        print("2. 构建完整层级关系...")
        
        # 找到所有根用户
        cursor.execute("SELECT id FROM users WHERE parent_id IS NULL")
        root_users = [row[0] for row in cursor.fetchall()]
        print(f"   发现 {len(root_users)} 个根用户")
        
        total_relationships = 0
        
        # 为每个根用户构建层级树
        for root_id in root_users:
            print(f"   处理用户 {root_id} 的层级树...")
            
            # 使用递归CTE一次性插入所有层级关系
            cursor.execute("""
                INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
                WITH RECURSIVE hierarchy_tree AS (
                    -- 起始：找到当前根用户的直接下属
                    SELECT %s as supervisor_id, id as subordinate_id, 1 as depth
                    FROM users 
                    WHERE parent_id = %s
                    
                    UNION ALL
                    
                    -- 递归：找到下属的下属
                    SELECT ht.supervisor_id, u.id, ht.depth + 1
                    FROM hierarchy_tree ht
                    JOIN users u ON u.parent_id = ht.subordinate_id
                    WHERE ht.depth < 10  -- 限制最大深度为10
                )
                SELECT supervisor_id, subordinate_id, depth
                FROM hierarchy_tree
            """, (root_id, root_id))
            
            inserted = cursor.rowcount
            total_relationships += inserted
            if inserted > 0:
                print(f"     插入了 {inserted} 条关系")
        
        print(f"   总共插入 {total_relationships:,} 条层级关系")
        
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
            print(f"   层级 {depth}: {count:,} 条关系")
        
        print(f"\n✅ 完整层级构建完成，总计 {final_count:,} 条关系")
        
        return final_count > 0
        
    except mysql.connector.Error as e:
        print(f"❌ 构建层级失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def update_financial_data():
    """更新财务数据，确保handle_by在新的用户范围内"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 更新财务数据 ===")
        
        # 1. 检查当前财务数据中有多少handle_by不在新用户范围内
        cursor.execute("""
            SELECT COUNT(*) FROM financial_funds 
            WHERE handle_by NOT IN (SELECT id FROM users)
        """)
        invalid_funds = cursor.fetchone()[0]
        print(f"1. 发现 {invalid_funds:,} 条财务记录的handle_by不在用户范围内")
        
        if invalid_funds > 0:
            # 2. 更新这些记录，随机分配给现有用户
            print("2. 更新无效的handle_by...")
            cursor.execute("""
                UPDATE financial_funds 
                SET handle_by = (SELECT id FROM users ORDER BY RAND() LIMIT 1)
                WHERE handle_by NOT IN (SELECT id FROM users)
            """)
            print(f"   已更新 {cursor.rowcount:,} 条记录")
        
        # 3. 检查结果
        cursor.execute("SELECT COUNT(*) FROM financial_funds")
        total_funds = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT handle_by) FROM financial_funds
        """)
        unique_handlers = cursor.fetchone()[0]
        
        print(f"3. 财务数据统计:")
        print(f"   总财务记录数: {total_funds:,}")
        print(f"   唯一处理人数: {unique_handlers:,}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 更新财务数据失败: {e}")
        return False
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
    """最终对比三种方法"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 最终对比测试 ===")
        
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
            WHERE f.handle_by IN (SELECT id FROM subordinates WHERE id != %s)
        """, (test_user_id, test_user_id))
        cte_count = cursor.fetchone()[0]
        
        # 物化视图
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_count = cursor.fetchone()[0]
        
        print(f"用户 {test_user_id} 可访问的财务记录:")
        print(f"  user_hierarchy方法: {hierarchy_count:,}")
        print(f"  递归CTE方法: {cte_count:,}")
        print(f"  物化视图: {mv_count:,}")
        
        # 检查下属数量
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy WHERE user_id = %s", (test_user_id,))
        hierarchy_subordinates = cursor.fetchone()[0]
        
        cursor.execute("""
            WITH RECURSIVE subordinates AS (
                SELECT id FROM users WHERE id = %s
                UNION ALL
                SELECT u.id FROM users u 
                JOIN subordinates s ON u.parent_id = s.id
            )
            SELECT COUNT(*) FROM subordinates WHERE id != %s
        """, (test_user_id, test_user_id))
        cte_subordinates = cursor.fetchone()[0]
        
        print(f"\n用户 {test_user_id} 的下属数量:")
        print(f"  user_hierarchy方法: {hierarchy_subordinates:,}")
        print(f"  递归CTE方法: {cte_subordinates:,}")
        
        # 总体统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_mv = cursor.fetchone()[0]
        print(f"\n物化视图总记录数: {total_mv:,}")
        
        # 一致性检查
        if hierarchy_count == cte_count == mv_count:
            print("\n🎉 所有方法结果完全一致！")
            return True
        else:
            print(f"\n差异分析:")
            print(f"  hierarchy vs CTE: {abs(hierarchy_count - cte_count):,}")
            print(f"  hierarchy vs MV: {abs(hierarchy_count - mv_count):,}")
            print(f"  CTE vs MV: {abs(cte_count - mv_count):,}")
            
            if hierarchy_subordinates == cte_subordinates:
                print("  ✅ 下属数量一致，说明层级结构统一")
            else:
                print(f"  ❌ 下属数量差异: {abs(hierarchy_subordinates - cte_subordinates):,}")
            
            return False
        
    except mysql.connector.Error as e:
        print(f"❌ 对比失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_time = time.time()
    
    print("开始重建系统：1万用户 + 完整层级...")
    
    # 1. 清理用户表
    if not cleanup_users_table():
        print("用户表清理失败，退出")
        exit(1)
    
    # 2. 构建完整层级
    if not build_complete_hierarchy():
        print("层级构建失败，退出")
        exit(1)
    
    # 3. 更新财务数据
    if not update_financial_data():
        print("财务数据更新失败，退出")
        exit(1)
    
    # 4. 刷新物化视图
    mv_count = refresh_materialized_view()
    if mv_count == 0:
        print("物化视图刷新失败，退出")
        exit(1)
    
    # 5. 最终对比
    success = final_comparison()
    
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")
    
    if success:
        print("\n🎉 重建成功！所有方法结果一致！")
    else:
        print("\n⚠️ 重建完成，但仍有差异需要进一步分析")