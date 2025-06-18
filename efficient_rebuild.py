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

def step1_backup_and_cleanup():
    """步骤1: 备份并清理到1万用户"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 步骤1: 备份并清理用户表 ===")
        
        # 检查当前状态
        cursor.execute("SELECT COUNT(*) FROM users")
        current_count = cursor.fetchone()[0]
        print(f"当前用户数: {current_count:,}")
        
        if current_count <= 10000:
            print("用户数已经是1万或更少，跳过清理")
            return True
        
        # 备份
        cursor.execute("DROP TABLE IF EXISTS users_backup_full")
        cursor.execute("CREATE TABLE users_backup_full AS SELECT * FROM users")
        print("✅ 备份完成")
        
        # 创建新的users表，只保留前1万个
        cursor.execute("DROP TABLE IF EXISTS users_new")
        cursor.execute("""
            CREATE TABLE users_new AS 
            SELECT * FROM users 
            ORDER BY id 
            LIMIT 10000
        """)
        
        # 修复parent_id
        cursor.execute("""
            UPDATE users_new 
            SET parent_id = CASE 
                WHEN id <= 100 THEN NULL
                WHEN id <= 1000 THEN ((id - 101) % 100) + 1
                WHEN id <= 5000 THEN ((id - 1001) % 1000) + 1
                ELSE ((id - 5001) % 5000) + 1
            END
        """)
        
        # 替换原表
        cursor.execute("DROP TABLE users")
        cursor.execute("RENAME TABLE users_new TO users")
        
        cursor.execute("SELECT COUNT(*) FROM users")
        new_count = cursor.fetchone()[0]
        print(f"✅ 用户表清理完成，新用户数: {new_count:,}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ 步骤1失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def step2_build_hierarchy():
    """步骤2: 构建完整层级"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 步骤2: 构建完整层级 ===")
        
        # 清空user_hierarchy
        cursor.execute("TRUNCATE TABLE user_hierarchy")
        
        # 分批构建层级关系
        print("构建层级关系...")
        
        # 使用一个大的INSERT语句，基于parent_id构建所有层级
        cursor.execute("""
            INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
            WITH RECURSIVE hierarchy AS (
                SELECT 
                    parent_id as user_id,
                    id as subordinate_id,
                    1 as depth
                FROM users 
                WHERE parent_id IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    h.user_id,
                    u.id as subordinate_id,
                    h.depth + 1
                FROM hierarchy h
                JOIN users u ON u.parent_id = h.subordinate_id
                WHERE h.depth < 5
            )
            SELECT user_id, subordinate_id, depth FROM hierarchy
        """)
        
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        hierarchy_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT depth, COUNT(*) 
            FROM user_hierarchy 
            GROUP BY depth 
            ORDER BY depth
        """)
        depth_stats = cursor.fetchall()
        
        print(f"✅ 层级关系构建完成: {hierarchy_count:,} 条")
        for depth, count in depth_stats:
            print(f"   层级 {depth}: {count:,} 条")
        
        return hierarchy_count > 0
        
    except mysql.connector.Error as e:
        print(f"❌ 步骤2失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def step3_fix_financial_data():
    """步骤3: 快速修复财务数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 步骤3: 修复财务数据 ===")
        
        # 检查无效的handle_by
        cursor.execute("""
            SELECT COUNT(*) FROM financial_funds 
            WHERE handle_by NOT IN (SELECT id FROM users)
        """)
        invalid_count = cursor.fetchone()[0]
        print(f"无效的财务记录: {invalid_count:,}")
        
        if invalid_count > 0:
            print("批量修复财务数据...")
            
            # 使用JOIN更新，比子查询更高效
            cursor.execute("""
                UPDATE financial_funds f
                SET handle_by = (f.handle_by % 10000) + 1
                WHERE f.handle_by NOT IN (SELECT id FROM users)
            """)
            
            print(f"✅ 修复了 {cursor.rowcount:,} 条记录")
        
        # 验证结果
        cursor.execute("""
            SELECT COUNT(*) FROM financial_funds 
            WHERE handle_by NOT IN (SELECT id FROM users)
        """)
        remaining_invalid = cursor.fetchone()[0]
        
        if remaining_invalid > 0:
            print(f"⚠️ 仍有 {remaining_invalid:,} 条无效记录")
            return False
        else:
            print("✅ 所有财务记录的handle_by都有效")
            return True
        
    except mysql.connector.Error as e:
        print(f"❌ 步骤3失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def step4_refresh_mv():
    """步骤4: 刷新物化视图"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 步骤4: 刷新物化视图 ===")
        
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
        """)
        
        cursor.execute("UPDATE mv_supervisor_financial SET last_updated = NOW()")
        
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        mv_count = cursor.fetchone()[0]
        
        print(f"✅ 物化视图刷新完成: {mv_count:,} 条记录")
        return mv_count
        
    except mysql.connector.Error as e:
        print(f"❌ 步骤4失败: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def step5_final_test():
    """步骤5: 最终测试对比"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("\n=== 步骤5: 最终测试 ===")
        
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
        
        # 总体统计
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
        total_mv = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_hierarchy")
        total_hierarchy = cursor.fetchone()[0]
        
        print(f"\n系统统计:")
        print(f"  总用户数: {total_users:,}")
        print(f"  总层级关系: {total_hierarchy:,}")
        print(f"  物化视图总记录: {total_mv:,}")
        
        # 一致性检查
        if hierarchy_count == cte_count == mv_count:
            print("\n🎉 所有方法结果完全一致！修复成功！")
            return True
        else:
            print(f"\n差异分析:")
            print(f"  hierarchy vs CTE: {abs(hierarchy_count - cte_count):,}")
            print(f"  hierarchy vs MV: {abs(hierarchy_count - mv_count):,}")
            print(f"  CTE vs MV: {abs(cte_count - mv_count):,}")
            return False
        
    except mysql.connector.Error as e:
        print(f"❌ 步骤5失败: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_time = time.time()
    
    print("开始高效重建系统...")
    
    # 执行所有步骤
    steps = [
        ("清理用户表", step1_backup_and_cleanup),
        ("构建层级", step2_build_hierarchy),
        ("修复财务数据", step3_fix_financial_data),
        ("刷新物化视图", step4_refresh_mv),
        ("最终测试", step5_final_test)
    ]
    
    for step_name, step_func in steps:
        print(f"\n{'='*50}")
        print(f"执行: {step_name}")
        print(f"{'='*50}")
        
        step_start = time.time()
        result = step_func()
        step_end = time.time()
        
        print(f"\n{step_name} 耗时: {step_end - step_start:.2f} 秒")
        
        if not result:
            print(f"❌ {step_name} 失败，停止执行")
            break
        else:
            print(f"✅ {step_name} 成功")
    
    end_time = time.time()
    print(f"\n总耗时: {end_time - start_time:.2f} 秒")