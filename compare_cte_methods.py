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

def compare_cte_methods():
    """对比递归CTE和user_hierarchy表的结果差异"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== CTE方法对比分析 ===\n")
        
        # 1. 检查users表是否有parent_id字段
        print("1. 检查users表结构:")
        cursor.execute("DESCRIBE users")
        columns = cursor.fetchall()
        has_parent_id = any('parent_id' in str(col) for col in columns)
        
        for col in columns:
            print(f"   {col[0]}: {col[1]}")
        
        if not has_parent_id:
            print("   ❌ users表没有parent_id字段，递归CTE无法正常工作")
        else:
            print("   ✅ users表有parent_id字段")
        
        print("\n2. 检查user_hierarchy表结构:")
        cursor.execute("DESCRIBE user_hierarchy")
        columns = cursor.fetchall()
        for col in columns:
            print(f"   {col[0]}: {col[1]}")
        
        # 2. 对比两种方法获取下属的结果
        test_supervisor_id = 1  # 使用ID=1作为测试用户
        
        print(f"\n3. 对比获取用户{test_supervisor_id}的下属:")
        
        # 方法1：使用user_hierarchy表
        cursor.execute("""
            SELECT subordinate_id as id FROM user_hierarchy 
            WHERE user_id = %s
            UNION
            SELECT %s as id
        """, (test_supervisor_id, test_supervisor_id))
        hierarchy_subordinates = set(row[0] for row in cursor.fetchall())
        print(f"   user_hierarchy方法: {len(hierarchy_subordinates)} 个下属")
        print(f"   前10个ID: {sorted(list(hierarchy_subordinates))[:10]}")
        
        # 方法2：尝试递归CTE（如果有parent_id字段）
        if has_parent_id:
            try:
                cursor.execute("""
                    WITH RECURSIVE subordinates AS (
                        SELECT id FROM users WHERE id = %s
                        UNION ALL
                        SELECT u.id FROM users u 
                        JOIN subordinates s ON u.parent_id = s.id
                    )
                    SELECT id FROM subordinates
                """, (test_supervisor_id,))
                cte_subordinates = set(row[0] for row in cursor.fetchall())
                print(f"   递归CTE方法: {len(cte_subordinates)} 个下属")
                print(f"   前10个ID: {sorted(list(cte_subordinates))[:10]}")
                
                # 对比差异
                only_in_hierarchy = hierarchy_subordinates - cte_subordinates
                only_in_cte = cte_subordinates - hierarchy_subordinates
                
                print(f"\n   差异分析:")
                print(f"   只在user_hierarchy中的ID数量: {len(only_in_hierarchy)}")
                print(f"   只在递归CTE中的ID数量: {len(only_in_cte)}")
                
                if only_in_hierarchy:
                    print(f"   只在user_hierarchy中的前10个ID: {sorted(list(only_in_hierarchy))[:10]}")
                if only_in_cte:
                    print(f"   只在递归CTE中的前10个ID: {sorted(list(only_in_cte))[:10]}")
                
            except mysql.connector.Error as e:
                print(f"   ❌ 递归CTE执行失败: {e}")
        
        # 3. 检查parent_id字段的数据情况
        if has_parent_id:
            print(f"\n4. 分析users表的parent_id数据:")
            
            cursor.execute("SELECT COUNT(*) FROM users WHERE parent_id IS NOT NULL")
            non_null_parent = cursor.fetchone()[0]
            print(f"   有parent_id的用户数量: {non_null_parent:,}")
            
            cursor.execute("SELECT COUNT(DISTINCT parent_id) FROM users WHERE parent_id IS NOT NULL")
            unique_parents = cursor.fetchone()[0]
            print(f"   不同的parent_id数量: {unique_parents:,}")
            
            # 检查parent_id的值范围
            cursor.execute("SELECT MIN(parent_id), MAX(parent_id) FROM users WHERE parent_id IS NOT NULL")
            result = cursor.fetchone()
            if result[0] is not None:
                print(f"   parent_id范围: {result[0]} - {result[1]}")
            
            # 检查循环引用
            cursor.execute("""
                SELECT COUNT(*) FROM users u1
                JOIN users u2 ON u1.parent_id = u2.id
                WHERE u2.parent_id = u1.id
            """)
            circular_refs = cursor.fetchone()[0]
            if circular_refs > 0:
                print(f"   ⚠️  发现 {circular_refs} 个循环引用")
            else:
                print("   ✅ 无循环引用")
        
        # 4. 对比查询财务数据的结果
        print(f"\n5. 对比查询用户{test_supervisor_id}可访问的财务数据:")
        
        # 使用user_hierarchy的查询
        cursor.execute("""
            SELECT COUNT(*) 
            FROM financial_funds f
            WHERE f.handle_by IN (
                SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
                UNION SELECT %s
            )
        """, (test_supervisor_id, test_supervisor_id))
        hierarchy_funds_count = cursor.fetchone()[0]
        print(f"   user_hierarchy方法: {hierarchy_funds_count:,} 条财务记录")
        
        # 使用递归CTE的查询（如果可用）
        if has_parent_id:
            try:
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
                """, (test_supervisor_id,))
                cte_funds_count = cursor.fetchone()[0]
                print(f"   递归CTE方法: {cte_funds_count:,} 条财务记录")
                
                # 计算差异
                difference = abs(hierarchy_funds_count - cte_funds_count)
                print(f"   差异: {difference:,} 条记录")
                
                if difference > 0:
                    percentage = (difference / max(hierarchy_funds_count, cte_funds_count)) * 100
                    print(f"   差异百分比: {percentage:.2f}%")
                
            except mysql.connector.Error as e:
                print(f"   ❌ 递归CTE财务查询失败: {e}")
        
        # 5. 检查物化视图与两种方法的对比
        print(f"\n6. 物化视图对比:")
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_supervisor_id,))
        mv_count = cursor.fetchone()[0]
        print(f"   物化视图中用户{test_supervisor_id}的记录数: {mv_count:,}")
        
        print(f"\n=== 结论 ===")
        if not has_parent_id:
            print("❌ users表缺少parent_id字段，递归CTE方法无法工作")
            print("✅ 应该使用基于user_hierarchy表的查询方法")
        else:
            if hierarchy_funds_count != cte_funds_count:
                print("❌ 两种方法返回的结果不一致")
                print("⚠️  需要检查数据一致性或选择正确的查询方法")
            else:
                print("✅ 两种方法返回相同结果")
        
        print(f"📊 物化视图记录数: {mv_count:,}")
        print(f"📊 user_hierarchy方法: {hierarchy_funds_count:,}")
        if has_parent_id and 'cte_funds_count' in locals():
            print(f"📊 递归CTE方法: {cte_funds_count:,}")
            
    except mysql.connector.Error as e:
        print(f"数据库查询错误: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    compare_cte_methods()