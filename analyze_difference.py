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

def analyze_difference():
    """详细分析CTE和user_hierarchy的差异"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 深度分析CTE与user_hierarchy的差异 ===\n")
        
        test_user_id = 1
        
        # 1. 获取两种方法的下属列表
        print("1. 获取两种方法的下属ID列表...")
        
        # user_hierarchy方法的下属
        cursor.execute("""
            SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
        """, (test_user_id,))
        hierarchy_subordinates = set(row[0] for row in cursor.fetchall())
        
        # 递归CTE方法的下属
        cursor.execute("""
            WITH RECURSIVE subordinates AS (
                SELECT id FROM users WHERE id = %s
                UNION ALL
                SELECT u.id FROM users u 
                JOIN subordinates s ON u.parent_id = s.id
            )
            SELECT id FROM subordinates WHERE id != %s
        """, (test_user_id, test_user_id))
        cte_subordinates = set(row[0] for row in cursor.fetchall())
        
        print(f"   user_hierarchy方法: {len(hierarchy_subordinates):,} 个下属")
        print(f"   递归CTE方法: {len(cte_subordinates):,} 个下属")
        
        # 2. 分析差异
        only_in_hierarchy = hierarchy_subordinates - cte_subordinates
        only_in_cte = cte_subordinates - hierarchy_subordinates
        common_subordinates = hierarchy_subordinates & cte_subordinates
        
        print(f"\n2. 下属差异分析:")
        print(f"   共同下属: {len(common_subordinates):,}")
        print(f"   只在user_hierarchy中: {len(only_in_hierarchy):,}")
        print(f"   只在递归CTE中: {len(only_in_cte):,}")
        
        # 3. 检查为什么递归CTE会包含这么多用户
        print(f"\n3. 分析递归CTE为什么包含 {len(cte_subordinates):,} 个下属:")
        
        # 检查递归层级分布
        cursor.execute("""
            WITH RECURSIVE subordinates AS (
                SELECT id, 0 as level FROM users WHERE id = %s
                UNION ALL
                SELECT u.id, s.level + 1 FROM users u 
                JOIN subordinates s ON u.parent_id = s.id
                WHERE s.level < 10
            )
            SELECT level, COUNT(*) as count
            FROM subordinates
            WHERE id != %s
            GROUP BY level
            ORDER BY level
        """, (test_user_id, test_user_id))
        
        cte_levels = cursor.fetchall()
        for level, count in cte_levels:
            print(f"   递归层级 {level}: {count:,} 个用户")
        
        # 4. 检查users表的parent_id分布
        print(f"\n4. 检查users表的parent_id分布:")
        cursor.execute("""
            SELECT parent_id, COUNT(*) as child_count
            FROM users
            WHERE parent_id IS NOT NULL
            GROUP BY parent_id
            ORDER BY child_count DESC
            LIMIT 10
        """)
        parent_stats = cursor.fetchall()
        print("   子用户最多的前10个parent_id:")
        for parent_id, child_count in parent_stats:
            print(f"     parent_id {parent_id}: {child_count:,} 个子用户")
        
        # 5. 检查用户1在parent_id中的情况
        cursor.execute("SELECT COUNT(*) FROM users WHERE parent_id = %s", (test_user_id,))
        direct_children = cursor.fetchone()[0]
        print(f"\n   用户{test_user_id}直接子用户数: {direct_children:,}")
        
        # 6. 分析财务数据差异
        print(f"\n5. 分析财务数据访问差异:")
        
        # 检查只在CTE中的用户有多少财务数据
        if only_in_cte:
            # 限制查询只检查前1000个用户，避免性能问题
            sample_cte_only = list(only_in_cte)[:1000]
            placeholders = ','.join(['%s'] * len(sample_cte_only))
            
            cursor.execute(f"""
                SELECT COUNT(*) FROM financial_funds
                WHERE handle_by IN ({placeholders})
            """, sample_cte_only)
            
            cte_only_funds = cursor.fetchone()[0]
            print(f"   只在CTE中的用户(前1000个)的财务记录: {cte_only_funds:,}")
        
        # 检查共同用户的财务数据
        if common_subordinates:
            sample_common = list(common_subordinates)[:1000]
            placeholders = ','.join(['%s'] * len(sample_common))
            
            cursor.execute(f"""
                SELECT COUNT(*) FROM financial_funds
                WHERE handle_by IN ({placeholders})
            """, sample_common)
            
            common_funds = cursor.fetchone()[0]
            print(f"   共同下属(前1000个)的财务记录: {common_funds:,}")
        
        # 7. 检查为什么user_hierarchy只有这么少的关系
        print(f"\n6. 分析user_hierarchy表的构建逻辑:")
        cursor.execute("""
            SELECT user_id, COUNT(*) as subordinate_count
            FROM user_hierarchy
            WHERE depth = 1
            GROUP BY user_id
            ORDER BY subordinate_count DESC
            LIMIT 10
        """)
        
        hierarchy_stats = cursor.fetchall()
        print("   下属最多的前10个用户:")
        for user_id, sub_count in hierarchy_stats:
            print(f"     用户 {user_id}: {sub_count:,} 个直接下属")
        
        # 8. 检查数据一致性问题
        print(f"\n7. 数据一致性检查:")
        
        # 检查parent_id指向的用户是否都存在
        cursor.execute("""
            SELECT COUNT(*) FROM users u1
            WHERE u1.parent_id IS NOT NULL
            AND u1.parent_id NOT IN (SELECT id FROM users)
        """)
        invalid_parents = cursor.fetchone()[0]
        print(f"   无效的parent_id数量: {invalid_parents:,}")
        
        # 检查循环引用（更深层的检查）
        cursor.execute("""
            SELECT COUNT(*) FROM users u1
            JOIN users u2 ON u1.parent_id = u2.id
            WHERE u2.parent_id = u1.id
        """)
        circular_refs = cursor.fetchone()[0]
        print(f"   直接循环引用: {circular_refs:,}")
        
        # 8. 结论分析
        print(f"\n=== 结论分析 ===")
        print("基于以上分析，差异的主要原因是:")
        print("1. 递归CTE基于users.parent_id构建了一个非常深的层级树")
        print("2. user_hierarchy表只包含直接的父子关系（depth=1）")
        print("3. 这两种数据结构代表了不同的业务逻辑:")
        print("   - users.parent_id: 可能是组织架构的完整层级")
        print("   - user_hierarchy: 可能是权限管理的特定关系")
        
        if direct_children > len(hierarchy_subordinates):
            print(f"\n⚠️  发现问题: 用户{test_user_id}在users表中有{direct_children:,}个直接下属")
            print(f"   但在user_hierarchy表中只有{len(hierarchy_subordinates):,}个下属")
            print("   这表明user_hierarchy表的数据不完整")
        
        print(f"\n建议:")
        print("1. 如果要基于完整组织架构，应该使用递归CTE")
        print("2. 如果要基于特定权限关系，应该使用user_hierarchy")
        print("3. 需要明确业务需求来决定使用哪种方法")
        
    except mysql.connector.Error as e:
        print(f"❌ 分析过程中出错: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    analyze_difference()