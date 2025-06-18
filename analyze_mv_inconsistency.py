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

def analyze_mv_inconsistency():
    """分析物化视图数据不一致问题"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 物化视图数据不一致问题分析 ===\n")
        
        test_user_id = 70
        
        # 1. 检查物化视图的构建SQL
        print("1. 当前物化视图构建逻辑:")
        print("   INSERT INTO mv_supervisor_financial")
        print("   FROM user_hierarchy h")
        print("   JOIN financial_funds f ON h.subordinate_id = f.handle_by")
        print("   JOIN users u ON f.handle_by = u.id")
        print("   WHERE h.depth > 0")  # 注意这里是 depth > 0
        
        # 2. 对比不同查询条件的结果
        print(f"\n2. 用户{test_user_id}的层级关系分析:")
        
        # 检查该用户的层级数据
        cursor.execute("""
            SELECT depth, COUNT(*) as count
            FROM user_hierarchy 
            WHERE user_id = %s
            GROUP BY depth
            ORDER BY depth
        """, (test_user_id,))
        
        depth_stats = cursor.fetchall()
        print("   层级分布:")
        for depth, count in depth_stats:
            print(f"     深度 {depth}: {count} 个下属")
        
        # 3. 分别查询不同depth条件的结果
        print(f"\n3. 不同depth条件的查询结果:")
        
        # depth > 0 (物化视图使用的条件)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s AND h.depth > 0
        """, (test_user_id,))
        depth_gt_0 = cursor.fetchone()[0]
        
        # depth >= 0 (其他方法可能使用的条件)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s AND h.depth >= 0
        """, (test_user_id,))
        depth_gte_0 = cursor.fetchone()[0]
        
        # 不加depth条件
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (test_user_id,))
        no_depth_filter = cursor.fetchone()[0]
        
        print(f"   depth > 0:  {depth_gt_0:,} 条记录")
        print(f"   depth >= 0: {depth_gte_0:,} 条记录") 
        print(f"   无depth过滤: {no_depth_filter:,} 条记录")
        
        # 4. 检查其他方法使用的条件
        print(f"\n4. 其他查询方法使用的条件分析:")
        
        # 直接JOIN方法
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (test_user_id,))
        direct_join = cursor.fetchone()[0]
        
        # 优化层级查询方法
        cursor.execute("""
            SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
        """, (test_user_id,))
        subordinates = [row[0] for row in cursor.fetchall()]
        
        if subordinates:
            placeholders = ', '.join(['%s'] * len(subordinates))
            cursor.execute(f"""
                SELECT COUNT(*) FROM financial_funds 
                WHERE handle_by IN ({placeholders})
            """, subordinates)
            optimized_query = cursor.fetchone()[0]
        else:
            optimized_query = 0
        
        print(f"   直接JOIN: {direct_join:,} 条记录")
        print(f"   优化查询: {optimized_query:,} 条记录")
        
        # 5. 检查物化视图实际数据
        cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (test_user_id,))
        mv_actual = cursor.fetchone()[0]
        print(f"   物化视图: {mv_actual:,} 条记录")
        
        # 6. 问题诊断
        print(f"\n5. 问题诊断:")
        if mv_actual == depth_gt_0:
            print("   ✅ 物化视图与 depth > 0 查询一致")
        else:
            print(f"   ❌ 物化视图与 depth > 0 查询不一致 (差异: {abs(mv_actual - depth_gt_0)})")
        
        if direct_join == no_depth_filter:
            print("   ✅ 直接JOIN查询无depth过滤")
        else:
            print("   ❌ 直接JOIN查询与预期不符")
        
        if optimized_query == no_depth_filter:
            print("   ✅ 优化查询与无depth过滤一致")
        else:
            print("   ❌ 优化查询存在问题")
        
        # 7. 找出差异的具体记录
        print(f"\n6. 差异记录分析:")
        
        # 找出depth=0但有财务数据的情况
        cursor.execute("""
            SELECT h.depth, COUNT(*) as count
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            WHERE h.user_id = %s
            GROUP BY h.depth
            ORDER BY h.depth
        """, (test_user_id,))
        
        depth_fund_stats = cursor.fetchall()
        print("   各深度的财务记录分布:")
        for depth, count in depth_fund_stats:
            print(f"     深度 {depth}: {count} 条财务记录")
        
        # 8. 检查是否存在depth=0的情况
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            WHERE h.user_id = %s AND h.depth = 0
        """, (test_user_id,))
        depth_0_records = cursor.fetchone()[0]
        
        print(f"\n   深度0的记录数: {depth_0_records:,}")
        
        if depth_0_records > 0:
            print("   💡 发现问题：物化视图排除了depth=0的记录")
            print("      这可能是supervisor自己处理的财务记录")
            
            # 检查depth=0的记录是什么
            cursor.execute("""
                SELECT h.subordinate_id, u.name, COUNT(*) as fund_count
                FROM user_hierarchy h
                JOIN financial_funds f ON h.subordinate_id = f.handle_by
                JOIN users u ON h.subordinate_id = u.id
                WHERE h.user_id = %s AND h.depth = 0
                GROUP BY h.subordinate_id, u.name
                LIMIT 5
            """, (test_user_id,))
            
            depth_0_details = cursor.fetchall()
            print("      深度0的记录详情（前5条）:")
            for sub_id, name, fund_count in depth_0_details:
                print(f"        用户{sub_id}({name}): {fund_count}条财务记录")
        
        # 9. 修复建议
        print(f"\n7. 修复建议:")
        print("   问题根源：物化视图使用 'WHERE h.depth > 0'")
        print("   其他查询使用所有层级关系（包括depth=0）")
        print()
        print("   解决方案：")
        print("   1. 修改物化视图构建SQL，改为 'WHERE h.depth >= 0'")
        print("   2. 或者统一所有查询都使用 'WHERE h.depth > 0'")
        print("   3. 需要确认业务逻辑：supervisor是否应该看到自己处理的记录")
        
        return depth_0_records > 0
        
    except mysql.connector.Error as e:
        print(f"❌ 分析过程中出错: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    has_depth_0_issue = analyze_mv_inconsistency()
    
    if has_depth_0_issue:
        print(f"\n{'='*60}")
        print("🔧 准备修复物化视图...")
        print("需要决定是否包含depth=0的记录")
        print("请检查业务需求后运行修复脚本")
    else:
        print(f"\n{'='*60}")
        print("🤔 未发现明显的depth相关问题")
        print("需要进一步分析数据不一致的原因")