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

def verify_materialized_view():
    """验证物化视图数据一致性"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("=== 数据库表基础信息 ===")
        
        # 检查各表的行数
        tables = ['user_hierarchy', 'financial_funds', 'users', 'mv_supervisor_financial']
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count:,} 行")
            except mysql.connector.Error as e:
                print(f"{table}: 表不存在或查询失败 - {e}")
        
        print("\n=== 检查JOIN条件的数据匹配情况 ===")
        
        # 检查user_hierarchy表的基本信息
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_hierarchy WHERE depth >= 0")
        supervisors_count = cursor.fetchone()[0]
        print(f"user_hierarchy中不同的supervisor数量: {supervisors_count:,}")
        
        cursor.execute("SELECT COUNT(DISTINCT subordinate_id) FROM user_hierarchy WHERE depth >= 0")
        subordinates_count = cursor.fetchone()[0]
        print(f"user_hierarchy中不同的subordinate数量: {subordinates_count:,}")
        
        # 检查financial_funds表的基本信息
        cursor.execute("SELECT COUNT(*) FROM financial_funds")
        funds_count = cursor.fetchone()[0]
        print(f"financial_funds总记录数: {funds_count:,}")
        
        cursor.execute("SELECT COUNT(DISTINCT handle_by) FROM financial_funds")
        handlers_count = cursor.fetchone()[0]
        print(f"financial_funds中不同的handle_by数量: {handlers_count:,}")
        
        # 检查users表的基本信息
        cursor.execute("SELECT COUNT(*) FROM users")
        users_count = cursor.fetchone()[0]
        print(f"users表总记录数: {users_count:,}")
        
        print("\n=== 检查JOIN匹配情况 ===")
        
        # 检查有多少subordinate_id能在financial_funds的handle_by中找到匹配
        cursor.execute("""
            SELECT COUNT(DISTINCT h.subordinate_id)
            FROM user_hierarchy h
            WHERE h.depth >= 0 
            AND h.subordinate_id IN (SELECT DISTINCT handle_by FROM financial_funds)
        """)
        matched_subordinates = cursor.fetchone()[0]
        print(f"能在financial_funds中找到匹配的subordinate数量: {matched_subordinates:,}")
        
        # 检查有多少handle_by能在users表中找到匹配
        cursor.execute("""
            SELECT COUNT(DISTINCT f.handle_by)
            FROM financial_funds f
            WHERE f.handle_by IN (SELECT id FROM users)
        """)
        matched_handlers = cursor.fetchone()[0]
        print(f"能在users表中找到匹配的handle_by数量: {matched_handlers:,}")
        
        print("\n=== 验证物化视图SQL结果 ===")
        
        # 执行物化视图的SELECT语句（不带LIMIT）
        materialized_view_sql = """
        SELECT COUNT(*)
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        JOIN users u ON f.handle_by = u.id
        WHERE h.depth >= 0
        """
        
        cursor.execute(materialized_view_sql)
        expected_count = cursor.fetchone()[0]
        print(f"物化视图SQL预期结果数量: {expected_count:,}")
        
        # 检查当前物化视图的实际行数
        try:
            cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
            actual_count = cursor.fetchone()[0]
            print(f"物化视图实际行数: {actual_count:,}")
            
            if expected_count == actual_count:
                print("✅ 物化视图数据一致！")
            else:
                print(f"❌ 物化视图数据不一致！差异: {abs(expected_count - actual_count):,} 行")
                
                # 检查是否因为LIMIT导致的截断
                if expected_count > 1000000:
                    print(f"⚠️  原始数据超过LIMIT 1000000，可能被截断")
                    
        except mysql.connector.Error as e:
            print(f"❌ 无法查询物化视图: {e}")
        
        print("\n=== 数据抽样检查 ===")
        
        # 抽取几条记录对比
        cursor.execute("""
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
        LIMIT 5
        """)
        
        sample_data = cursor.fetchall()
        print("原始JOIN查询前5条记录:")
        for row in sample_data:
            print(f"  supervisor_id={row[0]}, fund_id={row[1]}, handle_by={row[2]}, handler_name={row[3]}")
        
        try:
            cursor.execute("SELECT supervisor_id, fund_id, handle_by, handler_name FROM mv_supervisor_financial LIMIT 5")
            mv_data = cursor.fetchall()
            print("\n物化视图前5条记录:")
            for row in mv_data:
                print(f"  supervisor_id={row[0]}, fund_id={row[1]}, handle_by={row[2]}, handler_name={row[3]}")
        except mysql.connector.Error as e:
            print(f"无法查询物化视图样本数据: {e}")
            
    except mysql.connector.Error as e:
        print(f"数据库查询错误: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("开始验证物化视图数据...")
    verify_materialized_view()
    print("\n验证完成！")