#!/usr/bin/env python3
import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv
from prettytable import PrettyTable
import statistics

# 加载环境变量
load_dotenv()

# 数据库连接配置
config = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456'),
    'database': os.getenv('DB_NAME_V2', 'finance')
}

def connect_db():
    """连接数据库"""
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def test_materialized_view(supervisor_id, page_size=20, iterations=5):
    """测试物化视图性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        
        # 总数查询
        cursor.execute("""
            SELECT COUNT(*) FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
        """, (supervisor_id,))
        total_count = cursor.fetchone()[0]
        
        # 分页查询
        cursor.execute("""
            SELECT fund_id, handle_by, handler_name, department, order_id, customer_id, amount
            FROM mv_supervisor_financial 
            WHERE supervisor_id = %s
            ORDER BY fund_id ASC
            LIMIT %s
        """, (supervisor_id, page_size))
        
        data = cursor.fetchall()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)
    
    cursor.close()
    conn.close()
    
    return {
        'method': '物化视图',
        'times': times,
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': total_count,
        'returned_records': len(data) if 'data' in locals() else 0
    }

def test_direct_join(supervisor_id, page_size=20, iterations=5):
    """测试直接JOIN性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        
        # 总数查询
        cursor.execute("""
            SELECT COUNT(*) 
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
        """, (supervisor_id,))
        total_count = cursor.fetchone()[0]
        
        # 分页查询
        cursor.execute("""
            SELECT f.fund_id, f.handle_by, u.name as handler_name, u.department, f.order_id, f.customer_id, f.amount
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
            WHERE h.user_id = %s
            ORDER BY f.fund_id ASC
            LIMIT %s
        """, (supervisor_id, page_size))
        
        data = cursor.fetchall()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)
    
    cursor.close()
    conn.close()
    
    return {
        'method': '直接JOIN',
        'times': times,
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': total_count,
        'returned_records': len(data) if 'data' in locals() else 0
    }

def test_recursive_cte(supervisor_id, page_size=20, iterations=5):
    """测试递归CTE性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        
        # 总数查询
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
        """, (supervisor_id, supervisor_id))
        total_count = cursor.fetchone()[0]
        
        # 分页查询
        cursor.execute("""
            WITH RECURSIVE subordinates AS (
                SELECT id FROM users WHERE id = %s
                UNION ALL
                SELECT u.id FROM users u 
                JOIN subordinates s ON u.parent_id = s.id
            )
            SELECT f.fund_id, f.handle_by, u.name as handler_name, u.department, f.order_id, f.customer_id, f.amount
            FROM financial_funds f
            JOIN users u ON f.handle_by = u.id
            WHERE f.handle_by IN (SELECT id FROM subordinates WHERE id != %s)
            ORDER BY f.fund_id ASC
            LIMIT %s
        """, (supervisor_id, supervisor_id, page_size))
        
        data = cursor.fetchall()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)
    
    cursor.close()
    conn.close()
    
    return {
        'method': '递归CTE',
        'times': times,
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': total_count,
        'returned_records': len(data) if 'data' in locals() else 0
    }

def test_optimized_hierarchy(supervisor_id, page_size=20, iterations=5):
    """测试优化的层级查询性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        
        # 先获取下属列表
        cursor.execute("""
            SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
        """, (supervisor_id,))
        subordinates = [row[0] for row in cursor.fetchall()]
        
        if not subordinates:
            subordinates = [supervisor_id]
        
        # 构建IN子句
        placeholders = ', '.join(['%s'] * len(subordinates))
        
        # 总数查询
        cursor.execute(f"""
            SELECT COUNT(*) FROM financial_funds 
            WHERE handle_by IN ({placeholders})
        """, subordinates)
        total_count = cursor.fetchone()[0]
        
        # 分页查询
        cursor.execute(f"""
            SELECT f.fund_id, f.handle_by, u.name as handler_name, u.department, f.order_id, f.customer_id, f.amount
            FROM financial_funds f
            JOIN users u ON f.handle_by = u.id
            WHERE f.handle_by IN ({placeholders})
            ORDER BY f.fund_id ASC
            LIMIT %s
        """, subordinates + [page_size])
        
        data = cursor.fetchall()
        
        end_time = time.time()
        times.append((end_time - start_time) * 1000)
    
    cursor.close()
    conn.close()
    
    return {
        'method': '优化层级查询',
        'times': times,
        'avg_time': statistics.mean(times),
        'min_time': min(times),
        'max_time': max(times),
        'total_records': total_count,
        'returned_records': len(data) if 'data' in locals() else 0
    }

def get_test_users(limit=5):
    """获取测试用户"""
    conn = connect_db()
    if not conn:
        return []
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT supervisor_id, COUNT(*) as record_count
        FROM mv_supervisor_financial
        GROUP BY supervisor_id
        ORDER BY record_count DESC
        LIMIT %s
    """, (limit,))
    
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return results

def display_user_info(supervisor_id):
    """显示用户信息"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    # 获取用户基本信息
    cursor.execute("SELECT id, name, role, department FROM users WHERE id = %s", (supervisor_id,))
    user_info = cursor.fetchone()
    
    if not user_info:
        cursor.close()
        conn.close()
        return None
    
    # 获取下属数量
    cursor.execute("SELECT COUNT(*) FROM user_hierarchy WHERE user_id = %s", (supervisor_id,))
    subordinate_count = cursor.fetchone()[0]
    
    # 获取可访问记录数
    cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial WHERE supervisor_id = %s", (supervisor_id,))
    record_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return {
        'id': user_info[0],
        'name': user_info[1],
        'role': user_info[2],
        'department': user_info[3],
        'subordinate_count': subordinate_count,
        'record_count': record_count
    }

def run_comprehensive_test(supervisor_id, page_size=20, iterations=5):
    """运行综合性能测试"""
    print(f"\n{'='*80}")
    print(f"综合性能测试 - 用户ID: {supervisor_id}")
    print(f"{'='*80}")
    
    # 显示用户信息
    user_info = display_user_info(supervisor_id)
    if user_info:
        print(f"用户: {user_info['name']} ({user_info['role']}) - {user_info['department']}")
        print(f"下属数量: {user_info['subordinate_count']}")
        print(f"可访问记录数: {user_info['record_count']}")
    else:
        print(f"用户ID {supervisor_id} 不存在")
        return
    
    print(f"\n测试参数: 页大小={page_size}, 迭代次数={iterations}")
    
    # 测试所有方法
    methods = [
        ('物化视图', test_materialized_view),
        ('直接JOIN', test_direct_join),
        ('优化层级查询', test_optimized_hierarchy),
        ('递归CTE', test_recursive_cte)
    ]
    
    results = []
    
    for method_name, test_func in methods:
        print(f"\n正在测试 {method_name}...")
        try:
            result = test_func(supervisor_id, page_size, iterations)
            if result:
                results.append(result)
                print(f"✅ {method_name} 测试完成")
            else:
                print(f"❌ {method_name} 测试失败")
        except Exception as e:
            print(f"❌ {method_name} 测试出错: {e}")
    
    if results:
        display_comparison_results(results)
    else:
        print("❌ 所有测试都失败了")

def display_comparison_results(results):
    """显示对比结果"""
    print(f"\n{'='*80}")
    print("性能对比结果")
    print(f"{'='*80}")
    
    # 创建结果表格
    table = PrettyTable()
    table.field_names = ["方法", "平均耗时(ms)", "最小耗时(ms)", "最大耗时(ms)", "总记录数", "返回记录数"]
    
    # 按平均时间排序
    results.sort(key=lambda x: x['avg_time'])
    
    for result in results:
        table.add_row([
            result['method'],
            f"{result['avg_time']:.2f}",
            f"{result['min_time']:.2f}",
            f"{result['max_time']:.2f}",
            result['total_records'],
            result['returned_records']
        ])
    
    print(table)
    
    # 性能提升分析
    if len(results) > 1:
        fastest = results[0]
        print(f"\n🏆 最快方法: {fastest['method']} ({fastest['avg_time']:.2f}ms)")
        
        print(f"\n📊 相对性能提升:")
        for i, result in enumerate(results[1:], 1):
            speedup = result['avg_time'] / fastest['avg_time']
            print(f"   {fastest['method']} 比 {result['method']} 快 {speedup:.2f}x")
    
    # 数据一致性检查
    print(f"\n🔍 数据一致性检查:")
    total_records = [r['total_records'] for r in results]
    returned_records = [r['returned_records'] for r in results]
    
    if len(set(total_records)) == 1:
        print(f"   ✅ 所有方法返回相同的总记录数: {total_records[0]}")
    else:
        print(f"   ❌ 总记录数不一致: {set(total_records)}")
    
    if len(set(returned_records)) == 1:
        print(f"   ✅ 所有方法返回相同的页面记录数: {returned_records[0]}")
    else:
        print(f"   ❌ 页面记录数不一致: {set(returned_records)}")

def main():
    parser = argparse.ArgumentParser(description="综合性能测试")
    parser.add_argument("--list", action="store_true", help="列出管理记录最多的用户")
    parser.add_argument("--supervisor_id", type=int, help="指定要测试的用户ID")
    parser.add_argument("--page_size", type=int, default=20, help="每页记录数")
    parser.add_argument("--iterations", type=int, default=5, help="重复测试次数")
    parser.add_argument("--all", action="store_true", help="测试多个用户")
    
    args = parser.parse_args()
    
    if args.list:
        users = get_test_users(limit=10)
        if not users:
            print("未找到用户")
            return
        
        print("\n=== 管理记录最多的用户 ===")
        table = PrettyTable(["用户ID", "可访问记录数"])
        
        for user_id, record_count in users:
            table.add_row([user_id, record_count])
        
        print(table)
        return
    
    if args.all:
        # 测试多个用户
        users = get_test_users(limit=3)
        if not users:
            print("未找到测试用户")
            return
        
        print(f"将测试 {len(users)} 个用户的性能")
        
        for user_id, record_count in users:
            run_comprehensive_test(user_id, args.page_size, args.iterations)
        
        return
    
    if not args.supervisor_id:
        print("请使用 --supervisor_id 指定要测试的用户ID")
        print("或使用 --list 查看可用用户")
        print("或使用 --all 测试多个用户")
        return
    
    # 测试单个用户
    run_comprehensive_test(args.supervisor_id, args.page_size, args.iterations)

if __name__ == "__main__":
    main()