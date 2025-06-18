#!/usr/bin/env python3
import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv
from prettytable import PrettyTable

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

def test_materialized_view_performance(supervisor_id, page=1, page_size=10, sort_by="fund_id", sort_order="ASC", iterations=3):
    """测试物化视图查询性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    results = []
    
    # 处理排序
    valid_sort_fields = ["fund_id", "amount", "handle_by", "order_id", "customer_id"]
    valid_sort_orders = ["ASC", "DESC"]
    
    if sort_by not in valid_sort_fields:
        sort_by = "fund_id"
    
    if sort_order not in valid_sort_orders:
        sort_order = "ASC"
    
    for i in range(iterations):
        # 测试总数查询性能
        start_time = time.time()
        
        count_query = """
        SELECT COUNT(*) as total 
        FROM mv_supervisor_financial
        WHERE supervisor_id = %s
        """
        
        cursor.execute(count_query, (supervisor_id,))
        result = cursor.fetchone()
        total_records = result['total'] if result else 0
        
        count_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 测试分页查询性能
        start_time = time.time()
        offset = (page - 1) * page_size
        
        data_query = f"""
        SELECT fund_id, handle_by, handler_name, department, order_id, customer_id, amount
        FROM mv_supervisor_financial
        WHERE supervisor_id = %s
        ORDER BY {sort_by} {sort_order}
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(data_query, (supervisor_id, page_size, offset))
        data = cursor.fetchall()
        
        data_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 执行计划分析
        if i == 0:  # 只在第一次迭代时获取执行计划
            try:
                explain_query = f"EXPLAIN {data_query}"
                cursor.execute(explain_query, (supervisor_id, page_size, offset))
                explain_results = cursor.fetchall()
            except mysql.connector.Error as e:
                explain_results = [{"error": str(e)}]
        
        # 记录结果
        iteration_result = {
            "iteration": i + 1,
            "count_time": count_time,
            "data_time": data_time,
            "total_time": count_time + data_time,
            "total_records": total_records,
            "returned_records": len(data)
        }
        
        if i == 0:
            iteration_result["explain"] = explain_results
        
        results.append(iteration_result)
    
    cursor.close()
    conn.close()
    
    return results

def test_direct_join_performance(supervisor_id, page=1, page_size=10, sort_by="fund_id", sort_order="ASC", iterations=3):
    """测试直接JOIN查询性能（不使用物化视图）"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    results = []
    
    # 处理排序
    valid_sort_fields = ["fund_id", "amount", "handle_by", "order_id", "customer_id"]
    valid_sort_orders = ["ASC", "DESC"]
    
    if sort_by not in valid_sort_fields:
        sort_by = "fund_id"
    
    if sort_order not in valid_sort_orders:
        sort_order = "ASC"
    
    for i in range(iterations):
        # 测试总数查询性能
        start_time = time.time()
        
        count_query = """
        SELECT COUNT(*) as total 
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        JOIN users u ON f.handle_by = u.id
        WHERE h.user_id = %s
        """
        
        cursor.execute(count_query, (supervisor_id,))
        result = cursor.fetchone()
        total_records = result['total'] if result else 0
        
        count_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 测试分页查询性能
        start_time = time.time()
        offset = (page - 1) * page_size
        
        data_query = f"""
        SELECT f.fund_id, f.handle_by, u.name as handler_name, u.department, f.order_id, f.customer_id, f.amount
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        JOIN users u ON f.handle_by = u.id
        WHERE h.user_id = %s
        ORDER BY f.{sort_by} {sort_order}
        LIMIT %s OFFSET %s
        """
        
        cursor.execute(data_query, (supervisor_id, page_size, offset))
        data = cursor.fetchall()
        
        data_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 执行计划分析
        if i == 0:  # 只在第一次迭代时获取执行计划
            try:
                explain_query = f"EXPLAIN {data_query}"
                cursor.execute(explain_query, (supervisor_id, page_size, offset))
                explain_results = cursor.fetchall()
            except mysql.connector.Error as e:
                explain_results = [{"error": str(e)}]
        
        # 记录结果
        iteration_result = {
            "iteration": i + 1,
            "count_time": count_time,
            "data_time": data_time,
            "total_time": count_time + data_time,
            "total_records": total_records,
            "returned_records": len(data)
        }
        
        if i == 0:
            iteration_result["explain"] = explain_results
        
        results.append(iteration_result)
    
    cursor.close()
    conn.close()
    
    return results

def display_test_results(results, method_name):
    """显示测试结果"""
    if not results:
        return
    
    print(f"\n=== {method_name} 性能测试结果 ===")
    
    # 显示详细结果
    for i, result in enumerate(results):
        if i > 0:
            print("\n" + "-" * 40)
        
        print(f"\n迭代 {result['iteration']}:")
        print(f"获取总数用时: {result['count_time']:.2f}ms")
        print(f"获取数据用时: {result['data_time']:.2f}ms")
        print(f"总执行时间: {result['total_time']:.2f}ms")
        print(f"总记录数: {result['total_records']}")
        print(f"返回记录数: {result['returned_records']}")
        
        if i == 0 and "explain" in result:  # 只显示第一次迭代的执行计划
            print("\n查询执行计划:")
            for j, plan in enumerate(result['explain']):
                print(f"步骤 {j+1}: {plan}")
    
    # 计算平均值
    avg_count_time = sum(r['count_time'] for r in results) / len(results)
    avg_data_time = sum(r['data_time'] for r in results) / len(results)
    avg_total_time = sum(r['total_time'] for r in results) / len(results)
    
    print(f"\n=== {method_name} 平均性能 ===")
    print(f"获取总数平均用时: {avg_count_time:.2f}ms")
    print(f"获取数据平均用时: {avg_data_time:.2f}ms")
    print(f"总执行平均时间: {avg_total_time:.2f}ms")
    
    return avg_total_time

def find_supervisors(limit=10):
    """查找主管用户"""
    conn = connect_db()
    if not conn:
        return []
    
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT supervisor_id, COUNT(*) as record_count
    FROM mv_supervisor_financial
    GROUP BY supervisor_id
    ORDER BY record_count DESC
    LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results

def display_supervisor_info(supervisor_id):
    """显示主管信息"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    
    # 获取用户信息
    query = """
    SELECT id, name, role, department
    FROM users
    WHERE id = %s
    """
    
    cursor.execute(query, (supervisor_id,))
    supervisor = cursor.fetchone()
    
    if not supervisor:
        print(f"未找到ID为 {supervisor_id} 的用户")
        cursor.close()
        conn.close()
        return
    
    # 获取下属数量和可访问记录数
    cursor.execute("SELECT COUNT(*) as count FROM user_hierarchy WHERE user_id = %s", (supervisor_id,))
    subordinate_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM mv_supervisor_financial WHERE supervisor_id = %s", (supervisor_id,))
    record_count = cursor.fetchone()['count']
    
    print(f"\n=== 用户信息 ===")
    print(f"ID: {supervisor['id']}")
    print(f"姓名: {supervisor['name']}")
    print(f"角色: {supervisor['role']}")
    print(f"部门: {supervisor['department']}")
    print(f"下属数量: {subordinate_count}")
    print(f"可访问记录数: {record_count}")
    
    cursor.close()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="物化视图性能测试")
    parser.add_argument("--list", action="store_true", help="列出管理记录最多的用户")
    parser.add_argument("--supervisor_id", type=int, help="指定要测试的用户ID")
    parser.add_argument("--page", type=int, default=1, help="页码")
    parser.add_argument("--page_size", type=int, default=10, help="每页记录数")
    parser.add_argument("--sort_by", type=str, default="fund_id", 
                        choices=["fund_id", "amount", "handle_by", "order_id", "customer_id"], 
                        help="排序字段")
    parser.add_argument("--sort_order", type=str, default="ASC", choices=["ASC", "DESC"], help="排序方向")
    parser.add_argument("--iterations", type=int, default=3, help="重复测试次数")
    parser.add_argument("--compare", action="store_true", help="同时测试物化视图和直接JOIN的性能")
    
    args = parser.parse_args()
    
    if args.list:
        supervisors = find_supervisors(limit=20)
        if not supervisors:
            print("未找到用户")
            return
        
        print("\n=== 管理记录最多的用户 ===")
        table = PrettyTable(["用户ID", "可访问记录数"])
        
        for supervisor in supervisors:
            table.add_row([
                supervisor['supervisor_id'],
                supervisor['record_count']
            ])
        
        print(table)
        return
    
    if not args.supervisor_id:
        print("请使用 --supervisor_id 指定要测试的用户ID")
        return
    
    # 显示用户信息
    display_supervisor_info(args.supervisor_id)
    
    print(f"\n开始性能测试...")
    print(f"页码: {args.page}, 每页记录数: {args.page_size}")
    print(f"排序: {args.sort_by} {args.sort_order}")
    print(f"重复次数: {args.iterations}")
    
    if args.compare:
        # 对比测试
        print(f"\n{'='*60}")
        print("开始对比测试：物化视图 vs 直接JOIN")
        print(f"{'='*60}")
        
        # 测试物化视图
        mv_results = test_materialized_view_performance(
            args.supervisor_id,
            args.page,
            args.page_size,
            args.sort_by,
            args.sort_order,
            args.iterations
        )
        
        mv_avg_time = display_test_results(mv_results, "物化视图查询") if mv_results else float('inf')
        
        # 测试直接JOIN
        join_results = test_direct_join_performance(
            args.supervisor_id,
            args.page,
            args.page_size,
            args.sort_by,
            args.sort_order,
            args.iterations
        )
        
        join_avg_time = display_test_results(join_results, "直接JOIN查询") if join_results else float('inf')
        
        # 性能对比总结
        print(f"\n{'='*60}")
        print("性能对比总结")
        print(f"{'='*60}")
        print(f"物化视图平均耗时: {mv_avg_time:.2f}ms")
        print(f"直接JOIN平均耗时: {join_avg_time:.2f}ms")
        
        if mv_avg_time < join_avg_time:
            speedup = join_avg_time / mv_avg_time
            print(f"🎉 物化视图比直接JOIN快 {speedup:.2f}x")
        elif join_avg_time < mv_avg_time:
            speedup = mv_avg_time / join_avg_time
            print(f"⚠️ 直接JOIN比物化视图快 {speedup:.2f}x")
        else:
            print("两种方法性能相当")
        
    else:
        # 只测试物化视图
        results = test_materialized_view_performance(
            args.supervisor_id,
            args.page,
            args.page_size,
            args.sort_by,
            args.sort_order,
            args.iterations
        )
        
        if results:
            display_test_results(results, "物化视图查询")
        else:
            print("测试失败，请检查数据库连接和用户ID")

if __name__ == "__main__":
    main()