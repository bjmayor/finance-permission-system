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

def find_supervisors(limit=10):
    """查找主管用户"""
    conn = connect_db()
    if not conn:
        return []
    
    cursor = conn.cursor(dictionary=True)
    query = """
    SELECT u.id, u.name, u.department, COUNT(h.subordinate_id) as subordinate_count
    FROM users u
    JOIN user_hierarchy h ON u.id = h.user_id
    WHERE u.role = 'supervisor' AND h.depth > 0
    GROUP BY u.id, u.name, u.department
    ORDER BY subordinate_count DESC
    LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results

def test_supervisor_query(supervisor_id, page=1, page_size=10, sort_by="fund_id", sort_order="ASC", iterations=3):
    """测试主管权限下的财务列表查询性能"""
    conn = connect_db()
    if not conn:
        return None
    
    cursor = conn.cursor(dictionary=True)
    results = []
    
    for i in range(iterations):
        # 获取主管的下属
        start_time = time.time()
        
        subordinate_query = """
        SELECT subordinate_id 
        FROM user_hierarchy 
        WHERE user_id = %s AND depth > 0
        """
        cursor.execute(subordinate_query, (supervisor_id,))
        subordinates = cursor.fetchall()
        
        subordinate_ids = [row['subordinate_id'] for row in subordinates]
        if not subordinate_ids:
            # 如果没有下属，则只能看自己的数据
            subordinate_ids = [supervisor_id]
        
        subordinate_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 构建IN子句
        placeholders = ', '.join(['%s'] * len(subordinate_ids))
        
        # 计算总记录数
        start_time = time.time()
        
        count_query = f"""
        SELECT COUNT(*) as total 
        FROM financial_funds f
        WHERE f.handle_by IN ({placeholders})
        """
        
        cursor.execute(count_query, subordinate_ids)
        total = cursor.fetchone()['total']
        
        count_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 处理排序
        valid_sort_fields = ["fund_id", "amount", "handle_by"]
        valid_sort_orders = ["ASC", "DESC"]
        
        if sort_by not in valid_sort_fields:
            sort_by = "fund_id"
        
        if sort_order not in valid_sort_orders:
            sort_order = "ASC"
        
        order_clause = f"ORDER BY f.{sort_by} {sort_order}"
        
        # 分页查询数据
        start_time = time.time()
        offset = (page - 1) * page_size
        
        data_query = f"""
        SELECT 
            f.fund_id, 
            f.handle_by, 
            u.name as handler_name,
            u.department,
            f.order_id, 
            f.customer_id, 
            f.amount
        FROM financial_funds f
        JOIN users u ON f.handle_by = u.id
        WHERE f.handle_by IN ({placeholders})
        {order_clause}
        LIMIT %s OFFSET %s
        """
        
        params = list(subordinate_ids)
        params.extend([page_size, offset])
        
        cursor.execute(data_query, params)
        data = cursor.fetchall()
        
        data_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        # 查询执行计划
        explain_query = f"EXPLAIN {data_query}"
        cursor.execute(explain_query, params)
        explain_results = cursor.fetchall()
        
        # 记录每次迭代的结果
        iteration_result = {
            "iteration": i + 1,
            "subordinate_count": len(subordinate_ids),
            "subordinate_time": subordinate_time,
            "count_time": count_time,
            "data_time": data_time,
            "total_time": subordinate_time + count_time + data_time,
            "total_records": total,
            "returned_records": len(data),
            "explain": explain_results
        }
        
        results.append(iteration_result)
    
    cursor.close()
    conn.close()
    
    return results

def display_supervisor_info(supervisor_id):
    """显示主管信息"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    
    # 获取主管信息
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
    
    # 获取下属数量
    query = """
    SELECT COUNT(*) as count
    FROM user_hierarchy
    WHERE user_id = %s AND depth > 0
    """
    
    cursor.execute(query, (supervisor_id,))
    subordinate_count = cursor.fetchone()['count']
    
    print(f"\n=== 主管信息 ===")
    print(f"ID: {supervisor['id']}")
    print(f"姓名: {supervisor['name']}")
    print(f"角色: {supervisor['role']}")
    print(f"部门: {supervisor['department']}")
    print(f"下属数量: {subordinate_count}")
    
    cursor.close()
    conn.close()

def display_test_results(results):
    """显示测试结果"""
    if not results:
        return
    
    # 显示详细结果
    for i, result in enumerate(results):
        if i > 0:
            print("\n" + "-" * 40)
        
        print(f"\n迭代 {result['iteration']}:")
        print(f"下属数量: {result['subordinate_count']}")
        print(f"获取下属用时: {result['subordinate_time']:.2f}ms")
        print(f"获取总数用时: {result['count_time']:.2f}ms")
        print(f"获取数据用时: {result['data_time']:.2f}ms")
        print(f"总执行时间: {result['total_time']:.2f}ms")
        print(f"总记录数: {result['total_records']}")
        print(f"返回记录数: {result['returned_records']}")
        
        if i == 0:  # 只显示第一次迭代的执行计划
            print("\n查询执行计划:")
            for j, plan in enumerate(result['explain']):
                print(f"步骤 {j+1}: {plan}")
    
    # 计算平均值
    avg_subordinate_time = sum(r['subordinate_time'] for r in results) / len(results)
    avg_count_time = sum(r['count_time'] for r in results) / len(results)
    avg_data_time = sum(r['data_time'] for r in results) / len(results)
    avg_total_time = sum(r['total_time'] for r in results) / len(results)
    
    print("\n=== 平均性能 ===")
    print(f"获取下属平均用时: {avg_subordinate_time:.2f}ms")
    print(f"获取总数平均用时: {avg_count_time:.2f}ms")
    print(f"获取数据平均用时: {avg_data_time:.2f}ms")
    print(f"总执行平均时间: {avg_total_time:.2f}ms")

def main():
    parser = argparse.ArgumentParser(description="主管权限查询性能测试")
    parser.add_argument("--list", action="store_true", help="列出系统中的主管")
    parser.add_argument("--supervisor_id", type=int, help="指定要测试的主管ID")
    parser.add_argument("--page", type=int, default=1, help="页码")
    parser.add_argument("--page_size", type=int, default=10, help="每页记录数")
    parser.add_argument("--sort_by", type=str, default="fund_id", choices=["fund_id", "amount", "handle_by"], help="排序字段")
    parser.add_argument("--sort_order", type=str, default="ASC", choices=["ASC", "DESC"], help="排序方向")
    parser.add_argument("--iterations", type=int, default=3, help="重复测试次数")
    
    args = parser.parse_args()
    
    if args.list:
        supervisors = find_supervisors(limit=20)
        if not supervisors:
            print("未找到主管用户")
            return
        
        print("\n=== 系统中的主管用户 ===")
        table = PrettyTable(["ID", "姓名", "部门", "下属数量"])
        
        for supervisor in supervisors:
            table.add_row([
                supervisor['id'],
                supervisor['name'],
                supervisor['department'],
                supervisor['subordinate_count']
            ])
        
        print(table)
        return
    
    if not args.supervisor_id:
        print("请使用 --supervisor_id 指定要测试的主管ID")
        return
    
    # 显示主管信息
    display_supervisor_info(args.supervisor_id)
    
    print(f"\n开始测试主管(ID={args.supervisor_id})的查询性能...")
    print(f"页码: {args.page}, 每页记录数: {args.page_size}")
    print(f"排序: {args.sort_by} {args.sort_order}")
    print(f"重复次数: {args.iterations}\n")
    
    # 执行测试
    results = test_supervisor_query(
        args.supervisor_id,
        args.page,
        args.page_size,
        args.sort_by,
        args.sort_order,
        args.iterations
    )
    
    if results:
        display_test_results(results)
    else:
        print("测试失败，请检查数据库连接和主管ID")

if __name__ == "__main__":
    main()