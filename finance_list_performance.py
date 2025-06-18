#!/usr/bin/env python3
import os
import time
import argparse
import mysql.connector
from dotenv import load_dotenv

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

def query_with_timing(cursor, query, params=None, description="查询"):
    """执行查询并计时"""
    start_time = time.time()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000  # 转换为毫秒
    print(f"{description} - 执行时间: {execution_time:.2f}ms, 返回行数: {len(results)}")
    return results, execution_time

def test_basic_pagination(page=1, page_size=10):
    """测试基本分页查询性能"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    offset = (page - 1) * page_size
    
    # 查询总记录数
    count_query = "SELECT COUNT(*) as total FROM financial_funds"
    count_results, count_time = query_with_timing(cursor, count_query, description="总数查询")
    total = count_results[0]['total']
    
    # 分页查询数据
    data_query = """
    SELECT fund_id, handle_by, order_id, customer_id, amount
    FROM financial_funds
    ORDER BY fund_id
    LIMIT %s OFFSET %s
    """
    data_results, data_time = query_with_timing(
        cursor, data_query, [page_size, offset], 
        description="数据分页查询"
    )
    
    cursor.close()
    conn.close()
    
    print(f"总记录数: {total}, 页码: {page}/{(total + page_size - 1) // page_size}")
    return {
        "count_time": count_time,
        "data_time": data_time,
        "total_time": count_time + data_time
    }

def test_filtered_pagination(min_amount=None, max_amount=None, page=1, page_size=10):
    """测试带过滤条件的分页查询性能"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    offset = (page - 1) * page_size
    
    # 构建WHERE子句
    where_clause = ""
    params = []
    
    if min_amount is not None:
        where_clause = "WHERE amount >= %s"
        params.append(min_amount)
    
    if max_amount is not None:
        if where_clause:
            where_clause += " AND amount <= %s"
        else:
            where_clause = "WHERE amount <= %s"
        params.append(max_amount)
    
    # 查询总记录数
    count_query = f"SELECT COUNT(*) as total FROM financial_funds {where_clause}"
    count_results, count_time = query_with_timing(
        cursor, count_query, params, 
        description="带过滤条件的总数查询"
    )
    total = count_results[0]['total']
    
    # 分页查询数据
    data_query = f"""
    SELECT fund_id, handle_by, order_id, customer_id, amount
    FROM financial_funds {where_clause}
    ORDER BY fund_id
    LIMIT %s OFFSET %s
    """
    
    params_with_limit = params.copy()
    params_with_limit.extend([page_size, offset])
    data_results, data_time = query_with_timing(
        cursor, data_query, params_with_limit, 
        description="带过滤条件的数据分页查询"
    )
    
    cursor.close()
    conn.close()
    
    print(f"总记录数: {total}, 页码: {page}/{(total + page_size - 1) // page_size}")
    return {
        "count_time": count_time,
        "data_time": data_time,
        "total_time": count_time + data_time
    }

def test_complex_pagination(user_id=None, department=None, min_amount=None, page=1, page_size=10, sort_by="fund_id", sort_order="ASC"):
    """测试复杂权限下的财务列表查询性能（带权限过滤、分页和排序）"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    offset = (page - 1) * page_size
    
    # 构建WHERE子句
    where_conditions = []
    params = []
    
    if user_id is not None:
        # 如果指定了用户ID，先获取该用户的所有下属ID
        subordinate_query = """
        SELECT subordinate_id 
        FROM user_hierarchy 
        WHERE user_id = %s
        """
        subordinates, _ = query_with_timing(
            cursor, subordinate_query, [user_id], 
            description="获取用户权限范围"
        )
        
        if subordinates:
            subordinate_ids = [row['subordinate_id'] for row in subordinates]
            placeholders = ', '.join(['%s'] * len(subordinate_ids))
            where_conditions.append(f"f.handle_by IN ({placeholders})")
            params.extend(subordinate_ids)
        else:
            # 如果没有下属，则只能看自己的数据
            where_conditions.append("f.handle_by = %s")
            params.append(user_id)
    
    if department is not None:
        where_conditions.append("u.department = %s")
        params.append(department)
    
    if min_amount is not None:
        where_conditions.append("f.amount >= %s")
        params.append(min_amount)
    
    # 组合WHERE子句
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # 处理排序
    valid_sort_fields = ["fund_id", "amount", "handle_by"]
    valid_sort_orders = ["ASC", "DESC"]
    
    if sort_by not in valid_sort_fields:
        sort_by = "fund_id"
    
    if sort_order not in valid_sort_orders:
        sort_order = "ASC"
    
    order_clause = f"ORDER BY f.{sort_by} {sort_order}"
    
    # 查询总记录数
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM financial_funds f
    JOIN users u ON f.handle_by = u.id
    {where_clause}
    """
    count_results, count_time = query_with_timing(
        cursor, count_query, params, 
        description="复杂权限下的总数查询"
    )
    total = count_results[0]['total']
    
    # 分页查询数据
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
    {where_clause}
    {order_clause}
    LIMIT %s OFFSET %s
    """
    
    params_with_limit = params.copy()
    params_with_limit.extend([page_size, offset])
    data_results, data_time = query_with_timing(
        cursor, data_query, params_with_limit, 
        description="复杂权限下的数据分页查询"
    )
    
    # 添加索引分析
    print("\n索引使用情况分析:")
    explain_query = f"EXPLAIN {data_query}"
    cursor.execute(explain_query, params_with_limit)
    explain_results = cursor.fetchall()
    for i, row in enumerate(explain_results):
        print(f"表 {i+1}: {row}")
    
    cursor.close()
    conn.close()
    
    print(f"总记录数: {total}, 页码: {page}/{(total + page_size - 1) // page_size}")
    return {
        "count_time": count_time,
        "data_time": data_time,
        "total_time": count_time + data_time
    }

def test_optimized_complex_pagination(user_id=None, department=None, min_amount=None, page=1, page_size=10, sort_by="fund_id", sort_order="ASC"):
    """测试优化后的复杂权限财务列表查询性能（使用子查询优化）"""
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor(dictionary=True)
    offset = (page - 1) * page_size
    
    # 构建WHERE子句
    where_conditions = []
    params = []
    
    if user_id is not None:
        # 使用子查询替代IN子句，有时可能更高效
        where_conditions.append("""
        f.handle_by IN (
            SELECT subordinate_id 
            FROM user_hierarchy 
            WHERE user_id = %s
        )
        """)
        params.append(user_id)
    
    if department is not None:
        where_conditions.append("u.department = %s")
        params.append(department)
    
    if min_amount is not None:
        where_conditions.append("f.amount >= %s")
        params.append(min_amount)
    
    # 组合WHERE子句
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # 处理排序
    valid_sort_fields = ["fund_id", "amount", "handle_by"]
    valid_sort_orders = ["ASC", "DESC"]
    
    if sort_by not in valid_sort_fields:
        sort_by = "fund_id"
    
    if sort_order not in valid_sort_orders:
        sort_order = "ASC"
    
    order_clause = f"ORDER BY f.{sort_by} {sort_order}"
    
    # 优化1: 使用子查询计算总数，避免全表扫描
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM (
        SELECT 1
        FROM financial_funds f
        JOIN users u ON f.handle_by = u.id
        {where_clause}
        LIMIT 10000
    ) as temp
    """
    count_results, count_time = query_with_timing(
        cursor, count_query, params, 
        description="优化后的总数查询"
    )
    total = count_results[0]['total']
    
    # 优化2: 先获取ID，再查询详细数据
    # 这种方式在某些场景下可以提高性能，尤其是当结果集包含大量数据或连接多个表时
    id_query = f"""
    SELECT f.fund_id
    FROM financial_funds f
    JOIN users u ON f.handle_by = u.id
    {where_clause}
    {order_clause}
    LIMIT %s OFFSET %s
    """
    
    params_with_limit = params.copy()
    params_with_limit.extend([page_size, offset])
    id_results, id_time = query_with_timing(
        cursor, id_query, params_with_limit, 
        description="获取ID列表"
    )
    
    if id_results:
        fund_ids = [row['fund_id'] for row in id_results]
        placeholders = ', '.join(['%s'] * len(fund_ids))
        
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
        WHERE f.fund_id IN ({placeholders})
        {order_clause}
        """
        
        data_results, data_time = query_with_timing(
            cursor, data_query, fund_ids, 
            description="获取详细数据"
        )
    else:
        data_time = 0
    
    cursor.close()
    conn.close()
    
    print(f"总记录数: {total}, 页码: {page}/{(total + page_size - 1) // page_size}")
    return {
        "count_time": count_time,
        "id_time": id_time,
        "data_time": data_time,
        "total_time": count_time + id_time + data_time
    }

def main():
    parser = argparse.ArgumentParser(description="财务列表性能测试")
    parser.add_argument("--test", type=str, choices=[
        "basic", "filtered", "complex", "optimized"
    ], default="basic", help="测试类型")
    parser.add_argument("--page", type=int, default=1, help="页码")
    parser.add_argument("--page_size", type=int, default=10, help="每页记录数")
    parser.add_argument("--min_amount", type=float, help="最小金额")
    parser.add_argument("--max_amount", type=float, help="最大金额")
    parser.add_argument("--user_id", type=int, help="用户ID (权限控制)")
    parser.add_argument("--department", type=str, help="部门")
    parser.add_argument("--sort_by", type=str, default="fund_id", help="排序字段")
    parser.add_argument("--sort_order", type=str, default="ASC", choices=["ASC", "DESC"], help="排序方向")
    parser.add_argument("--iterations", type=int, default=1, help="重复测试次数")
    
    args = parser.parse_args()
    
    print(f"=== 执行 {args.test} 测试，重复 {args.iterations} 次 ===\n")
    
    total_times = []
    
    for i in range(args.iterations):
        if i > 0:
            print(f"\n--- 迭代 {i+1} ---")
            
        if args.test == "basic":
            result = test_basic_pagination(args.page, args.page_size)
        elif args.test == "filtered":
            result = test_filtered_pagination(
                args.min_amount, args.max_amount,
                args.page, args.page_size
            )
        elif args.test == "complex":
            result = test_complex_pagination(
                args.user_id, args.department, args.min_amount,
                args.page, args.page_size,
                args.sort_by, args.sort_order
            )
        elif args.test == "optimized":
            result = test_optimized_complex_pagination(
                args.user_id, args.department, args.min_amount,
                args.page, args.page_size,
                args.sort_by, args.sort_order
            )
        
        if result:
            total_times.append(result["total_time"])
    
    if total_times:
        avg_time = sum(total_times) / len(total_times)
        print(f"\n平均总执行时间: {avg_time:.2f}ms")

if __name__ == "__main__":
    main()