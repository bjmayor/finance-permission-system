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

def paginate_users(page=1, page_size=10, role=None, department=None):
    """
    用户分页查询
    :param page: 页码（从1开始）
    :param page_size: 每页记录数
    :param role: 可选，按角色筛选
    :param department: 可选，按部门筛选
    :return: 元组 (总记录数, 当前页数据)
    """
    conn = connect_db()
    if not conn:
        return 0, []
    
    cursor = conn.cursor(dictionary=True)
    
    # 构建WHERE子句
    where_clause = ""
    params = []
    
    if role:
        where_clause = "WHERE role = %s"
        params.append(role)
    
    if department:
        if where_clause:
            where_clause += " AND department = %s"
        else:
            where_clause = "WHERE department = %s"
        params.append(department)
    
    # 查询总记录数
    count_query = f"SELECT COUNT(*) as total FROM users {where_clause}"
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 分页查询数据
    offset = (page - 1) * page_size
    query = f"""
    SELECT id, name, role, department, parent_id
    FROM users {where_clause}
    ORDER BY id
    LIMIT %s OFFSET %s
    """
    
    params.extend([page_size, offset])
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return total, results

def paginate_financial_funds(page=1, page_size=10, min_amount=None, max_amount=None, user_id=None):
    """
    财务资金分页查询
    :param page: 页码（从1开始）
    :param page_size: 每页记录数
    :param min_amount: 可选，最小金额
    :param max_amount: 可选，最大金额
    :param user_id: 可选，处理人ID
    :return: 元组 (总记录数, 当前页数据)
    """
    conn = connect_db()
    if not conn:
        return 0, []
    
    cursor = conn.cursor(dictionary=True)
    
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
    
    if user_id is not None:
        if where_clause:
            where_clause += " AND handle_by = %s"
        else:
            where_clause = "WHERE handle_by = %s"
        params.append(user_id)
    
    # 查询总记录数
    count_query = f"SELECT COUNT(*) as total FROM financial_funds {where_clause}"
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 分页查询数据
    offset = (page - 1) * page_size
    query = f"""
    SELECT fund_id, handle_by, order_id, customer_id, amount
    FROM financial_funds {where_clause}
    ORDER BY fund_id
    LIMIT %s OFFSET %s
    """
    
    params.extend([page_size, offset])
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return total, results

def paginate_customer_orders(page=1, page_size=10, customer_id=None):
    """
    客户订单关联查询（多表JOIN）
    :param page: 页码（从1开始）
    :param page_size: 每页记录数
    :param customer_id: 可选，客户ID
    :return: 元组 (总记录数, 当前页数据)
    """
    conn = connect_db()
    if not conn:
        return 0, []
    
    cursor = conn.cursor(dictionary=True)
    
    # 构建WHERE子句
    where_clause = ""
    params = []
    
    if customer_id is not None:
        where_clause = "WHERE c.customer_id = %s"
        params.append(customer_id)
    
    # 查询总记录数
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM customers c
    JOIN orders o ON c.admin_user_id = o.user_id
    {where_clause}
    """
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 分页查询数据
    offset = (page - 1) * page_size
    query = f"""
    SELECT c.customer_id, o.order_id, o.user_id, u.name as user_name
    FROM customers c
    JOIN orders o ON c.admin_user_id = o.user_id
    JOIN users u ON o.user_id = u.id
    {where_clause}
    ORDER BY c.customer_id, o.order_id
    LIMIT %s OFFSET %s
    """
    
    params.extend([page_size, offset])
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return total, results

def paginate_complex_report(page=1, page_size=10, min_amount=None, department=None):
    """
    复杂报表查询示例（多表JOIN + 条件过滤）
    :param page: 页码（从1开始）
    :param page_size: 每页记录数
    :param min_amount: 可选，最小金额
    :param department: 可选，部门
    :return: 元组 (总记录数, 当前页数据)
    """
    conn = connect_db()
    if not conn:
        return 0, []
    
    cursor = conn.cursor(dictionary=True)
    
    # 构建WHERE子句
    where_clause = ""
    params = []
    
    if min_amount is not None:
        where_clause = "WHERE f.amount >= %s"
        params.append(min_amount)
    
    if department is not None:
        if where_clause:
            where_clause += " AND u.department = %s"
        else:
            where_clause = "WHERE u.department = %s"
        params.append(department)
    
    # 查询总记录数
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM financial_funds f
    JOIN users u ON f.handle_by = u.id
    JOIN customers c ON f.customer_id = c.customer_id
    JOIN orders o ON f.order_id = o.order_id
    {where_clause}
    """
    cursor.execute(count_query, params)
    total = cursor.fetchone()['total']
    
    # 分页查询数据
    offset = (page - 1) * page_size
    query = f"""
    SELECT 
        f.fund_id, 
        f.amount, 
        u.id as user_id, 
        u.name as user_name, 
        u.department, 
        c.customer_id, 
        o.order_id
    FROM financial_funds f
    JOIN users u ON f.handle_by = u.id
    JOIN customers c ON f.customer_id = c.customer_id
    JOIN orders o ON f.order_id = o.order_id
    {where_clause}
    ORDER BY f.fund_id
    LIMIT %s OFFSET %s
    """
    
    params.extend([page_size, offset])
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return total, results

def paginate_user_subordinates(user_id, page=1, page_size=10):
    """
    查询用户的所有下级（利用user_hierarchy表）
    :param user_id: 用户ID
    :param page: 页码（从1开始）
    :param page_size: 每页记录数
    :return: 元组 (总记录数, 当前页数据)
    """
    conn = connect_db()
    if not conn:
        return 0, []
    
    cursor = conn.cursor(dictionary=True)
    
    # 查询总记录数
    count_query = """
    SELECT COUNT(*) as total 
    FROM user_hierarchy h
    JOIN users u ON h.subordinate_id = u.id
    WHERE h.user_id = %s AND h.depth > 0
    """
    cursor.execute(count_query, [user_id])
    total = cursor.fetchone()['total']
    
    # 分页查询数据
    offset = (page - 1) * page_size
    query = """
    SELECT 
        u.id, 
        u.name, 
        u.role, 
        u.department, 
        h.depth as level
    FROM user_hierarchy h
    JOIN users u ON h.subordinate_id = u.id
    WHERE h.user_id = %s AND h.depth > 0
    ORDER BY h.depth, u.id
    LIMIT %s OFFSET %s
    """
    
    cursor.execute(query, [user_id, page_size, offset])
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return total, results

def display_results(title, results, total, page, page_size):
    """格式化显示结果"""
    if not results:
        print(f"{title}: 没有找到记录")
        return
    
    # 创建表格
    table = PrettyTable()
    table.field_names = results[0].keys()
    
    for result in results:
        table.add_row(result.values())
    
    total_pages = (total + page_size - 1) // page_size
    
    print(f"\n=== {title} ===")
    print(f"总记录数: {total}, 当前页: {page}/{total_pages}, 每页显示: {page_size}")
    print(table)

def main():
    parser = argparse.ArgumentParser(description="财务权限系统分页查询示例")
    parser.add_argument("--query", type=str, choices=[
        "users", "funds", "customer_orders", "complex", "subordinates"
    ], default="users", help="要执行的查询类型")
    parser.add_argument("--page", type=int, default=1, help="页码")
    parser.add_argument("--page_size", type=int, default=10, help="每页记录数")
    parser.add_argument("--role", type=str, help="用户角色 (users查询)")
    parser.add_argument("--department", type=str, help="部门 (users或complex查询)")
    parser.add_argument("--min_amount", type=float, help="最小金额 (funds或complex查询)")
    parser.add_argument("--max_amount", type=float, help="最大金额 (funds查询)")
    parser.add_argument("--user_id", type=int, help="用户ID (funds或subordinates查询)")
    parser.add_argument("--customer_id", type=int, help="客户ID (customer_orders查询)")
    
    args = parser.parse_args()
    
    if args.query == "users":
        total, results = paginate_users(
            page=args.page, 
            page_size=args.page_size, 
            role=args.role, 
            department=args.department
        )
        display_results("用户列表", results, total, args.page, args.page_size)
    
    elif args.query == "funds":
        total, results = paginate_financial_funds(
            page=args.page, 
            page_size=args.page_size, 
            min_amount=args.min_amount, 
            max_amount=args.max_amount,
            user_id=args.user_id
        )
        display_results("财务资金列表", results, total, args.page, args.page_size)
    
    elif args.query == "customer_orders":
        total, results = paginate_customer_orders(
            page=args.page, 
            page_size=args.page_size, 
            customer_id=args.customer_id
        )
        display_results("客户订单关联", results, total, args.page, args.page_size)
    
    elif args.query == "complex":
        total, results = paginate_complex_report(
            page=args.page, 
            page_size=args.page_size, 
            min_amount=args.min_amount, 
            department=args.department
        )
        display_results("复杂财务报表", results, total, args.page, args.page_size)
    
    elif args.query == "subordinates":
        if not args.user_id:
            print("错误: 查询下属必须指定 --user_id 参数")
            return
        
        total, results = paginate_user_subordinates(
            user_id=args.user_id,
            page=args.page, 
            page_size=args.page_size
        )
        display_results(f"用户 {args.user_id} 的下属列表", results, total, args.page, args.page_size)

if __name__ == "__main__":
    main()