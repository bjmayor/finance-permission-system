import mysql.connector
import time
import os
from typing import List, Dict, Any, Tuple

# Database connection details from environment variables
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "123456")
DB_NAME = os.environ.get("DB_NAME", "finance")

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def get_subordinate_ids(cursor, supervisor_id: int) -> List[int]:
    """1. Get a list of employee IDs managed by the supervisor."""
    query = """
        SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s
    """
    cursor.execute(query, (supervisor_id,))
    return [item[0] for item in cursor.fetchall()]

def get_order_ids_for_users(cursor, user_ids: List[int]) -> List[int]:
    """2. Get a list of authorized order_ids from the orders table."""
    if not user_ids:
        return []
    placeholders = ','.join(['%s'] * len(user_ids))
    query = f"SELECT order_id FROM orders WHERE user_id IN ({placeholders})"
    cursor.execute(query, tuple(user_ids))
    return [item[0] for item in cursor.fetchall()]

def get_customer_ids_for_users(cursor, user_ids: List[int]) -> List[int]:
    """3. Get a list of authorized customer_ids from the customers table."""
    if not user_ids:
        return []
    placeholders = ','.join(['%s'] * len(user_ids))
    query = f"SELECT customer_id FROM customers WHERE admin_user_id IN ({placeholders})"
    cursor.execute(query, tuple(user_ids))
    return [item[0] for item in cursor.fetchall()]

def get_financial_funds_with_pagination_v1(cursor, handle_by_ids: List[int], order_ids: List[int],
                                          customer_ids: List[int], page: int = 1, page_size: int = 20,
                                          sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], int]:
    """
    方案1: 使用临时表 + 全局排序分页
    适用于中等数据量，需要精确分页的场景
    """
    # 创建临时表存储权限ID
    temp_table_name = f"temp_permission_ids_{int(time.time() * 1000)}"

    try:
        # 创建临时表
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                id_value INT,
                id_type ENUM('handle_by', 'order_id', 'customer_id'),
                INDEX(id_value, id_type)
            )
        """)

        # 插入权限ID
        for handle_by_id in handle_by_ids:
            cursor.execute(f"INSERT INTO {temp_table_name} (id_value, id_type) VALUES (%s, 'handle_by')", (handle_by_id,))

        for order_id in order_ids:
            cursor.execute(f"INSERT INTO {temp_table_name} (id_value, id_type) VALUES (%s, 'order_id')", (order_id,))

        for customer_id in customer_ids:
            cursor.execute(f"INSERT INTO {temp_table_name} (id_value, id_type) VALUES (%s, 'customer_id')", (customer_id,))

        # 获取总数
        count_query = f"""
            SELECT COUNT(DISTINCT f.fund_id)
            FROM financial_funds f
            WHERE EXISTS (
                SELECT 1 FROM {temp_table_name} t
                WHERE (t.id_type = 'handle_by' AND t.id_value = f.handle_by)
                   OR (t.id_type = 'order_id' AND t.id_value = f.order_id)
                   OR (t.id_type = 'customer_id' AND t.id_value = f.customer_id)
            )
        """
        cursor.execute(count_query)
        total_count = cursor.fetchone()[0]

        # 分页查询
        offset = (page - 1) * page_size
        main_query = f"""
            SELECT DISTINCT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
                   u.name as handler_name, u.department
            FROM financial_funds f
            JOIN users u ON f.handle_by = u.id
            WHERE EXISTS (
                SELECT 1 FROM {temp_table_name} t
                WHERE (t.id_type = 'handle_by' AND t.id_value = f.handle_by)
                   OR (t.id_type = 'order_id' AND t.id_value = f.order_id)
                   OR (t.id_type = 'customer_id' AND t.id_value = f.customer_id)
            )
            ORDER BY f.{sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """

        cursor.execute(main_query, (page_size, offset))
        results = cursor.fetchall()

        return results, total_count

    finally:
        # 清理临时表
        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_table_name}")

def get_financial_funds_with_pagination_v2(cursor, handle_by_ids: List[int], order_ids: List[int],
                                          customer_ids: List[int], page: int = 1, page_size: int = 20,
                                          sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], int]:
    """
    方案2: 使用游标分页 (Cursor-based pagination)
    适用于大数据量，可以接受近似分页的场景
    """

    # 构建条件
    conditions = []
    params = []

    if handle_by_ids:
        placeholders = ','.join(['%s'] * len(handle_by_ids))
        conditions.append(f"f.handle_by IN ({placeholders})")
        params.extend(handle_by_ids)

    if order_ids:
        placeholders = ','.join(['%s'] * len(order_ids))
        conditions.append(f"f.order_id IN ({placeholders})")
        params.extend(order_ids)

    if customer_ids:
        placeholders = ','.join(['%s'] * len(customer_ids))
        conditions.append(f"f.customer_id IN ({placeholders})")
        params.extend(customer_ids)

    if not conditions:
        return [], 0

    where_clause = ' OR '.join(conditions)

    # 获取总数 (可能不完全准确，但性能更好)
    count_query = f"""
        SELECT COUNT(*)
        FROM financial_funds f
        WHERE {where_clause}
    """
    cursor.execute(count_query, tuple(params))
    total_count = cursor.fetchone()[0]

    # 游标分页查询
    offset = (page - 1) * page_size
    main_query = f"""
        SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
               u.name as handler_name, u.department
        FROM financial_funds f
        JOIN users u ON f.handle_by = u.id
        WHERE {where_clause}
        ORDER BY f.{sort_by} {sort_order}
        LIMIT %s OFFSET %s
    """

    # 添加分页参数
    final_params = list(params) + [page_size, offset]
    cursor.execute(main_query, tuple(final_params))
    results = cursor.fetchall()

    return results, total_count

def get_financial_funds_with_pagination_v3(cursor, handle_by_ids: List[int], order_ids: List[int],
                                          customer_ids: List[int], last_id: int = 0, page_size: int = 20,
                                          sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], bool]:
    """
    方案3: 基于 last_id 的流式分页
    适用于超大数据量，实时性要求高的场景
    """

    # 构建条件
    conditions = []
    params = []

    if handle_by_ids:
        placeholders = ','.join(['%s'] * len(handle_by_ids))
        conditions.append(f"f.handle_by IN ({placeholders})")
        params.extend(handle_by_ids)

    if order_ids:
        placeholders = ','.join(['%s'] * len(order_ids))
        conditions.append(f"f.order_id IN ({placeholders})")
        params.extend(order_ids)

    if customer_ids:
        placeholders = ','.join(['%s'] * len(customer_ids))
        conditions.append(f"f.customer_id IN ({placeholders})")
        params.extend(customer_ids)

    if not conditions:
        return [], False

    where_clause = ' OR '.join(conditions)

    # 添加 last_id 条件
    if last_id > 0:
        if sort_order.upper() == "ASC":
            where_clause += f" AND f.{sort_by} > %s"
        else:
            where_clause += f" AND f.{sort_by} < %s"
        params.append(last_id)

    # 查询比请求多一条记录来判断是否有下一页
    query = f"""
        SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
               u.name as handler_name, u.department
        FROM financial_funds f
        JOIN users u ON f.handle_by = u.id
        WHERE {where_clause}
        ORDER BY f.{sort_by} {sort_order}
        LIMIT %s
    """

    params.append(page_size + 1)
    cursor.execute(query, tuple(params))
    results = cursor.fetchall()

    # 判断是否有下一页
    has_next = len(results) > page_size
    if has_next:
        results = results[:-1]  # 移除多查询的那条记录

    return results, has_next

def test_pagination_approaches():
    """测试不同分页方案的性能"""
    supervisor_id = 2

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("=== 获取权限数据 ===")
        # 步骤 1-3: 获取权限ID
        subordinate_ids = get_subordinate_ids(cursor, supervisor_id)
        if supervisor_id not in subordinate_ids:
            subordinate_ids.append(supervisor_id)

        order_ids = get_order_ids_for_users(cursor, subordinate_ids)
        customer_ids = get_customer_ids_for_users(cursor, subordinate_ids)

        print(f"权限范围: {len(subordinate_ids)} 用户, {len(order_ids)} 订单, {len(customer_ids)} 客户")

        # 测试方案1: 临时表分页
        print("\n=== 方案1: 临时表 + 精确分页 ===")
        start_time = time.time()
        results_v1, total_v1 = get_financial_funds_with_pagination_v1(
            cursor, subordinate_ids, order_ids, customer_ids,
            page=1, page_size=20, sort_by="fund_id", sort_order="ASC"
        )
        time_v1 = time.time() - start_time
        print(f"方案1 - 第1页: {len(results_v1)} 条记录, 总计: {total_v1}, 耗时: {time_v1:.4f}s")

        # 测试方案2: 游标分页
        print("\n=== 方案2: 游标分页 ===")
        start_time = time.time()
        results_v2, total_v2 = get_financial_funds_with_pagination_v2(
            cursor, subordinate_ids, order_ids, customer_ids,
            page=1, page_size=20, sort_by="fund_id", sort_order="ASC"
        )
        time_v2 = time.time() - start_time
        print(f"方案2 - 第1页: {len(results_v2)} 条记录, 总计: {total_v2}, 耗时: {time_v2:.4f}s")

        # 测试方案3: 流式分页
        print("\n=== 方案3: 流式分页 ===")
        start_time = time.time()
        results_v3, has_next = get_financial_funds_with_pagination_v3(
            cursor, subordinate_ids, order_ids, customer_ids,
            last_id=0, page_size=20, sort_by="fund_id", sort_order="ASC"
        )
        time_v3 = time.time() - start_time
        print(f"方案3 - 第1页: {len(results_v3)} 条记录, 有下一页: {has_next}, 耗时: {time_v3:.4f}s")

        # 显示第一条记录作为样例
        if results_v1:
            print(f"\n样例记录: fund_id={results_v1[0][0]}, handler={results_v1[0][5]}")

        print(f"\n性能对比:")
        print(f"方案1 (临时表): {time_v1:.4f}s - 精确但较慢")
        print(f"方案2 (游标分页): {time_v2:.4f}s - 平衡方案")
        print(f"方案3 (流式分页): {time_v3:.4f}s - 最快但无总数")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    """主函数"""
    test_pagination_approaches()

if __name__ == "__main__":
    main()
