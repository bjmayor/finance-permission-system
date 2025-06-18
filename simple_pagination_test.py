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

def get_financial_funds_cursor_pagination(cursor, handle_by_ids: List[int], order_ids: List[int],
                                        customer_ids: List[int], page: int = 1, page_size: int = 20,
                                        sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], int]:
    """
    方案2: 使用游标分页 (Cursor-based pagination)
    适用于中等数据量，可以接受近似分页的场景
    """

    # 构建条件 - 分批处理以避免IN子句过大
    batch_size = 1000
    all_results = []

    # 计算需要多少批次
    max_ids = max(len(handle_by_ids), len(order_ids), len(customer_ids))
    total_batches = (max_ids + batch_size - 1) // batch_size if max_ids > 0 else 0

    print(f"使用游标分页，处理 {total_batches} 批次，每批最大 {batch_size} 个ID")

    estimated_total = 0

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = start_idx + batch_size

        # 获取当前批次的ID
        batch_handle_by = handle_by_ids[start_idx:end_idx] if start_idx < len(handle_by_ids) else []
        batch_order_ids = order_ids[start_idx:end_idx] if start_idx < len(order_ids) else []
        batch_customer_ids = customer_ids[start_idx:end_idx] if start_idx < len(customer_ids) else []

        conditions = []
        params = []

        if batch_handle_by:
            placeholders = ','.join(['%s'] * len(batch_handle_by))
            conditions.append(f"f.handle_by IN ({placeholders})")
            params.extend(batch_handle_by)

        if batch_order_ids:
            placeholders = ','.join(['%s'] * len(batch_order_ids))
            conditions.append(f"f.order_id IN ({placeholders})")
            params.extend(batch_order_ids)

        if batch_customer_ids:
            placeholders = ','.join(['%s'] * len(batch_customer_ids))
            conditions.append(f"f.customer_id IN ({placeholders})")
            params.extend(batch_customer_ids)

        if not conditions:
            continue

        where_clause = ' OR '.join(conditions)

        # 只在第一批次时估算总数（为了性能考虑）
        if batch_idx == 0:
            count_query = f"""
                SELECT COUNT(*)
                FROM financial_funds f
                WHERE {where_clause}
            """
            cursor.execute(count_query, tuple(params))
            batch_count = cursor.fetchone()[0]
            # 简单估算总数
            estimated_total = batch_count * total_batches

        # 获取当前批次的数据
        offset = max(0, (page - 1) * page_size - len(all_results))
        remaining_needed = page_size - len(all_results)

        if remaining_needed <= 0:
            break

        query = f"""
            SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
                   u.name as handler_name, u.department
            FROM financial_funds f
            JOIN users u ON f.handle_by = u.id
            WHERE {where_clause}
            ORDER BY f.{sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """

        batch_params = list(params) + [remaining_needed + offset, offset]
        cursor.execute(query, tuple(batch_params))
        batch_results = cursor.fetchall()
        all_results.extend(batch_results)

        print(f"批次 {batch_idx + 1}: 获取 {len(batch_results)} 条记录")

        # 如果已经获取足够的记录，退出
        if len(all_results) >= page_size:
            all_results = all_results[:page_size]
            break

    return all_results, estimated_total

def get_financial_funds_stream_pagination(cursor, handle_by_ids: List[int], order_ids: List[int],
                                        customer_ids: List[int], last_id: int = 0, page_size: int = 20,
                                        sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], bool]:
    """
    方案3: 基于 last_id 的流式分页
    适用于超大数据量，实时性要求高的场景
    """

    # 分批处理大ID列表
    batch_size = 1000
    all_results = []

    max_ids = max(len(handle_by_ids), len(order_ids), len(customer_ids))
    total_batches = (max_ids + batch_size - 1) // batch_size if max_ids > 0 else 0

    print(f"使用流式分页，处理 {total_batches} 批次")

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = start_idx + batch_size

        batch_handle_by = handle_by_ids[start_idx:end_idx] if start_idx < len(handle_by_ids) else []
        batch_order_ids = order_ids[start_idx:end_idx] if start_idx < len(order_ids) else []
        batch_customer_ids = customer_ids[start_idx:end_idx] if start_idx < len(customer_ids) else []

        conditions = []
        params = []

        if batch_handle_by:
            placeholders = ','.join(['%s'] * len(batch_handle_by))
            conditions.append(f"f.handle_by IN ({placeholders})")
            params.extend(batch_handle_by)

        if batch_order_ids:
            placeholders = ','.join(['%s'] * len(batch_order_ids))
            conditions.append(f"f.order_id IN ({placeholders})")
            params.extend(batch_order_ids)

        if batch_customer_ids:
            placeholders = ','.join(['%s'] * len(batch_customer_ids))
            conditions.append(f"f.customer_id IN ({placeholders})")
            params.extend(batch_customer_ids)

        if not conditions:
            continue

        where_clause = ' OR '.join(conditions)

        # 添加 last_id 条件用于流式分页
        if last_id > 0:
            if sort_order.upper() == "ASC":
                where_clause += f" AND f.{sort_by} > %s"
            else:
                where_clause += f" AND f.{sort_by} < %s"
            params.append(last_id)

        # 查询比需要多一条记录来判断是否有下一页
        remaining_needed = page_size - len(all_results) + 1

        query = f"""
            SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
                   u.name as handler_name, u.department
            FROM financial_funds f
            JOIN users u ON f.handle_by = u.id
            WHERE {where_clause}
            ORDER BY f.{sort_by} {sort_order}
            LIMIT %s
        """

        params.append(remaining_needed)
        cursor.execute(query, tuple(params))
        batch_results = cursor.fetchall()
        all_results.extend(batch_results)

        print(f"批次 {batch_idx + 1}: 获取 {len(batch_results)} 条记录")

        # 如果已获取足够记录或者当前批次没有更多数据，退出
        if len(all_results) > page_size or len(batch_results) == 0:
            break

    # 判断是否有下一页
    has_next = len(all_results) > page_size
    if has_next:
        all_results = all_results[:-1]  # 移除多查询的那条记录

    return all_results[:page_size], has_next

def test_simplified_pagination():
    """测试简化的分页方案"""
    supervisor_id = 2

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("=== 获取权限数据 ===")
        start_time = time.time()

        # 步骤 1-3: 获取权限ID
        subordinate_ids = get_subordinate_ids(cursor, supervisor_id)
        if supervisor_id not in subordinate_ids:
            subordinate_ids.append(supervisor_id)

        order_ids = get_order_ids_for_users(cursor, subordinate_ids)
        customer_ids = get_customer_ids_for_users(cursor, subordinate_ids)

        setup_time = time.time() - start_time
        print(f"权限范围: {len(subordinate_ids)} 用户, {len(order_ids)} 订单, {len(customer_ids)} 客户")
        print(f"权限数据获取耗时: {setup_time:.4f}s")

        # 测试方案2: 游标分页
        print("\n=== 方案2: 游标分页 ===")
        start_time = time.time()
        results_v2, total_v2 = get_financial_funds_cursor_pagination(
            cursor, subordinate_ids, order_ids, customer_ids,
            page=1, page_size=20, sort_by="fund_id", sort_order="ASC"
        )
        time_v2 = time.time() - start_time
        print(f"方案2结果: {len(results_v2)} 条记录, 估计总数: {total_v2}, 耗时: {time_v2:.4f}s")

        # 测试方案3: 流式分页
        print("\n=== 方案3: 流式分页 ===")
        start_time = time.time()
        results_v3, has_next = get_financial_funds_stream_pagination(
            cursor, subordinate_ids, order_ids, customer_ids,
            last_id=0, page_size=20, sort_by="fund_id", sort_order="ASC"
        )
        time_v3 = time.time() - start_time
        print(f"方案3结果: {len(results_v3)} 条记录, 有下一页: {has_next}, 耗时: {time_v3:.4f}s")

        # 显示样例数据
        if results_v2:
            print(f"\n样例记录: fund_id={results_v2[0][0]}, handler={results_v2[0][5]}, amount={results_v2[0][4]}")

        print(f"\n=== 性能总结 ===")
        print(f"权限查询: {setup_time:.4f}s")
        print(f"游标分页: {time_v2:.4f}s (有总数估算)")
        print(f"流式分页: {time_v3:.4f}s (无总数，但最快)")
        print(f"总耗时: {setup_time + min(time_v2, time_v3):.4f}s")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    """主函数"""
    test_simplified_pagination()

if __name__ == "__main__":
    main()
