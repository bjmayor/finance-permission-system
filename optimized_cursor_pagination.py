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
        database=DB_NAME,
        autocommit=True
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

def estimate_total_count(cursor, handle_by_ids: List[int], order_ids: List[int], customer_ids: List[int]) -> int:
    """估算总记录数 - 使用采样方法"""

    # 使用更小的样本来估算
    sample_size = min(100, len(handle_by_ids), len(order_ids), len(customer_ids))

    if sample_size == 0:
        return 0

    # 采样查询
    sample_handle_by = handle_by_ids[:sample_size] if handle_by_ids else []
    sample_order_ids = order_ids[:sample_size] if order_ids else []
    sample_customer_ids = customer_ids[:sample_size] if customer_ids else []

    conditions = []
    params = []

    if sample_handle_by:
        placeholders = ','.join(['%s'] * len(sample_handle_by))
        conditions.append(f"handle_by IN ({placeholders})")
        params.extend(sample_handle_by)

    if sample_order_ids:
        placeholders = ','.join(['%s'] * len(sample_order_ids))
        conditions.append(f"order_id IN ({placeholders})")
        params.extend(sample_order_ids)

    if sample_customer_ids:
        placeholders = ','.join(['%s'] * len(sample_customer_ids))
        conditions.append(f"customer_id IN ({placeholders})")
        params.extend(sample_customer_ids)

    if not conditions:
        return 0

    where_clause = ' OR '.join(conditions)

    # 使用 EXPLAIN 而不是实际 COUNT 来估算
    query = f"""
        SELECT COUNT(*)
        FROM financial_funds
        WHERE {where_clause}
        LIMIT 10000
    """

    cursor.execute(query, tuple(params))
    sample_count = cursor.fetchone()[0]

    # 基于采样比例估算总数
    total_ids = len(handle_by_ids) + len(order_ids) + len(customer_ids)
    sample_ids = len(sample_handle_by) + len(sample_order_ids) + len(sample_customer_ids)

    if sample_ids > 0:
        estimated_total = int(sample_count * (total_ids / sample_ids))
    else:
        estimated_total = 0

    return estimated_total

def get_financial_funds_optimized_pagination(cursor, handle_by_ids: List[int], order_ids: List[int],
                                           customer_ids: List[int], page: int = 1, page_size: int = 20,
                                           sort_by: str = "fund_id", sort_order: str = "ASC",
                                           estimate_total: bool = True) -> Tuple[List[Any], int]:
    """
    优化的游标分页实现
    """

    # 智能批次大小 - 根据页面位置调整
    if page <= 10:
        batch_size = 500  # 前几页使用较小批次，确保快速响应
    else:
        batch_size = 1000  # 后面页使用较大批次，提高效率

    offset = (page - 1) * page_size

    # 只在第一页时估算总数
    total_count = 0
    if page == 1 and estimate_total:
        print("正在估算总记录数...")
        start_time = time.time()
        total_count = estimate_total_count(cursor, handle_by_ids, order_ids, customer_ids)
        print(f"总数估算耗时: {time.time() - start_time:.4f}s, 估算结果: {total_count}")

    # 计算需要跳过多少个批次
    max_ids = max(len(handle_by_ids), len(order_ids), len(customer_ids))
    total_batches = (max_ids + batch_size - 1) // batch_size if max_ids > 0 else 0

    print(f"优化分页: 第{page}页, 每页{page_size}条, 使用{batch_size}批次大小")

    all_results = []
    current_offset = 0

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

        # 先查询当前批次的总数来确定是否需要跳过
        if current_offset < offset:
            count_query = f"""
                SELECT COUNT(*)
                FROM financial_funds f
                WHERE {where_clause}
            """
            cursor.execute(count_query, tuple(params))
            batch_count = cursor.fetchone()[0]

            if current_offset + batch_count <= offset:
                # 整个批次都需要跳过
                current_offset += batch_count
                print(f"跳过批次 {batch_idx + 1}: {batch_count} 条记录")
                continue
            else:
                # 部分跳过
                skip_in_batch = offset - current_offset
                current_offset = offset
        else:
            skip_in_batch = 0

        # 计算本批次需要取多少条记录
        remaining_needed = page_size - len(all_results)

        if remaining_needed <= 0:
            break

        # 优化的查询 - 只查询必要的字段
        query = f"""
            SELECT f.fund_id, f.handle_by, f.order_id, f.customer_id, f.amount,
                   u.name as handler_name, u.department
            FROM financial_funds f
            FORCE INDEX (PRIMARY)
            JOIN users u ON f.handle_by = u.id
            WHERE {where_clause}
            ORDER BY f.{sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """

        batch_params = list(params) + [remaining_needed, skip_in_batch]
        cursor.execute(query, tuple(batch_params))
        batch_results = cursor.fetchall()
        all_results.extend(batch_results)

        print(f"批次 {batch_idx + 1}: 跳过 {skip_in_batch}, 获取 {len(batch_results)} 条记录")

        # 如果已经获取足够的记录，退出
        if len(all_results) >= page_size:
            all_results = all_results[:page_size]
            break

    return all_results, total_count

def smart_pagination_service(supervisor_id: int, page: int = 1, page_size: int = 20,
                           sort_by: str = "fund_id", sort_order: str = "ASC") -> Dict[str, Any]:
    """
    智能分页服务 - 集成完整的分页逻辑
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        total_start_time = time.time()

        print(f"=== 智能分页查询: 用户{supervisor_id}, 第{page}页 ===")

        # 步骤 1-3: 获取权限ID (这部分保持快速)
        step_start = time.time()
        subordinate_ids = get_subordinate_ids(cursor, supervisor_id)
        if supervisor_id not in subordinate_ids:
            subordinate_ids.append(supervisor_id)

        order_ids = get_order_ids_for_users(cursor, subordinate_ids)
        customer_ids = get_customer_ids_for_users(cursor, subordinate_ids)

        permissions_time = time.time() - step_start
        print(f"权限查询: {len(subordinate_ids)} 用户, {len(order_ids)} 订单, {len(customer_ids)} 客户 ({permissions_time:.4f}s)")

        # 步骤 4: 优化分页查询
        step_start = time.time()
        results, total_count = get_financial_funds_optimized_pagination(
            cursor, subordinate_ids, order_ids, customer_ids,
            page=page, page_size=page_size, sort_by=sort_by, sort_order=sort_order,
            estimate_total=(page == 1)  # 只在第一页估算总数
        )
        pagination_time = time.time() - step_start

        total_time = time.time() - total_start_time

        # 计算分页信息
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        has_next = page < total_pages
        has_prev = page > 1

        response = {
            "data": [
                {
                    "fund_id": row[0],
                    "handle_by": row[1],
                    "order_id": row[2],
                    "customer_id": row[3],
                    "amount": float(row[4]),
                    "handler_name": row[5],
                    "department": row[6]
                } for row in results
            ],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": has_prev
            },
            "performance": {
                "permissions_time": round(permissions_time, 4),
                "pagination_time": round(pagination_time, 4),
                "total_time": round(total_time, 4)
            }
        }

        print(f"查询完成: {len(results)} 条记录, 总耗时: {total_time:.4f}s")
        return response

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return {"error": str(err)}
    finally:
        if conn and conn.is_connected():
            conn.close()

def test_optimized_pagination():
    """测试优化后的分页"""
    supervisor_id = 2

    print("=== 测试优化分页性能 ===")

    # 测试第1页
    result1 = smart_pagination_service(supervisor_id, page=1, page_size=20)
    if "error" not in result1:
        print(f"第1页: {len(result1['data'])} 条记录")
        print(f"性能: {result1['performance']}")
        print(f"分页: {result1['pagination']}")

    print("\n" + "="*50)

    # 测试第5页
    result5 = smart_pagination_service(supervisor_id, page=5, page_size=20)
    if "error" not in result5:
        print(f"第5页: {len(result5['data'])} 条记录")
        print(f"性能: {result5['performance']}")

    print("\n" + "="*50)

    # 测试第10页
    result10 = smart_pagination_service(supervisor_id, page=10, page_size=20)
    if "error" not in result10:
        print(f"第10页: {len(result10['data'])} 条记录")
        print(f"性能: {result10['performance']}")

def main():
    """主函数"""
    test_optimized_pagination()

if __name__ == "__main__":
    main()
