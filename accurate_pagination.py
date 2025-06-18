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

def create_temp_permission_table(cursor, handle_by_ids: List[int], order_ids: List[int],
                                customer_ids: List[int], table_suffix: str) -> str:
    """创建临时权限表，确保数据准确性"""
    temp_table_name = f"temp_permissions_{table_suffix}"

    # 创建临时表
    cursor.execute(f"""
        CREATE TEMPORARY TABLE {temp_table_name} (
            fund_id INT PRIMARY KEY,
            handle_by INT,
            order_id INT,
            customer_id INT,
            amount DECIMAL(15,2),
            INDEX idx_handle_by (handle_by),
            INDEX idx_order_id (order_id),
            INDEX idx_customer_id (customer_id)
        ) ENGINE=MEMORY
    """)

    print(f"创建临时表 {temp_table_name}")

    # 分批插入数据，确保所有符合权限的记录都被包含
    batch_size = 1000

    # 处理 handle_by 权限
    if handle_by_ids:
        for i in range(0, len(handle_by_ids), batch_size):
            batch = handle_by_ids[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))

            cursor.execute(f"""
                INSERT IGNORE INTO {temp_table_name}
                (fund_id, handle_by, order_id, customer_id, amount)
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE handle_by IN ({placeholders})
            """, tuple(batch))
            print(f"插入 handle_by 批次 {i//batch_size + 1}: {len(batch)} 个ID")

    # 处理 order_id 权限
    if order_ids:
        for i in range(0, len(order_ids), batch_size):
            batch = order_ids[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))

            cursor.execute(f"""
                INSERT IGNORE INTO {temp_table_name}
                (fund_id, handle_by, order_id, customer_id, amount)
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE order_id IN ({placeholders})
            """, tuple(batch))
            print(f"插入 order_id 批次 {i//batch_size + 1}: {len(batch)} 个ID")

    # 处理 customer_id 权限
    if customer_ids:
        for i in range(0, len(customer_ids), batch_size):
            batch = customer_ids[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))

            cursor.execute(f"""
                INSERT IGNORE INTO {temp_table_name}
                (fund_id, handle_by, order_id, customer_id, amount)
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE customer_id IN ({placeholders})
            """, tuple(batch))
            print(f"插入 customer_id 批次 {i//batch_size + 1}: {len(batch)} 个ID")

    return temp_table_name

def get_accurate_pagination(cursor, temp_table_name: str, page: int = 1, page_size: int = 20,
                           sort_by: str = "fund_id", sort_order: str = "ASC") -> Tuple[List[Any], int]:
    """使用临时表进行准确的分页查询"""

    # 获取精确的总数
    cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
    total_count = cursor.fetchone()[0]

    # 计算偏移量
    offset = (page - 1) * page_size

    # 分页查询
    query = f"""
        SELECT t.fund_id, t.handle_by, t.order_id, t.customer_id, t.amount,
               u.name as handler_name, u.department
        FROM {temp_table_name} t
        JOIN users u ON t.handle_by = u.id
        ORDER BY t.{sort_by} {sort_order}
        LIMIT %s OFFSET %s
    """

    cursor.execute(query, (page_size, offset))
    results = cursor.fetchall()

    return results, total_count

def accurate_pagination_service(supervisor_id: int, page: int = 1, page_size: int = 20,
                               sort_by: str = "fund_id", sort_order: str = "ASC") -> Dict[str, Any]:
    """
    准确的分页服务 - 保证数据完整性和实时性
    """
    conn = None
    temp_table_name = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        total_start_time = time.time()

        print(f"=== 准确分页查询: 用户{supervisor_id}, 第{page}页 ===")

        # 步骤 1-3: 获取权限ID
        step_start = time.time()
        subordinate_ids = get_subordinate_ids(cursor, supervisor_id)
        if supervisor_id not in subordinate_ids:
            subordinate_ids.append(supervisor_id)

        order_ids = get_order_ids_for_users(cursor, subordinate_ids)
        customer_ids = get_customer_ids_for_users(cursor, subordinate_ids)

        permissions_time = time.time() - step_start
        print(f"权限查询: {len(subordinate_ids)} 用户, {len(order_ids)} 订单, {len(customer_ids)} 客户 ({permissions_time:.4f}s)")

        # 步骤 4: 创建临时表并插入所有符合权限的数据
        step_start = time.time()
        table_suffix = f"{supervisor_id}_{int(time.time() * 1000)}"
        temp_table_name = create_temp_permission_table(cursor, subordinate_ids, order_ids,
                                                      customer_ids, table_suffix)
        temp_table_time = time.time() - step_start
        print(f"临时表创建耗时: {temp_table_time:.4f}s")

        # 步骤 5: 精确分页查询
        step_start = time.time()
        results, total_count = get_accurate_pagination(cursor, temp_table_name,
                                                      page, page_size, sort_by, sort_order)
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
                "temp_table_time": round(temp_table_time, 4),
                "pagination_time": round(pagination_time, 4),
                "total_time": round(total_time, 4)
            },
            "data_integrity": {
                "accurate": True,
                "real_time": True,
                "method": "temporary_table_with_memory_engine"
            }
        }

        print(f"查询完成: {len(results)} 条记录, 总计: {total_count}, 总耗时: {total_time:.4f}s")
        return response

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return {"error": str(err)}
    finally:
        # 清理临时表
        if temp_table_name and conn and conn.is_connected():
            try:
                cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_table_name}")
                print(f"清理临时表: {temp_table_name}")
            except:
                pass

        if conn and conn.is_connected():
            conn.close()

def cached_pagination_service(supervisor_id: int, page: int = 1, page_size: int = 20,
                             sort_by: str = "fund_id", sort_order: str = "ASC",
                             cache_duration: int = 300) -> Dict[str, Any]:
    """
    带缓存的分页服务 - 在保证数据准确性的前提下提升性能
    """
    # 这里可以集成 Redis 或内存缓存
    # 缓存键: supervisor_id + sort_by + sort_order
    # 缓存内容: 临时表的所有数据
    # 缓存时长: 5分钟（可配置）

    # 暂时直接调用准确分页服务
    return accurate_pagination_service(supervisor_id, page, page_size, sort_by, sort_order)

def simulate_concurrent_load_temp_table(num_users: int = 10, requests_per_user: int = 5):
    """模拟临时表方案的并发负载测试"""
    from concurrent.futures import ThreadPoolExecutor
    import threading

    def user_request(user_id: int, request_id: int):
        try:
            page = (request_id % 10) + 1  # 随机页面
            result = accurate_pagination_service(user_id, page=page, page_size=20)

            if "error" in result:
                return {
                    "user_id": user_id,
                    "request_id": request_id,
                    "page": page,
                    "success": False,
                    "error": result["error"]
                }

            return {
                "user_id": user_id,
                "request_id": request_id,
                "page": page,
                "success": True,
                "total_time": result["performance"]["total_time"],
                "temp_table_time": result["performance"]["temp_table_time"],
                "pagination_time": result["performance"]["pagination_time"],
                "total_count": result["pagination"]["total_count"]
            }
        except Exception as e:
            return {
                "user_id": user_id,
                "request_id": request_id,
                "success": False,
                "error": str(e)
            }

    print(f"=== 临时表方案并发负载测试: {num_users} 用户, 每用户 {requests_per_user} 请求 ===")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=min(num_users, 20)) as executor:
        futures = []

        for user_id in range(2, 2 + num_users):  # 从用户ID 2开始
            for request_id in range(requests_per_user):
                future = executor.submit(user_request, user_id, request_id)
                futures.append(future)

        # 收集结果
        results = []
        for future in futures:
            try:
                result = future.result(timeout=60)  # 60秒超时
                results.append(result)
            except Exception as e:
                print(f"请求超时或失败: {e}")
                results.append({
                    "success": False,
                    "error": str(e)
                })

    total_time = time.time() - start_time

    # 统计结果
    successful_requests = [r for r in results if r.get("success", False)]
    failed_requests = [r for r in results if not r.get("success", False)]

    if successful_requests:
        avg_response_time = sum(r["total_time"] for r in successful_requests) / len(successful_requests)
        max_response_time = max(r["total_time"] for r in successful_requests)
        min_response_time = min(r["total_time"] for r in successful_requests)

        avg_temp_table_time = sum(r["temp_table_time"] for r in successful_requests) / len(successful_requests)
        avg_pagination_time = sum(r["pagination_time"] for r in successful_requests) / len(successful_requests)
    else:
        avg_response_time = max_response_time = min_response_time = 0
        avg_temp_table_time = avg_pagination_time = 0

    print(f"总耗时: {total_time:.2f}s")
    print(f"成功请求: {len(successful_requests)}/{len(results)}")
    print(f"失败请求: {len(failed_requests)}")
    print(f"平均响应时间: {avg_response_time:.4f}s")
    print(f"  - 临时表创建: {avg_temp_table_time:.4f}s")
    print(f"  - 分页查询: {avg_pagination_time:.4f}s")
    print(f"最大响应时间: {max_response_time:.4f}s")
    print(f"最小响应时间: {min_response_time:.4f}s")
    print(f"QPS: {len(successful_requests) / total_time:.2f}")

    # 打印失败的请求详情
    if failed_requests:
        print(f"\n失败请求详情:")
        for i, req in enumerate(failed_requests[:5]):  # 只显示前5个
            print(f"  {i+1}. User {req.get('user_id', 'N/A')}: {req.get('error', 'Unknown error')}")

def test_accurate_pagination():
    """测试准确分页"""
    supervisor_id = 2

    print("=== 测试准确分页性能 ===")

    # 测试第1页
    result1 = accurate_pagination_service(supervisor_id, page=1, page_size=20)
    if "error" not in result1:
        print(f"第1页: {len(result1['data'])} 条记录")
        print(f"性能: {result1['performance']}")
        print(f"分页: {result1['pagination']}")
        print(f"数据完整性: {result1['data_integrity']}")

    print("\n" + "="*50)

    # 测试第5页
    result5 = accurate_pagination_service(supervisor_id, page=5, page_size=20)
    if "error" not in result5:
        print(f"第5页: {len(result5['data'])} 条记录")
        print(f"性能: {result5['performance']}")

    print("\n" + "="*50)

    # 测试第10页
    result10 = accurate_pagination_service(supervisor_id, page=10, page_size=20)
    if "error" not in result10:
        print(f"第10页: {len(result10['data'])} 条记录")
        print(f"性能: {result10['performance']}")

def main():
    """主函数"""
    # 单用户测试
    test_accurate_pagination()

    print("\n" + "="*80 + "\n")

    # 小规模并发测试 - 5个用户
    simulate_concurrent_load_temp_table(num_users=5, requests_per_user=2)

    print("\n" + "="*80 + "\n")

    # 中等规模并发测试 - 10个用户
    simulate_concurrent_load_temp_table(num_users=10, requests_per_user=2)

    print("\n" + "="*80 + "\n")

    # 更高并发测试 - 20个用户
    simulate_concurrent_load_temp_table(num_users=20, requests_per_user=1)

if __name__ == "__main__":
    main()
