import mysql.connector
import time
import os
import threading
import hashlib
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import redis
import json

# Database connection details from environment variables
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "123456")
DB_NAME = os.environ.get("DB_NAME", "finance")

# Redis connection for caching
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

class ConnectionPool:
    """数据库连接池管理"""
    def __init__(self, pool_size=20):
        self.pool_size = pool_size
        self.pool = []
        self.lock = threading.Lock()
        self._create_pool()

    def _create_pool(self):
        for _ in range(self.pool_size):
            conn = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                autocommit=True,
                pool_reset_session=False
            )
            self.pool.append(conn)

    def get_connection(self):
        with self.lock:
            if self.pool:
                return self.pool.pop()
            else:
                # 如果池为空，创建新连接
                return mysql.connector.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    autocommit=True
                )

    def return_connection(self, conn):
        with self.lock:
            if len(self.pool) < self.pool_size and conn.is_connected():
                self.pool.append(conn)
            else:
                try:
                    conn.close()
                except:
                    pass

# 全局连接池
db_pool = ConnectionPool()

class PermissionCache:
    """权限缓存管理"""
    def __init__(self):
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1
            )
            # 测试连接
            self.redis_client.ping()
            self.cache_enabled = True
        except:
            print("Redis不可用，使用内存缓存")
            self.redis_client = None
            self.cache_enabled = False
            self.memory_cache = {}
            self.cache_lock = threading.Lock()

    def get_cache_key(self, supervisor_id: int, action: str = "permissions") -> str:
        """生成缓存键"""
        return f"finance_permission:{supervisor_id}:{action}"

    def get_permissions(self, supervisor_id: int) -> Dict:
        """获取缓存的权限数据"""
        cache_key = self.get_cache_key(supervisor_id)

        if self.cache_enabled and self.redis_client:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except:
                pass
        elif not self.cache_enabled:
            with self.cache_lock:
                return self.memory_cache.get(cache_key, None)

        return None

    def set_permissions(self, supervisor_id: int, permissions_data: Dict, ttl: int = 300):
        """缓存权限数据"""
        cache_key = self.get_cache_key(supervisor_id)

        if self.cache_enabled and self.redis_client:
            try:
                self.redis_client.setex(cache_key, ttl, json.dumps(permissions_data))
            except:
                pass
        elif not self.cache_enabled:
            with self.cache_lock:
                self.memory_cache[cache_key] = permissions_data
                # 简单的TTL实现：定时清理
                threading.Timer(ttl, lambda: self.memory_cache.pop(cache_key, None)).start()

# 全局缓存
permission_cache = PermissionCache()

def get_user_permissions(supervisor_id: int) -> Dict:
    """获取用户权限数据（带缓存）"""
    # 先检查缓存
    cached_permissions = permission_cache.get_permissions(supervisor_id)
    if cached_permissions:
        print(f"命中权限缓存: 用户{supervisor_id}")
        return cached_permissions

    # 缓存未命中，查询数据库
    conn = db_pool.get_connection()
    try:
        cursor = conn.cursor()

        # 获取下属ID
        cursor.execute("SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s", (supervisor_id,))
        subordinate_ids = [row[0] for row in cursor.fetchall()]
        if supervisor_id not in subordinate_ids:
            subordinate_ids.append(supervisor_id)

        # 获取订单ID
        order_ids = []
        if subordinate_ids:
            placeholders = ','.join(['%s'] * len(subordinate_ids))
            cursor.execute(f"SELECT order_id FROM orders WHERE user_id IN ({placeholders})",
                         tuple(subordinate_ids))
            order_ids = [row[0] for row in cursor.fetchall()]

        # 获取客户ID
        customer_ids = []
        if subordinate_ids:
            cursor.execute(f"SELECT customer_id FROM customers WHERE admin_user_id IN ({placeholders})",
                         tuple(subordinate_ids))
            customer_ids = [row[0] for row in cursor.fetchall()]

        permissions_data = {
            "subordinate_ids": subordinate_ids,
            "order_ids": order_ids,
            "customer_ids": customer_ids,
            "timestamp": time.time()
        }

        # 缓存权限数据
        permission_cache.set_permissions(supervisor_id, permissions_data)

        print(f"查询权限数据: 用户{supervisor_id}, {len(subordinate_ids)} 下属, {len(order_ids)} 订单, {len(customer_ids)} 客户")
        return permissions_data

    finally:
        db_pool.return_connection(conn)

def execute_batch_query(query: str, params: List, batch_size: int = 1000) -> List:
    """批量执行查询，避免IN子句过大"""
    if not params:
        return []

    conn = db_pool.get_connection()
    try:
        cursor = conn.cursor()
        all_results = []

        for i in range(0, len(params), batch_size):
            batch_params = params[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch_params))
            batch_query = query.format(placeholders=placeholders)

            cursor.execute(batch_query, tuple(batch_params))
            batch_results = cursor.fetchall()
            all_results.extend(batch_results)

        return all_results

    finally:
        db_pool.return_connection(conn)

def get_financial_funds_parallel(handle_by_ids: List[int], order_ids: List[int],
                                customer_ids: List[int]) -> List:
    """并行查询财务数据，避免临时表"""

    with ThreadPoolExecutor(max_workers=3) as executor:
        # 并行执行三个权限维度的查询
        futures = []

        if handle_by_ids:
            query = """
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE handle_by IN ({placeholders})
            """
            future = executor.submit(execute_batch_query, query, handle_by_ids)
            futures.append(future)

        if order_ids:
            query = """
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE order_id IN ({placeholders})
            """
            future = executor.submit(execute_batch_query, query, order_ids)
            futures.append(future)

        if customer_ids:
            query = """
                SELECT fund_id, handle_by, order_id, customer_id, amount
                FROM financial_funds
                WHERE customer_id IN ({placeholders})
            """
            future = executor.submit(execute_batch_query, query, customer_ids)
            futures.append(future)

        # 收集所有结果
        all_funds = []
        for future in futures:
            try:
                results = future.result(timeout=10)  # 10秒超时
                all_funds.extend(results)
            except Exception as e:
                print(f"并行查询异常: {e}")

    # 去重（基于fund_id）
    unique_funds = {}
    for fund in all_funds:
        fund_id = fund[0]
        if fund_id not in unique_funds:
            unique_funds[fund_id] = fund

    return list(unique_funds.values())

def get_paginated_financial_data(supervisor_id: int, page: int = 1, page_size: int = 20,
                                sort_by: str = "fund_id", sort_order: str = "ASC") -> Dict[str, Any]:
    """高并发分页查询 - 无临时表版本"""

    start_time = time.time()

    # 步骤1: 获取权限数据（带缓存）
    permissions_start = time.time()
    permissions = get_user_permissions(supervisor_id)
    permissions_time = time.time() - permissions_start

    # 步骤2: 并行查询财务数据
    query_start = time.time()
    all_funds = get_financial_funds_parallel(
        permissions["subordinate_ids"],
        permissions["order_ids"],
        permissions["customer_ids"]
    )
    query_time = time.time() - query_start

    # 步骤3: 内存排序和分页
    sort_start = time.time()

    # 排序
    reverse = (sort_order.upper() == "DESC")
    if sort_by == "fund_id":
        all_funds.sort(key=lambda x: x[0], reverse=reverse)
    elif sort_by == "amount":
        all_funds.sort(key=lambda x: x[4], reverse=reverse)
    elif sort_by == "handle_by":
        all_funds.sort(key=lambda x: x[1], reverse=reverse)
    else:
        all_funds.sort(key=lambda x: x[0], reverse=reverse)  # 默认按fund_id排序

    # 分页
    total_count = len(all_funds)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_funds = all_funds[start_idx:end_idx]

    sort_time = time.time() - sort_start

    # 步骤4: 获取用户信息
    user_start = time.time()
    fund_data = []
    if page_funds:
        # 获取唯一的handle_by IDs
        handle_by_ids = list(set(fund[1] for fund in page_funds))

        conn = db_pool.get_connection()
        try:
            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(handle_by_ids))
            cursor.execute(f"""
                SELECT id, name, department
                FROM users
                WHERE id IN ({placeholders})
            """, tuple(handle_by_ids))

            user_info = {row[0]: {"name": row[1], "department": row[2]}
                        for row in cursor.fetchall()}

            # 组装最终数据
            for fund in page_funds:
                user = user_info.get(fund[1], {"name": "未知", "department": "未知"})
                fund_data.append({
                    "fund_id": fund[0],
                    "handle_by": fund[1],
                    "order_id": fund[2],
                    "customer_id": fund[3],
                    "amount": float(fund[4]),
                    "handler_name": user["name"],
                    "department": user["department"]
                })
        finally:
            db_pool.return_connection(conn)

    user_time = time.time() - user_start
    total_time = time.time() - start_time

    # 分页信息
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    has_next = page < total_pages
    has_prev = page > 1

    return {
        "data": fund_data,
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
            "query_time": round(query_time, 4),
            "sort_time": round(sort_time, 4),
            "user_time": round(user_time, 4),
            "total_time": round(total_time, 4)
        },
        "optimization": {
            "method": "parallel_query_with_cache",
            "cache_hit": permissions_time < 0.01,
            "concurrent_safe": True,
            "memory_sorting": True
        }
    }

def simulate_concurrent_load(num_users: int = 10, requests_per_user: int = 5):
    """模拟并发负载测试"""

    def user_request(user_id: int, request_id: int):
        try:
            page = (request_id % 10) + 1  # 随机页面
            result = get_paginated_financial_data(user_id, page=page, page_size=20)

            return {
                "user_id": user_id,
                "request_id": request_id,
                "page": page,
                "success": True,
                "total_time": result["performance"]["total_time"],
                "total_count": result["pagination"]["total_count"],
                "cache_hit": result["optimization"]["cache_hit"]
            }
        except Exception as e:
            return {
                "user_id": user_id,
                "request_id": request_id,
                "success": False,
                "error": str(e)
            }

    print(f"=== 模拟并发负载: {num_users} 用户, 每用户 {requests_per_user} 请求 ===")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_users) as executor:
        futures = []

        for user_id in range(2, 2 + num_users):  # 从用户ID 2开始
            for request_id in range(requests_per_user):
                future = executor.submit(user_request, user_id, request_id)
                futures.append(future)

        # 收集结果
        results = []
        for future in futures:
            try:
                result = future.result(timeout=30)
                results.append(result)
            except Exception as e:
                print(f"请求超时或失败: {e}")

    total_time = time.time() - start_time

    # 统计结果
    successful_requests = [r for r in results if r.get("success", False)]
    failed_requests = [r for r in results if not r.get("success", False)]
    cache_hits = [r for r in successful_requests if r.get("cache_hit", False)]

    if successful_requests:
        avg_response_time = sum(r["total_time"] for r in successful_requests) / len(successful_requests)
        max_response_time = max(r["total_time"] for r in successful_requests)
        min_response_time = min(r["total_time"] for r in successful_requests)
    else:
        avg_response_time = max_response_time = min_response_time = 0

    print(f"总耗时: {total_time:.2f}s")
    print(f"成功请求: {len(successful_requests)}/{len(results)}")
    print(f"失败请求: {len(failed_requests)}")
    print(f"缓存命中: {len(cache_hits)}/{len(successful_requests)}")
    print(f"平均响应时间: {avg_response_time:.4f}s")
    print(f"最大响应时间: {max_response_time:.4f}s")
    print(f"最小响应时间: {min_response_time:.4f}s")
    print(f"QPS: {len(successful_requests) / total_time:.2f}")

def main():
    """主函数 - 测试高并发分页"""

    # 单用户测试
    print("=== 单用户测试 ===")
    result = get_paginated_financial_data(2, page=1, page_size=20)
    print(f"查询结果: {len(result['data'])} 条记录")
    print(f"性能: {result['performance']}")
    print(f"优化信息: {result['optimization']}")

    print("\n" + "="*60 + "\n")

    # 并发测试
    simulate_concurrent_load(num_users=20, requests_per_user=3)

if __name__ == "__main__":
    main()
