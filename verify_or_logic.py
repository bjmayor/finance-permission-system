import mysql.connector
import time
import os
from typing import List, Dict, Any, Set

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

def get_user_permissions(cursor, supervisor_id: int) -> Dict[str, List[int]]:
    """获取用户权限范围，模拟 main.py 中的 get_accessible_data_scope"""

    # 获取下属ID (handle_by权限)
    cursor.execute("SELECT subordinate_id FROM user_hierarchy WHERE user_id = %s", (supervisor_id,))
    handle_by_ids = [row[0] for row in cursor.fetchall()]
    if supervisor_id not in handle_by_ids:
        handle_by_ids.append(supervisor_id)

    # 获取订单ID (order_id权限)
    if handle_by_ids:
        placeholders = ','.join(['%s'] * len(handle_by_ids))
        cursor.execute(f"SELECT order_id FROM orders WHERE user_id IN ({placeholders})",
                      tuple(handle_by_ids))
        order_ids = [row[0] for row in cursor.fetchall()]
    else:
        order_ids = []

    # 获取客户ID (customer_id权限)
    if handle_by_ids:
        cursor.execute(f"SELECT customer_id FROM customers WHERE admin_user_id IN ({placeholders})",
                      tuple(handle_by_ids))
        customer_ids = [row[0] for row in cursor.fetchall()]
    else:
        customer_ids = []

    return {
        "handle_by": handle_by_ids,
        "order_ids": order_ids,
        "customer_ids": customer_ids
    }

def get_funds_with_direct_or_query(cursor, permissions: Dict[str, List[int]]) -> Set[int]:
    """使用直接的OR查询获取财务记录，模拟 main.py 中的逻辑"""

    conditions = []
    params = []

    if permissions["handle_by"]:
        placeholders = ','.join(['%s'] * len(permissions["handle_by"]))
        conditions.append(f"handle_by IN ({placeholders})")
        params.extend(permissions["handle_by"])

    if permissions["order_ids"]:
        placeholders = ','.join(['%s'] * len(permissions["order_ids"]))
        conditions.append(f"order_id IN ({placeholders})")
        params.extend(permissions["order_ids"])

    if permissions["customer_ids"]:
        placeholders = ','.join(['%s'] * len(permissions["customer_ids"]))
        conditions.append(f"customer_id IN ({placeholders})")
        params.extend(permissions["customer_ids"])

    if not conditions:
        return set()

    # 使用OR逻辑的SQL查询
    query = f"""
        SELECT DISTINCT fund_id
        FROM financial_funds
        WHERE {' OR '.join(conditions)}
        ORDER BY fund_id
    """

    cursor.execute(query, tuple(params))
    return {row[0] for row in cursor.fetchall()}

def get_funds_with_temp_table_approach(cursor, permissions: Dict[str, List[int]],
                                     table_suffix: str) -> Set[int]:
    """使用临时表方法获取财务记录"""

    temp_table_name = f"temp_verification_{table_suffix}"

    try:
        # 创建临时表
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                fund_id INT PRIMARY KEY
            ) ENGINE=MEMORY
        """)

        # 分别插入三种权限的记录（使用INSERT IGNORE避免重复）
        batch_size = 1000

        # 插入 handle_by 权限的记录
        if permissions["handle_by"]:
            for i in range(0, len(permissions["handle_by"]), batch_size):
                batch = permissions["handle_by"][i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                cursor.execute(f"""
                    INSERT IGNORE INTO {temp_table_name} (fund_id)
                    SELECT fund_id FROM financial_funds
                    WHERE handle_by IN ({placeholders})
                """, tuple(batch))

        # 插入 order_id 权限的记录
        if permissions["order_ids"]:
            for i in range(0, len(permissions["order_ids"]), batch_size):
                batch = permissions["order_ids"][i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                cursor.execute(f"""
                    INSERT IGNORE INTO {temp_table_name} (fund_id)
                    SELECT fund_id FROM financial_funds
                    WHERE order_id IN ({placeholders})
                """, tuple(batch))

        # 插入 customer_id 权限的记录
        if permissions["customer_ids"]:
            for i in range(0, len(permissions["customer_ids"]), batch_size):
                batch = permissions["customer_ids"][i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                cursor.execute(f"""
                    INSERT IGNORE INTO {temp_table_name} (fund_id)
                    SELECT fund_id FROM financial_funds
                    WHERE customer_id IN ({placeholders})
                """, tuple(batch))

        # 查询最终结果
        cursor.execute(f"SELECT fund_id FROM {temp_table_name} ORDER BY fund_id")
        return {row[0] for row in cursor.fetchall()}

    finally:
        # 清理临时表
        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {temp_table_name}")

def analyze_permission_overlap(cursor, permissions: Dict[str, List[int]]) -> Dict[str, Any]:
    """分析权限重叠情况，验证OR逻辑的必要性"""

    # 分别获取三种权限对应的fund_id集合
    handle_by_funds = set()
    if permissions["handle_by"]:
        placeholders = ','.join(['%s'] * len(permissions["handle_by"]))
        cursor.execute(f"SELECT fund_id FROM financial_funds WHERE handle_by IN ({placeholders})",
                      tuple(permissions["handle_by"]))
        handle_by_funds = {row[0] for row in cursor.fetchall()}

    order_id_funds = set()
    if permissions["order_ids"]:
        placeholders = ','.join(['%s'] * len(permissions["order_ids"]))
        cursor.execute(f"SELECT fund_id FROM financial_funds WHERE order_id IN ({placeholders})",
                      tuple(permissions["order_ids"]))
        order_id_funds = {row[0] for row in cursor.fetchall()}

    customer_id_funds = set()
    if permissions["customer_ids"]:
        placeholders = ','.join(['%s'] * len(permissions["customer_ids"]))
        cursor.execute(f"SELECT fund_id FROM financial_funds WHERE customer_id IN ({placeholders})",
                      tuple(permissions["customer_ids"]))
        customer_id_funds = {row[0] for row in cursor.fetchall()}

    # 计算重叠
    handle_order_overlap = handle_by_funds & order_id_funds
    handle_customer_overlap = handle_by_funds & customer_id_funds
    order_customer_overlap = order_id_funds & customer_id_funds
    three_way_overlap = handle_by_funds & order_id_funds & customer_id_funds

    # 计算OR逻辑的总结果
    union_result = handle_by_funds | order_id_funds | customer_id_funds

    return {
        "handle_by_count": len(handle_by_funds),
        "order_id_count": len(order_id_funds),
        "customer_id_count": len(customer_id_funds),
        "handle_order_overlap": len(handle_order_overlap),
        "handle_customer_overlap": len(handle_customer_overlap),
        "order_customer_overlap": len(order_customer_overlap),
        "three_way_overlap": len(three_way_overlap),
        "total_unique_funds": len(union_result),
        "sum_without_dedup": len(handle_by_funds) + len(order_id_funds) + len(customer_id_funds),
        "overlap_examples": {
            "handle_order": list(handle_order_overlap)[:5],
            "handle_customer": list(handle_customer_overlap)[:5],
            "order_customer": list(order_customer_overlap)[:5],
            "three_way": list(three_way_overlap)[:5]
        }
    }

def verify_or_logic_implementation(supervisor_id: int = 2):
    """验证OR逻辑实现是否正确"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print(f"=== 验证用户 {supervisor_id} 的OR逻辑实现 ===\n")

        # 1. 获取权限范围
        print("步骤1: 获取用户权限范围")
        start_time = time.time()
        permissions = get_user_permissions(cursor, supervisor_id)
        permissions_time = time.time() - start_time

        print(f"  handle_by权限: {len(permissions['handle_by'])} 个用户")
        print(f"  order_id权限: {len(permissions['order_ids'])} 个订单")
        print(f"  customer_id权限: {len(permissions['customer_ids'])} 个客户")
        print(f"  权限查询耗时: {permissions_time:.4f}s\n")

        # 2. 分析权限重叠情况
        print("步骤2: 分析权限重叠情况")
        start_time = time.time()
        overlap_analysis = analyze_permission_overlap(cursor, permissions)
        analysis_time = time.time() - start_time

        print(f"  handle_by对应的财务记录: {overlap_analysis['handle_by_count']} 条")
        print(f"  order_id对应的财务记录: {overlap_analysis['order_id_count']} 条")
        print(f"  customer_id对应的财务记录: {overlap_analysis['customer_id_count']} 条")
        print(f"  三个维度记录总和: {overlap_analysis['sum_without_dedup']} 条")
        print(f"  去重后的唯一记录: {overlap_analysis['total_unique_funds']} 条")
        print(f"  重复记录数量: {overlap_analysis['sum_without_dedup'] - overlap_analysis['total_unique_funds']}")
        print(f"  权限重叠分析耗时: {analysis_time:.4f}s\n")

        print("  权限重叠详情:")
        print(f"    handle_by ∩ order_id: {overlap_analysis['handle_order_overlap']} 条重叠")
        print(f"    handle_by ∩ customer_id: {overlap_analysis['handle_customer_overlap']} 条重叠")
        print(f"    order_id ∩ customer_id: {overlap_analysis['order_customer_overlap']} 条重叠")
        print(f"    三方重叠: {overlap_analysis['three_way_overlap']} 条\n")

        # 3. 使用直接OR查询
        print("步骤3: 使用直接OR查询（模拟main.py逻辑）")
        start_time = time.time()
        or_query_result = get_funds_with_direct_or_query(cursor, permissions)
        or_query_time = time.time() - start_time

        print(f"  OR查询结果: {len(or_query_result)} 条记录")
        print(f"  OR查询耗时: {or_query_time:.4f}s\n")

        # 4. 使用临时表方法
        print("步骤4: 使用临时表方法")
        start_time = time.time()
        table_suffix = f"{supervisor_id}_{int(time.time() * 1000)}"
        temp_table_result = get_funds_with_temp_table_approach(cursor, permissions, table_suffix)
        temp_table_time = time.time() - start_time

        print(f"  临时表结果: {len(temp_table_result)} 条记录")
        print(f"  临时表耗时: {temp_table_time:.4f}s\n")

        # 5. 验证结果一致性
        print("步骤5: 验证结果一致性")
        results_match = or_query_result == temp_table_result

        print(f"  结果是否一致: {'✅ 是' if results_match else '❌ 否'}")
        print(f"  OR查询记录数: {len(or_query_result)}")
        print(f"  临时表记录数: {len(temp_table_result)}")
        print(f"  理论计算记录数: {overlap_analysis['total_unique_funds']}")

        if not results_match:
            print("\n  ❌ 结果不一致详情:")
            only_in_or = or_query_result - temp_table_result
            only_in_temp = temp_table_result - or_query_result
            print(f"    只在OR查询中: {len(only_in_or)} 条")
            print(f"    只在临时表中: {len(only_in_temp)} 条")
            if only_in_or:
                print(f"    OR查询独有样例: {list(only_in_or)[:5]}")
            if only_in_temp:
                print(f"    临时表独有样例: {list(only_in_temp)[:5]}")
        else:
            print("  ✅ 两种方法得到完全相同的结果！")

        # 6. 性能对比
        print(f"\n步骤6: 性能对比")
        print(f"  OR查询耗时: {or_query_time:.4f}s")
        print(f"  临时表耗时: {temp_table_time:.4f}s")
        print(f"  性能差异: {((temp_table_time - or_query_time) / or_query_time * 100):+.1f}%")

        # 7. 结论
        print(f"\n=== 验证结论 ===")
        if results_match:
            print("✅ 临时表方法正确实现了OR逻辑")
            print("✅ INSERT IGNORE成功处理了权限重叠问题")
            print("✅ 两种方法在逻辑上完全等价")
            if overlap_analysis['sum_without_dedup'] > overlap_analysis['total_unique_funds']:
                print(f"✅ 成功去重了 {overlap_analysis['sum_without_dedup'] - overlap_analysis['total_unique_funds']} 条重复记录")
        else:
            print("❌ 临时表方法存在逻辑错误")
            print("❌ 需要检查实现细节")

        return {
            "results_match": results_match,
            "or_query_count": len(or_query_result),
            "temp_table_count": len(temp_table_result),
            "or_query_time": or_query_time,
            "temp_table_time": temp_table_time,
            "overlap_analysis": overlap_analysis
        }

    finally:
        conn.close()

def test_multiple_users():
    """测试多个用户的OR逻辑实现"""

    print("=== 测试多个用户的OR逻辑实现 ===\n")

    test_users = [2, 3, 4]  # 测试不同角色的用户

    for user_id in test_users:
        print(f"--- 测试用户 {user_id} ---")
        result = verify_or_logic_implementation(user_id)

        if result["results_match"]:
            print("✅ 验证通过")
        else:
            print("❌ 验证失败")

        print(f"记录数: {result['or_query_count']}, 性能: OR={result['or_query_time']:.4f}s, 临时表={result['temp_table_time']:.4f}s\n")

if __name__ == "__main__":
    # 验证单个用户
    verify_or_logic_implementation(supervisor_id=2)

    print("\n" + "="*80 + "\n")

    # 测试多个用户
    test_multiple_users()
