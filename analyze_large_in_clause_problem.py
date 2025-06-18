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

def test_large_in_clause_limits():
    """测试大IN子句的MySQL限制"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("=== 测试大IN子句的MySQL限制 ===\n")

        # 测试不同大小的IN子句
        test_sizes = [1000, 5000, 10000, 20000, 50000, 100000]

        for size in test_sizes:
            print(f"测试 IN 子句大小: {size}")

            # 生成测试数据
            id_list = list(range(1, size + 1))
            placeholders = ','.join(['%s'] * len(id_list))

            try:
                start_time = time.time()

                # 测试简单的IN查询
                query = f"SELECT COUNT(*) FROM financial_funds WHERE handle_by IN ({placeholders})"
                cursor.execute(query, tuple(id_list))
                result = cursor.fetchone()[0]

                execution_time = time.time() - start_time

                print(f"  ✅ 成功: {result} 条记录, 耗时: {execution_time:.4f}s")

                # 分析性能警告
                if execution_time > 1.0:
                    print(f"  ⚠️  性能警告: 查询耗时 {execution_time:.4f}s > 1秒")
                if execution_time > 5.0:
                    print(f"  🚨 严重警告: 查询耗时 {execution_time:.4f}s > 5秒")

            except mysql.connector.Error as e:
                print(f"  ❌ 失败: {e}")
                if "max_allowed_packet" in str(e):
                    print(f"  📦 数据包大小限制: 超过 max_allowed_packet")
                elif "too many" in str(e).lower():
                    print(f"  📊 参数数量限制: IN 子句参数过多")
            except Exception as e:
                print(f"  ❌ 其他错误: {e}")

            print()

    finally:
        conn.close()

def test_complex_or_query_performance():
    """测试复杂OR查询的性能问题"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("=== 测试复杂OR查询的性能问题 ===\n")

        # 获取真实的权限数据
        cursor.execute("SELECT subordinate_id FROM user_hierarchy WHERE user_id = 2")
        handle_by_ids = [row[0] for row in cursor.fetchall()][:1000]  # 限制到1000个

        cursor.execute("SELECT order_id FROM orders WHERE user_id IN (2,3,4,5,6) LIMIT 5000")
        order_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT customer_id FROM customers WHERE admin_user_id IN (2,3,4,5,6) LIMIT 5000")
        customer_ids = [row[0] for row in cursor.fetchall()]

        print(f"测试数据规模:")
        print(f"  handle_by_ids: {len(handle_by_ids)} 个")
        print(f"  order_ids: {len(order_ids)} 个")
        print(f"  customer_ids: {len(customer_ids)} 个")
        print(f"  总参数数量: {len(handle_by_ids) + len(order_ids) + len(customer_ids)}\n")

        # 方案1: 单个大OR查询
        print("方案1: 单个大OR查询")
        try:
            start_time = time.time()

            conditions = []
            params = []

            if handle_by_ids:
                placeholders = ','.join(['%s'] * len(handle_by_ids))
                conditions.append(f"handle_by IN ({placeholders})")
                params.extend(handle_by_ids)

            if order_ids:
                placeholders = ','.join(['%s'] * len(order_ids))
                conditions.append(f"order_id IN ({placeholders})")
                params.extend(order_ids)

            if customer_ids:
                placeholders = ','.join(['%s'] * len(customer_ids))
                conditions.append(f"customer_id IN ({placeholders})")
                params.extend(customer_ids)

            query = f"""
                SELECT COUNT(DISTINCT fund_id)
                FROM financial_funds
                WHERE {' OR '.join(conditions)}
            """

            cursor.execute(query, tuple(params))
            result = cursor.fetchone()[0]

            or_query_time = time.time() - start_time
            print(f"  ✅ 成功: {result} 条记录")
            print(f"  ⏱️  查询耗时: {or_query_time:.4f}s")
            print(f"  📊 SQL参数数量: {len(params)}")

            # 分析SQL语句大小
            sql_size = len(query) + sum(len(str(p)) for p in params)
            print(f"  📦 SQL语句大小: {sql_size:,} 字节")

        except Exception as e:
            print(f"  ❌ 大OR查询失败: {e}")
            or_query_time = float('inf')

        print()

        # 方案2: 临时表方案
        print("方案2: 临时表方案")
        try:
            start_time = time.time()

            temp_table_name = f"temp_test_{int(time.time() * 1000)}"

            # 创建临时表
            cursor.execute(f"""
                CREATE TEMPORARY TABLE {temp_table_name} (
                    fund_id INT PRIMARY KEY
                ) ENGINE=MEMORY
            """)

            # 分批插入，避免大IN子句
            batch_size = 1000
            total_inserted = 0

            # 插入 handle_by 权限
            if handle_by_ids:
                for i in range(0, len(handle_by_ids), batch_size):
                    batch = handle_by_ids[i:i + batch_size]
                    placeholders = ','.join(['%s'] * len(batch))
                    cursor.execute(f"""
                        INSERT IGNORE INTO {temp_table_name} (fund_id)
                        SELECT fund_id FROM financial_funds
                        WHERE handle_by IN ({placeholders})
                    """, tuple(batch))
                    total_inserted += cursor.rowcount

            # 插入 order_id 权限
            if order_ids:
                for i in range(0, len(order_ids), batch_size):
                    batch = order_ids[i:i + batch_size]
                    placeholders = ','.join(['%s'] * len(batch))
                    cursor.execute(f"""
                        INSERT IGNORE INTO {temp_table_name} (fund_id)
                        SELECT fund_id FROM financial_funds
                        WHERE order_id IN ({placeholders})
                    """, tuple(batch))
                    total_inserted += cursor.rowcount

            # 插入 customer_id 权限
            if customer_ids:
                for i in range(0, len(customer_ids), batch_size):
                    batch = customer_ids[i:i + batch_size]
                    placeholders = ','.join(['%s'] * len(batch))
                    cursor.execute(f"""
                        INSERT IGNORE INTO {temp_table_name} (fund_id)
                        SELECT fund_id FROM financial_funds
                        WHERE customer_id IN ({placeholders})
                    """, tuple(batch))
                    total_inserted += cursor.rowcount

            # 获取最终结果
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            result = cursor.fetchone()[0]

            temp_table_time = time.time() - start_time

            print(f"  ✅ 成功: {result} 条记录")
            print(f"  ⏱️  查询耗时: {temp_table_time:.4f}s")
            print(f"  📊 最大单次IN参数: {batch_size}")
            print(f"  🔄 分批次数: {(len(handle_by_ids) + len(order_ids) + len(customer_ids) + batch_size - 1) // batch_size}")

            # 清理临时表
            cursor.execute(f"DROP TEMPORARY TABLE {temp_table_name}")

        except Exception as e:
            print(f"  ❌ 临时表方案失败: {e}")
            temp_table_time = float('inf')

        print()

        # 性能对比
        print("=== 性能对比 ===")
        if or_query_time != float('inf') and temp_table_time != float('inf'):
            improvement = ((or_query_time - temp_table_time) / or_query_time) * 100
            print(f"大OR查询耗时: {or_query_time:.4f}s")
            print(f"临时表耗时: {temp_table_time:.4f}s")
            print(f"性能提升: {improvement:+.1f}%")
        elif or_query_time == float('inf'):
            print("❌ 大OR查询失败，临时表方案是唯一可行方案")
        else:
            print("⚠️  两种方案都有问题")

    finally:
        conn.close()

def analyze_mysql_limits():
    """分析MySQL的相关限制"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("=== MySQL相关限制分析 ===\n")

        # 查询MySQL配置
        limits_to_check = [
            'max_allowed_packet',
            'max_prepared_stmt_count',
            'max_connections',
            'tmp_table_size',
            'max_heap_table_size',
            'thread_stack'
        ]

        print("MySQL配置参数:")
        for limit in limits_to_check:
            try:
                cursor.execute(f"SHOW VARIABLES LIKE '{limit}'")
                result = cursor.fetchone()
                if result:
                    value = result[1]
                    # 转换字节为MB（如果是数字）
                    try:
                        bytes_value = int(value)
                        mb_value = bytes_value / (1024 * 1024)
                        print(f"  {limit}: {value} ({mb_value:.1f} MB)")
                    except:
                        print(f"  {limit}: {value}")
                else:
                    print(f"  {limit}: 未找到")
            except:
                print(f"  {limit}: 查询失败")

        print()

        # 分析大IN子句的问题
        print("大IN子句问题分析:")
        print("  1. 📦 max_allowed_packet 限制:")
        print("     - SQL语句大小不能超过此限制")
        print("     - 50,000个整数ID ≈ 500KB SQL语句")
        print("     - 默认配置通常是16MB，但实际限制更复杂")
        print()

        print("  2. 🧠 内存使用问题:")
        print("     - 大IN子句需要大量内存来优化查询计划")
        print("     - 可能导致临时表溢出到磁盘")
        print("     - 影响整体数据库性能")
        print()

        print("  3. ⚡ 查询优化问题:")
        print("     - MySQL优化器处理大IN子句效率低")
        print("     - 可能选择次优的执行计划")
        print("     - 索引使用效率下降")
        print()

        print("  4. 🚫 硬限制:")
        print("     - MySQL对SQL语句长度有隐含限制")
        print("     - 预处理语句参数数量限制")
        print("     - 连接超时可能在大查询中触发")
        print()

        # 临时表方案的优势
        print("临时表方案解决方案:")
        print("  ✅ 避免大IN子句:")
        print("     - 每次最多1000个参数")
        print("     - SQL语句大小可控")
        print("     - 不会触发MySQL限制")
        print()

        print("  ✅ 更好的内存管理:")
        print("     - 使用MEMORY引擎的临时表")
        print("     - 分批处理减少内存峰值")
        print("     - 利用INSERT IGNORE自动去重")
        print()

        print("  ✅ 更好的查询优化:")
        print("     - 每个小查询都能得到最优执行计划")
        print("     - 充分利用索引")
        print("     - 可预测的性能表现")
        print()

        print("  ✅ 更好的扩展性:")
        print("     - 可以处理任意大小的ID集合")
        print("     - 易于添加新的权限维度")
        print("     - 支持复杂的权限逻辑")

    finally:
        conn.close()

def demonstrate_real_world_scenario():
    """演示真实世界场景下的问题"""

    print("=== 真实世界场景演示 ===\n")

    scenarios = [
        {
            "name": "小型企业",
            "users": 50,
            "orders_per_user": 100,
            "customers_per_user": 80,
            "description": "50用户，每人100订单80客户"
        },
        {
            "name": "中型企业",
            "users": 500,
            "orders_per_user": 200,
            "customers_per_user": 150,
            "description": "500用户，每人200订单150客户"
        },
        {
            "name": "大型企业",
            "users": 2000,
            "orders_per_user": 500,
            "customers_per_user": 300,
            "description": "2000用户，每人500订单300客户"
        },
        {
            "name": "超大型企业",
            "users": 10000,
            "orders_per_user": 1000,
            "customers_per_user": 800,
            "description": "10000用户，每人1000订单800客户"
        }
    ]

    for scenario in scenarios:
        print(f"场景: {scenario['name']} ({scenario['description']})")

        total_users = scenario['users']
        total_orders = total_users * scenario['orders_per_user']
        total_customers = total_users * scenario['customers_per_user']
        total_ids = total_users + total_orders + total_customers

        # 估算SQL语句大小
        avg_id_length = 6  # 假设平均ID长度为6字符
        estimated_sql_size = total_ids * avg_id_length + total_ids * 2  # ID + 逗号空格
        estimated_sql_mb = estimated_sql_size / (1024 * 1024)

        print(f"  总用户数: {total_users:,}")
        print(f"  总订单数: {total_orders:,}")
        print(f"  总客户数: {total_customers:,}")
        print(f"  IN子句总参数: {total_ids:,}")
        print(f"  估算SQL大小: {estimated_sql_mb:.2f} MB")

        # 风险评估
        if total_ids > 100000:
            print(f"  🚨 高风险: 参数过多，几乎肯定会失败")
        elif total_ids > 50000:
            print(f"  ⚠️  中风险: 可能触发MySQL限制")
        elif total_ids > 10000:
            print(f"  🔶 低风险: 性能可能受影响")
        else:
            print(f"  ✅ 安全: 在合理范围内")

        if estimated_sql_mb > 16:
            print(f"  📦 可能超过默认max_allowed_packet (16MB)")

        # 临时表方案的批次数
        batch_size = 1000
        num_batches = (total_ids + batch_size - 1) // batch_size
        print(f"  🔄 临时表方案需要: {num_batches} 个批次")

        print()

def main():
    """主函数"""

    print("大IN子句问题深度分析\n")
    print("="*60 + "\n")

    # 1. 测试MySQL限制
    test_large_in_clause_limits()
    print()

    # 2. 测试复杂OR查询性能
    test_complex_or_query_performance()
    print()

    # 3. 分析MySQL限制
    analyze_mysql_limits()
    print()

    # 4. 真实场景演示
    demonstrate_real_world_scenario()

    print("="*60)
    print("结论:")
    print("✅ 临时表方案是解决大IN子句问题的最佳方案")
    print("✅ 避免了MySQL的各种限制和性能问题")
    print("✅ 提供了可预测和稳定的性能表现")
    print("✅ 支持任意规模的权限数据")

if __name__ == "__main__":
    main()
