#!/usr/bin/env python3
"""
完整的财务权限业务需求分析
基于main.py的原始需求，分析当前物化视图遗漏的权限维度
"""

import os
import mysql.connector
from dotenv import load_dotenv
from prettytable import PrettyTable
import time

# 加载环境变量
load_dotenv()

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
        return mysql.connector.connect(**config)
    except mysql.connector.Error as e:
        print(f"数据库连接失败: {e}")
        return None

def analyze_original_requirements():
    """分析原始业务需求"""
    print("=" * 80)
    print("原始业务需求分析（基于main.py）")
    print("=" * 80)
    
    print("""
    🎯 完整的财务权限逻辑包含三个维度：
    
    1️⃣ 资金处理人维度（handle_by）
       └── 用户可以查看其下属处理的所有资金记录
    
    2️⃣ 订单维度（order_id）  
       └── 用户可以查看其下属创建的订单相关的资金记录
    
    3️⃣ 客户维度（customer_id）
       └── 用户可以查看其下属管理的客户相关的资金记录
    
    📋 权限判断逻辑（原始代码）：
    
    if (fund.handle_by in scope["handle_by"] or           # 处理人权限
        fund.order_id in scope["order_ids"] or           # 订单权限  
        fund.customer_id in scope["customer_ids"]):      # 客户权限
        filtered_funds.append(fund)
    
    ⚠️  当前物化视图只实现了第1个维度（handle_by），
        遗漏了订单和客户两个重要的权限维度！
    """)

def check_current_implementation():
    """检查当前实现的缺失"""
    print("\n" + "=" * 80)
    print("当前实现缺失分析")
    print("=" * 80)
    
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    # 检查当前物化视图的JOIN逻辑
    print("""
    🔍 当前物化视图的JOIN逻辑：
    
    SELECT h.user_id AS supervisor_id, f.fund_id, ...
    FROM user_hierarchy h
    JOIN financial_funds f ON h.subordinate_id = f.handle_by  ← 只考虑了handle_by
    JOIN users u ON f.handle_by = u.id
    
    ❌ 缺失的权限维度：
    """)
    
    # 检查orders表
    try:
        cursor.execute("SELECT COUNT(*) FROM orders")
        orders_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM orders")
        order_creators = cursor.fetchone()[0]
        
        print(f"    📋 订单维度：")
        print(f"       • orders表记录数: {orders_count:,}")
        print(f"       • 订单创建人数: {order_creators:,}")
        print(f"       • 当前状态: ❌ 未包含在物化视图中")
        
    except mysql.connector.Error as e:
        print(f"    📋 订单维度: ❌ orders表不存在 ({e})")
    
    # 检查customers表
    try:
        cursor.execute("SELECT COUNT(*) FROM customers")
        customers_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT admin_user_id) FROM customers")
        customer_admins = cursor.fetchone()[0]
        
        print(f"    👥 客户维度：")
        print(f"       • customers表记录数: {customers_count:,}")
        print(f"       • 客户管理员数: {customer_admins:,}")
        print(f"       • 当前状态: ❌ 未包含在物化视图中")
        
    except mysql.connector.Error as e:
        print(f"    👥 客户维度: ❌ customers表不存在 ({e})")
    
    cursor.close()
    conn.close()

def analyze_missing_data():
    """分析遗漏的数据量"""
    print("\n" + "=" * 80)
    print("遗漏数据量分析")
    print("=" * 80)
    
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    test_supervisor = 70
    
    print(f"🔬 以用户{test_supervisor}为例分析遗漏的权限数据：")
    
    # 当前物化视图的数据（只包含handle_by维度）
    cursor.execute("""
        SELECT COUNT(*) FROM mv_supervisor_financial 
        WHERE supervisor_id = %s
    """, (test_supervisor,))
    current_mv_count = cursor.fetchone()[0]
    
    print(f"\n📊 当前物化视图数据: {current_mv_count:,} 条")
    
    # 分析应该包含的完整数据
    print(f"\n🔍 应该包含的完整权限数据分析：")
    
    # 1. handle_by维度（已实现）
    cursor.execute("""
        SELECT COUNT(*)
        FROM user_hierarchy h
        JOIN financial_funds f ON h.subordinate_id = f.handle_by
        WHERE h.user_id = %s
    """, (test_supervisor,))
    handle_by_count = cursor.fetchone()[0]
    
    print(f"   1️⃣ 处理人维度 (已实现): {handle_by_count:,} 条")
    
    # 2. order_id维度（缺失）
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_hierarchy h
            JOIN orders o ON h.subordinate_id = o.user_id
            JOIN financial_funds f ON o.order_id = f.order_id
            WHERE h.user_id = %s
        """, (test_supervisor,))
        order_count = cursor.fetchone()[0]
        
        print(f"   2️⃣ 订单维度 (缺失): {order_count:,} 条")
        
    except mysql.connector.Error as e:
        print(f"   2️⃣ 订单维度: ❌ 无法计算 ({e})")
        order_count = 0
    
    # 3. customer_id维度（缺失）
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM user_hierarchy h
            JOIN customers c ON h.subordinate_id = c.admin_user_id
            JOIN financial_funds f ON c.customer_id = f.customer_id
            WHERE h.user_id = %s
        """, (test_supervisor,))
        customer_count = cursor.fetchone()[0]
        
        print(f"   3️⃣ 客户维度 (缺失): {customer_count:,} 条")
        
    except mysql.connector.Error as e:
        print(f"   3️⃣ 客户维度: ❌ 无法计算 ({e})")
        customer_count = 0
    
    # 4. 计算去重后的总数（使用UNION避免重复）
    if order_count > 0 or customer_count > 0:
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT f.fund_id
                    FROM user_hierarchy h
                    JOIN financial_funds f ON (
                        h.subordinate_id = f.handle_by OR
                        h.subordinate_id IN (SELECT o.user_id FROM orders o WHERE o.order_id = f.order_id) OR
                        h.subordinate_id IN (SELECT c.admin_user_id FROM customers c WHERE c.customer_id = f.customer_id)
                    )
                    WHERE h.user_id = %s
                ) t
            """, (test_supervisor,))
            total_unique_count = cursor.fetchone()[0]
            
            print(f"\n📈 完整权限数据 (去重后): {total_unique_count:,} 条")
            print(f"📉 当前遗漏数据: {total_unique_count - current_mv_count:,} 条")
            
            if total_unique_count > current_mv_count:
                missing_percentage = ((total_unique_count - current_mv_count) / total_unique_count) * 100
                print(f"⚠️  遗漏比例: {missing_percentage:.1f}%")
            
        except mysql.connector.Error as e:
            print(f"❌ 无法计算完整数据量: {e}")
    
    cursor.close()
    conn.close()

def propose_solution():
    """提出解决方案"""
    print("\n" + "=" * 80)
    print("解决方案建议")
    print("=" * 80)
    
    print("""
    🛠️  修复物化视图的解决方案：
    
    方案1: 扩展当前物化视图 (推荐)
    ┌─────────────────────────────────────────────────────────────┐
    │ INSERT INTO mv_supervisor_financial_v2                      │
    │ SELECT DISTINCT                                             │
    │     h.user_id AS supervisor_id,                             │
    │     f.fund_id,                                              │
    │     f.handle_by,                                            │
    │     u.name AS handler_name,                                 │
    │     u.department,                                           │
    │     f.order_id,                                             │
    │     f.customer_id,                                          │
    │     f.amount,                                               │
    │     CASE                                                    │
    │         WHEN h.subordinate_id = f.handle_by THEN 'handle'   │
    │         WHEN h.subordinate_id = o.user_id THEN 'order'      │
    │         WHEN h.subordinate_id = c.admin_user_id THEN 'customer' │
    │     END AS permission_type                                  │
    │ FROM user_hierarchy h                                       │
    │ JOIN financial_funds f ON (                                 │
    │     h.subordinate_id = f.handle_by OR                       │
    │     EXISTS (SELECT 1 FROM orders o                          │
    │             WHERE o.order_id = f.order_id                   │
    │             AND o.user_id = h.subordinate_id) OR            │
    │     EXISTS (SELECT 1 FROM customers c                       │
    │             WHERE c.customer_id = f.customer_id             │
    │             AND c.admin_user_id = h.subordinate_id)         │
    │ )                                                           │
    │ LEFT JOIN users u ON f.handle_by = u.id                     │
    │ LEFT JOIN orders o ON f.order_id = o.order_id               │
    │ LEFT JOIN customers c ON f.customer_id = c.customer_id      │
    └─────────────────────────────────────────────────────────────┘
    
    方案2: 分别建立三个维度的物化视图
    • mv_supervisor_financial_handle (处理人维度)
    • mv_supervisor_financial_order (订单维度)  
    • mv_supervisor_financial_customer (客户维度)
    
    方案3: 运行时UNION查询 (不推荐，性能差)
    • 查询时动态UNION三个维度的结果
    
    🎯 推荐方案1的优势：
    ✅ 一次查询获得完整权限数据
    ✅ 新增permission_type字段标识权限来源
    ✅ 保持单表查询的高性能
    ✅ 便于理解和维护
    
    ⚠️  注意事项：
    • 使用DISTINCT避免重复记录
    • 需要处理LEFT JOIN的NULL值
    • 物化视图大小可能增加50-100%
    • 刷新时间会相应增加
    """)

def estimate_impact():
    """评估修复影响"""
    print("\n" + "=" * 80)
    print("修复影响评估")
    print("=" * 80)
    
    conn = connect_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    # 估算新物化视图的大小
    cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial")
    current_size = cursor.fetchone()[0]
    
    print(f"📊 影响评估：")
    print(f"   当前物化视图大小: {current_size:,} 条")
    
    # 尝试估算完整大小
    try:
        cursor.execute("""
            SELECT COUNT(DISTINCT f.fund_id)
            FROM user_hierarchy h
            JOIN financial_funds f ON (
                h.subordinate_id = f.handle_by OR
                h.subordinate_id IN (SELECT COALESCE(o.user_id, -1) FROM orders o WHERE o.order_id = f.order_id) OR
                h.subordinate_id IN (SELECT COALESCE(c.admin_user_id, -1) FROM customers c WHERE c.customer_id = f.customer_id)
            )
        """)
        estimated_size = cursor.fetchone()[0]
        
        size_increase = estimated_size - current_size
        increase_percentage = (size_increase / current_size) * 100 if current_size > 0 else 0
        
        print(f"   预估完整物化视图大小: {estimated_size:,} 条")
        print(f"   预估增加: {size_increase:,} 条 ({increase_percentage:.1f}%)")
        
        # 存储空间估算
        avg_row_size = 100  # 字节
        current_storage = (current_size * avg_row_size) / (1024 * 1024)  # MB
        estimated_storage = (estimated_size * avg_row_size) / (1024 * 1024)  # MB
        
        print(f"   当前存储占用: ~{current_storage:.1f} MB")
        print(f"   预估存储占用: ~{estimated_storage:.1f} MB")
        
        # 刷新时间估算
        current_refresh_time = 180  # 秒（基于之前的测试）
        estimated_refresh_time = current_refresh_time * (estimated_size / current_size)
        
        print(f"   当前刷新时间: ~{current_refresh_time} 秒")
        print(f"   预估刷新时间: ~{estimated_refresh_time:.0f} 秒")
        
    except mysql.connector.Error as e:
        print(f"   ❌ 无法估算完整大小: {e}")
    
    cursor.close()
    conn.close()
    
    print(f"""
    🎯 修复建议：
    
    1. 📋 短期方案（立即实施）：
       • 明确告知业务方当前权限不完整
       • 提供手动查询接口补充遗漏的权限数据
    
    2. 🔧 中期方案（1-2周实施）：
       • 重新设计和实现完整的物化视图
       • 充分测试新的权限逻辑
       • 灰度发布，确保数据准确性
    
    3. 📈 长期方案（持续优化）：
       • 建立权限数据的自动化测试
       • 监控物化视图的性能和准确性
       • 考虑分区或其他优化策略
    """)

def main():
    """主函数"""
    print("🔍 完整财务权限业务需求分析")
    print("基于main.py原始需求，分析当前实现的缺失")
    
    analyze_original_requirements()
    check_current_implementation()
    analyze_missing_data()
    propose_solution()
    estimate_impact()
    
    print("\n" + "=" * 80)
    print("🎯 核心结论")
    print("=" * 80)
    print("""
    ❌ 当前问题：
    • 物化视图只实现了1/3的权限逻辑（handle_by维度）
    • 遗漏了订单权限和客户权限两个重要维度
    • 导致用户看不到应该有权限访问的部分财务数据
    
    ✅ 解决方案：
    • 重新设计物化视图，包含完整的三维权限逻辑
    • 使用UNION或EXISTS子查询实现多维度权限
    • 增加permission_type字段标识权限来源
    
    📊 预期效果：
    • 物化视图大小可能增加50-100%
    • 权限数据完整性100%
    • 查询性能依然保持在毫秒级
    
    🚨 紧急程度：高
    当前的权限不完整可能影响业务决策，建议尽快修复。
    """)

if __name__ == "__main__":
    main()