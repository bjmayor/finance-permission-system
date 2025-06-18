#!/usr/bin/env python3
"""
Materialized View Redesign - Step 2

Redesigns the materialized view according to specifications:
• Keep single MV (`finance_permission_mv`) with new column `permission_type ENUM('handle','order','customer')`
• Add surrogate bigint PK (`mv_id`) only for maintenance; **no indexes during load**
• Partition the MV by **hash(permission_type)** or **list(permission_type)** if DB supports
"""

import os
import time
import mysql.connector
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Load environment variables
load_dotenv()

# Database connection configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456'),
    'database': os.getenv('DB_NAME_V2', 'finance'),
    'autocommit': False,
    'charset': 'utf8mb4'
}

def get_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Get database connection"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def check_mysql_version() -> Dict[str, Any]:
    """Check MySQL version and partitioning support"""
    conn = get_db_connection()
    if not conn:
        return {'version': None, 'supports_partitioning': False}
    
    cursor = conn.cursor()
    
    try:
        # Get MySQL version
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        
        # Check if partitioning is supported
        cursor.execute("SHOW PLUGINS WHERE Name = 'partition' AND Status = 'ACTIVE'")
        partitioning_support = cursor.fetchone() is not None
        
        return {
            'version': version,
            'supports_partitioning': partitioning_support
        }
        
    except mysql.connector.Error as e:
        print(f"⚠️ Error checking MySQL capabilities: {e}")
        return {'version': None, 'supports_partitioning': False}
    finally:
        cursor.close()
        conn.close()

def backup_existing_materialized_view() -> bool:
    """Backup existing materialized view before redesign"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("=== Backing up existing materialized view ===")
        
        # Check if old MV exists
        cursor.execute("SHOW TABLES LIKE 'mv_supervisor_financial'")
        if cursor.fetchone():
            # Create backup
            cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial_backup_redesign")
            cursor.execute("""
                CREATE TABLE mv_supervisor_financial_backup_redesign AS 
                SELECT * FROM mv_supervisor_financial
            """)
            
            cursor.execute("SELECT COUNT(*) FROM mv_supervisor_financial_backup_redesign")
            backup_count = cursor.fetchone()[0]
            
            print(f"✅ Backed up {backup_count:,} records from existing materialized view")
        else:
            print("ℹ️ No existing materialized view found to backup")
        
        conn.commit()
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Backup failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def create_redesigned_materialized_view() -> bool:
    """Create the new redesigned materialized view"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== Creating redesigned materialized view ===")
        
        # Check MySQL capabilities
        mysql_info = check_mysql_version()
        print(f"MySQL Version: {mysql_info['version']}")
        print(f"Partitioning Support: {mysql_info['supports_partitioning']}")
        
        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS finance_permission_mv")
        cursor.execute("DROP TABLE IF EXISTS mv_supervisor_financial")
        
        # Create the new redesigned materialized view
        if mysql_info['supports_partitioning']:
            print("Creating partitioned materialized view...")
            
            # Create with LIST partitioning by permission_type for better performance
            create_sql = """
                CREATE TABLE finance_permission_mv (
                    mv_id BIGINT NOT NULL AUTO_INCREMENT,
                    supervisor_id INT NOT NULL,
                    fund_id INT NOT NULL,
                    handle_by INT NOT NULL,
                    handler_name VARCHAR(255),
                    department VARCHAR(100),
                    order_id INT,
                    customer_id INT,
                    amount DECIMAL(15, 2),
                    permission_type ENUM('handle','order','customer') NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (mv_id, permission_type)
                ) ENGINE=InnoDB 
                  DEFAULT CHARSET=utf8mb4 
                  COLLATE=utf8mb4_general_ci
                  ROW_FORMAT=COMPRESSED
                PARTITION BY LIST COLUMNS(permission_type) (
                    PARTITION p_handle VALUES IN ('handle'),
                    PARTITION p_order VALUES IN ('order'),
                    PARTITION p_customer VALUES IN ('customer')
                )
            """
        else:
            print("Creating non-partitioned materialized view (partitioning not supported)...")
            
            # Create without partitioning as fallback
            create_sql = """
                CREATE TABLE finance_permission_mv (
                    mv_id BIGINT NOT NULL AUTO_INCREMENT,
                    supervisor_id INT NOT NULL,
                    fund_id INT NOT NULL,
                    handle_by INT NOT NULL,
                    handler_name VARCHAR(255),
                    department VARCHAR(100),
                    order_id INT,
                    customer_id INT,
                    amount DECIMAL(15, 2),
                    permission_type ENUM('handle','order','customer') NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (mv_id)
                ) ENGINE=InnoDB 
                  DEFAULT CHARSET=utf8mb4 
                  COLLATE=utf8mb4_general_ci
                  ROW_FORMAT=COMPRESSED
            """
        
        cursor.execute(create_sql)
        conn.commit()
        
        print("✅ Redesigned materialized view 'finance_permission_mv' created successfully")
        print("📝 Key features:")
        print("   • Surrogate BIGINT primary key (mv_id) for maintenance")
        print("   • permission_type ENUM('handle','order','customer') column")
        if mysql_info['supports_partitioning']:
            print("   • LIST partitioned by permission_type for independent loading/querying")
        print("   • NO indexes during load (will be added after data population)")
        print("   • Compressed row format for space efficiency")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Failed to create redesigned materialized view: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def populate_redesigned_materialized_view() -> bool:
    """Populate the redesigned materialized view with data from all three dimensions"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== Populating redesigned materialized view ===")
        print("ℹ️ Loading data without indexes for optimal performance")
        
        start_time = time.time()
        total_records = 0
        
        # 1. Load HANDLE dimension data
        print("\n1. Loading HANDLE permission dimension...")
        handle_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_mv 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT 
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'handle' as permission_type
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
        """)
        
        handle_count = cursor.rowcount
        handle_time = time.time() - handle_start
        total_records += handle_count
        
        print(f"   ✅ HANDLE dimension: {handle_count:,} records loaded in {handle_time:.2f}s")
        
        # 2. Load ORDER dimension data
        print("\n2. Loading ORDER permission dimension...")
        order_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_mv 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT DISTINCT
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'order' as permission_type
            FROM user_hierarchy h
            JOIN orders o ON h.subordinate_id = o.user_id
            JOIN financial_funds f ON o.order_id = f.order_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE f.order_id IS NOT NULL
        """)
        
        order_count = cursor.rowcount
        order_time = time.time() - order_start
        total_records += order_count
        
        print(f"   ✅ ORDER dimension: {order_count:,} records loaded in {order_time:.2f}s")
        
        # 3. Load CUSTOMER dimension data
        print("\n3. Loading CUSTOMER permission dimension...")
        customer_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_mv 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT DISTINCT
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'customer' as permission_type
            FROM user_hierarchy h
            JOIN customers c ON h.subordinate_id = c.admin_user_id
            JOIN financial_funds f ON c.customer_id = f.customer_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE f.customer_id IS NOT NULL
        """)
        
        customer_count = cursor.rowcount
        customer_time = time.time() - customer_start
        total_records += customer_count
        
        print(f"   ✅ CUSTOMER dimension: {customer_count:,} records loaded in {customer_time:.2f}s")
        
        # Update all timestamps
        print("\n4. Updating timestamps...")
        cursor.execute("UPDATE finance_permission_mv SET last_updated = NOW()")
        
        conn.commit()
        
        total_time = time.time() - start_time
        
        print(f"\n✅ Data loading completed successfully")
        print(f"📊 Summary:")
        print(f"   • Total records loaded: {total_records:,}")
        print(f"   • HANDLE records: {handle_count:,}")
        print(f"   • ORDER records: {order_count:,}")
        print(f"   • CUSTOMER records: {customer_count:,}")
        print(f"   • Total loading time: {total_time:.2f}s")
        print(f"   • Average loading speed: {total_records/total_time:.0f} records/second")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Data loading failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def create_post_load_indexes() -> bool:
    """Create indexes after data loading for optimal performance"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== Creating post-load indexes for query optimization ===")
        
        indexes = [
            {
                'name': 'idx_supervisor_type',
                'sql': 'CREATE INDEX idx_supervisor_type ON finance_permission_mv (supervisor_id, permission_type)',
                'description': 'Optimizes queries by supervisor and permission type'
            },
            {
                'name': 'idx_supervisor_fund',
                'sql': 'CREATE INDEX idx_supervisor_fund ON finance_permission_mv (supervisor_id, fund_id)',
                'description': 'Optimizes queries by supervisor and fund'
            },
            {
                'name': 'idx_permission_type',
                'sql': 'CREATE INDEX idx_permission_type ON finance_permission_mv (permission_type)',
                'description': 'Optimizes queries filtering by permission type'
            },
            {
                'name': 'idx_supervisor_amount',
                'sql': 'CREATE INDEX idx_supervisor_amount ON finance_permission_mv (supervisor_id, amount DESC)',
                'description': 'Optimizes queries with amount sorting'
            },
            {
                'name': 'idx_last_updated',
                'sql': 'CREATE INDEX idx_last_updated ON finance_permission_mv (last_updated)',
                'description': 'Optimizes incremental update queries'
            }
        ]
        
        for i, index in enumerate(indexes, 1):
            print(f"\n{i}. Creating {index['name']}...")
            print(f"   Purpose: {index['description']}")
            
            start_time = time.time()
            cursor.execute(index['sql'])
            index_time = time.time() - start_time
            
            print(f"   ✅ Created in {index_time:.2f}s")
        
        conn.commit()
        
        print(f"\n✅ All {len(indexes)} indexes created successfully")
        print("🚀 Materialized view is now ready for high-performance queries")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Index creation failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def verify_redesigned_materialized_view() -> bool:
    """Verify the redesigned materialized view structure and data"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n=== Verifying redesigned materialized view ===")
        
        # 1. Check table structure
        print("\n1. Verifying table structure...")
        cursor.execute("DESCRIBE finance_permission_mv")
        columns = cursor.fetchall()
        
        expected_columns = ['mv_id', 'supervisor_id', 'fund_id', 'handle_by', 'handler_name', 
                           'department', 'order_id', 'customer_id', 'amount', 'permission_type', 'last_updated']
        
        actual_columns = [col[0] for col in columns]
        
        if all(col in actual_columns for col in expected_columns):
            print("   ✅ All expected columns present")
        else:
            missing = set(expected_columns) - set(actual_columns)
            print(f"   ❌ Missing columns: {missing}")
            return False
        
        # 2. Check partitioning (if supported)
        cursor.execute("""
            SELECT PARTITION_NAME, PARTITION_EXPRESSION, PARTITION_DESCRIPTION 
            FROM INFORMATION_SCHEMA.PARTITIONS 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'finance_permission_mv'
            AND PARTITION_NAME IS NOT NULL
        """)
        
        partitions = cursor.fetchall()
        if partitions:
            print("\n2. Verifying partitioning...")
            for partition in partitions:
                print(f"   • Partition: {partition[0]} - {partition[2]}")
            print("   ✅ Partitioning configured correctly")
        else:
            print("\n2. No partitioning detected (may not be supported)")
        
        # 3. Check data distribution
        print("\n3. Verifying data distribution...")
        cursor.execute("""
            SELECT permission_type, COUNT(*) as record_count
            FROM finance_permission_mv
            GROUP BY permission_type
            ORDER BY permission_type
        """)
        
        distribution = cursor.fetchall()
        total_records = 0
        
        for perm_type, count in distribution:
            print(f"   • {perm_type.upper()} dimension: {count:,} records")
            total_records += count
        
        print(f"   • Total records: {total_records:,}")
        
        # 4. Check indexes
        print("\n4. Verifying indexes...")
        cursor.execute("""
            SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'finance_permission_mv'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """)
        
        indexes = cursor.fetchall()
        if indexes:
            current_index = None
            for index_name, column_name, non_unique in indexes:
                if index_name != current_index:
                    print(f"   • {index_name}: ", end="")
                    current_index = index_name
                print(f"{column_name} ", end="")
            print("\n   ✅ Indexes created successfully")
        else:
            print("   ⚠️ No indexes found")
        
        # 5. Performance test
        print("\n5. Testing query performance...")
        
        # Test query by supervisor and permission type
        cursor.execute("""
            SELECT supervisor_id FROM finance_permission_mv 
            WHERE permission_type = 'handle'
            LIMIT 1
        """)
        
        test_supervisor = cursor.fetchone()
        if test_supervisor:
            supervisor_id = test_supervisor[0]
            
            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM finance_permission_mv 
                WHERE supervisor_id = %s AND permission_type = 'handle'
            """, (supervisor_id,))
            
            result = cursor.fetchone()[0]
            query_time = (time.time() - start_time) * 1000
            
            print(f"   • Sample query: {result:,} records in {query_time:.2f}ms")
            
            if query_time < 50:
                print("   ✅ Query performance excellent")
            elif query_time < 200:
                print("   ✅ Query performance good")
            else:
                print("   ⚠️ Query performance may need optimization")
        
        print("\n✅ Verification completed successfully")
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to execute the materialized view redesign"""
    print("🏗️ Materialized View Redesign - Step 2")
    print("=" * 60)
    print("Redesigning materialized view with:")
    print("• Single MV 'finance_permission_mv' with permission_type ENUM")
    print("• Surrogate BIGINT primary key (mv_id)")
    print("• Partitioning by permission_type (if supported)")
    print("• No indexes during load for optimal performance")
    print("=" * 60)
    
    overall_start = time.time()
    
    # Step 1: Backup existing materialized view
    if not backup_existing_materialized_view():
        print("❌ Failed to backup existing materialized view")
        return
    
    # Step 2: Create redesigned materialized view
    if not create_redesigned_materialized_view():
        print("❌ Failed to create redesigned materialized view")
        return
    
    # Step 3: Populate with data
    if not populate_redesigned_materialized_view():
        print("❌ Failed to populate materialized view")
        return
    
    # Step 4: Create indexes for query optimization
    if not create_post_load_indexes():
        print("❌ Failed to create post-load indexes")
        return
    
    # Step 5: Verify the result
    if not verify_redesigned_materialized_view():
        print("❌ Verification failed")
        return
    
    total_time = time.time() - overall_start
    
    print("\n" + "=" * 60)
    print("🎉 Materialized View Redesign Completed Successfully!")
    print("=" * 60)
    print(f"⏱️ Total execution time: {total_time:.2f} seconds")
    print("\n🚀 Key improvements:")
    print("  • Unified permission model with three dimensions")
    print("  • Partitioned by permission_type for independent processing")
    print("  • Optimized loading process (no indexes during load)")
    print("  • Post-load index creation for query performance")
    print("  • Surrogate key for maintenance operations")
    print("\n📝 The materialized view 'finance_permission_mv' is now ready for use")
    print("   with improved performance and maintainability.")
    
if __name__ == "__main__":
    main()

