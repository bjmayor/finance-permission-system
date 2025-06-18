#!/usr/bin/env python3
"""
Step 4: High-Speed Bulk Load Pipeline Implementation

This script implements both approaches for high-speed bulk loading:
- Approach A: Single SQL with UNION ALL
- Approach B: Staging for parallelism

Both approaches create the finance_permission_mv materialized view
with optimal performance characteristics.
"""

import os
import time
import argparse
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

def execute_sql_file(cursor, file_path: str) -> bool:
    """Execute SQL commands from a file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
            
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for statement in statements:
            # Skip comments and empty statements
            if statement.startswith('--') or statement.startswith('/*') or not statement:
                continue
                
            cursor.execute(statement)
            
        return True
    except Exception as e:
        print(f"❌ Error executing SQL file {file_path}: {e}")
        return False

def approach_a_single_sql() -> bool:
    """Implement Approach A: Single SQL with UNION ALL"""
    print("\n" + "=" * 70)
    print("🚀 Approach A: Single SQL with UNION ALL")
    print("=" * 70)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        start_time = time.time()
        
        print("📋 Step 1: Creating materialized view structure...")
        
        # Drop existing table
        cursor.execute("DROP TABLE IF EXISTS finance_permission_mv")
        
        # Create the materialized view table
        cursor.execute("""
            CREATE TABLE finance_permission_mv (
                mv_id BIGINT NOT NULL AUTO_INCREMENT,
                supervisor_id INT NOT NULL COMMENT 'ID of the supervisor user',
                fund_id INT NOT NULL COMMENT 'ID of the financial fund record',
                handle_by INT NOT NULL COMMENT 'ID of the user who handled the transaction',
                
                handler_name VARCHAR(255) COMMENT 'Name of the handler',
                department VARCHAR(100) COMMENT 'Department of the handler',
                
                order_id INT COMMENT 'Associated order ID',
                customer_id INT COMMENT 'Associated customer ID',
                amount DECIMAL(15, 2) COMMENT 'Transaction amount',
                
                permission_type ENUM('handle','order','customer') NOT NULL 
                    COMMENT 'Type of permission dimension',
                
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                PRIMARY KEY (mv_id)
            ) 
            ENGINE=InnoDB 
            DEFAULT CHARSET=utf8mb4 
            COLLATE=utf8mb4_general_ci
            ROW_FORMAT=COMPRESSED
            COMMENT='Finance permission materialized view - Approach A'
        """)
        
        conn.commit()
        table_time = time.time() - start_time
        print(f"   ✅ Table created in {table_time:.2f}s")
        
        print("\n📊 Step 2: Loading data with single UNION ALL query...")
        load_start = time.time()
        
        # Execute the large UNION ALL query
        cursor.execute("""
            INSERT INTO finance_permission_mv 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            
            -- HANDLE dimension: Direct subordinate handling permissions
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
            
            UNION ALL
            
            -- ORDER dimension: Order ownership permissions
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
            
            UNION ALL
            
            -- CUSTOMER dimension: Customer administration permissions
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
        
        load_count = cursor.rowcount
        load_time = time.time() - load_start
        conn.commit()
        
        print(f"   ✅ Loaded {load_count:,} records in {load_time:.2f}s")
        print(f"   📈 Loading speed: {load_count/load_time:.0f} records/second")
        
        print("\n🕐 Step 3: Updating timestamps...")
        cursor.execute("UPDATE finance_permission_mv SET last_updated = NOW()")
        conn.commit()
        
        print("\n🔍 Step 4: Creating indexes...")
        index_start = time.time()
        
        indexes = [
            ("idx_supervisor_type", "(supervisor_id, permission_type)"),
            ("idx_supervisor_fund", "(supervisor_id, fund_id)"),
            ("idx_permission_type", "(permission_type)"),
            ("idx_supervisor_amount", "(supervisor_id, amount DESC)"),
            ("idx_last_updated", "(last_updated)")
        ]
        
        for idx_name, idx_cols in indexes:
            cursor.execute(f"CREATE INDEX {idx_name} ON finance_permission_mv {idx_cols}")
            print(f"   • Created {idx_name}")
        
        conn.commit()
        index_time = time.time() - index_start
        total_time = time.time() - start_time
        
        print(f"   ✅ All indexes created in {index_time:.2f}s")
        
        # Get final statistics
        cursor.execute("SELECT COUNT(*) FROM finance_permission_mv")
        final_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT permission_type, COUNT(*) as count
            FROM finance_permission_mv
            GROUP BY permission_type
            ORDER BY permission_type
        """)
        distribution = cursor.fetchall()
        
        print(f"\n📊 Final Results:")
        print(f"   🎯 Total records: {final_count:,}")
        print(f"   ⏱️ Total time: {total_time:.2f}s")
        print(f"   🚀 Overall speed: {final_count/total_time:.0f} records/second")
        print(f"\n   📋 Distribution by type:")
        for perm_type, count in distribution:
            print(f"     • {perm_type}: {count:,}")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Approach A failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def approach_b_staging_parallel() -> bool:
    """Implement Approach B: Staging for parallelism"""
    print("\n" + "=" * 70)
    print("🚀 Approach B: Staging for Parallelism")
    print("=" * 70)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        start_time = time.time()
        
        print("📋 Step 1: Creating staging table...")
        
        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS finance_permission_stage")
        cursor.execute("DROP TABLE IF EXISTS finance_permission_mv")
        
        # Create staging table
        cursor.execute("""
            CREATE TABLE finance_permission_stage (
                stage_id BIGINT NOT NULL AUTO_INCREMENT,
                supervisor_id INT NOT NULL,
                fund_id INT NOT NULL,
                handle_by INT NOT NULL,
                handler_name VARCHAR(255),
                department VARCHAR(100),
                order_id INT,
                customer_id INT,
                amount DECIMAL(15, 2),
                permission_type ENUM('handle','order','customer') NOT NULL,
                load_batch INT DEFAULT 1 COMMENT 'Batch number for parallel loading',
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (stage_id),
                KEY idx_temp_batch (load_batch, permission_type)
            ) 
            ENGINE=InnoDB 
            DEFAULT CHARSET=utf8mb4 
            COLLATE=utf8mb4_general_ci
            COMMENT='Staging table for parallel bulk loading'
        """)
        
        conn.commit()
        
        print("\n⚙️ Step 2: Optimizing MySQL settings for bulk load...")
        
        # Optimize for bulk operations
        optimizations = [
            "SET SESSION foreign_key_checks = 0",
            "SET SESSION unique_checks = 0",
            "SET SESSION autocommit = 0"
        ]
        
        for opt in optimizations:
            cursor.execute(opt)
        
        print("\n📊 Step 3: Loading data in parallel batches...")
        
        # Batch 1: HANDLE dimension
        print("   🔄 Loading HANDLE dimension...")
        batch1_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_stage 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type, load_batch)
            SELECT 
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'handle' as permission_type,
                1 as load_batch
            FROM user_hierarchy h
            JOIN financial_funds f ON h.subordinate_id = f.handle_by
            JOIN users u ON f.handle_by = u.id
        """)
        
        batch1_count = cursor.rowcount
        conn.commit()
        batch1_time = time.time() - batch1_start
        print(f"     ✅ HANDLE: {batch1_count:,} records in {batch1_time:.2f}s")
        
        # Batch 2: ORDER dimension
        print("   🔄 Loading ORDER dimension...")
        batch2_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_stage 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type, load_batch)
            SELECT DISTINCT
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'order' as permission_type,
                2 as load_batch
            FROM user_hierarchy h
            JOIN orders o ON h.subordinate_id = o.user_id
            JOIN financial_funds f ON o.order_id = f.order_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE f.order_id IS NOT NULL
        """)
        
        batch2_count = cursor.rowcount
        conn.commit()
        batch2_time = time.time() - batch2_start
        print(f"     ✅ ORDER: {batch2_count:,} records in {batch2_time:.2f}s")
        
        # Batch 3: CUSTOMER dimension
        print("   🔄 Loading CUSTOMER dimension...")
        batch3_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_stage 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type, load_batch)
            SELECT DISTINCT
                h.user_id AS supervisor_id,
                f.fund_id,
                f.handle_by,
                u.name AS handler_name,
                u.department,
                f.order_id,
                f.customer_id,
                f.amount,
                'customer' as permission_type,
                3 as load_batch
            FROM user_hierarchy h
            JOIN customers c ON h.subordinate_id = c.admin_user_id
            JOIN financial_funds f ON c.customer_id = f.customer_id
            LEFT JOIN users u ON f.handle_by = u.id
            WHERE f.customer_id IS NOT NULL
        """)
        
        batch3_count = cursor.rowcount
        conn.commit()
        batch3_time = time.time() - batch3_start
        print(f"     ✅ CUSTOMER: {batch3_count:,} records in {batch3_time:.2f}s")
        
        staging_total = batch1_count + batch2_count + batch3_count
        staging_time = batch1_time + batch2_time + batch3_time
        
        print(f"\n   📊 Staging Summary: {staging_total:,} records in {staging_time:.2f}s")
        
        print("\n🔍 Step 4: Creating staging indexes...")
        staging_indexes = [
            "CREATE INDEX idx_stage_supervisor_type ON finance_permission_stage (supervisor_id, permission_type)",
            "CREATE INDEX idx_stage_fund ON finance_permission_stage (fund_id)",
            "CREATE INDEX idx_stage_unique ON finance_permission_stage (supervisor_id, fund_id, permission_type)"
        ]
        
        for idx_sql in staging_indexes:
            cursor.execute(idx_sql)
        
        conn.commit()
        
        print("\n📋 Step 5: Creating final materialized view...")
        
        cursor.execute("""
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
            ) 
            ENGINE=InnoDB 
            DEFAULT CHARSET=utf8mb4 
            COLLATE=utf8mb4_general_ci
            ROW_FORMAT=COMPRESSED
            COMMENT='Final materialized view - Approach B'
        """)
        
        print("\n🔄 Step 6: Transferring data from staging to final...")
        transfer_start = time.time()
        
        cursor.execute("""
            INSERT INTO finance_permission_mv 
                (supervisor_id, fund_id, handle_by, handler_name, department, 
                 order_id, customer_id, amount, permission_type)
            SELECT DISTINCT
                supervisor_id,
                fund_id,
                handle_by,
                handler_name,
                department,
                order_id,
                customer_id,
                amount,
                permission_type
            FROM finance_permission_stage
            ORDER BY supervisor_id, permission_type, fund_id
        """)
        
        transfer_count = cursor.rowcount
        transfer_time = time.time() - transfer_start
        
        cursor.execute("UPDATE finance_permission_mv SET last_updated = NOW()")
        conn.commit()
        
        print(f"   ✅ Transferred {transfer_count:,} records in {transfer_time:.2f}s")
        
        print("\n🔍 Step 7: Creating production indexes...")
        
        # Re-enable optimizations
        cursor.execute("SET SESSION foreign_key_checks = 1")
        cursor.execute("SET SESSION unique_checks = 1")
        cursor.execute("SET SESSION autocommit = 1")
        
        production_indexes = [
            ("idx_supervisor_type", "(supervisor_id, permission_type)"),
            ("idx_supervisor_fund", "(supervisor_id, fund_id)"),
            ("idx_permission_type", "(permission_type)"),
            ("idx_supervisor_amount", "(supervisor_id, amount DESC)"),
            ("idx_last_updated", "(last_updated)")
        ]
        
        for idx_name, idx_cols in production_indexes:
            cursor.execute(f"CREATE INDEX {idx_name} ON finance_permission_mv {idx_cols}")
            print(f"   • Created {idx_name}")
        
        conn.commit()
        
        total_time = time.time() - start_time
        
        print(f"\n📊 Step 8: Final verification...")
        
        # Get final statistics
        cursor.execute("SELECT COUNT(*) FROM finance_permission_mv")
        final_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT permission_type, COUNT(*) as count
            FROM finance_permission_mv
            GROUP BY permission_type
            ORDER BY permission_type
        """)
        distribution = cursor.fetchall()
        
        print(f"\n📊 Final Results:")
        print(f"   🎯 Total records: {final_count:,}")
        print(f"   ⏱️ Total time: {total_time:.2f}s")
        print(f"   🚀 Overall speed: {final_count/total_time:.0f} records/second")
        print(f"\n   📋 Distribution by type:")
        for perm_type, count in distribution:
            print(f"     • {perm_type}: {count:,}")
        
        print(f"\n🧹 Cleanup: Dropping staging table...")
        cursor.execute("DROP TABLE finance_permission_stage")
        conn.commit()
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Approach B failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def verify_materialized_view() -> bool:
    """Verify the created materialized view"""
    print("\n" + "=" * 70)
    print("🔍 Verifying Materialized View")
    print("=" * 70)
    
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Check table exists
        cursor.execute("SHOW TABLES LIKE 'finance_permission_mv'")
        if not cursor.fetchone():
            print("❌ finance_permission_mv table not found")
            return False
        
        # Get table info
        cursor.execute("""
            SELECT 
                table_rows,
                ROUND((data_length + index_length) / (1024 * 1024), 2) as size_mb
            FROM information_schema.tables
            WHERE table_schema = DATABASE() 
                AND table_name = 'finance_permission_mv'
        """)
        
        table_info = cursor.fetchone()
        if table_info:
            rows, size_mb = table_info
            print(f"   📊 Table rows: ~{rows:,}")
            print(f"   💾 Table size: {size_mb} MB")
        
        # Check indexes
        cursor.execute("""
            SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'finance_permission_mv'
            GROUP BY INDEX_NAME
            ORDER BY INDEX_NAME
        """)
        
        indexes = cursor.fetchall()
        print(f"\n   🔍 Indexes ({len(indexes)}):")
        for idx_name, columns in indexes:
            print(f"     • {idx_name}: {columns}")
        
        # Performance test
        print(f"\n   ⚡ Performance test:")
        
        # Get a test supervisor
        cursor.execute("""
            SELECT supervisor_id 
            FROM finance_permission_mv 
            WHERE permission_type = 'handle'
            LIMIT 1
        """)
        
        test_result = cursor.fetchone()
        if test_result:
            test_supervisor = test_result[0]
            
            start_time = time.time()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM finance_permission_mv 
                WHERE supervisor_id = %s AND permission_type = 'handle'
            """, (test_supervisor,))
            
            result = cursor.fetchone()[0]
            query_time = (time.time() - start_time) * 1000
            
            print(f"     • Sample query: {result:,} records in {query_time:.2f}ms")
            
            if query_time < 50:
                print(f"     ✅ Excellent performance")
            elif query_time < 200:
                print(f"     ✅ Good performance")
            else:
                print(f"     ⚠️ Performance may need optimization")
        
        print(f"\n✅ Materialized view verification completed")
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="High-Speed Bulk Load Pipeline Implementation")
    parser.add_argument("--approach", choices=['a', 'b', 'both'], default='both',
                       help="Which approach to run: a=Single SQL, b=Staging Parallel, both=Both approaches")
    parser.add_argument("--verify", action='store_true',
                       help="Verify the materialized view after creation")
    parser.add_argument("--no-drop", action='store_true',
                       help="Don't drop existing tables (for testing)")
    
    args = parser.parse_args()
    
    print("🏗️ High-Speed Bulk Load Pipeline Implementation")
    print("=" * 70)
    print("This script implements both approaches for creating finance_permission_mv:")
    print("• Approach A: Single SQL with UNION ALL (simpler, atomic)")
    print("• Approach B: Staging for parallelism (faster, more complex)")
    print("=" * 70)
    
    success = True
    
    if args.approach in ['a', 'both']:
        print(f"\n🎯 Starting Approach A...")
        if not approach_a_single_sql():
            success = False
            print(f"❌ Approach A failed")
        else:
            print(f"✅ Approach A completed successfully")
    
    if args.approach in ['b', 'both'] and success:
        print(f"\n🎯 Starting Approach B...")
        if not approach_b_staging_parallel():
            success = False
            print(f"❌ Approach B failed")
        else:
            print(f"✅ Approach B completed successfully")
    
    if args.verify and success:
        verify_materialized_view()
    
    if success:
        print(f"\n🎉 High-Speed Bulk Load Pipeline completed successfully!")
        print(f"The finance_permission_mv materialized view is ready for use.")
    else:
        print(f"\n❌ Pipeline execution failed. Please check the errors above.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

