import os
import random
import time
from typing import List, Dict, Set
import mysql.connector
from dotenv import load_dotenv
from main import User, FinancialFund, Order, Customer, PermissionService

# Load environment variables from .env file
load_dotenv()

def get_database_stats(config=None):
    """Get database statistics"""
    if config is None:
        config = {
            'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT_V2', '3306')),
            'user': os.getenv('DB_USER_V2', 'root'),
            'password': os.getenv('DB_PASSWORD_V2', '123456'),
            'database': os.getenv('DB_NAME_V2', 'finance')
        }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("\n=== Database Statistics ===")
    
    # Get table row counts
    tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"Table '{table}' contains {count:,} records")
        except mysql.connector.Error as e:
            print(f"Error getting count for table '{table}': {e}")
    
    # Get database size
    try:
        cursor.execute(f"SELECT table_schema, SUM(data_length + index_length)/1024/1024 AS size_mb FROM information_schema.tables WHERE table_schema = '{conn.database}' GROUP BY table_schema")
        result = cursor.fetchone()
        if result:
            print(f"Database '{conn.database}' size: {result[1]:.2f} MB")
    except mysql.connector.Error as e:
        print(f"Error getting database size: {e}")
    
    # Get index information
    try:
        cursor.execute(f"""
        SELECT table_name, index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
        FROM information_schema.statistics
        WHERE table_schema = '{conn.database}'
        GROUP BY table_name, index_name
        ORDER BY table_name, index_name
        """)
        
        print("\nIndexes in the database:")
        for row in cursor.fetchall():
            print(f"  {row[0]}.{row[1]} ({row[2]})")
    except mysql.connector.Error as e:
        print(f"Error getting index information: {e}")
    
    conn.close()

class MySQLPermissionService(PermissionService):
    """MySQL-backed implementation of PermissionService"""
    
    def __init__(self, config=None):
        self.config = config or {
            'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
            'port': int(os.getenv('DB_PORT_V2', '3306')),
            'user': os.getenv('DB_USER_V2', 'root'),
            'password': os.getenv('DB_PASSWORD_V2', '123456'),
            'database': os.getenv('DB_NAME_V2', 'finance')
        }
        self.setup_database()
        # We don't call the parent's __init__ as we're replacing its functionality
        
    def get_connection(self):
        """Get a database connection"""
        return mysql.connector.connect(**self.config)
        
    def setup_database(self):
        """Create database tables if they don't exist"""
        # First connect without specifying database
        base_config = {k: v for k, v in self.config.items() if k != 'database'}
        conn = mysql.connector.connect(**base_config)
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
        conn.close()
        
        # Now connect with the database
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Temporarily disable foreign key checks during table creation
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Create Users table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            department VARCHAR(100) NOT NULL,
            parent_id INT,
            INDEX idx_users_role (role),
            INDEX idx_users_parent_id (parent_id)
        )
        ''')
        
        # Create Orders table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY,
            user_id INT NOT NULL,
            INDEX idx_orders_user_id (user_id)
        )
        ''')
        
        # Create Customers table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY,
            admin_user_id INT NOT NULL,
            INDEX idx_customers_admin_user_id (admin_user_id)
        )
        ''')
        
        # Create FinancialFunds table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_funds (
            fund_id INT PRIMARY KEY,
            handle_by INT NOT NULL,
            order_id INT NOT NULL,
            customer_id INT NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            INDEX idx_funds_handle_by (handle_by),
            INDEX idx_funds_order_id (order_id),
            INDEX idx_funds_customer_id (customer_id)
        )
        ''')
        
        # Create user hierarchy table for faster subordinate lookup
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_hierarchy (
            user_id INT NOT NULL,
            subordinate_id INT NOT NULL,
            depth INT NOT NULL,
            PRIMARY KEY (user_id, subordinate_id),
            INDEX idx_hierarchy_user_id (user_id),
            INDEX idx_hierarchy_subordinate_id (subordinate_id)
        )
        ''')
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        conn.close()
    
    def populate_test_data(self, num_records=1000000):
        """Populate database with test data"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Set optimizations for bulk insert
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        cursor.execute("SET autocommit = 0")
        
        print(f"Starting database population with {num_records:,} records per table...")
        start_time = time.time()
        
        # Add base users (from the original example)
        base_users = [
            (1, "超级管理员", "admin", "总部", None),
            (2, "财务主管", "supervisor", "华东区", 1),
            (3, "财务专员", "staff", "华东区", 2),
            (4, "财务专员", "staff", "华南区", 1)
        ]
        
        # Clear existing data in reverse order of dependencies
        print("Clearing existing data...")
        cursor.execute("DROP TABLE IF EXISTS user_hierarchy")
        cursor.execute("DROP TABLE IF EXISTS financial_funds")
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS users")
        
        # Recreate tables
        self.setup_database()
        
        # Get a fresh connection
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Set optimizations again
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        cursor.execute("SET autocommit = 0")
        
        # Insert base users
        cursor.executemany(
            "INSERT INTO users (id, name, role, department, parent_id) VALUES (%s, %s, %s, %s, %s)",
            base_users
        )
        conn.commit()
        
        # Generate all data in memory first
        print("Preparing data in memory...")
        
        # Prepare user data (starting from ID 5)
        roles = ["staff"] * 80 + ["supervisor"] * 15 + ["admin"] * 5  # Distribution: 80% staff, 15% supervisors, 5% admin
        departments = ["华东区", "华南区", "华北区", "西南区", "东北区", "西北区"]
        
        # Pre-generate all random choices to avoid repeated calls
        role_choices = [random.choice(roles) for _ in range(num_records)]
        dept_choices = [random.choice(departments) for _ in range(num_records)]
        
        # Generate and insert users in larger batches
        print("Generating users...")
        user_batch_size = 10000
        progress_step = max(1, num_records // 10)  # Show progress at 10% intervals
        
        for i in range(0, num_records, user_batch_size):
            batch_size = min(user_batch_size, num_records - i)
            user_batch = []
            
            for j in range(batch_size):
                idx = i + j
                user_id = idx + 5  # Start from ID 5
                name = f"用户{user_id}"
                role = role_choices[idx]
                department = dept_choices[idx]
                parent_id = random.randint(1, 4) if role != "admin" else None
                
                user_batch.append((user_id, name, role, department, parent_id))
            
            cursor.executemany(
                "INSERT INTO users (id, name, role, department, parent_id) VALUES (%s, %s, %s, %s, %s)",
                user_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} users ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert orders in larger batches
        print("Generating orders...")
        order_batch_size = 10000
        max_user_id = num_records + 4
        
        # Pre-generate random user IDs
        user_id_choices = [random.randint(1, max_user_id) for _ in range(num_records)]
        
        for i in range(0, num_records, order_batch_size):
            batch_size = min(order_batch_size, num_records - i)
            order_batch = []
            
            for j in range(batch_size):
                idx = i + j
                order_id = idx + 2001  # Start from 2001 to preserve original IDs
                user_id = user_id_choices[idx]
                
                order_batch.append((order_id, user_id))
            
            cursor.executemany(
                "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
                order_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} orders ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert customers in larger batches
        print("Generating customers...")
        customer_batch_size = 10000
        
        # Pre-generate random admin user IDs
        admin_user_id_choices = [random.randint(1, max_user_id) for _ in range(num_records)]
        
        for i in range(0, num_records, customer_batch_size):
            batch_size = min(customer_batch_size, num_records - i)
            customer_batch = []
            
            for j in range(batch_size):
                idx = i + j
                customer_id = idx + 3001  # Start from 3001 to preserve original IDs
                admin_user_id = admin_user_id_choices[idx]
                
                customer_batch.append((customer_id, admin_user_id))
            
            cursor.executemany(
                "INSERT INTO customers (customer_id, admin_user_id) VALUES (%s, %s)",
                customer_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} customers ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert financial funds in larger batches
        print("Generating financial funds...")
        fund_batch_size = 10000
        
        # Pre-generate random values
        handle_by_choices = [random.randint(1, max_user_id) for _ in range(num_records)]
        order_id_max = 2001 + num_records - 1
        order_id_choices = [random.randint(2001, order_id_max) for _ in range(num_records)]
        customer_id_max = 3001 + num_records - 1
        customer_id_choices = [random.randint(3001, customer_id_max) for _ in range(num_records)]
        amount_choices = [round(random.uniform(1000, 1000000), 2) for _ in range(num_records)]
        
        for i in range(0, num_records, fund_batch_size):
            batch_size = min(fund_batch_size, num_records - i)
            fund_batch = []
            
            for j in range(batch_size):
                idx = i + j
                fund_id = idx + 1001  # Start from 1001 to preserve original IDs
                handle_by = handle_by_choices[idx]
                order_id = order_id_choices[idx]
                customer_id = customer_id_choices[idx]
                amount = amount_choices[idx]
                
                fund_batch.append((fund_id, handle_by, order_id, customer_id, amount))
            
            cursor.executemany(
                "INSERT INTO financial_funds (fund_id, handle_by, order_id, customer_id, amount) VALUES (%s, %s, %s, %s, %s)",
                fund_batch
            )
            conn.commit()
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} financial funds ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate hierarchical relationships for base users only to avoid excessive computation
        print("Building user hierarchy table...")
            
        # Everyone is their own subordinate at depth 0
        cursor.execute("""
        INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT id, id, 0 FROM users WHERE id <= 4
        """)
            
        # Add direct subordinates (depth 1)
        cursor.execute("""
        INSERT INTO user_hierarchy (user_id, subordinate_id, depth)
        SELECT u1.id, u2.id, 1
        FROM users u1
        JOIN users u2 ON u2.parent_id = u1.id
        WHERE u1.id <= 4 AND u2.id <= 4
        """)
        # Process all data
        
        # Re-enable constraints and commit
        cursor.execute("SET foreign_key_checks = 1")
        cursor.execute("SET unique_checks = 1")
        cursor.execute("SET autocommit = 1")
        conn.commit()
        
        end_time = time.time()
        print(f"Database population completed in {end_time - start_time:.2f} seconds")
        
        # Analyze tables for better query performance
        print("Analyzing tables for query optimization...")
        cursor.execute("ANALYZE TABLE users")
        cursor.execute("ANALYZE TABLE orders")
        cursor.execute("ANALYZE TABLE customers")
        cursor.execute("ANALYZE TABLE financial_funds")
        cursor.execute("ANALYZE TABLE user_hierarchy")
        
        conn.close()
    
    def get_user(self, user_id):
        """Get a user by ID"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE id = %s", (user_id,))
        user_data = cursor.fetchone()
        
        conn.close()
        
        if user_data:
            return User(user_data['id'], user_data['name'], user_data['role'], 
                       user_data['department'], user_data['parent_id'])
        return None
    
    def get_users(self):
        """Get all users"""
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT id, name, role, department, parent_id FROM users")
        users_data = cursor.fetchall()
        
        conn.close()
        
        users = {}
        for user_data in users_data:
            users[user_data['id']] = User(user_data['id'], user_data['name'], user_data['role'], 
                                         user_data['department'], user_data['parent_id'])
        
        return users
    
    def get_subordinates(self, user_id: int) -> Set[int]:
        """Get all subordinates for a user"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        subordinates = {user_id}  # Always include the user themselves
        
        # Use recursive approach directly - simpler and more reliable
        cursor.execute("""
        WITH RECURSIVE subordinates AS (
            SELECT id FROM users WHERE id = %s
            UNION ALL
            SELECT u.id FROM users u JOIN subordinates s ON u.parent_id = s.id
        )
        SELECT id FROM subordinates
        """, (user_id,))
        
        subordinates = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return subordinates
    
    def get_accessible_data_scope(self, user: User) -> Dict:
        """获取数据权限范围"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        scope = {"handle_by": set(), "order_ids": set(), "customer_ids": set()}
        
        if user.role == "admin":
            # Admin can access everything - just flag as admin
            scope["is_admin"] = True
            
            # For admin, sample a small set of users, orders and customers for testing
            cursor.execute("SELECT id FROM users LIMIT 100")
            scope["handle_by"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT order_id FROM orders LIMIT 100")
            scope["order_ids"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT customer_id FROM customers LIMIT 100")
            scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
            
        elif user.role == "supervisor":
            # Get subordinates
            subordinates = self.get_subordinates(user.id)
            scope["handle_by"] = subordinates
            
            # Convert subordinates to a comma-separated string for SQL IN clause
            if subordinates:
                subordinates_str = ','.join(str(id) for id in subordinates)
                
                # Get orders handled by subordinates
                cursor.execute(f"""
                SELECT order_id 
                FROM orders 
                WHERE user_id IN ({subordinates_str})
                LIMIT 1000
                """)
                
                scope["order_ids"] = {row[0] for row in cursor.fetchall()}
                
                # Get customers administered by subordinates
                cursor.execute(f"""
                SELECT customer_id 
                FROM customers 
                WHERE admin_user_id IN ({subordinates_str})
                LIMIT 1000
                """)
                
                scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
        
        elif user.role == "staff":
            # Staff can only access their own data
            scope["handle_by"] = {user.id}
            
            cursor.execute("SELECT order_id FROM orders WHERE user_id = %s LIMIT 1000", (user.id,))
            scope["order_ids"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT customer_id FROM customers WHERE admin_user_id = %s LIMIT 1000", (user.id,))
            scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return scope

class MySQLFinancialService:
    """MySQL-backed implementation of FinancialService"""
    
    def __init__(self, permission_svc):
        self.permission_svc = permission_svc
    
    def get_funds(self, user: User) -> List[FinancialFund]:
        """获取财务列表"""
        scope = self.permission_svc.get_accessible_data_scope(user)
        is_admin = scope.get("is_admin", False)
        
        conn = self.permission_svc.get_connection()
        cursor = conn.cursor()
        
        filtered_funds = []
        
        if is_admin:
            # For admin, just get a sample of funds directly
            cursor.execute("""
            SELECT fund_id, handle_by, order_id, customer_id, amount 
            FROM financial_funds 
            LIMIT 1000
            """)
            
            for row in cursor.fetchall():
                fund = FinancialFund(row[0], row[1], row[2], row[3], row[4])
                filtered_funds.append(fund)
        else:
            # For non-admin users, use the scope to filter funds
            conditions = []
            params = []
            
            # Handle handle_by condition
            if scope["handle_by"]:
                handle_by_list = list(scope["handle_by"])
                if len(handle_by_list) <= 1000:
                    placeholders = ', '.join(['%s'] * len(handle_by_list))
                    conditions.append(f"handle_by IN ({placeholders})")
                    params.extend(handle_by_list)
            
            # Handle order_ids condition
            if scope["order_ids"]:
                order_ids_list = list(scope["order_ids"])
                if len(order_ids_list) <= 1000:
                    placeholders = ', '.join(['%s'] * len(order_ids_list))
                    conditions.append(f"order_id IN ({placeholders})")
                    params.extend(order_ids_list)
            
            # Handle customer_ids condition
            if scope["customer_ids"]:
                customer_ids_list = list(scope["customer_ids"])
                if len(customer_ids_list) <= 1000:
                    placeholders = ', '.join(['%s'] * len(customer_ids_list))
                    conditions.append(f"customer_id IN ({placeholders})")
                    params.extend(customer_ids_list)
            
            # Build and execute query if we have conditions
            if conditions:
                query = """
                SELECT fund_id, handle_by, order_id, customer_id, amount 
                FROM financial_funds 
                WHERE {} 
                LIMIT 1000
                """.format(" OR ".join(conditions))
                
                cursor.execute(query, params)
                
                for row in cursor.fetchall():
                    fund = FinancialFund(row[0], row[1], row[2], row[3], row[4])
                    filtered_funds.append(fund)
        
        conn.close()
        return filtered_funds

class MySQLApiGateway:
    """MySQL-backed implementation of ApiGateway"""
    
    def __init__(self, config=None):
        self.permission_svc = MySQLPermissionService(config)
        self.financial_svc = MySQLFinancialService(self.permission_svc)
        self.current_user = None
    
    def authenticate(self, role: str):
        """模拟用户认证"""
        conn = self.permission_svc.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # First try to get one of the original test users with this role (IDs 1-4)
        cursor.execute("""
        SELECT id, name, role, department, parent_id 
        FROM users 
        WHERE role = %s AND id <= 4 
        LIMIT 1
        """, (role,))
        
        user_data = cursor.fetchone()
        
        # If not found, get any user with this role
        if not user_data:
            cursor.execute("""
            SELECT id, name, role, department, parent_id 
            FROM users 
            WHERE role = %s 
            LIMIT 1
            """, (role,))
            
            user_data = cursor.fetchone()
        
        conn.close()
        
        if user_data:
            self.current_user = User(user_data['id'], user_data['name'], user_data['role'], 
                                    user_data['department'], user_data['parent_id'])
        else:
            # Default to admin (user ID 1)
            conn = self.permission_svc.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE id = 1")
            user_data = cursor.fetchone()
            conn.close()
            self.current_user = User(user_data['id'], user_data['name'], user_data['role'], 
                                    user_data['department'], user_data['parent_id'])
    
    def get_funds(self):
        """获取财务数据API"""
        if not self.current_user:
            raise Exception("请先登录")
        
        return self.financial_svc.get_funds(self.current_user)

def run_mysql_benchmark(config=None, iterations=3):
    """Run benchmark tests on MySQL implementation"""
    print("\n=== MySQL Implementation Benchmark ===")
    
    # Initialize the gateway
    gateway = MySQLApiGateway(config)
    
    # Test admin role
    print("\n--- Admin Role Performance ---")
    admin_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("admin")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        admin_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {execution_time:.4f} seconds")
    
    avg_admin_time = sum(admin_times) / len(admin_times)
    print(f"Average execution time: {avg_admin_time:.4f} seconds")
    
    # Test supervisor role
    print("\n--- Supervisor Role Performance ---")
    supervisor_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("supervisor")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        supervisor_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {execution_time:.4f} seconds")
    
    avg_supervisor_time = sum(supervisor_times) / len(supervisor_times)
    print(f"Average execution time: {avg_supervisor_time:.4f} seconds")
    
    # Test staff role
    print("\n--- Staff Role Performance ---")
    staff_times = []
    for i in range(iterations):
        start_time = time.time()
        gateway.authenticate("staff")
        funds = gateway.get_funds()
        end_time = time.time()
        execution_time = end_time - start_time
        staff_times.append(execution_time)
        print(f"Iteration {i+1}: Retrieved {len(funds)} funds in {execution_time:.4f} seconds")
    
    avg_staff_time = sum(staff_times) / len(staff_times)
    print(f"Average execution time: {avg_staff_time:.4f} seconds")
    
    return {
        "admin": avg_admin_time,
        "supervisor": avg_supervisor_time,
        "staff": avg_staff_time
    }

def get_database_stats(config=None):
    """Get database statistics"""
    conn = mysql.connector.connect(**config) if config else mysql.connector.connect(
        host=os.getenv('DB_HOST_V2', '127.0.0.1'),
        port=int(os.getenv('DB_PORT_V2', '3306')),
        user=os.getenv('DB_USER_V2', 'root'),
        password=os.getenv('DB_PASSWORD_V2', '123456'),
        database=os.getenv('DB_NAME_V2', 'finance')
    )
    
    cursor = conn.cursor()
    
    print("\n=== Database Statistics ===")
    
    # Get table row counts
    tables = ["users", "orders", "customers", "financial_funds", "user_hierarchy"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table '{table}' contains {count:,} records")
    
    # Get database size
    cursor.execute(f"SELECT table_schema, SUM(data_length + index_length)/1024/1024 AS size_mb FROM information_schema.tables WHERE table_schema = '{conn.database}' GROUP BY table_schema")
    result = cursor.fetchone()
    if result:
        print(f"Database '{conn.database}' size: {result[1]:.2f} MB")
    
    # Get index information
    cursor.execute(f"""
    SELECT table_name, index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
    FROM information_schema.statistics
    WHERE table_schema = '{conn.database}'
    GROUP BY table_name, index_name
    ORDER BY table_name, index_name
    """)
    
    print("\nIndexes in the database:")
    for row in cursor.fetchall():
        print(f"  {row[0]}.{row[1]} ({row[2]})")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Finance Permission System with MySQL")
    parser.add_argument("--init", action="store_true", help="Initialize the database")
    parser.add_argument("--records", type=int, default=1000000, help="Number of records per table (default: 1,000,000)")
    parser.add_argument("--benchmark", action="store_true", help="Run benchmark tests")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    
    args = parser.parse_args()
    
    # Load config from .env
    config = {
        'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT_V2', '3306')),
        'user': os.getenv('DB_USER_V2', 'root'),
        'password': os.getenv('DB_PASSWORD_V2', '123456'),
        'database': os.getenv('DB_NAME_V2', 'finance')
    }
    
    if args.init:
        svc = MySQLPermissionService(config)
        svc.populate_test_data(args.records)
    
    if args.stats:
        get_database_stats(config)
    
    if args.benchmark:
        run_mysql_benchmark(config)
    
    # If no args specified,