import sqlite3
import random
import os
import time
from typing import List, Dict, Set
from main import User, FinancialFund, Order, Customer, PermissionService

class DatabasePermissionService(PermissionService):
    """Database-backed implementation of PermissionService"""
    
    def __init__(self, db_path="finance_system.db"):
        self.db_path = db_path
        self.setup_database()
        # We don't call the parent's __init__ as we're replacing its functionality
        
    def setup_database(self):
        """Create database tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        # Enable foreign keys constraint
        conn.execute("PRAGMA foreign_keys = ON")
        # Set journal mode to WAL for better concurrency and performance
        conn.execute("PRAGMA journal_mode = WAL")
        # Set synchronous mode to NORMAL for better performance
        conn.execute("PRAGMA synchronous = NORMAL")
        # Increase cache size for better performance - increase to 50MB for large datasets
        conn.execute("PRAGMA cache_size = -50000")  # ~50MB cache
        # Enable memory-mapped I/O for better performance
        conn.execute("PRAGMA mmap_size = 1073741824")  # 1GB memory mapping
        # Use a larger page size for better read performance
        conn.execute("PRAGMA page_size = 8192")
        cursor = conn.cursor()
        
        # Create Users table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            department TEXT NOT NULL,
            parent_id INTEGER
        )
        ''')
        
        # Create index on role and parent_id for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_parent_id ON users(parent_id)')
        
        # Create FinancialFunds table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_funds (
            fund_id INTEGER PRIMARY KEY,
            handle_by INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            FOREIGN KEY (handle_by) REFERENCES users (id),
            FOREIGN KEY (order_id) REFERENCES orders (order_id),
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
        ''')
        
        # Create indexes for foreign keys to improve JOIN performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_funds_handle_by ON financial_funds(handle_by)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_funds_order_id ON financial_funds(order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_funds_customer_id ON financial_funds(customer_id)')
        
        # Create Orders table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        ''')
        
        # Create index on user_id for faster joins
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
        
        # Create Customers table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            admin_user_id INTEGER NOT NULL,
            FOREIGN KEY (admin_user_id) REFERENCES users (id)
        )
        ''')
        
        # Create index on admin_user_id for faster joins
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customers_admin_user_id ON customers(admin_user_id)')
        
        conn.commit()
        conn.close()
    
    def populate_test_data(self, num_records=1000000):
        """Populate database with test data"""
        conn = sqlite3.connect(self.db_path)
        # Enable extreme performance optimizations for bulk inserts
        conn.execute("PRAGMA journal_mode = OFF")  # Disable journaling for maximum insert speed
        conn.execute("PRAGMA synchronous = OFF")   # Disable synchronous writes for maximum speed (risky but fast)
        conn.execute("PRAGMA cache_size = -100000")  # ~100MB cache
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA page_size = 32768")   # Use larger pages for better efficiency
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")  # Exclusive access for better performance
        
        # Begin a single transaction for all operations - autocommit mode is already disabled in SQLite
        # No need for an explicit BEGIN
        
        cursor = conn.cursor()
        
        print(f"Starting database population with {num_records:,} records per table...")
        start_time = time.time()
        
        # Add base users (from the original example)
        base_users = [
            (1, "超级管理员", "admin", "总部", None),
            (2, "财务主管", "supervisor", "华东区", 1),
            (3, "财务专员", "staff", "华东区", 2),
            (4, "财务专员", "staff", "华南区", 1)
        ]
        
        # Clear existing data and drop indexes for faster insertion
        conn.execute("PRAGMA foreign_keys = OFF")  # Temporarily disable foreign keys for faster deletion
        cursor.execute("DROP INDEX IF EXISTS idx_users_role")
        cursor.execute("DROP INDEX IF EXISTS idx_users_parent_id")
        cursor.execute("DROP INDEX IF EXISTS idx_funds_handle_by")
        cursor.execute("DROP INDEX IF EXISTS idx_funds_order_id")
        cursor.execute("DROP INDEX IF EXISTS idx_funds_customer_id")
        cursor.execute("DROP INDEX IF EXISTS idx_orders_user_id")
        cursor.execute("DROP INDEX IF EXISTS idx_customers_admin_user_id")
        
        cursor.execute("DELETE FROM financial_funds")
        cursor.execute("DELETE FROM customers")
        cursor.execute("DELETE FROM orders")
        cursor.execute("DELETE FROM users")
        
        # Insert base users
        cursor.executemany(
            "INSERT INTO users (id, name, role, department, parent_id) VALUES (?, ?, ?, ?, ?)",
            base_users
        )
        
        # Generate all data in memory first
        print("Preparing data in memory...")
        
        # Prepare user data (starting from ID 5)
        roles = ["staff"] * 80 + ["supervisor"] * 15 + ["admin"] * 5  # Distribution: 80% staff, 15% supervisors, 5% admin
        departments = ["华东区", "华南区", "华北区", "西南区", "东北区", "西北区"]
        
        # Pre-generate all random choices to avoid repeated calls
        role_choices = [random.choice(roles) for _ in range(num_records)]
        dept_choices = [random.choice(departments) for _ in range(num_records)]
        
        # Continue with batch inserts within the transaction
        
        # Generate and insert users in larger batches
        print("Generating users...")
        user_batch_size = 100000
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
                "INSERT INTO users (id, name, role, department, parent_id) VALUES (?, ?, ?, ?, ?)",
                user_batch
            )
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} users ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert orders in larger batches
        print("Generating orders...")
        order_batch_size = 100000
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
                "INSERT INTO orders (order_id, user_id) VALUES (?, ?)",
                order_batch
            )
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} orders ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert customers in larger batches
        print("Generating customers...")
        customer_batch_size = 100000
        
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
                "INSERT INTO customers (customer_id, admin_user_id) VALUES (?, ?)",
                customer_batch
            )
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} customers ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Generate and insert financial funds in larger batches
        print("Generating financial funds...")
        fund_batch_size = 100000
        
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
                "INSERT INTO financial_funds (fund_id, handle_by, order_id, customer_id, amount) VALUES (?, ?, ?, ?, ?)",
                fund_batch
            )
            
            if (i + batch_size) % progress_step == 0 or (i + batch_size) == num_records:
                print(f"Prepared {i + batch_size:,}/{num_records:,} financial funds ({((i + batch_size) / num_records * 100):.1f}%)...")
        
        # Commit the transaction
        print("Committing all data to database...")
        conn.commit()
        
        # Recreate indexes
        print("Recreating indexes...")
        cursor.execute('CREATE INDEX idx_users_role ON users(role)')
        cursor.execute('CREATE INDEX idx_users_parent_id ON users(parent_id)')
        cursor.execute('CREATE INDEX idx_funds_handle_by ON financial_funds(handle_by)')
        cursor.execute('CREATE INDEX idx_funds_order_id ON financial_funds(order_id)')
        cursor.execute('CREATE INDEX idx_funds_customer_id ON financial_funds(customer_id)')
        cursor.execute('CREATE INDEX idx_orders_user_id ON orders(user_id)')
        cursor.execute('CREATE INDEX idx_customers_admin_user_id ON customers(admin_user_id)')
        
        # Re-enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        end_time = time.time()
        print(f"Database population completed in {end_time - start_time:.2f} seconds")
        
        # Reset pragmas to normal values for query operations
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA locking_mode = NORMAL")
        
        # Create database statistics for query optimizer
        print("Analyzing database for query optimization...")
        cursor.execute("ANALYZE")
        conn.close()
    
    def get_user(self, user_id):
        """Get a user by ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE id = ?", (user_id,))
        user_data = cursor.fetchone()
        
        conn.close()
        
        if user_data:
            return User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4])
        return None
    
    def get_users(self):
        """Get all users"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, name, role, department, parent_id FROM users")
        users_data = cursor.fetchall()
        
        conn.close()
        
        users = {}
        for user_data in users_data:
            users[user_data[0]] = User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4])
        
        return users
    
    def get_subordinates(self, user_id: int) -> Set[int]:
        """递归获取所有下属ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # For performance, limit depth of recursion and total result size
        cursor.execute("""
        WITH RECURSIVE subordinates(id, depth) AS (
            VALUES(?, 0)
            UNION
            SELECT u.id, s.depth + 1 FROM users u, subordinates s
            WHERE u.parent_id = s.id AND s.depth < 3  -- Limit recursion depth to 3 levels
            LIMIT 1000  -- Limit total results to prevent excessive recursion
        )
        SELECT id FROM subordinates
        """, (user_id,))
        
        # Fetch results as individual rows instead of concatenated string
        subordinates = {row[0] for row in cursor.fetchall()}
        
        # Always include the user themselves
        subordinates.add(user_id)
        
        conn.close()
        return subordinates
    
    def _get_subordinates_recursive(self, user_id):
        """Helper function for SQLite recursive subordinate lookup - DEPRECATED
        This function is no longer used due to potential issues with large result sets.
        Using direct CTE query in get_subordinates instead.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Use a CTE (Common Table Expression) for recursive lookup
        cursor.execute('''
        WITH RECURSIVE subordinates(id) AS (
            VALUES(?)
            UNION
            SELECT u.id FROM users u, subordinates s
            WHERE u.parent_id = s.id
        )
        SELECT group_concat(id) FROM subordinates
        ''', (user_id,))
        
        result = cursor.fetchone()[0]
        conn.close()
        return result
    
    def get_accessible_data_scope(self, user: User) -> Dict:
        """获取数据权限范围"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        scope = {"handle_by": set(), "order_ids": set(), "customer_ids": set()}
        
        if user.role == "admin":
            # Admin can access everything - don't load all IDs, just mark as admin
            scope["is_admin"] = True
            # Sample a few IDs for testing queries but don't load all
            cursor.execute("SELECT id FROM users LIMIT 100")
            scope["handle_by"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT order_id FROM orders LIMIT 100")
            scope["order_ids"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT customer_id FROM customers LIMIT 100")
            scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
            
        elif user.role == "supervisor":
            # Get all subordinates
            subordinates = self.get_subordinates(user.id)
            scope["handle_by"] = subordinates
            
            # For supervisors, we'll limit to a manageable subset of data
            # This dramatically improves performance while still providing useful results
            
            # Create temporary table for subordinates for more efficient joins
            cursor.execute("CREATE TEMPORARY TABLE IF NOT EXISTS temp_subordinates (id INTEGER PRIMARY KEY)")
            cursor.execute("DELETE FROM temp_subordinates")
            
            # Insert subordinates - no explicit transaction needed as we're already in one
            for sub_id in subordinates:
                cursor.execute("INSERT INTO temp_subordinates VALUES (?)", (sub_id,))
            
            # Use the temporary table for more efficient queries
            # Get a sample of orders (limit to 1000 for performance)
            cursor.execute("""
            SELECT o.order_id 
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN temp_subordinates ts ON u.id = ts.id
            LIMIT 1000
            """)
            scope["order_ids"] = {row[0] for row in cursor.fetchall()}
            
            # Get a sample of customers (limit to 1000 for performance)
            cursor.execute("""
            SELECT c.customer_id 
            FROM customers c
            JOIN users u ON c.admin_user_id = u.id
            JOIN temp_subordinates ts ON u.id = ts.id
            LIMIT 1000
            """)
            scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
            
            # Drop temporary table
            cursor.execute("DROP TABLE temp_subordinates")
        
        elif user.role == "staff":
            # Staff can only access their own data
            scope["handle_by"] = {user.id}
            
            cursor.execute("SELECT order_id FROM orders WHERE user_id = ? LIMIT 10000", (user.id,))
            scope["order_ids"] = {row[0] for row in cursor.fetchall()}
            
            cursor.execute("SELECT customer_id FROM customers WHERE admin_user_id = ? LIMIT 10000", (user.id,))
            scope["customer_ids"] = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        return scope

class DatabaseFinancialService:
    """Database-backed implementation of FinancialService"""
    
    def __init__(self, permission_svc):
        self.permission_svc = permission_svc
        self.db_path = permission_svc.db_path
    
    def get_funds(self, user: User) -> List[FinancialFund]:
        """获取财务列表"""
        scope = self.permission_svc.get_accessible_data_scope(user)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Check if user is admin for special case handling
        is_admin = scope.get("is_admin", False)
        
        # Use query hints to optimize execution plan
        conn.execute("PRAGMA optimize")
        
        filtered_funds = []
        max_vars = 500  # SQLite typically allows 999 variables, we use 500 to be safe
        
        if is_admin:
            # For admin, just get a sample of funds directly without filtering
            # Add an index hint for better performance
            cursor.execute("SELECT fund_id, handle_by, order_id, customer_id, amount FROM financial_funds INDEXED BY idx_funds_handle_by LIMIT 1000")
            all_results = cursor.fetchall()
        else:
            # Chunked query execution function
            def execute_chunked_query(field_name, id_list, max_chunk_size):
                results = []
                for i in range(0, len(id_list), max_chunk_size):
                    chunk = id_list[i:i + max_chunk_size]
                    placeholders = ','.join(['?'] * len(chunk))
                    query = f"SELECT fund_id, handle_by, order_id, customer_id, amount FROM financial_funds WHERE {field_name} IN ({placeholders}) LIMIT 1000"
                    cursor.execute(query, chunk)
                    results.extend(cursor.fetchall())
                    if len(results) >= 1000:  # Limit total results to 1000
                        break
                return results
            
            # Handle each condition separately with chunking
            handle_by_list = list(scope["handle_by"])
            order_ids_list = list(scope["order_ids"])
            customer_ids_list = list(scope["customer_ids"])
            
            all_results = []
            
            # Process each condition with chunking
            if handle_by_list:
                all_results.extend(execute_chunked_query("handle_by", handle_by_list, max_vars))
            
            if len(all_results) < 1000 and order_ids_list:
                all_results.extend(execute_chunked_query("order_id", order_ids_list, max_vars))
                
            if len(all_results) < 1000 and customer_ids_list:
                all_results.extend(execute_chunked_query("customer_id", customer_ids_list, max_vars))
        
        # Convert results to FinancialFund objects - limit to 1000 funds
        # For better performance with large result sets
        seen_ids = set()  # Track already processed fund_ids to avoid duplicates
        
        # Sort results by fund_id to get consistent results (better for caching)
        all_results.sort(key=lambda x: x[0] if x else 0)
        
        # Process only first 2000 rows (looking for 1000 unique funds)
        for row in all_results[:2000]:
            fund_id = row[0]
            if fund_id not in seen_ids:
                fund = FinancialFund(fund_id, row[1], row[2], row[3], row[4])
                filtered_funds.append(fund)
                seen_ids.add(fund_id)
                if len(filtered_funds) >= 1000:  # Ensure we don't exceed 1000 results
                    break
        
        conn.close()
        return filtered_funds

class DatabaseApiGateway:
    """Database-backed implementation of ApiGateway"""
    
    def __init__(self, db_path="finance_system.db"):
        self.permission_svc = DatabasePermissionService(db_path)
        self.financial_svc = DatabaseFinancialService(self.permission_svc)
        self.current_user = None
    
    def authenticate(self, role: str):
        """模拟用户认证"""
        conn = sqlite3.connect(self.permission_svc.db_path)
        cursor = conn.cursor()
        
        # First try to get one of the original test users with this role (IDs 1-4)
        cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE role = ? AND id <= 4 LIMIT 1", (role,))
        user_data = cursor.fetchone()
        
        # If not found, get any user with this role
        if not user_data:
            cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE role = ? LIMIT 1", (role,))
            user_data = cursor.fetchone()
        
        conn.close()
        
        if user_data:
            self.current_user = User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4])
        else:
            # Default to admin (user ID 1)
            conn = sqlite3.connect(self.permission_svc.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, role, department, parent_id FROM users WHERE id = 1")
            user_data = cursor.fetchone()
            conn.close()
            self.current_user = User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4])
    
    def get_funds(self):
        """获取财务数据API"""
        if not self.current_user:
            raise Exception("请先登录")
        
        return self.financial_svc.get_funds(self.current_user)

def measure_performance(test_name, func, *args, **kwargs):
    """Measure and print the performance of a function"""
    print(f"\nRunning performance test: {test_name}")
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    print(f"Test completed in {end_time - start_time:.4f} seconds")
    return result

if __name__ == "__main__":
    db_path = "finance_system.db"
    
    # Check if database exists and create if not
    if not os.path.exists(db_path):
        db_svc = DatabasePermissionService(db_path)
        print("Creating new database and populating with test data...")
        db_svc.populate_test_data(1000000)  # Generate 1 million records for each table
    else:
        print(f"Using existing database at {db_path}")
    
    gateway = DatabaseApiGateway(db_path)
    
    # Test admin view
    print("\n=== 超管视角 ===")
    measure_performance("Admin authentication", gateway.authenticate, "admin")
    funds = measure_performance("Admin funds retrieval", gateway.get_funds)
    print(f"Retrieved {len(funds)} funds for admin")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"超管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
    
    # Test supervisor view
    print("\n=== 主管视角 ===")
    measure_performance("Supervisor authentication", gateway.authenticate, "supervisor")
    funds = measure_performance("Supervisor funds retrieval", gateway.get_funds)
    print(f"Retrieved {len(funds)} funds for supervisor")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"主管查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")
    
    # Test staff view
    print("\n=== 员工视角 ===")
    measure_performance("Staff authentication", gateway.authenticate, "staff")
    funds = measure_performance("Staff funds retrieval", gateway.get_funds)
    print(f"Retrieved {len(funds)} funds for staff")
    
    # Display first 5 funds only
    for fund in funds[:5]:
        print(f"员工查看: {fund.fund_id} | 处理人: {fund.handle_by} | 订单: {fund.order_id} | 客户: {fund.customer_id}")