#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import mysql.connector

# Load environment variables from .env file
load_dotenv()

# Get MySQL connection details from environment variables
config = {
    'host': os.getenv('DB_HOST_V2', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT_V2', '3306')),
    'user': os.getenv('DB_USER_V2', 'root'),
    'password': os.getenv('DB_PASSWORD_V2', '123456')
}

print("MySQL Connection Test")
print("=====================")
print(f"Host: {config['host']}")
print(f"Port: {config['port']}")
print(f"User: {config['user']}")
print(f"Password: {'*' * len(config['password'])}")

try:
    # Test connection
    print("\nAttempting to connect to MySQL server...")
    conn = mysql.connector.connect(**config)
    print("Connection successful!")
    
    # Get server information
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()[0]
    print(f"MySQL server version: {version}")
    
    # List all databases
    print("\nAvailable databases:")
    cursor.execute("SHOW DATABASES")
    for db in cursor:
        print(f"- {db[0]}")
    
    # Create test database
    db_name = os.getenv('DB_NAME_V2', 'finance')
    print(f"\nChecking if database '{db_name}' exists...")
    
    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    if cursor.fetchone():
        print(f"Database '{db_name}' already exists")
    else:
        print(f"Creating database '{db_name}'...")
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully")
    
    # Close connection
    conn.close()
    print("\nConnection closed successfully")
    
except mysql.connector.Error as err:
    print(f"Error: {err}")

print("\nTest completed")