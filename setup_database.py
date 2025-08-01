# setup_database.py - Complete database setup and verification script
"""
Database Setup Script for Mapping Validation System

This script will:
1. Test MySQL connection
2. Create database if it doesn't exist
3. Create tables with proper structure
4. Verify the setup
5. Run basic functionality tests

Run this before starting your Streamlit application.
"""

import mysql.connector
import sys
import os
from pathlib import Path
import logging
from typing import Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'http://mapping-process.cjjrhjl6dwxu.us-east-1.rds.amazonaws.com'),
    'user': os.getenv('DB_USER', 'mapping'),
    'password': os.getenv('DB_PASSWORD', 'wo0066upzahPfwB4U'),
    'charset': 'utf8mb4',
    'autocommit': True
}

DATABASE_NAME = os.getenv('DB_NAME', 'mapping_validation_db')

def print_banner():
    """Print welcome banner"""
    print("=" * 60)
    print("🔍 Mapping Validation System - Database Setup")
    print("=" * 60)
    print()

def test_mysql_connection() -> Tuple[bool, str]:
    """Test basic MySQL connection without database"""
    print("🔌 Testing MySQL connection...")
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            server_info = connection.get_server_info()
            connection.close()
            return True, f"✅ Connected to MySQL Server version {server_info}"
        else:
            return False, "❌ Failed to connect to MySQL"
    except mysql.connector.Error as e:
        return False, f"❌ MySQL Error: {e.msg if hasattr(e, 'msg') else str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error: {str(e)}"

def create_database() -> Tuple[bool, str]:
    """Create database if it doesn't exist"""
    print(f"🗄️ Creating database '{DATABASE_NAME}'...")
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Check if database exists
        cursor.execute("SHOW DATABASES LIKE %s", (DATABASE_NAME,))
        if cursor.fetchone():
            cursor.close()
            connection.close()
            return True, f"✅ Database '{DATABASE_NAME}' already exists"
        
        # Create database
        cursor.execute(f"""
            CREATE DATABASE {DATABASE_NAME} 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
        """)
        
        cursor.close()
        connection.close()
        return True, f"✅ Database '{DATABASE_NAME}' created successfully"
        
    except mysql.connector.Error as e:
        return False, f"❌ Failed to create database: {e.msg if hasattr(e, 'msg') else str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error creating database: {str(e)}"

def create_tables() -> Tuple[bool, str]:
    """Create required tables"""
    print("📋 Creating tables...")
    try:
        # Connect to the specific database
        config_with_db = DB_CONFIG.copy()
        config_with_db['database'] = DATABASE_NAME
        
        connection = mysql.connector.connect(**config_with_db)
        cursor = connection.cursor()
        
        # Create main table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS processed_mappings (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            -- Original input columns
            vendor_product_description TEXT,
            company_location VARCHAR(255),
            vendor_name VARCHAR(255),
            vendor_id VARCHAR(100),
            quantity VARCHAR(100),
            stems_bunch VARCHAR(100),
            unit_type VARCHAR(100),
            staging_id VARCHAR(100),
            object_mapping_id VARCHAR(100),
            company_id VARCHAR(100),
            user_id VARCHAR(100),
            product_mapping_id VARCHAR(100),
            email VARCHAR(255),
            
            -- Processing results
            cleaned_input TEXT,
            applied_synonyms TEXT,
            removed_blacklist_words TEXT,
            best_match TEXT,
            similarity_percentage VARCHAR(10),
            matched_words TEXT,
            missing_words TEXT,
            
            -- Catalog results
            catalog_id VARCHAR(100),
            categoria VARCHAR(255),
            variedad VARCHAR(255),
            color VARCHAR(255),
            grado VARCHAR(255),
            
            -- User validation
            accept_map VARCHAR(10) DEFAULT '',
            deny_map VARCHAR(10) DEFAULT '',
            action VARCHAR(50) DEFAULT '',
            word VARCHAR(255) DEFAULT '',
            
            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            -- Indexes
            INDEX idx_vendor_name (vendor_name),
            INDEX idx_similarity (similarity_percentage),
            INDEX idx_catalog_id (catalog_id),
            INDEX idx_accept_map (accept_map),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_table_sql)
        
        # Create summary view
        create_view_sql = """
        CREATE OR REPLACE VIEW mapping_summary AS
        SELECT 
            vendor_name,
            COUNT(*) as total_mappings,
            SUM(CASE WHEN accept_map = 'True' THEN 1 ELSE 0 END) as accepted_mappings,
            SUM(CASE WHEN deny_map = 'True' THEN 1 ELSE 0 END) as denied_mappings,
            AVG(CASE WHEN similarity_percentage != '' THEN CAST(similarity_percentage AS DECIMAL(5,2)) END) as avg_similarity,
            DATE(created_at) as processing_date
        FROM processed_mappings 
        WHERE similarity_percentage IS NOT NULL 
          AND similarity_percentage != ''
        GROUP BY vendor_name, DATE(created_at)
        ORDER BY processing_date DESC, total_mappings DESC
        """
        
        cursor.execute(create_view_sql)
        
        cursor.close()
        connection.close()
        
        return True, "✅ Tables and views created successfully"
        
    except mysql.connector.Error as e:
        return False, f"❌ Failed to create tables: {e.msg if hasattr(e, 'msg') else str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error creating tables: {str(e)}"

def verify_setup() -> Tuple[bool, str]:
    """Verify the complete setup"""
    print("🔍 Verifying setup...")
    try:
        config_with_db = DB_CONFIG.copy()
        config_with_db['database'] = DATABASE_NAME
        
        connection = mysql.connector.connect(**config_with_db)
        cursor = connection.cursor()
        
        # Check table exists and get structure
        cursor.execute("SHOW TABLES LIKE 'processed_mappings'")
        if not cursor.fetchone():
            return False, "❌ Table 'processed_mappings' not found"
        
        # Check table structure
        cursor.execute("DESCRIBE processed_mappings")
        columns = cursor.fetchall()
        column_count = len(columns)
        
        # Check view exists
        cursor.execute("SHOW FULL TABLES WHERE TABLE_TYPE LIKE 'VIEW'")
        views = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return True, f"✅ Setup verified: {column_count} columns in main table, {len(views)} views created"
        
    except mysql.connector.Error as e:
        return False, f"❌ Verification failed: {e.msg if hasattr(e, 'msg') else str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error during verification: {str(e)}"

def test_basic_operations() -> Tuple[bool, str]:
    """Test basic database operations"""
    print("🧪 Testing basic operations...")
    try:
        config_with_db = DB_CONFIG.copy()
        config_with_db['database'] = DATABASE_NAME
        
        connection = mysql.connector.connect(**config_with_db)
        cursor = connection.cursor()
        
        # Test insert
        test_data = [
            "Test Product", "Test Location", "Test Vendor", "V001",
            "100", "50", "Stems", "S001", "M001", "C001", "U001", "P001", "test@example.com",
            "test product", "", "", "test match", "95", "test", "", "CAT001",
            "Test Category", "Test Variety", "Red", "A", "", "", "", ""
        ]
        
        insert_sql = """
        INSERT INTO processed_mappings (
            vendor_product_description, company_location, vendor_name, vendor_id,
            quantity, stems_bunch, unit_type, staging_id, object_mapping_id,
            company_id, user_id, product_mapping_id, email, cleaned_input,
            applied_synonyms, removed_blacklist_words, best_match, similarity_percentage,
            matched_words, missing_words, catalog_id, categoria, variedad,
            color, grado, accept_map, deny_map, action, word
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        cursor.execute(insert_sql, test_data)
        
        # Test select
        cursor.execute("SELECT COUNT(*) FROM processed_mappings")
        count = cursor.fetchone()[0]
        
        # Clean up test data
        cursor.execute("DELETE FROM processed_mappings WHERE vendor_name = 'Test Vendor'")
        
        cursor.close()
        connection.close()
        
        return True, f"✅ Basic operations successful (inserted and retrieved {count} record)"
        
    except mysql.connector.Error as e:
        return False, f"❌ Operations test failed: {e.msg if hasattr(e, 'msg') else str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error during operations test: {str(e)}"

def print_summary():
    """Print setup summary and next steps"""
    print()
    print("=" * 60)
    print("🎉 Database Setup Complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Run your Streamlit application: streamlit run streamlit_app.py")
    print("2. Test database connection using the sidebar controls")
    print("3. Upload and process your TSV files")
    print("4. Use 'Save to Database' to persist your mappings")
    print()
    print("Database configuration:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  User: {DB_CONFIG['user']}")
    print(f"  Database: {DATABASE_NAME}")
    print()
    print("Troubleshooting:")
    print("- Ensure MySQL server is running")
    print("- Check database credentials in .env file")
    print("- Verify user has CREATE/INSERT/SELECT permissions")
    print()

def main():
    """Main setup function"""
    print_banner()
    
    # Test MySQL connection
    success, message = test_mysql_connection()
    print(message)
    if not success:
        print("\n❌ Setup failed at MySQL connection test")
        print("Please check:")
        print("- MySQL server is running")
        print("- Database credentials are correct")
        print("- Network connectivity")
        sys.exit(1)
    
    # Create database
    success, message = create_database()
    print(message)
    if not success:
        print(f"\n❌ Setup failed during database creation")
        sys.exit(1)
    
    # Create tables
    success, message = create_tables()
    print(message)
    if not success:
        print(f"\n❌ Setup failed during table creation")
        sys.exit(1)
    
    # Verify setup
    success, message = verify_setup()
    print(message)
    if not success:
        print(f"\n❌ Setup verification failed")
        sys.exit(1)
    
    # Test operations
    success, message = test_basic_operations()
    print(message)
    if not success:
        print(f"\n⚠️ Warning: Basic operations test failed")
        print("Database is created but may have permission issues")
    
    print_summary()

if __name__ == "__main__":
    main()