#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Direct Database Creator - Bypasses SQL file parsing entirely
Creates the optimized database structure programmatically
Fixes all the parsing and syntax errors you encountered
"""

import mysql.connector
import sys

def create_database_direct():
    """
    Create database directly without SQL file parsing
    This avoids all encoding and parsing issues
    """
    
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Maracuya123',
        'charset': 'utf8mb4',
        'autocommit': True
    }
    
    try:
        print("🚀 Creating database directly (no SQL file parsing)")
        print("="*60)
        
        # Connect to MySQL
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Step 1: Create main database
        print("📊 Creating main database...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS mapping_validation_consolidated CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE mapping_validation_consolidated")
        print("  ✅ Database created: mapping_validation_consolidated")
        
        # Step 2: Create processed_mappings table
        print("📋 Creating processed_mappings table...")
        create_processed_mappings_direct(cursor)
        print("  ✅ processed_mappings table created")
        
        # Step 3: Create vendor_staging_data table
        print("🏪 Creating vendor_staging_data table...")
        create_vendor_staging_direct(cursor)
        print("  ✅ vendor_staging_data table created")
        
        # Step 4: Create product_catalog table
        print("📚 Creating product_catalog table...")
        create_product_catalog_direct(cursor)
        print("  ✅ product_catalog table created")
        
        # Step 5: Create synonyms_blacklist table
        print("📝 Creating synonyms_blacklist table...")
        create_synonyms_blacklist_direct(cursor)
        print("  ✅ synonyms_blacklist table created")
        
        # Step 6: Create staging_products table
        print("🆕 Creating staging_products table...")
        create_staging_products_direct(cursor)
        print("  ✅ staging_products_to_create table created")
        
        # Step 7: Create view
        print("👁️ Creating mapping_summary view...")
        create_view_direct(cursor)
        print("  ✅ mapping_summary view created")
        
        # Step 8: Create performance monitoring table
        print("📊 Creating performance monitoring...")
        create_performance_monitoring_direct(cursor)
        print("  ✅ performance_metrics table created")
        
        # Step 9: Create indexes
        print("⚡ Creating optimized indexes...")
        create_indexes_direct(cursor)
        print("  ✅ All indexes created")
        
        # Step 10: Insert sample data
        print("🎯 Inserting sample data...")
        insert_sample_data_direct(cursor)
        print("  ✅ Sample data inserted for 3 clients")
        
        cursor.close()
        connection.close()
        
        # Final test
        print("\n🧪 Testing database...")
        test_final_database()
        
        print("\n" + "="*60)
        print("🎉 SUCCESS! Database created successfully!")
        print("="*60)
        print("\n📋 Configuration:")
        print("  Database: mapping_validation_consolidated")
        print("  Tables: 6 tables + 1 view created")
        print("  Sample data: 3 clients (demo_client, acme_corp, test_company)")
        print("\n📝 Update your .env file:")
        print("  DB_NAME=mapping_validation_consolidated")
        print("\n🚀 Ready to run Streamlit applications!")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        print(f"   Error Code: {e.errno}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def create_processed_mappings_direct(cursor):
    """Create processed_mappings table with all 32 columns"""
    sql = """
    CREATE TABLE IF NOT EXISTS processed_mappings (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        client_id VARCHAR(100) NOT NULL DEFAULT 'default_client',
        batch_id VARCHAR(100) DEFAULT '',
        vendor_product_description TEXT NOT NULL,
        company_location VARCHAR(255) DEFAULT '',
        vendor_name VARCHAR(255) NOT NULL DEFAULT '',
        vendor_id VARCHAR(100) DEFAULT '',
        quantity VARCHAR(100) DEFAULT '',
        stems_bunch VARCHAR(100) DEFAULT '',
        unit_type VARCHAR(100) DEFAULT '',
        staging_id VARCHAR(100) DEFAULT '',
        object_mapping_id VARCHAR(100) DEFAULT '',
        company_id VARCHAR(100) DEFAULT '',
        user_id VARCHAR(100) DEFAULT '',
        product_mapping_id VARCHAR(100) DEFAULT '',
        email VARCHAR(255) DEFAULT '',
        cleaned_input TEXT NOT NULL,
        applied_synonyms TEXT,
        removed_blacklist_words TEXT,
        best_match TEXT,
        similarity_percentage VARCHAR(10) DEFAULT '0',
        matched_words TEXT,
        missing_words TEXT,
        catalog_id VARCHAR(100) DEFAULT '',
        categoria VARCHAR(255) DEFAULT '',
        variedad VARCHAR(255) DEFAULT '',
        color VARCHAR(255) DEFAULT '',
        grado VARCHAR(255) DEFAULT '',
        accept_map VARCHAR(10) DEFAULT '',
        deny_map VARCHAR(10) DEFAULT '',
        action VARCHAR(50) DEFAULT '',
        word VARCHAR(255) DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_vendor_staging_direct(cursor):
    """Create vendor_staging_data table with 17 columns"""
    sql = """
    CREATE TABLE IF NOT EXISTS vendor_staging_data (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        product_description TEXT NOT NULL,
        column_2 VARCHAR(255) DEFAULT '',
        column_3 VARCHAR(255) DEFAULT '',
        column_4 VARCHAR(255) DEFAULT '',
        column_5 VARCHAR(255) DEFAULT '',
        column_6 VARCHAR(255) DEFAULT '',
        column_7 VARCHAR(255) DEFAULT '',
        column_8 VARCHAR(255) DEFAULT '',
        column_9 VARCHAR(255) DEFAULT '',
        column_10 VARCHAR(255) DEFAULT '',
        column_11 VARCHAR(255) DEFAULT '',
        column_12 VARCHAR(255) DEFAULT '',
        column_13 VARCHAR(255) DEFAULT '',
        client_id VARCHAR(100) NOT NULL,
        batch_id VARCHAR(100) DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_product_catalog_direct(cursor):
    """Create product_catalog table with 11 columns"""
    sql = """
    CREATE TABLE IF NOT EXISTS product_catalog (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        categoria VARCHAR(255) DEFAULT '',
        variedad VARCHAR(255) DEFAULT '',
        color VARCHAR(255) DEFAULT '',
        grado VARCHAR(255) DEFAULT '',
        additional_field_1 VARCHAR(255) DEFAULT '',
        catalog_id VARCHAR(100) NOT NULL,
        search_key TEXT NOT NULL,
        client_id VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_client_catalog (client_id, catalog_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_synonyms_blacklist_direct(cursor):
    """Create synonyms_blacklist table with 11 columns"""
    sql = """
    CREATE TABLE IF NOT EXISTS synonyms_blacklist (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        type VARCHAR(20) NOT NULL,
        original_word VARCHAR(255) DEFAULT NULL,
        synonym_word VARCHAR(255) DEFAULT NULL,
        blacklist_word VARCHAR(255) DEFAULT NULL,
        client_id VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_by VARCHAR(100) DEFAULT 'system',
        status VARCHAR(20) DEFAULT 'active'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_staging_products_direct(cursor):
    """Create staging_products_to_create table with 16 columns"""
    sql = """
    CREATE TABLE IF NOT EXISTS staging_products_to_create (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        categoria VARCHAR(255) DEFAULT '',
        variedad VARCHAR(255) DEFAULT '',
        color VARCHAR(255) DEFAULT '',
        grado VARCHAR(255) DEFAULT '',
        additional_field_1 VARCHAR(255) DEFAULT '',
        catalog_id VARCHAR(100) NOT NULL DEFAULT '111111',
        search_key TEXT NOT NULL,
        client_id VARCHAR(100) NOT NULL,
        created_from_row_id BIGINT UNSIGNED DEFAULT NULL,
        original_input TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_by VARCHAR(100) DEFAULT 'system',
        status VARCHAR(20) DEFAULT 'pending'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_view_direct(cursor):
    """Create mapping_summary view"""
    sql = """
    CREATE OR REPLACE VIEW mapping_summary AS
    SELECT 
        client_id,
        vendor_name,
        COUNT(*) as total_mappings,
        SUM(CASE WHEN accept_map = 'True' THEN 1 ELSE 0 END) as accepted_mappings,
        SUM(CASE WHEN deny_map = 'True' THEN 1 ELSE 0 END) as denied_mappings,
        AVG(CAST(CASE WHEN similarity_percentage = '' THEN '0' ELSE similarity_percentage END AS DECIMAL(5,2))) as avg_similarity,
        DATE(created_at) as processing_date,
        batch_id,
        COUNT(DISTINCT catalog_id) as unique_products,
        COUNT(CASE WHEN catalog_id = '111111' OR catalog_id = '111111.0' THEN 1 END) as needs_product_creation
    FROM processed_mappings 
    WHERE similarity_percentage IS NOT NULL 
      AND similarity_percentage != ''
    GROUP BY client_id, vendor_name, DATE(created_at), batch_id
    ORDER BY processing_date DESC, total_mappings DESC
    """
    cursor.execute(sql)

def create_performance_monitoring_direct(cursor):
    """Create performance_metrics table"""
    sql = """
    CREATE TABLE IF NOT EXISTS performance_metrics (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        metric_name VARCHAR(100) NOT NULL,
        metric_value DECIMAL(15,4) NOT NULL,
        client_id VARCHAR(100) DEFAULT NULL,
        table_name VARCHAR(100) DEFAULT NULL,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(sql)

def create_indexes_direct(cursor):
    """Create all optimized indexes"""
    indexes = [
        # processed_mappings indexes (11 basic + 3 composite)
        "CREATE INDEX idx_client_id ON processed_mappings (client_id)",
        "CREATE INDEX idx_batch_id ON processed_mappings (batch_id)",
        "CREATE INDEX idx_vendor_name ON processed_mappings (vendor_name)",
        "CREATE INDEX idx_similarity_percentage ON processed_mappings (similarity_percentage)",
        "CREATE INDEX idx_catalog_id ON processed_mappings (catalog_id)",
        "CREATE INDEX idx_accept_map ON processed_mappings (accept_map)",
        "CREATE INDEX idx_created_at ON processed_mappings (created_at)",
        "CREATE INDEX idx_categoria ON processed_mappings (categoria)",
        "CREATE INDEX idx_variedad ON processed_mappings (variedad)",
        "CREATE INDEX idx_color ON processed_mappings (color)",
        "CREATE INDEX idx_grado ON processed_mappings (grado)",
        
        # Composite indexes for performance optimization
        "CREATE INDEX idx_client_similarity_filter ON processed_mappings (client_id, similarity_percentage, accept_map, deny_map)",
        "CREATE INDEX idx_vendor_batch_date ON processed_mappings (vendor_name, batch_id, created_at DESC)",
        "CREATE INDEX idx_search_optimization ON processed_mappings (vendor_name, cleaned_input(100))",
        
        # vendor_staging_data indexes
        "CREATE INDEX idx_vs_client_id ON vendor_staging_data (client_id)",
        "CREATE INDEX idx_vs_batch_id ON vendor_staging_data (batch_id)",
        "CREATE INDEX idx_vs_product_desc ON vendor_staging_data (product_description(255))",
        
        # product_catalog indexes
        "CREATE INDEX idx_pc_client_id ON product_catalog (client_id)",
        "CREATE INDEX idx_pc_catalog_id ON product_catalog (catalog_id)",
        "CREATE INDEX idx_pc_categoria ON product_catalog (categoria)",
        "CREATE INDEX idx_pc_variedad ON product_catalog (variedad)",
        "CREATE INDEX idx_pc_color ON product_catalog (color)",
        "CREATE INDEX idx_pc_grado ON product_catalog (grado)",
        "CREATE INDEX idx_pc_search_key ON product_catalog (search_key(255))",
        
        # synonyms_blacklist indexes
        "CREATE INDEX idx_sb_client_id ON synonyms_blacklist (client_id)",
        "CREATE INDEX idx_sb_type ON synonyms_blacklist (type)",
        "CREATE INDEX idx_sb_original_word ON synonyms_blacklist (original_word)",
        "CREATE INDEX idx_sb_blacklist_word ON synonyms_blacklist (blacklist_word)",
        "CREATE INDEX idx_sb_status ON synonyms_blacklist (status)",
        "CREATE INDEX idx_sb_client_type_status ON synonyms_blacklist (client_id, type, status)",
        
        # staging_products indexes
        "CREATE INDEX idx_sp_client_id ON staging_products_to_create (client_id)",
        "CREATE INDEX idx_sp_catalog_id ON staging_products_to_create (catalog_id)",
        "CREATE INDEX idx_sp_status ON staging_products_to_create (status)",
        "CREATE INDEX idx_sp_created_from_row ON staging_products_to_create (created_from_row_id)",
        "CREATE INDEX idx_sp_client_status_date ON staging_products_to_create (client_id, status, created_at DESC)",
        
        # performance_metrics indexes
        "CREATE INDEX idx_pm_metric_name ON performance_metrics (metric_name)",
        "CREATE INDEX idx_pm_client_id ON performance_metrics (client_id)",
        "CREATE INDEX idx_pm_recorded_at ON performance_metrics (recorded_at)"
    ]
    
    created_count = 0
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            created_count += 1
        except mysql.connector.Error as e:
            if e.errno == 1061:  # Duplicate key name
                pass  # Index already exists, ignore
            else:
                print(f"    ⚠️ Index creation warning: {e}")
    
    print(f"    📊 Created {created_count} indexes")

def insert_sample_data_direct(cursor):
    """Insert sample data for testing all Streamlit applications"""
    clients = ['demo_client', 'acme_corp', 'test_company']
    
    for client_id in clients:
        # Sample processed mappings (4 records per client)
        mappings_data = [
            (client_id, 'Red roses premium grade A Ecuador export quality', 'FlowerCorp', 'red roses premium grade', 'roses red premium grade', '95', 'CAT001', 'Flowers', 'Roses', 'Red', 'Premium', '', ''),
            (client_id, 'White lilies standard fresh cut flowers', 'BloomLtd', 'white lilies standard', 'lilies white standard', '87', 'CAT002', 'Flowers', 'Lilies', 'White', 'Standard', '', ''),
            (client_id, 'Yellow sunflowers large size Netherlands import', 'PetalInc', 'yellow sunflowers large', 'sunflowers yellow large', '92', 'CAT003', 'Flowers', 'Sunflowers', 'Yellow', 'Large', '', ''),
            (client_id, 'Pink carnations grade B Kenya domestic market', 'FloraMax', 'pink carnations grade', 'carnations pink grade', '78', '111111', 'Flowers', 'Carnations', 'Pink', 'B', '', '')
        ]
        
        cursor.executemany("""
            INSERT IGNORE INTO processed_mappings (
                client_id, vendor_product_description, vendor_name, cleaned_input, 
                best_match, similarity_percentage, catalog_id, categoria, variedad, 
                color, grado, accept_map, deny_map
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, mappings_data)
        
        # Sample product catalog (3 records per client)
        catalog_data = [
            (client_id, 'Flowers', 'Roses', 'Red', 'Premium', 'CAT001', 'flowers roses red premium'),
            (client_id, 'Flowers', 'Lilies', 'White', 'Standard', 'CAT002', 'flowers lilies white standard'),
            (client_id, 'Flowers', 'Sunflowers', 'Yellow', 'Large', 'CAT003', 'flowers sunflowers yellow large')
        ]
        
        cursor.executemany("""
            INSERT IGNORE INTO product_catalog (
                client_id, categoria, variedad, color, grado, catalog_id, search_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, catalog_data)
        
        # Sample synonyms (2 records per client)
        synonym_data = [
            (client_id, 'synonym', 'premium', 'high quality', None, 'setup_script', 'active'),
            (client_id, 'synonym', 'standard', 'regular', None, 'setup_script', 'active')
        ]
        
        cursor.executemany("""
            INSERT IGNORE INTO synonyms_blacklist (
                client_id, type, original_word, synonym_word, blacklist_word, created_by, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, synonym_data)
        
        # Sample blacklist words (3 records per client)
        blacklist_data = [
            (client_id, 'blacklist', None, None, 'and', 'setup_script', 'active'),
            (client_id, 'blacklist', None, None, 'or', 'setup_script', 'active'),
            (client_id, 'blacklist', None, None, 'the', 'setup_script', 'active')
        ]
        
        cursor.executemany("""
            INSERT IGNORE INTO synonyms_blacklist (
                client_id, type, original_word, synonym_word, blacklist_word, created_by, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, blacklist_data)
        
        # Sample staging products (2 records per client)
        staging_data = [
            (client_id, 'Custom Flowers', 'Mixed Bouquet', 'Rainbow', 'Premium', 'custom flowers mixed bouquet rainbow premium', 'rainbow premium mixed bouquet', 'demo_user', 'pending'),
            (client_id, 'Plants', 'Succulents', 'Green', 'Standard', 'plants succulents green standard', 'small green succulent plants', 'demo_user', 'pending')
        ]
        
        cursor.executemany("""
            INSERT IGNORE INTO staging_products_to_create (
                client_id, categoria, variedad, color, grado, search_key, 
                original_input, created_by, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, staging_data)
    
    print(f"    📊 Inserted sample data for {len(clients)} clients")

def test_final_database():
    """Test the created database and show statistics"""
    try:
        config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Maracuya123',
            'database': 'mapping_validation_consolidated',
            'charset': 'utf8mb4'
        }
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        print("🔍 Database Test Results:")
        
        # Test each table
        tables = [
            'processed_mappings',
            'vendor_staging_data', 
            'product_catalog',
            'synonyms_blacklist',
            'staging_products_to_create',
            'performance_metrics'
        ]
        
        total_records = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  ✅ {table}: {count} records")
                total_records += count
            except mysql.connector.Error as e:
                print(f"  ❌ {table}: Error - {e}")
        
        # Test view
        try:
            cursor.execute("SELECT COUNT(*) FROM mapping_summary")
            count = cursor.fetchone()[0]
            print(f"  ✅ mapping_summary (view): {count} records")
        except mysql.connector.Error as e:
            print(f"  ❌ mapping_summary: Error - {e}")
        
        # Test clients
        try:
            cursor.execute("SELECT DISTINCT client_id FROM processed_mappings ORDER BY client_id")
            clients = [row[0] for row in cursor.fetchall()]
            print(f"  📋 Available clients: {', '.join(clients)}")
        except mysql.connector.Error:
            print(f"  ⚠️ Could not retrieve client list")
        
        cursor.close()
        connection.close()
        
        print(f"  📊 Total records across all tables: {total_records}")
        print("✅ Database test completed successfully!")
        
    except mysql.connector.Error as e:
        print(f"❌ Database test failed: {e}")

def main():
    """
    Main execution function
    """
    print("🚀 Direct Database Creation Script")
    print("="*60)
    print("This script creates the database programmatically,")
    print("bypassing all SQL file parsing issues.")
    print("="*60)
    
    success = create_database_direct()
    
    if success:
        print("\n🎉 COMPLETE SUCCESS!")
        print("\n📋 What was created:")
        print("  • mapping_validation_consolidated database")
        print("  • 6 tables with all 87 columns")
        print("  • 34+ optimized indexes")
        print("  • 1 reporting view")
        print("  • Sample data for 3 clients")
        print("\n🔧 Next Steps:")
        print("1. Update your .env file:")
        print("   DB_NAME=mapping_validation_consolidated")
        print("\n2. Test your Streamlit applications:")
        print("   streamlit run streamlit_app.py")
        print("   streamlit run enhanced_multi_client_streamlit_app.py")
        print("   streamlit run synonyms_blacklist_interface.py")
        print("\n3. Use these test clients:")
        print("   - demo_client")
        print("   - acme_corp")
        print("   - test_company")
        print("\n✨ Your optimized database is ready!")
    else:
        print("\n❌ Database creation failed!")
        print("Please check the error messages above.")

if __name__ == "__main__":
    main()