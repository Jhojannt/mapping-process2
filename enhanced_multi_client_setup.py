# enhanced_multi_client_setup.py - Complete setup script for enhanced multi-client system

"""
Enhanced Multi-Client Database Setup Script

This script will:
1. Test MySQL connection
2. Create enhanced database infrastructure for multi-client support
3. Set up all required databases per client (5 databases total)
4. Create sample clients with test data
5. Verify the complete enhanced setup
6. Run functionality tests across all components

Run this before starting your enhanced multi-client applications.
"""

import mysql.connector
import sys
import os
from typing import Tuple, List, Dict, Any
import logging
from datetime import datetime
import json
from Enhanced_MultiClient_Database import create_enhanced_client_databases

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
    'charset': 'utf8mb4',
    'autocommit': True
}

DB_CONFIG2 = {
    'host': os.getenv('DB_HOST', 'http://mapping-process.cjjrhjl6dwxu.us-east-1.rds.amazonaws.com'),
    'user': os.getenv('DB_USER', 'mapping'),
    'password': os.getenv('DB_PASSWORD', 'wo0066upzahPfwB4U'),
    'charset': 'utf8mb4',
    'autocommit': True
}

BASE_DATABASE_NAME = os.getenv('BASE_DB_NAME', 'mapping_validation')

def print_banner():
    """Print enhanced welcome banner"""
    print("=" * 80)
    print("🔧 Enhanced Multi-Client Mapping Validation System - Database Setup")
    print("=" * 80)
    print("Features: Row-level processing, Staging products, Synonyms management")
    print("=" * 80)
    print()

def test_mysql_connection() -> Tuple[bool, str]:
    """Test basic MySQL connection"""
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

def create_sample_clients_with_data() -> List[str]:
    """Create sample clients with test data for all enhanced features"""
    sample_clients = ["demo_client", "acme_corp", "test_company"]
    created_clients = []
    
    print("🏢 Creating enhanced sample client databases...")
    
    for client_id in sample_clients:
        print(f"\n📊 Setting up client: {client_id}")
        success, message = create_enhanced_client_databases(client_id)
        print(f"  Database creation: {message}")
        
        if success:
            created_clients.append(client_id)
            
            # Add sample data to each client
            success = add_sample_data_to_client(client_id)
            if success:
                print(f"  ✅ Sample data added to {client_id}")
            else:
                print(f"  ⚠️ Failed to add sample data to {client_id}")
    
    return created_clients

def add_sample_data_to_client(client_id: str) -> bool:
    """Add sample data to client databases"""
    try:
        # Sample data for different databases
        success = True
        
        # 1. Add sample data to vendor staging area
        success &= add_vendor_staging_sample_data(client_id)
        
        # 2. Add sample data to product catalog
        success &= add_product_catalog_sample_data(client_id)
        
        # 3. Add sample synonyms and blacklist
        success &= add_synonyms_blacklist_sample_data(client_id)
        
        # 4. Add sample staging products
        success &= add_staging_products_sample_data(client_id)
        
        return success
        
    except Exception as e:
        logger.error(f"Error adding sample data to client {client_id}: {str(e)}")
        return False

def add_vendor_staging_sample_data(client_id: str) -> bool:
    """Add sample data to vendor staging area"""
    try:
        db_name = f"vendor_staging_area_{client_id}"
        config = DB_CONFIG.copy()
        config['database'] = db_name
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        sample_data = [
            ("Red roses premium grade A", "Flowers", "Roses", "Red", "Premium", "Ecuador", "Vendor1", "Fresh", "Export", "Bundle", "50stems", "Air", "Cold"),
            ("White lilies standard quality", "Flowers", "Lilies", "White", "Standard", "Colombia", "Vendor2", "Fresh", "Domestic", "Box", "25stems", "Ground", "Normal"),
            ("Yellow sunflowers large size", "Flowers", "Sunflowers", "Yellow", "Large", "Netherlands", "Vendor3", "Fresh", "Export", "Bundle", "20stems", "Air", "Cold"),
            ("Pink carnations small grade B", "Flowers", "Carnations", "Pink", "B", "Kenya", "Vendor4", "Fresh", "Domestic", "Box", "100stems", "Ground", "Normal"),
            ("Purple orchids premium exotic", "Flowers", "Orchids", "Purple", "Premium", "Thailand", "Vendor5", "Fresh", "Export", "Individual", "1stem", "Air", "Climate")
        ]
        
        insert_sql = """
        INSERT INTO vendor_staging_data 
        (product_description, column_2, column_3, column_4, column_5, column_6, 
         column_7, column_8, column_9, column_10, column_11, column_12, column_13, 
         client_id, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_id = f"sample_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        for data_row in sample_data:
            values = data_row + (client_id, batch_id)
            cursor.execute(insert_sql, values)
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error adding vendor staging sample data: {str(e)}")
        return False

def add_product_catalog_sample_data(client_id: str) -> bool:
    """Add sample data to product catalog"""
    try:
        db_name = f"product_catalog_{client_id}"
        config = DB_CONFIG.copy()
        config['database'] = db_name
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        sample_products = [
            ("Flowers", "Roses", "Red", "Premium", "Field1", "CAT001", "flowers roses red premium"),
            ("Flowers", "Roses", "White", "Premium", "Field1", "CAT002", "flowers roses white premium"),
            ("Flowers", "Lilies", "White", "Standard", "Field2", "CAT003", "flowers lilies white standard"),
            ("Flowers", "Sunflowers", "Yellow", "Large", "Field3", "CAT004", "flowers sunflowers yellow large"),
            ("Flowers", "Carnations", "Pink", "B", "Field4", "CAT005", "flowers carnations pink b grade"),
            ("Flowers", "Orchids", "Purple", "Premium", "Field5", "CAT006", "flowers orchids purple premium exotic"),
            ("Flowers", "Tulips", "Mixed", "Standard", "Field6", "CAT007", "flowers tulips mixed standard"),
            ("Flowers", "Daisies", "White", "Standard", "Field7", "CAT008", "flowers daisies white standard")
        ]
        
        insert_sql = """
        INSERT INTO product_catalog 
        (categoria, variedad, color, grado, additional_field_1, catalog_id, search_key, client_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for product in sample_products:
            values = product + (client_id,)
            cursor.execute(insert_sql, values)
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error adding product catalog sample data: {str(e)}")
        return False

def add_synonyms_blacklist_sample_data(client_id: str) -> bool:
    """Add sample synonyms and blacklist data"""
    try:
        db_name = f"synonyms_blacklist_{client_id}"
        config = DB_CONFIG.copy()
        config['database'] = db_name
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Sample synonyms
        sample_synonyms = [
            ("premium", "high quality"),
            ("grade a", "top quality"),
            ("standard", "regular"),
            ("large", "big"),
            ("small", "mini"),
            ("exotic", "special")
        ]
        
        # Sample blacklist words
        sample_blacklist = [
            "and", "or", "the", "a", "an", "with", "from", "to", "for", "of",
            "fresh", "cut", "stem", "bunch", "box", "pack"
        ]
        
        # Insert synonyms
        synonym_sql = """
        INSERT INTO synonyms_blacklist 
        (type, original_word, synonym_word, client_id, created_by, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        for original, replacement in sample_synonyms:
            cursor.execute(synonym_sql, ('synonym', original, replacement, client_id, 'setup_script', 'active'))
        
        # Insert blacklist words
        blacklist_sql = """
        INSERT INTO synonyms_blacklist 
        (type, blacklist_word, client_id, created_by, status)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        for word in sample_blacklist:
            cursor.execute(blacklist_sql, ('blacklist', word, client_id, 'setup_script', 'active'))
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error adding synonyms/blacklist sample data: {str(e)}")
        return False

def add_staging_products_sample_data(client_id: str) -> bool:
    """Add sample staging products data"""
    try:
        config = DB_CONFIG.copy()
        config['database'] = "staging_products_to_create"
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        sample_staging_products = [
            ("Custom Flowers", "Mixed Bouquet", "Rainbow", "Premium", "Field1", "mixed flowers rainbow premium bouquet", "test product 1"),
            ("Plants", "Succulents", "Green", "Small", "Field2", "plants succulents green small", "test product 2"),
            ("Flowers", "Peonies", "Pink", "Luxury", "Field3", "flowers peonies pink luxury", "test product 3")
        ]
        
        insert_sql = """
        INSERT INTO staging_products_to_create 
        (categoria, variedad, color, grado, additional_field_1, catalog_id, 
         search_key, client_id, created_from_row_id, original_input, created_by, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for i, product in enumerate(sample_staging_products):
            categoria, variedad, color, grado, field1, search_key, original_input = product
            values = (categoria, variedad, color, grado, field1, "111111", 
                     search_key, client_id, i+1, original_input, 'setup_script', 'pending')
            cursor.execute(insert_sql, values)
        
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error adding staging products sample data: {str(e)}")
        return False

def verify_enhanced_client_setup(client_id: str) -> Tuple[bool, str]:
    """Verify complete enhanced client database setup"""
    print(f"🔍 Verifying enhanced setup for client '{client_id}'...")
    
    try:
        verification_results = []
        
        # Database types to verify
        db_types = {
            "main": f"mapping_validation_{client_id}",
            "vendor_staging": f"vendor_staging_area_{client_id}",
            "product_catalog": f"product_catalog_{client_id}",
            "synonyms_blacklist": f"synonyms_blacklist_{client_id}",
            "staging_products": "staging_products_to_create"
        }
        
        for db_type, db_name in db_types.items():
            success, message = verify_single_database(db_name, db_type, client_id)
            verification_results.append(f"{db_type}: {message}")
        
        # Check if all verifications passed
        all_passed = all("✅" in result for result in verification_results)
        
        if all_passed:
            return True, " | ".join(verification_results)
        else:
            return False, " | ".join(verification_results)
        
    except Exception as e:
        return False, f"❌ Verification error: {str(e)}"

def verify_single_database(db_name: str, db_type: str, client_id: str) -> Tuple[bool, str]:
    """Verify a single database and its contents"""
    try:
        config = DB_CONFIG.copy()
        config['database'] = db_name
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Check tables based on database type
        if db_type == "main":
            cursor.execute("SHOW TABLES LIKE 'processed_mappings'")
            if not cursor.fetchone():
                return False, "❌ Missing processed_mappings table"
        
        elif db_type == "vendor_staging":
            cursor.execute("SHOW TABLES LIKE 'vendor_staging_data'")
            if not cursor.fetchone():
                return False, "❌ Missing vendor_staging_data table"
            
            # Check sample data
            cursor.execute("SELECT COUNT(*) FROM vendor_staging_data WHERE client_id = %s", (client_id,))
            count = cursor.fetchone()[0]
            if count == 0:
                return False, "❌ No sample data"
        
        elif db_type == "product_catalog":
            cursor.execute("SHOW TABLES LIKE 'product_catalog'")
            if not cursor.fetchone():
                return False, "❌ Missing product_catalog table"
            
            # Check sample data
            cursor.execute("SELECT COUNT(*) FROM product_catalog WHERE client_id = %s", (client_id,))
            count = cursor.fetchone()[0]
            if count == 0:
                return False, "❌ No sample data"
        
        elif db_type == "synonyms_blacklist":
            cursor.execute("SHOW TABLES LIKE 'synonyms_blacklist'")
            if not cursor.fetchone():
                return False, "❌ Missing synonyms_blacklist table"
            
            # Check sample data
            cursor.execute("SELECT COUNT(*) FROM synonyms_blacklist WHERE client_id = %s", (client_id,))
            count = cursor.fetchone()[0]
            if count == 0:
                return False, "❌ No sample data"
        
        elif db_type == "staging_products":
            cursor.execute("SHOW TABLES LIKE 'staging_products_to_create'")
            if not cursor.fetchone():
                return False, "❌ Missing staging_products_to_create table"
            
            # Check sample data
            cursor.execute("SELECT COUNT(*) FROM staging_products_to_create WHERE client_id = %s", (client_id,))
            count = cursor.fetchone()[0]
            if count == 0:
                return False, "❌ No sample data"
        
        cursor.close()
        connection.close()
        
        return True, "✅ Verified"
        
    except mysql.connector.Error as e:
        return False, f"❌ DB Error: {str(e)[:50]}"
    except Exception as e:
        return False, f"❌ Error: {str(e)[:50]}"

def test_enhanced_operations(client_id: str) -> Tuple[bool, str]:
    """Test enhanced operations for a client"""
    print(f"🧪 Testing enhanced operations for client '{client_id}'...")
    
    try:
        from Enhanced_MultiClient_Database import EnhancedMultiClientDatabase
        from row_level_processing import RowLevelProcessor
        
        # Test database connections
        db = EnhancedMultiClientDatabase(client_id)
        
        # Test connecting to each database type
        db_types = ["main", "vendor_staging", "product_catalog", "synonyms_blacklist", "staging_products"]
        connection_results = []
        
        for db_type in db_types:
            success = db.connect_to_database(db_type)
            connection_results.append(f"{db_type}: {'✅' if success else '❌'}")
        
        # Test row processor
        processor = RowLevelProcessor(client_id)
        
        # Test getting combined catalog
        catalog_data = processor._get_combined_catalog_data()
        catalog_count = len(catalog_data)
        
        # Test synonyms/blacklist retrieval
        synonyms_data = db.get_synonyms_blacklist()
        synonym_count = len(synonyms_data.get('synonyms', {}))
        blacklist_count = len(synonyms_data.get('blacklist', {}).get('input', []))
        
        # Test staging products
        staging_df = db.get_staging_products(client_id)
        staging_count = len(staging_df) if staging_df is not None else 0
        
        results = [
            f"Connections: {len([r for r in connection_results if '✅' in r])}/{len(connection_results)}",
            f"Catalog: {catalog_count} items",
            f"Synonyms: {synonym_count}",
            f"Blacklist: {blacklist_count}",
            f"Staging: {staging_count}"
        ]
        
        return True, " | ".join(results)
        
    except Exception as e:
        return False, f"❌ Operations test failed: {str(e)}"

def get_existing_enhanced_clients() -> List[str]:
    """Get list of existing enhanced client databases"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Get all databases that match any client pattern
        patterns = [
            f"{BASE_DATABASE_NAME}_%",
            f"vendor_staging_area_%",
            f"product_catalog_%",
            f"synonyms_blacklist_%"
        ]
        
        all_client_ids = set()
        
        for pattern in patterns:
            cursor.execute("SHOW DATABASES LIKE %s", (pattern,))
            databases = cursor.fetchall()
            
            for (db_name,) in databases:
                # Extract client ID from different database patterns
                if db_name.startswith(f"{BASE_DATABASE_NAME}_"):
                    client_id = db_name[len(f"{BASE_DATABASE_NAME}_"):]
                    all_client_ids.add(client_id)
                elif db_name.startswith("vendor_staging_area_"):
                    client_id = db_name[len("vendor_staging_area_"):]
                    all_client_ids.add(client_id)
                elif db_name.startswith("product_catalog_"):
                    client_id = db_name[len("product_catalog_"):]
                    all_client_ids.add(client_id)
                elif db_name.startswith("synonyms_blacklist_"):
                    client_id = db_name[len("synonyms_blacklist_"):]
                    all_client_ids.add(client_id)
        
        cursor.close()
        connection.close()
        
        return sorted(list(all_client_ids))
        
    except Exception as e:
        logger.error(f"Error getting existing enhanced clients: {str(e)}")
        return []

def print_enhanced_summary(created_clients: List[str], existing_clients: List[str]):
    """Print enhanced setup summary and next steps"""
    print()
    print("=" * 80)
    print("🎉 Enhanced Multi-Client Database Setup Complete!")
    print("=" * 80)
    print()
    
    all_clients = list(set(created_clients + existing_clients))
    
    if all_clients:
        print("📊 Available Enhanced Clients:")
        for client_id in sorted(all_clients):
            status = "✅ Created" if client_id in created_clients else "📋 Existing"
            print(f"  - {client_id} ({status})")
            print(f"    📊 mapping_validation_{client_id}")
            print(f"    🏪 vendor_staging_area_{client_id}")
            print(f"    📋 product_catalog_{client_id}")
            print(f"    📝 synonyms_blacklist_{client_id}")
            print(f"    🆕 staging_products_to_create (shared)")
            print()
    else:
        print("⚠️  No clients available. Create clients using the applications.")
    
    print("🚀 Next steps:")
    print("1. Run the main enhanced application:")
    print("   streamlit run enhanced_multi_client_streamlit_app.py")
    print()
    print("2. Run the synonyms/blacklist manager (separate interface):")
    print("   streamlit run synonyms_blacklist_interface.py --server.port 8502")
    print()
    print("3. Access applications:")
    print("   • Main App: http://localhost:8501")
    print("   • Synonyms Manager: http://localhost:8502")
    print()
    print("🔧 Enhanced Features Available:")
    print("  ✅ Row-level fuzzy matching with re-processing")
    print("  ✅ Dynamic synonyms and blacklist management")
    print("  ✅ Staging products creation (catalog_id: 111111)")
    print("  ✅ Combined catalog search (master + staging)")
    print("  ✅ Advanced exclusion filters")
    print("  ✅ Separate synonyms/blacklist web interface")
    print("  ✅ Complete client data isolation")
    print()
    print("💡 Workflow:")
    print("  1. Select client in main app")
    print("  2. Upload and process files")
    print("  3. Use row-level actions (re-run fuzzy, edit products)")
    print("  4. Save new products to staging")
    print("  5. Manage synonyms/blacklist in separate interface")
    print("  6. Export and download results")
    print()

def main():
    """Main enhanced setup function"""
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
    
    # Get existing enhanced clients
    existing_clients = get_existing_enhanced_clients()
    if existing_clients:
        print(f"📋 Found existing enhanced clients: {', '.join(existing_clients)}")
    
    # Create sample clients with enhanced features
    created_clients = create_sample_clients_with_data()
    
    # Verify each enhanced client setup
    all_clients = list(set(created_clients + existing_clients))
    verified_clients = []
    
    print(f"\n🔍 Verifying {len(all_clients)} enhanced client setups...")
    for client_id in all_clients:
        success, message = verify_enhanced_client_setup(client_id)
        print(f"  {client_id}: {message}")
        if success:
            verified_clients.append(client_id)
        else:
            print(f"    ⚠️ Warning: Enhanced setup verification failed for {client_id}")
    
    # Test enhanced operations for verified clients
    print(f"\n🧪 Testing enhanced operations...")
    for client_id in verified_clients[:2]:  # Test first 2 clients
        success, message = test_enhanced_operations(client_id)
        print(f"  {client_id}: {message}")
        if not success:
            print(f"    ⚠️ Warning: Enhanced operations test failed for {client_id}")
    
    print_enhanced_summary(created_clients, existing_clients)

if __name__ == "__main__":
    main()