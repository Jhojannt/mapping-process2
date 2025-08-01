# enhanced_multi_client_database.py - Enhanced database with staging and synonyms management
import mysql.connector
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
import logging
from datetime import datetime
import os
import hashlib
from pathlib import Path
import json

class EnhancedMultiClientDatabase:
    """
    Enhanced database handler for multiple clients with staging products and synonyms management
    """
    
    def __init__(self, client_id: str = None):
        self.client_id = client_id
        self.base_db_name = os.getenv('BASE_DB_NAME', 'mapping_validation')
        
        # Database configuration
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
            'charset': 'utf8mb4',
            'autocommit': True,
            'connection_timeout': 30,
            'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'
        }
        
        self.connection_config2 = {
            'host': os.getenv('DB_HOST', 'http://mapping-process.cjjrhjl6dwxu.us-east-1.rds.amazonaws.com'),
            'user': os.getenv('DB_USER', 'mapping'),
            'password': os.getenv('DB_PASSWORD', 'wo0066upzahPfwB4U'),
            'database': os.getenv('DB_NAME', 'mapping_validation_db'),
            'charset': 'utf8mb4',
            'autocommit': True,
            'connection_timeout': 30,
            'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'
        }
        
        self.connection = None
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"{__name__}_{self.client_id}")
    
    def get_client_database_name(self, table_type: str = "main") -> str:
        """Get client-specific database name based on table type"""
        if not self.client_id:
            raise ValueError("Client ID not specified")
        
        db_names = {
            "main": f"mapping_validation_{self.client_id}",
            "vendor_staging": f"vendor_staging_area_{self.client_id}",
            "product_catalog": f"product_catalog_{self.client_id}",
            "synonyms_blacklist": f"synonyms_blacklist_{self.client_id}",
            "staging_products": "staging_products_to_create"  # Global staging
        }
        
        return db_names.get(table_type, f"mapping_validation_{self.client_id}")
    
    def create_all_client_databases(self) -> Tuple[bool, str]:
        """Create all required databases and tables for a client"""
        if not self.client_id:
            return False, "No client ID specified"
        
        try:
            # Connect without specifying database
            connection = mysql.connector.connect(**self.connection_config)
            cursor = connection.cursor()
            
            results = []
            
            # 1. Create main mapping validation database
            success, msg = self._create_mapping_validation_db(cursor)
            results.append(f"Mapping DB: {msg}")
            
            # 2. Create vendor staging area database
            success, msg = self._create_vendor_staging_db(cursor)
            results.append(f"Vendor Staging: {msg}")
            
            # 3. Create product catalog database
            success, msg = self._create_product_catalog_db(cursor)
            results.append(f"Product Catalog: {msg}")
            
            # 4. Create synonyms/blacklist database
            success, msg = self._create_synonyms_blacklist_db(cursor)
            results.append(f"Synonyms/Blacklist: {msg}")
            
            # 5. Create global staging products database
            success, msg = self._create_staging_products_db(cursor)
            results.append(f"Staging Products: {msg}")
            
            cursor.close()
            connection.close()
            
            return True, " | ".join(results)
            
        except Exception as e:
            return False, f"Error creating client databases: {str(e)}"
    
    def _create_mapping_validation_db(self, cursor) -> Tuple[bool, str]:
        """Create mapping validation database (existing structure)"""
        db_name = self.get_client_database_name("main")
        
        try:
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            # Create main table (keep existing structure)
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS processed_mappings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                client_id VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100),
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
                cleaned_input TEXT,
                applied_synonyms TEXT,
                removed_blacklist_words TEXT,
                best_match TEXT,
                similarity_percentage VARCHAR(10),
                matched_words TEXT,
                missing_words TEXT,
                catalog_id VARCHAR(100),
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                accept_map VARCHAR(10) DEFAULT '',
                deny_map VARCHAR(10) DEFAULT '',
                action VARCHAR(50) DEFAULT '',
                word VARCHAR(255) DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_client_id (client_id),
                INDEX idx_batch_id (batch_id),
                INDEX idx_vendor_name (vendor_name),
                INDEX idx_similarity (similarity_percentage),
                INDEX idx_catalog_id (catalog_id),
                INDEX idx_accept_map (accept_map),
                INDEX idx_created_at (created_at),
                INDEX idx_categoria (categoria),
                INDEX idx_variedad (variedad),
                INDEX idx_color (color),
                INDEX idx_grado (grado)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_vendor_staging_db(self, cursor) -> Tuple[bool, str]:
        """Create vendor staging area database (13 columns, first will be analyzed)"""
        db_name = self.get_client_database_name("vendor_staging")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS vendor_staging_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                product_description TEXT NOT NULL,  -- Column 1: Main analysis column
                column_2 VARCHAR(255),
                column_3 VARCHAR(255),
                column_4 VARCHAR(255),
                column_5 VARCHAR(255),
                column_6 VARCHAR(255),
                column_7 VARCHAR(255),
                column_8 VARCHAR(255),
                column_9 VARCHAR(255),
                column_10 VARCHAR(255),
                column_11 VARCHAR(255),
                column_12 VARCHAR(255),
                column_13 VARCHAR(255),
                client_id VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_client_id (client_id),
                INDEX idx_product_description (product_description(255)),
                INDEX idx_batch_id (batch_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_product_catalog_db(self, cursor) -> Tuple[bool, str]:
        """Create product catalog database (master catalog)"""
        db_name = self.get_client_database_name("product_catalog")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS product_catalog (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                additional_field_1 VARCHAR(255),
                catalog_id VARCHAR(100) UNIQUE,
                search_key TEXT,  -- For fuzzy matching
                client_id VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_client_id (client_id),
                INDEX idx_catalog_id (catalog_id),
                INDEX idx_categoria (categoria),
                INDEX idx_variedad (variedad),
                INDEX idx_color (color),
                INDEX idx_grado (grado),
                INDEX idx_search_key (search_key(255))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_synonyms_blacklist_db(self, cursor) -> Tuple[bool, str]:
        """Create synonyms and blacklist database"""
        db_name = self.get_client_database_name("synonyms_blacklist")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS synonyms_blacklist (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                type VARCHAR(20) NOT NULL,  -- 'synonym' or 'blacklist'
                original_word VARCHAR(255),  -- For synonyms: original word
                synonym_word VARCHAR(255),   -- For synonyms: replacement word
                blacklist_word VARCHAR(255), -- For blacklist: word to remove
                client_id VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_by VARCHAR(100),  -- Track who added it
                status VARCHAR(20) DEFAULT 'active',  -- active, inactive
                INDEX idx_client_id (client_id),
                INDEX idx_type (type),
                INDEX idx_original_word (original_word),
                INDEX idx_blacklist_word (blacklist_word),
                INDEX idx_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_staging_products_db(self, cursor) -> Tuple[bool, str]:
        """Create global staging products to create database"""
        db_name = self.get_client_database_name("staging_products")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS staging_products_to_create (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                additional_field_1 VARCHAR(255),
                catalog_id VARCHAR(100) DEFAULT '111111',  -- Always 111111 as specified
                search_key TEXT,  -- For fuzzy matching
                client_id VARCHAR(100) NOT NULL,
                created_from_row_id BIGINT,  -- Reference to original mapping row
                original_input TEXT,  -- Original input that created this
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_by VARCHAR(100),
                status VARCHAR(20) DEFAULT 'pending',  -- pending, approved, rejected
                INDEX idx_client_id (client_id),
                INDEX idx_catalog_id (catalog_id),
                INDEX idx_categoria (categoria),
                INDEX idx_variedad (variedad),
                INDEX idx_color (color),
                INDEX idx_grado (grado),
                INDEX idx_status (status),
                INDEX idx_created_from_row (created_from_row_id),
                INDEX idx_search_key (search_key(255))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def connect_to_database(self, db_type: str = "main") -> bool:
        """Connect to specific client database"""
        if not self.client_id:
            return False
        
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name(db_type)
            
            self.connection = mysql.connector.connect(**config)
            return self.connection.is_connected()
        except Exception as e:
            self.logger.error(f"Failed to connect to {db_type} database: {str(e)}")
            return False
    
    def save_product_to_staging(self, categoria: str, variedad: str, color: str, 
                               grado: str, original_row_id: int, original_input: str,
                               created_by: str = None) -> Tuple[bool, str]:
        """Save a new product to staging products to create"""
        try:
            # Connect to staging products database
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("staging_products")
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # Create search key for fuzzy matching
            search_key = f"{categoria} {variedad} {color} {grado}".strip().lower()
            
            insert_sql = """
            INSERT INTO staging_products_to_create 
            (categoria, variedad, color, grado, catalog_id, search_key, client_id, 
             created_from_row_id, original_input, created_by, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (categoria, variedad, color, grado, '111111', search_key, 
                     self.client_id, original_row_id, original_input, created_by, 'pending')
            
            cursor.execute(insert_sql, values)
            new_id = cursor.lastrowid
            
            cursor.close()
            connection.close()
            
            return True, f"Product saved to staging with ID {new_id}"
            
        except Exception as e:
            return False, f"Error saving product to staging: {str(e)}"
    
    def get_staging_products(self, client_id: str = None) -> Optional[pd.DataFrame]:
        """Get staging products for client"""
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("staging_products")
            connection = mysql.connector.connect(**config)
            
            query = """
            SELECT * FROM staging_products_to_create 
            WHERE client_id = %s 
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, connection, params=[client_id or self.client_id])
            connection.close()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting staging products: {str(e)}")
            return None
    
    def update_synonyms_blacklist(self, synonym_data: List[Dict], blacklist_data: List[str]) -> Tuple[bool, str]:
        """Update synonyms and blacklist in database"""
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("synonyms_blacklist")
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # Clear existing data for this client
            cursor.execute("DELETE FROM synonyms_blacklist WHERE client_id = %s", (self.client_id,))
            
            # Insert synonyms
            for synonym in synonym_data:
                if isinstance(synonym, dict) and len(synonym) == 1:
                    original, replacement = list(synonym.items())[0]
                    cursor.execute("""
                        INSERT INTO synonyms_blacklist 
                        (type, original_word, synonym_word, client_id, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ('synonym', original, replacement, self.client_id, 'active'))
            
            # Insert blacklist
            for word in blacklist_data:
                cursor.execute("""
                    INSERT INTO synonyms_blacklist 
                    (type, blacklist_word, client_id, status)
                    VALUES (%s, %s, %s, %s)
                """, ('blacklist', word, self.client_id, 'active'))
            
            cursor.close()
            connection.close()
            
            return True, f"Updated {len(synonym_data)} synonyms and {len(blacklist_data)} blacklist words"
            
        except Exception as e:
            return False, f"Error updating synonyms/blacklist: {str(e)}"
    
    def get_synonyms_blacklist(self) -> Dict[str, Any]:
        """Get current synonyms and blacklist for client"""
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("synonyms_blacklist")
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            
            # Get synonyms
            cursor.execute("""
                SELECT original_word, synonym_word FROM synonyms_blacklist 
                WHERE client_id = %s AND type = 'synonym' AND status = 'active'
            """, (self.client_id,))
            synonyms = {row['original_word']: row['synonym_word'] for row in cursor.fetchall()}
            
            # Get blacklist
            cursor.execute("""
                SELECT blacklist_word FROM synonyms_blacklist 
                WHERE client_id = %s AND type = 'blacklist' AND status = 'active'
            """, (self.client_id,))
            blacklist = [row['blacklist_word'] for row in cursor.fetchall()]
            
            cursor.close()
            connection.close()
            
            return {
                'synonyms': synonyms,
                'blacklist': {'input': blacklist}
            }
            
        except Exception as e:
            self.logger.error(f"Error getting synonyms/blacklist: {str(e)}")
            return {'synonyms': {}, 'blacklist': {'input': []}}
    
    def get_combined_catalog_for_fuzzy(self) -> List[str]:
        """Get combined catalog (master + staging) for fuzzy matching"""
        try:
            catalogs = []
            
            # Get master catalog
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("product_catalog")
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT search_key FROM product_catalog 
                WHERE client_id = %s AND search_key IS NOT NULL
            """, (self.client_id,))
            
            catalogs.extend([row[0] for row in cursor.fetchall()])
            cursor.close()
            connection.close()
            
            # Get staging catalog
            config['database'] = self.get_client_database_name("staging_products")
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT search_key FROM staging_products_to_create 
                WHERE client_id = %s AND search_key IS NOT NULL AND status = 'pending'
            """, (self.client_id,))
            
            catalogs.extend([row[0] for row in cursor.fetchall()])
            cursor.close()
            connection.close()
            
            return catalogs
            
        except Exception as e:
            self.logger.error(f"Error getting combined catalog: {str(e)}")
            return []
    
    def get_filterable_columns(self) -> List[str]:
        """Get list of columns that can be filtered for exclusion"""
        return [
            'vendor_product_description',
            'company_location', 
            'vendor_name',
            'vendor_id',
            'quantity',
            'stems_bunch',
            'unit_type',
            'staging_id',
            'object_mapping_id',
            'company_id',
            'user_id',
            'product_mapping_id',
            'email',
            'cleaned_input',
            'best_match',
            'similarity_percentage',
            'catalog_id',
            'categoria',
            'variedad',
            'color',
            'grado'
        ]


# Convenience functions
def create_enhanced_client_databases(client_id: str) -> Tuple[bool, str]:
    """Create all enhanced databases for a client"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.create_all_client_databases()

def save_new_product_to_staging(client_id: str, categoria: str, variedad: str, 
                                color: str, grado: str, original_row_id: int,
                                original_input: str, created_by: str = None) -> Tuple[bool, str]:
    """Save new product to staging"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.save_product_to_staging(categoria, variedad, color, grado, 
                                     original_row_id, original_input, created_by)

def get_client_staging_products(client_id: str) -> Optional[pd.DataFrame]:
    """Get staging products for client"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.get_staging_products(client_id)

def update_client_synonyms_blacklist(client_id: str, synonyms: List[Dict], 
                                    blacklist: List[str]) -> Tuple[bool, str]:
    """Update client synonyms and blacklist"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.update_synonyms_blacklist(synonyms, blacklist)

def get_client_synonyms_blacklist(client_id: str) -> Dict[str, Any]:
    """Get client synonyms and blacklist"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.get_synonyms_blacklist()


if __name__ == "__main__":
    # Test enhanced multi-client database system
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Testing Enhanced Multi-Client Database System...")
    
    test_client = "test_enhanced_client"
    
    # Test creating enhanced databases
    print(f"\n1. Creating enhanced databases for client: {test_client}...")
    success, message = create_enhanced_client_databases(test_client)
    print(f"Create result: {message}")
    
    if success:
        print(f"\n2. Testing staging product save...")
        success, message = save_new_product_to_staging(
            test_client, "Test Category", "Test Variety", "Red", "A", 
            1, "test product input", "test_user"
        )
        print(f"Save result: {message}")
        
        print(f"\n3. Getting staging products...")
        staging_df = get_client_staging_products(test_client)
        if staging_df is not None:
            print(f"Found {len(staging_df)} staging products")
        
        print(f"\n4. Testing synonyms/blacklist...")
        test_synonyms = [{"old_word": "new_word"}, {"test": "example"}]
        test_blacklist = ["unwanted", "remove_this"]
        
        success, message = update_client_synonyms_blacklist(test_client, test_synonyms, test_blacklist)
        print(f"Update result: {message}")
        
        if success:
            data = get_client_synonyms_blacklist(test_client)
            print(f"Retrieved synonyms: {data['synonyms']}")
            print(f"Retrieved blacklist: {data['blacklist']}")
    
    print("\n✅ Enhanced multi-client database testing completed!")