# enhanced_multi_client_database.py - Complete Multi-Client Database System
"""
Enhanced Multi-Client Database System

A comprehensive database architecture that provides:
- Complete client data isolation
- Individual client database structures
- Staging products management
- Synonyms and blacklist per client
- Row-level processing support
- Robust connection management
"""

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
    Enhanced database handler for multiple clients with complete data isolation
    """
    
    def __init__(self, client_id: str = None):
        self.client_id = client_id
        self.base_db_name = os.getenv('BASE_DB_NAME', 'mapping_validation')
        
        # Primary database configuration (local)
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
            'charset': 'utf8mb4',
            'autocommit': True,
            'connection_timeout': 30,
            'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'
        }
        
        # Secondary database configuration (AWS RDS - if needed)
        self.connection_config_aws = {
            'host': 'mapping-process.cjjrhjl6dwxu.us-east-1.rds.amazonaws.com',
            'user': 'mapping',
            'password': 'wo0066upzahPfwB4U',
            'database': 'mapping_validation_db',
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
        self.logger = logging.getLogger(f"EnhancedDB_{self.client_id}")
    
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
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test database connection"""
        try:
            connection = mysql.connector.connect(**self.connection_config)
            if connection.is_connected():
                server_info = connection.get_server_info()
                connection.close()
                return True, f"Connected to MySQL Server version {server_info}"
            else:
                return False, "Failed to connect to MySQL"
        except mysql.connector.Error as e:
            return False, f"MySQL Error: {e.msg if hasattr(e, 'msg') else str(e)}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
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
        """Create mapping validation database with complete structure"""
        db_name = self.get_client_database_name("main")
        
        try:
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            # Create main table with all required columns
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS processed_mappings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                client_id VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100),
                
                -- Original input columns (13 columns from TSV)
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
                
                -- Catalog matching results
                catalog_id VARCHAR(100),
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                
                -- User validation actions
                accept_map VARCHAR(10) DEFAULT '',
                deny_map VARCHAR(10) DEFAULT '',
                action VARCHAR(50) DEFAULT '',
                word VARCHAR(255) DEFAULT '',
                
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                -- Indexes for performance
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
            
            # Create summary view for analytics
            create_view_sql = f"""
            CREATE OR REPLACE VIEW mapping_summary AS
            SELECT 
                vendor_name,
                COUNT(*) as total_mappings,
                SUM(CASE WHEN accept_map = 'True' THEN 1 ELSE 0 END) as accepted_mappings,
                SUM(CASE WHEN deny_map = 'True' THEN 1 ELSE 0 END) as denied_mappings,
                AVG(CASE WHEN similarity_percentage != '' AND similarity_percentage IS NOT NULL 
                    THEN CAST(similarity_percentage AS DECIMAL(5,2)) END) as avg_similarity,
                DATE(created_at) as processing_date
            FROM processed_mappings 
            WHERE client_id = '{self.client_id}'
              AND similarity_percentage IS NOT NULL 
              AND similarity_percentage != ''
            GROUP BY vendor_name, DATE(created_at)
            ORDER BY processing_date DESC, total_mappings DESC
            """
            
            cursor.execute(create_view_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_vendor_staging_db(self, cursor) -> Tuple[bool, str]:
        """Create vendor staging area database (13 columns for raw vendor data)"""
        db_name = self.get_client_database_name("vendor_staging")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS vendor_staging_data (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                
                -- 13 columns matching TSV structure
                product_description TEXT NOT NULL,  -- Column 1: Main analysis column
                company_location VARCHAR(255),       -- Column 2
                vendor_name VARCHAR(255),           -- Column 3
                vendor_id VARCHAR(100),             -- Column 4
                quantity VARCHAR(100),              -- Column 5
                stems_bunch VARCHAR(100),           -- Column 6
                unit_type VARCHAR(100),             -- Column 7
                staging_id VARCHAR(100),            -- Column 8
                object_mapping_id VARCHAR(100),     -- Column 9
                company_id VARCHAR(100),            -- Column 10
                user_id VARCHAR(100),               -- Column 11
                product_mapping_id VARCHAR(100),    -- Column 12
                email VARCHAR(255),                 -- Column 13
                
                -- Metadata
                client_id VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100),
                processing_status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Indexes
                INDEX idx_client_id (client_id),
                INDEX idx_product_description (product_description(255)),
                INDEX idx_batch_id (batch_id),
                INDEX idx_vendor_name (vendor_name),
                INDEX idx_processing_status (processing_status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_sql)
            
            return True, "✅ Created"
        except Exception as e:
            return False, f"❌ Error: {str(e)}"
    
    def _create_product_catalog_db(self, cursor) -> Tuple[bool, str]:
        """Create product catalog database (master catalog for fuzzy matching)"""
        db_name = self.get_client_database_name("product_catalog")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS product_catalog (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                
                -- Product classification (matching second TSV structure)
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                additional_field_1 VARCHAR(255),
                catalog_id VARCHAR(100) UNIQUE,
                
                -- For fuzzy matching
                search_key TEXT,  -- Concatenated searchable text
                
                -- Metadata
                client_id VARCHAR(100) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                
                -- Indexes for performance
                INDEX idx_client_id (client_id),
                INDEX idx_catalog_id (catalog_id),
                INDEX idx_categoria (categoria),
                INDEX idx_variedad (variedad),
                INDEX idx_color (color),
                INDEX idx_grado (grado),
                INDEX idx_search_key (search_key(255)),
                INDEX idx_is_active (is_active)
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
                
                -- For synonyms
                original_word VARCHAR(255),  -- Original word to replace
                synonym_word VARCHAR(255),   -- Replacement word
                
                -- For blacklist
                blacklist_word VARCHAR(255), -- Word to remove
                
                -- Metadata
                client_id VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_by VARCHAR(100),  -- Track who added it
                status VARCHAR(20) DEFAULT 'active',  -- active, inactive
                
                -- Indexes
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
        """Create global staging products database for products pending creation"""
        db_name = self.get_client_database_name("staging_products")
        
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute(f"USE {db_name}")
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS staging_products_to_create (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                
                -- Product details
                categoria VARCHAR(255),
                variedad VARCHAR(255),
                color VARCHAR(255),
                grado VARCHAR(255),
                additional_field_1 VARCHAR(255),
                
                -- Always 111111 for staging products as specified
                catalog_id VARCHAR(100) DEFAULT '111111',
                
                -- For fuzzy matching
                search_key TEXT,
                
                -- Traceability
                client_id VARCHAR(100) NOT NULL,
                created_from_row_id BIGINT,  -- Reference to original mapping row
                original_input TEXT,  -- Original input that created this
                
                -- Workflow management
                status VARCHAR(20) DEFAULT 'pending',  -- pending, approved, rejected, created
                approved_by VARCHAR(100),
                approved_at TIMESTAMP NULL,
                notes TEXT,
                
                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_by VARCHAR(100),
                
                -- Indexes
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
            self.logger.error("No client ID specified for database connection")
            return False
        
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name(db_type)
            
            self.connection = mysql.connector.connect(**config)
            if self.connection.is_connected():
                self.logger.info(f"Connected to {db_type} database for client {self.client_id}")
                return True
            else:
                self.logger.error(f"Failed to connect to {db_type} database for client {self.client_id}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to connect to {db_type} database: {str(e)}")
            return False
    
    def disconnect(self):
        """Safely disconnect from database"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def save_processed_data(self, df: pd.DataFrame, batch_id: str = None) -> Tuple[bool, str]:
        """Save processed DataFrame to client-specific database"""
        if not self.client_id:
            return False, "No client ID specified"
        
        if df is None or len(df) == 0:
            return False, "No data to save"
        
        try:
            # Connect to client main database
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("main")
            
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # Generate batch ID if not provided
            if not batch_id:
                batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Prepare insert query for all 13 original columns plus processing results
            insert_query = """
            INSERT INTO processed_mappings (
                client_id, batch_id,
                vendor_product_description, company_location, vendor_name, vendor_id,
                quantity, stems_bunch, unit_type, staging_id, object_mapping_id,
                company_id, user_id, product_mapping_id, email,
                cleaned_input, applied_synonyms, removed_blacklist_words,
                best_match, similarity_percentage, matched_words, missing_words,
                catalog_id, categoria, variedad, color, grado,
                accept_map, deny_map, action, word
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Column mapping from DataFrame to database fields
            expected_columns = [
                'Vendor Product Description', 'Company Location', 'Vendor Name', 'Vendor ID',
                'Quantity', 'Stems / Bunch', 'Unit Type', 'Staging ID', 'Object Mapping ID',
                'Company ID', 'User ID', 'Product Mapping ID', 'Email',
                'Cleaned input', 'Applied Synonyms', 'Removed Blacklist Words',
                'Best match', 'Similarity %', 'Matched Words', 'Missing Words',
                'Catalog ID', 'Categoria', 'Variedad', 'Color', 'Grado',
                'Accept Map', 'Deny Map', 'Action', 'Word'
            ]
            
            # Prepare data for insertion
            records_inserted = 0
            
            for _, row in df.iterrows():
                record = [self.client_id, batch_id]  # client_id and batch_id first
                
                for col in expected_columns:
                    value = row.get(col, '') if col in df.columns else ''
                    if pd.isna(value) or value is None:
                        record.append('')
                    else:
                        record.append(str(value))
                
                try:
                    cursor.execute(insert_query, tuple(record))
                    records_inserted += 1
                except Exception as e:
                    self.logger.error(f"Error inserting row: {str(e)}")
                    continue
            
            cursor.close()
            connection.close()
            
            success_msg = f"Successfully inserted {records_inserted} records for client {self.client_id}"
            self.logger.info(success_msg)
            return True, success_msg
            
        except Exception as e:
            error_msg = f"Error saving processed data: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def load_processed_data(self) -> Optional[pd.DataFrame]:
        """Load all processed data for current client"""
        if not self.client_id:
            return None
        
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("main")
            
            connection = mysql.connector.connect(**config)
            
            query = """
            SELECT 
                vendor_product_description as 'Vendor Product Description',
                company_location as 'Company Location',
                vendor_name as 'Vendor Name',
                vendor_id as 'Vendor ID',
                quantity as 'Quantity',
                stems_bunch as 'Stems / Bunch',
                unit_type as 'Unit Type',
                staging_id as 'Staging ID',
                object_mapping_id as 'Object Mapping ID',
                company_id as 'Company ID',
                user_id as 'User ID',
                product_mapping_id as 'Product Mapping ID',
                email as 'Email',
                cleaned_input as 'Cleaned input',
                applied_synonyms as 'Applied Synonyms',
                removed_blacklist_words as 'Removed Blacklist Words',
                best_match as 'Best match',
                similarity_percentage as 'Similarity %',
                matched_words as 'Matched Words',
                missing_words as 'Missing Words',
                catalog_id as 'Catalog ID',
                categoria as 'Categoria',
                variedad as 'Variedad',
                color as 'Color',
                grado as 'Grado',
                accept_map as 'Accept Map',
                deny_map as 'Deny Map',
                action as 'Action',
                word as 'Word',
                created_at,
                updated_at
            FROM processed_mappings 
            WHERE client_id = %s
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, connection, params=[self.client_id])
            connection.close()
            
            self.logger.info(f"Loaded {len(df)} records for client {self.client_id}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading processed data: {str(e)}")
            return None
    
    def save_product_to_staging(self, categoria: str, variedad: str, color: str, 
                               grado: str, original_row_id: int, original_input: str,
                               created_by: str = None) -> Tuple[bool, str]:
        """Save a new product to staging products database"""
        try:
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
            
            success_msg = f"Product saved to staging with ID {new_id}"
            self.logger.info(success_msg)
            return True, success_msg
            
        except Exception as e:
            error_msg = f"Error saving product to staging: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def get_staging_products(self) -> Optional[pd.DataFrame]:
        """Get staging products for current client"""
        if not self.client_id:
            return None
        
        try:
            config = self.connection_config.copy()
            config['database'] = self.get_client_database_name("staging_products")
            connection = mysql.connector.connect(**config)
            
            query = """
            SELECT * FROM staging_products_to_create 
            WHERE client_id = %s 
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, connection, params=[self.client_id])
            connection.close()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting staging products: {str(e)}")
            return None
    
    def update_synonyms_blacklist(self, synonym_data: List[Dict], blacklist_data: List[str]) -> Tuple[bool, str]:
        """Update synonyms and blacklist for current client"""
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
            
            success_msg = f"Updated {len(synonym_data)} synonyms and {len(blacklist_data)} blacklist words"
            self.logger.info(success_msg)
            return True, success_msg
            
        except Exception as e:
            error_msg = f"Error updating synonyms/blacklist: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
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


# Convenience functions for easy integration
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
    return db.get_staging_products()

def update_client_synonyms_blacklist(client_id: str, synonyms: List[Dict], 
                                    blacklist: List[str]) -> Tuple[bool, str]:
    """Update client synonyms and blacklist"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.update_synonyms_blacklist(synonyms, blacklist)

def get_client_synonyms_blacklist(client_id: str) -> Dict[str, Any]:
    """Get client synonyms and blacklist"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.get_synonyms_blacklist()

def load_client_processed_data(client_id: str) -> Optional[pd.DataFrame]:
    """Load processed data for specific client"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.load_processed_data()

def save_client_processed_data(client_id: str, df: pd.DataFrame, batch_id: str = None) -> Tuple[bool, str]:
    """Save processed data for specific client"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.save_processed_data(df, batch_id)

def test_client_database_connection(client_id: str = None) -> Tuple[bool, str]:
    """Test database connection for client"""
    db = EnhancedMultiClientDatabase(client_id)
    return db.test_connection()

def get_available_clients() -> List[str]:
    """Get list of available clients by scanning databases"""
    try:
        import mysql.connector
        import os
        
        connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
            'charset': 'utf8mb4',
            'autocommit': True
        }
        
        connection = mysql.connector.connect(**connection_config)
        cursor = connection.cursor()
        
        # Discover client databases by pattern matching
        cursor.execute("SHOW DATABASES LIKE 'mapping_validation_%'")
        databases = cursor.fetchall()
        
        clients = []
        for (db_name,) in databases:
            if db_name.startswith('mapping_validation_'):
                client_id = db_name[len('mapping_validation_'):]
                if client_id and client_id not in ['db', 'test', 'template']:
                    clients.append(client_id)
        
        cursor.close()
        connection.close()
        
        return sorted(list(set(clients)))
        
    except Exception as e:
        print(f"Error discovering clients: {str(e)}")
        return []

def verify_client_database_structure(client_id: str) -> Tuple[bool, Dict[str, str]]:
    """Verify that all required databases exist for a client"""
    try:
        db = EnhancedMultiClientDatabase(client_id)
        
        db_types = ["main", "vendor_staging", "product_catalog", "synonyms_blacklist", "staging_products"]
        results = {}
        all_exist = True
        
        for db_type in db_types:
            try:
                success = db.connect_to_database(db_type)
                if success:
                    results[db_type] = "✅ Connected"
                    db.disconnect()
                else:
                    results[db_type] = "❌ Connection failed"
                    all_exist = False
            except Exception as e:
                results[db_type] = f"❌ Error: {str(e)}"
                all_exist = False
        
        return all_exist, results
        
    except Exception as e:
        return False, {"error": f"Verification failed: {str(e)}"}

# Database maintenance functions
def cleanup_client_data(client_id: str, older_than_days: int = 30) -> Tuple[bool, str]:
    """Clean up old data for a client (older than specified days)"""
    try:
        db = EnhancedMultiClientDatabase(client_id)
        
        config = db.connection_config.copy()
        config['database'] = db.get_client_database_name("main")
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Delete old records
        cleanup_query = """
        DELETE FROM processed_mappings 
        WHERE client_id = %s 
        AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)
        """
        
        cursor.execute(cleanup_query, (client_id, older_than_days))
        deleted_count = cursor.rowcount
        
        cursor.close()
        connection.close()
        
        return True, f"Cleaned up {deleted_count} old records for client {client_id}"
        
    except Exception as e:
        return False, f"Cleanup failed: {str(e)}"

def get_client_statistics(client_id: str) -> Dict[str, Any]:
    """Get comprehensive statistics for a client"""
    try:
        db = EnhancedMultiClientDatabase(client_id)
        
        config = db.connection_config.copy()
        config['database'] = db.get_client_database_name("main")
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)
        
        # Get basic statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN accept_map = 'True' THEN 1 END) as accepted_records,
            COUNT(CASE WHEN deny_map = 'True' THEN 1 END) as denied_records,
            COUNT(CASE WHEN accept_map = '' AND deny_map = '' THEN 1 END) as pending_records,
            AVG(CASE WHEN similarity_percentage != '' AND similarity_percentage IS NOT NULL 
                THEN CAST(similarity_percentage AS DECIMAL(5,2)) END) as avg_similarity,
            MIN(created_at) as oldest_record,
            MAX(created_at) as newest_record,
            COUNT(DISTINCT vendor_name) as unique_vendors,
            COUNT(DISTINCT batch_id) as total_batches
        FROM processed_mappings 
        WHERE client_id = %s
        """
        
        cursor.execute(stats_query, (client_id,))
        stats = cursor.fetchone()
        
        # Get top vendors
        vendors_query = """
        SELECT vendor_name, COUNT(*) as record_count
        FROM processed_mappings 
        WHERE client_id = %s AND vendor_name IS NOT NULL AND vendor_name != ''
        GROUP BY vendor_name
        ORDER BY record_count DESC
        LIMIT 10
        """
        
        cursor.execute(vendors_query, (client_id,))
        top_vendors = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Get staging products count
        staging_df = db.get_staging_products()
        staging_count = len(staging_df) if staging_df is not None else 0
        
        # Get synonyms and blacklist counts
        synonyms_blacklist = db.get_synonyms_blacklist()
        synonyms_count = len(synonyms_blacklist.get('synonyms', {}))
        blacklist_count = len(synonyms_blacklist.get('blacklist', {}).get('input', []))
        
        return {
            'client_id': client_id,
            'main_stats': stats,
            'top_vendors': top_vendors,
            'staging_products_count': staging_count,
            'synonyms_count': synonyms_count,
            'blacklist_count': blacklist_count
        }
        
    except Exception as e:
        return {'error': f"Failed to get statistics: {str(e)}"}

# Export/Import functions
def export_client_configuration(client_id: str) -> Dict[str, Any]:
    """Export client configuration (synonyms, blacklist, etc.)"""
    try:
        db = EnhancedMultiClientDatabase(client_id)
        
        # Get synonyms and blacklist
        synonyms_blacklist = db.get_synonyms_blacklist()
        
        # Get staging products
        staging_df = db.get_staging_products()
        staging_products = staging_df.to_dict('records') if staging_df is not None else []
        
        configuration = {
            'client_id': client_id,
            'export_timestamp': datetime.now().isoformat(),
            'synonyms': synonyms_blacklist.get('synonyms', {}),
            'blacklist': synonyms_blacklist.get('blacklist', {}).get('input', []),
            'staging_products': staging_products
        }
        
        return configuration
        
    except Exception as e:
        return {'error': f"Export failed: {str(e)}"}

def import_client_configuration(client_id: str, configuration: Dict[str, Any]) -> Tuple[bool, str]:
    """Import client configuration"""
    try:
        db = EnhancedMultiClientDatabase(client_id)
        
        # Import synonyms and blacklist
        if 'synonyms' in configuration and 'blacklist' in configuration:
            synonyms_list = [
                {original: replacement} 
                for original, replacement in configuration['synonyms'].items()
            ]
            
            success, message = db.update_synonyms_blacklist(
                synonyms_list, 
                configuration['blacklist']
            )
            
            if not success:
                return False, f"Failed to import synonyms/blacklist: {message}"
        
        # Note: Staging products import would require more complex logic
        # as they reference specific row IDs
        
        return True, f"Successfully imported configuration for client {client_id}"
        
    except Exception as e:
        return False, f"Import failed: {str(e)}"


if __name__ == "__main__":
    # Test the enhanced multi-client database system
    print("🧪 Testing Enhanced Multi-Client Database System...")
    
    # Test connection
    success, message = test_client_database_connection()
    print(f"Connection test: {message}")
    
    if success:
        # Test client discovery
        available_clients = get_available_clients()
        print(f"Available clients: {available_clients}")
        
        # Test creating a new client
        test_client = "test_enhanced_client"
        print(f"\nCreating test client: {test_client}")
        success, message = create_enhanced_client_databases(test_client)
        print(f"Creation result: {message}")
        
        if success:
            # Test database structure verification
            structure_ok, structure_results = verify_client_database_structure(test_client)
            print(f"Structure verification: {structure_ok}")
            for db_type, result in structure_results.items():
                print(f"  {db_type}: {result}")
            
            # Test getting statistics
            stats = get_client_statistics(test_client)
            print(f"Client statistics: {stats}")
    
    print("\n✅ Enhanced Multi-Client Database System testing completed!")