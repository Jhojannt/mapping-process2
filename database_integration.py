# database_integration.py - Enhanced with individual row operations and verification
import mysql.connector
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
import logging
from datetime import datetime
import os
import hashlib
from pathlib import Path

class MappingDatabase:
    """
    Enhanced database handler for individual row operations and verification
    Supports granular control over database insertions and existence checking
    """
    
    def __init__(self):
        # Database configuration - can be overridden by environment variables
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
            'database': os.getenv('DB_NAME', 'mapping_validation_db'),
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
        self.logger = logging.getLogger(__name__)
        
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test database connection and return status with detailed message
        Returns: (success: bool, message: str)
        """
        try:
            # First try to connect without specifying database
            test_config = self.connection_config.copy()
            database_name = test_config.pop('database')
            
            # Test basic MySQL connection
            test_conn = mysql.connector.connect(**test_config)
            
            if not test_conn.is_connected():
                return False, "Failed to establish basic MySQL connection"
            
            cursor = test_conn.cursor()
            
            # Check if database exists
            cursor.execute("SHOW DATABASES LIKE %s", (database_name,))
            db_exists = cursor.fetchone() is not None
            
            if not db_exists:
                return False, f"Database '{database_name}' does not exist. Please create it first."
            
            # Test connection to specific database
            cursor.execute(f"USE {database_name}")
            
            # Check if required table exists
            cursor.execute("SHOW TABLES LIKE 'processed_mappings'")
            table_exists = cursor.fetchone() is not None
            
            cursor.close()
            test_conn.close()
            
            if not table_exists:
                return False, f"Table 'processed_mappings' does not exist in database '{database_name}'. Please run the setup SQL script."
            
            return True, f"Successfully connected to database '{database_name}' with table 'processed_mappings'"
            
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            return False, f"Database connection failed: {error_msg}"
        except Exception as e:
            return False, f"Unexpected error during connection test: {str(e)}"
    
    def connect(self) -> bool:
        """
        Establish connection to MySQL database
        Returns True if successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(**self.connection_config)
            if self.connection.is_connected():
                self.logger.info("Database connection established successfully")
                return True
            else:
                self.logger.error("Database connection failed - connection not active")
                return False
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Database connection failed: {error_msg}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connection: {str(e)}")
            return False
    
    def disconnect(self):
        """Close database connection safely"""
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {str(e)}")
    
    def ensure_connection(self) -> bool:
        """Ensure database connection is active, reconnect if necessary"""
        try:
            if not self.connection or not self.connection.is_connected():
                return self.connect()
            return True
        except Exception as e:
            self.logger.error(f"Error checking connection status: {str(e)}")
            return self.connect()
    
    def generate_row_hash(self, row_data: Dict[str, Any]) -> str:
        """
        Generate a unique hash for a row based on key identifying fields
        Used for duplicate detection and verification
        """
        # Use key fields to create a unique identifier
        key_fields = [
            'vendor_product_description',
            'vendor_name', 
            'cleaned_input',
            'best_match'
        ]
        
        hash_string = ""
        for field in key_fields:
            value = str(row_data.get(field, '')).strip().lower()
            hash_string += value + "|"
        
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    def insert_single_row(self, row_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Insert a single row into the database with duplicate checking
        Returns: (success: bool, message: str)
        """
        if not self.ensure_connection():
            return False, "Failed to establish database connection"
        
        try:
            cursor = self.connection.cursor()
            
            # Check for duplicates first
            row_hash = self.generate_row_hash(row_data)
            
            # Check if similar row already exists
            duplicate_check_query = """
            SELECT id FROM processed_mappings 
            WHERE vendor_product_description = %s 
            AND vendor_name = %s 
            AND cleaned_input = %s
            LIMIT 1
            """
            
            duplicate_params = (
                str(row_data.get('Vendor Product Description', '')),
                str(row_data.get('Vendor Name', '')),
                str(row_data.get('Cleaned input', ''))
            )
            
            cursor.execute(duplicate_check_query, duplicate_params)
            existing_row = cursor.fetchone()
            
            if existing_row:
                cursor.close()
                return False, f"Duplicate row detected. Row ID {existing_row[0]} already exists in database."
            
            # Prepare insert query
            insert_query = """
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
            
            # Column mapping from DataFrame columns to database fields
            expected_columns = [
                'Vendor Product Description', 'Company Location', 'Vendor Name', 'Vendor ID',
                'Quantity', 'Stems / Bunch', 'Unit Type', 'Staging ID', 'Object Mapping ID',
                'Company ID', 'User ID', 'Product Mapping ID', 'Email', 'Cleaned input',
                'Applied Synonyms', 'Removed Blacklist Words', 'Best match', 'Similarity %',
                'Matched Words', 'Missing Words', 'Catalog ID', 'Categoria', 'Variedad',
                'Color', 'Grado', 'Accept Map', 'Deny Map', 'Action', 'Word'
            ]
            
            # Prepare row data
            insert_data = []
            for col in expected_columns:
                value = row_data.get(col, '')
                if pd.isna(value) or value is None:
                    insert_data.append('')
                else:
                    insert_data.append(str(value))
            
            # Execute insert
            cursor.execute(insert_query, tuple(insert_data))
            row_id = cursor.lastrowid
            
            cursor.close()
            
            success_msg = f"Successfully inserted row with ID {row_id}"
            self.logger.info(success_msg)
            return True, success_msg
            
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Single row insertion failed: {error_msg}")
            return False, f"Database insertion failed: {error_msg}"
        except Exception as e:
            error_msg = f"Unexpected error during single row insertion: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def verify_row_exists(self, row_data: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
        """
        Check if a row exists in the database based on key identifying fields
        Returns: (exists: bool, row_id: Optional[int])
        """
        if not self.ensure_connection():
            return False, None
        
        try:
            cursor = self.connection.cursor()
            
            # Search for row using multiple criteria for robust matching
            search_query = """
            SELECT id, created_at FROM processed_mappings 
            WHERE vendor_product_description = %s 
            AND vendor_name = %s 
            AND cleaned_input = %s
            ORDER BY created_at DESC
            LIMIT 1
            """
            
            search_params = (
                str(row_data.get('Vendor Product Description', '')),
                str(row_data.get('Vendor Name', '')),
                str(row_data.get('Cleaned input', ''))
            )
            
            cursor.execute(search_query, search_params)
            result = cursor.fetchone()
            
            cursor.close()
            
            if result:
                row_id, created_at = result
                self.logger.info(f"Row found in database: ID {row_id}, created {created_at}")
                return True, row_id
            else:
                self.logger.info("Row not found in database")
                return False, None
                
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Row verification failed: {error_msg}")
            return False, None
        except Exception as e:
            self.logger.error(f"Unexpected error during row verification: {str(e)}")
            return False, None
    
    def get_row_details(self, row_id: int) -> Optional[Dict[str, Any]]:
        """
        Get complete details of a specific row by ID
        Returns: Dictionary with row data or None if not found
        """
        if not self.ensure_connection():
            return None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            query = "SELECT * FROM processed_mappings WHERE id = %s"
            cursor.execute(query, (row_id,))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting row details: {str(e)}")
            return None
    
    def update_single_row(self, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Update a specific row in the database
        Returns: (success: bool, message: str)
        """
        if not self.ensure_connection():
            return False, "Failed to establish database connection"
        
        try:
            cursor = self.connection.cursor()
            
            # Build dynamic update query
            update_fields = []
            update_values = []
            
            # Allowed update fields (excluding id and timestamps)
            allowed_fields = [
                'accept_map', 'deny_map', 'action', 'word',
                'categoria', 'variedad', 'color', 'grado'
            ]
            
            for field, value in updated_data.items():
                if field in allowed_fields:
                    update_fields.append(f"{field} = %s")
                    update_values.append(str(value) if value is not None else '')
            
            if not update_fields:
                return False, "No valid fields to update"
            
            update_query = f"""
            UPDATE processed_mappings 
            SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP 
            WHERE id = %s
            """
            
            update_values.append(row_id)
            
            cursor.execute(update_query, tuple(update_values))
            affected_rows = cursor.rowcount
            
            cursor.close()
            
            if affected_rows > 0:
                success_msg = f"Successfully updated row ID {row_id}"
                self.logger.info(success_msg)
                return True, success_msg
            else:
                return False, f"No row found with ID {row_id}"
                
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Row update failed: {error_msg}")
            return False, f"Update failed: {error_msg}"
        except Exception as e:
            error_msg = f"Unexpected error during row update: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def delete_single_row(self, row_id: int) -> Tuple[bool, str]:
        """
        Delete a specific row from the database
        Returns: (success: bool, message: str)
        """
        if not self.ensure_connection():
            return False, "Failed to establish database connection"
        
        try:
            cursor = self.connection.cursor()
            
            # First check if row exists
            cursor.execute("SELECT id FROM processed_mappings WHERE id = %s", (row_id,))
            if not cursor.fetchone():
                cursor.close()
                return False, f"Row with ID {row_id} not found"
            
            # Delete the row
            delete_query = "DELETE FROM processed_mappings WHERE id = %s"
            cursor.execute(delete_query, (row_id,))
            
            cursor.close()
            
            success_msg = f"Successfully deleted row ID {row_id}"
            self.logger.info(success_msg)
            return True, success_msg
            
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Row deletion failed: {error_msg}")
            return False, f"Deletion failed: {error_msg}"
        except Exception as e:
            error_msg = f"Unexpected error during row deletion: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def get_table_structure(self) -> Optional[List[Tuple]]:
        """
        Get the complete table structure
        Returns: List of tuples with column information
        """
        if not self.ensure_connection():
            return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("DESCRIBE processed_mappings")
            columns = cursor.fetchall()
            cursor.close()
            return columns
            
        except Exception as e:
            self.logger.error(f"Error getting table structure: {str(e)}")
            return None
    
    def insert_processed_data(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Insert processed DataFrame into database with detailed progress tracking
        Enhanced to handle individual row failures gracefully
        Returns: (success: bool, message: str)
        """
        if not self.ensure_connection():
            return False, "Failed to establish database connection"
        
        if df.empty:
            return False, "DataFrame is empty - nothing to insert"
        
        try:
            cursor = self.connection.cursor()
            
            # Clear existing data (optional - remove if you want to keep historical data)
            cursor.execute("DELETE FROM processed_mappings")
            self.logger.info("Cleared existing data from processed_mappings table")
            
            # SQL insert statement for all columns
            insert_query = """
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
            
            # Column mapping from DataFrame to database fields
            expected_columns = [
                'Vendor Product Description', 'Company Location', 'Vendor Name', 'Vendor ID',
                'Quantity', 'Stems / Bunch', 'Unit Type', 'Staging ID', 'Object Mapping ID',
                'Company ID', 'User ID', 'Product Mapping ID', 'Email', 'Cleaned input',
                'Applied Synonyms', 'Removed Blacklist Words', 'Best match', 'Similarity %',
                'Matched Words', 'Missing Words', 'Catalog ID', 'Categoria', 'Variedad',
                'Color', 'Grado', 'Accept Map', 'Deny Map', 'Action', 'Word'
            ]
            
            # Validate DataFrame structure
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                self.logger.warning(f"Missing columns in DataFrame: {missing_columns}")
            
            # Prepare data for insertion
            records_inserted = 0
            records_failed = 0
            batch_size = 1000
            total_rows = len(df)
            
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_data = []
                
                for _, row in batch_df.iterrows():
                    record = []
                    for col in expected_columns:
                        value = row.get(col, '') if col in df.columns else ''
                        # Convert to string and handle None/NaN values
                        if pd.isna(value) or value is None:
                            record.append('')
                        else:
                            record.append(str(value))
                    
                    batch_data.append(tuple(record))
                
                try:
                    # Execute batch insert
                    cursor.executemany(insert_query, batch_data)
                    records_inserted += len(batch_data)
                except mysql.connector.Error as e:
                    self.logger.error(f"Batch insert failed: {e}")
                    records_failed += len(batch_data)
                
                # Log progress
                progress_pct = ((records_inserted + records_failed) / total_rows) * 100
                self.logger.info(f"Processed: {records_inserted + records_failed}/{total_rows} records ({progress_pct:.1f}%)")
            
            cursor.close()
            
            if records_failed > 0:
                success_msg = f"Inserted {records_inserted} records, {records_failed} failed"
                self.logger.warning(success_msg)
                return True, success_msg
            else:
                success_msg = f"Successfully inserted {records_inserted} records into database"
                self.logger.info(success_msg)
                return True, success_msg
            
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Database insertion failed: {error_msg}")
            return False, f"Database insertion failed: {error_msg}"
        except Exception as e:
            error_msg = f"Unexpected error during insertion: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def get_all_mappings(self) -> Optional[pd.DataFrame]:
        """
        Retrieve all mappings from database as DataFrame with error handling
        """
        if not self.ensure_connection():
            self.logger.error("Failed to establish database connection for data retrieval")
            return None
        
        try:
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
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, self.connection)
            self.logger.info(f"Retrieved {len(df)} records from database")
            return df
            
        except mysql.connector.Error as e:
            error_msg = f"MySQL Error {e.errno}: {e.msg}" if hasattr(e, 'errno') else str(e)
            self.logger.error(f"Database query failed: {error_msg}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error during data retrieval: {str(e)}")
            return None


# Enhanced convenience functions for individual row operations
def insert_single_row_to_database(row_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Insert a single row to the database
    Returns: (success: bool, message: str)
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            success, message = db.insert_single_row(row_data)
            db.disconnect()
            return success, message
        return False, "Failed to connect to database"
    except Exception as e:
        error_msg = f"Error inserting single row: {str(e)}"
        logging.error(error_msg)
        return False, error_msg
    finally:
        db.disconnect()


def verify_row_in_database(row_data: Dict[str, Any]) -> bool:
    """
    Check if a row exists in the database
    Returns: True if exists, False otherwise
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            exists, row_id = db.verify_row_exists(row_data)
            db.disconnect()
            return exists
        return False
    except Exception as e:
        logging.error(f"Error verifying row: {str(e)}")
        return False
    finally:
        db.disconnect()


def get_database_table_structure() -> Optional[List[Tuple]]:
    """
    Get database table structure for display
    Returns: List of column information tuples
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            structure = db.get_table_structure()
            db.disconnect()
            return structure
        return None
    except Exception as e:
        logging.error(f"Error getting table structure: {str(e)}")
        return None
    finally:
        db.disconnect()


# Existing convenience functions with enhanced error handling
def test_database_connection() -> Tuple[bool, str]:
    """
    Test database connection and return detailed status
    Returns: (success: bool, message: str)
    """
    db = MappingDatabase()
    try:
        return db.test_connection()
    except Exception as e:
        return False, f"Error testing database connection: {str(e)}"
    finally:
        db.disconnect()


def save_processed_data_to_database(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Convenience function to save processed DataFrame to database
    Returns: (success: bool, message: str)
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            success, message = db.insert_processed_data(df)
            db.disconnect()
            return success, message
        return False, "Failed to connect to database"
    except Exception as e:
        error_msg = f"Error saving to database: {str(e)}"
        logging.error(error_msg)
        return False, error_msg
    finally:
        db.disconnect()


def load_processed_data_from_database() -> Optional[pd.DataFrame]:
    """
    Convenience function to load all processed data from database
    Returns: DataFrame with all records or None if failed
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            df = db.get_all_mappings()
            db.disconnect()
            return df
        return None
    except Exception as e:
        logging.error(f"Error loading from database: {str(e)}")
        return None
    finally:
        db.disconnect()


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Test database connection with detailed output
    print("Testing database connection...")
    success, message = test_database_connection()
    print(f"Connection test result: {message}")
    
    if success:
        print("Testing individual row operations...")
        
        # Test single row insertion
        test_row = {
            'Vendor Product Description': 'Test Individual Row',
            'Vendor Name': 'Test Vendor Individual',
            'Cleaned input': 'test individual row cleaned',
            'Best match': 'test individual match',
            'Similarity %': '95',
            'Catalog ID': 'TEST001'
        }
        
        print("Testing single row insertion...")
        insert_success, insert_msg = insert_single_row_to_database(test_row)
        print(f"Insert result: {insert_msg}")
        
        if insert_success:
            print("Testing row verification...")
            exists = verify_row_in_database(test_row)
            print(f"Row exists in database: {exists}")
            
            print("Testing table structure retrieval...")
            structure = get_database_table_structure()
            if structure:
                print(f"Table has {len(structure)} columns")
            else:
                print("Failed to retrieve table structure")
        
        print("Individual row operations test completed!")
    else:
        print("Cannot test individual operations without database connection")