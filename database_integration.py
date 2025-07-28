# database_integration.py
import mysql.connector
import pandas as pd
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime

class MappingDatabase:
    """
    Database handler for storing processed mapping validation data
    Connects to local MySQL instance and manages data persistence
    """
    
    def __init__(self):
        self.connection_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Maracuya123',
            'database': 'mapping_validation_db',
            'charset': 'utf8mb4',
            'autocommit': True
        }
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to MySQL database
        Returns True if successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(**self.connection_config)
            logging.info("Database connection established successfully")
            return True
        except mysql.connector.Error as e:
            logging.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Database connection closed")
    
    def insert_processed_data(self, df: pd.DataFrame) -> bool:
        """
        Insert processed DataFrame into database
        Maps CSV columns to database columns
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        try:
            cursor = self.connection.cursor()
            
            # SQL insert statement for all 29 columns
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
            
            # Column mapping from CSV headers to database fields
            column_mapping = {
                'Vendor Product Description': 'vendor_product_description',
                'Company Location': 'company_location', 
                'Vendor Name': 'vendor_name',
                'Vendor ID': 'vendor_id',
                'Quantity': 'quantity',
                'Stems / Bunch': 'stems_bunch',
                'Unit Type': 'unit_type',
                'Staging ID': 'staging_id',
                'Object Mapping ID': 'object_mapping_id',
                'Company ID': 'company_id',
                'User ID': 'user_id',
                'Product Mapping ID': 'product_mapping_id',
                'Email': 'email',
                'Cleaned input': 'cleaned_input',
                'Applied Synonyms': 'applied_synonyms',
                'Removed Blacklist Words': 'removed_blacklist_words',
                'Best match': 'best_match',
                'Similarity %': 'similarity_percentage',
                'Matched Words': 'matched_words',
                'Missing Words': 'missing_words',
                'Catalog ID': 'catalog_id',
                'Categoria': 'categoria',
                'Variedad': 'variedad',
                'Color': 'color',
                'Grado': 'grado',
                'Accept Map': 'accept_map',
                'Deny Map': 'deny_map',
                'Action': 'action',
                'Word': 'word'
            }
            
            # Prepare data for insertion
            records_inserted = 0
            batch_size = 1000
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                batch_data = []
                
                for _, row in batch_df.iterrows():
                    record = []
                    for csv_col in column_mapping.keys():
                        value = row.get(csv_col, '')
                        # Convert to string and handle None values
                        record.append(str(value) if value is not None else '')
                    
                    batch_data.append(tuple(record))
                
                # Execute batch insert
                cursor.executemany(insert_query, batch_data)
                records_inserted += len(batch_data)
                
                logging.info(f"Inserted batch: {records_inserted}/{len(df)} records")
            
            cursor.close()
            logging.info(f"Successfully inserted {records_inserted} records into database")
            return True
            
        except mysql.connector.Error as e:
            logging.error(f"Database insertion failed: {e}")
            return False
    
    def get_all_mappings(self) -> Optional[pd.DataFrame]:
        """
        Retrieve all mappings from database as DataFrame
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return None
        
        try:
            query = """
            SELECT 
                vendor_product_description, company_location, vendor_name, vendor_id,
                quantity, stems_bunch, unit_type, staging_id, object_mapping_id,
                company_id, user_id, product_mapping_id, email, cleaned_input,
                applied_synonyms, removed_blacklist_words, best_match, similarity_percentage,
                matched_words, missing_words, catalog_id, categoria, variedad,
                color, grado, accept_map, deny_map, action, word,
                created_at, updated_at
            FROM processed_mappings 
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, self.connection)
            logging.info(f"Retrieved {len(df)} records from database")
            return df
            
        except mysql.connector.Error as e:
            logging.error(f"Database query failed: {e}")
            return None
    
    def get_mappings_by_criteria(self, 
                                vendor_name: Optional[str] = None,
                                similarity_min: Optional[int] = None,
                                accepted_only: bool = False) -> Optional[pd.DataFrame]:
        """
        Retrieve filtered mappings based on criteria
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return None
        
        try:
            conditions = []
            params = []
            
            if vendor_name:
                conditions.append("vendor_name LIKE %s")
                params.append(f"%{vendor_name}%")
            
            if similarity_min:
                conditions.append("CAST(similarity_percentage AS UNSIGNED) >= %s")
                params.append(similarity_min)
            
            if accepted_only:
                conditions.append("accept_map = %s")
                params.append("True")
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"""
            SELECT * FROM processed_mappings 
            {where_clause}
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, self.connection, params=params)
            logging.info(f"Retrieved {len(df)} filtered records from database")
            return df
            
        except mysql.connector.Error as e:
            logging.error(f"Filtered query failed: {e}")
            return None
    
    def clear_all_data(self) -> bool:
        """
        Clear all data from processed_mappings table
        Use with caution!
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("DELETE FROM processed_mappings")
            affected_rows = cursor.rowcount
            cursor.close()
            
            logging.info(f"Cleared {affected_rows} records from database")
            return True
            
        except mysql.connector.Error as e:
            logging.error(f"Data clearing failed: {e}")
            return False


def save_processed_data_to_database(df: pd.DataFrame) -> bool:
    """
    Convenience function to save processed DataFrame to database
    Usage: save_processed_data_to_database(your_processed_df)
    """
    db = MappingDatabase()
    
    try:
        if db.connect():
            success = db.insert_processed_data(df)
            db.disconnect()
            return success
        return False
    except Exception as e:
        logging.error(f"Error saving to database: {e}")
        return False


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
        logging.error(f"Error loading from database: {e}")
        return None


# Example usage integration with your existing logic.py
def enhanced_process_files_with_database(df1: pd.DataFrame, df2: pd.DataFrame, 
                                       dictionary: dict, save_to_db: bool = True) -> pd.DataFrame:
    """
    Enhanced version of process_files that also saves to database
    """
    # Import your existing processing function
    from logic import process_files
    
    # Process the files using your existing logic
    result_df = process_files(df1, df2, dictionary)
    
    # Save to database if requested
    if save_to_db:
        success = save_processed_data_to_database(result_df)
        if success:
            logging.info("Data successfully saved to database")
        else:
            logging.warning("Failed to save data to database")
    
    return result_df


# Integration example with your existing streamlit app
def integrate_with_streamlit():
    """
    Example of how to integrate database functionality with your Streamlit app
    Add this to your streamlit_app.py
    """
    import streamlit as st
    
    # Add database save option to your export_data function
    def enhanced_export_data():
        if st.session_state.processed_data is not None:
            export_df = st.session_state.processed_data.copy()
            
            # Update DataFrame with form data (your existing logic)
            for idx in export_df.index:
                export_df.loc[idx, 'Accept Map'] = st.session_state.form_data.get(f"accept_{idx}", False)
                export_df.loc[idx, 'Deny Map'] = st.session_state.form_data.get(f"deny_{idx}", False)
                # ... rest of your existing update logic
            
            # New: Save to database option
            col1, col2 = st.columns(2)
            
            with col1:
                # Your existing CSV download
                output = BytesIO()
                export_df.to_csv(output, sep=";", index=False, encoding="utf-8")
                st.download_button(
                    label="📥 Download CSV",
                    data=output.getvalue(),
                    file_name="confirmed_mappings.csv",
                    mime="text/csv"
                )
            
            with col2:
                # New database save button
                if st.button("💾 Save to Database", type="primary"):
                    if save_processed_data_to_database(export_df):
                        st.success("✅ Data saved to database successfully!")
                    else:
                        st.error("❌ Failed to save to database")


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Test database connection
    db = MappingDatabase()
    if db.connect():
        print("Database connection test successful!")
        db.disconnect()
    else:
        print("Database connection test failed!")
        
    # Example: Load sample data and test insertion
    # Uncomment these lines to test with sample data
    """
    import pandas as pd
    
    # Create sample data matching your CSV structure
    sample_data = {
        'Vendor Product Description': ['Sample Product 1', 'Sample Product 2'],
        'Company Location': ['Location 1', 'Location 2'],
        'Vendor Name': ['Vendor A', 'Vendor B'],
        # ... add all 29 columns with sample data
    }
    
    sample_df = pd.DataFrame(sample_data)
    
    # Test saving to database
    if save_processed_data_to_database(sample_df):
        print("Sample data saved successfully!")
        
        # Test loading from database
        loaded_df = load_processed_data_from_database()
        if loaded_df is not None:
            print(f"Loaded {len(loaded_df)} records from database")
    """