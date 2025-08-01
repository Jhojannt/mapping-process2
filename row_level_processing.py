# row_level_processing.py - Individual row processing and fuzzy matching
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from typing import Dict, Any, Tuple, List
import logging
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words
from Enhanced_MultiClient_Database import EnhancedMultiClientDatabase

class RowLevelProcessor:
    """
    Handles individual row processing, fuzzy matching, and product creation
    """
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.db = EnhancedMultiClientDatabase(client_id)
        self.logger = logging.getLogger(f"RowProcessor_{client_id}")
    
    def reprocess_single_row(self, row_data: Dict[str, Any], 
                           update_synonyms_blacklist: bool = True) -> Tuple[bool, Dict[str, Any]]:
        """
        Re-process a single row with updated synonyms/blacklist and combined catalog
        
        Args:
            row_data: Dictionary containing row data
            update_synonyms_blacklist: Whether to update synonyms/blacklist before processing
            
        Returns:
            (success: bool, updated_row_data: Dict[str, Any])
        """
        try:
            self.logger.info(f"Starting reprocessing for row ID: {row_data.get('id', 'unknown')}")
            
            # 1. Update synonyms and blacklist if requested
            if update_synonyms_blacklist:
                success = self._update_synonyms_blacklist_from_row(row_data)
                if not success:
                    self.logger.warning("Failed to update synonyms/blacklist, continuing with existing data")
            
            # 2. Get current synonyms and blacklist
            synonyms_blacklist = self.db.get_synonyms_blacklist()
            
            # 3. Get original input
            original_input = str(row_data.get('Vendor Product Description', ''))
            if not original_input.strip():
                return False, row_data
            
            # 4. Clean the input
            cleaned_input = clean_text(original_input)
            
            # 5. Apply synonyms
            cleaned_with_synonyms, applied_synonyms = apply_synonyms(
                cleaned_input, synonyms_blacklist.get('synonyms', {})
            )
            
            # 6. Apply blacklist
            final_cleaned, removed_blacklist = remove_blacklist(
                cleaned_with_synonyms, synonyms_blacklist.get('blacklist', {}).get('input', [])
            )
            
            # 7. Get combined catalog for fuzzy matching
            combined_catalog = self._get_combined_catalog_data()
            
            if not combined_catalog:
                self.logger.warning("No catalog data available for fuzzy matching")
                return False, row_data
            
            # 8. Perform fuzzy matching
            match_result = self._perform_fuzzy_matching(final_cleaned, combined_catalog)
            
            # 9. Update row data with new results
            updated_row = row_data.copy()
            updated_row.update({
                'Cleaned input': final_cleaned,
                'Applied Synonyms': ", ".join([f"{o}→{n}" for o, n in applied_synonyms]),
                'Removed Blacklist Words': " ".join(removed_blacklist),
                'Best match': match_result['best_match'],
                'Similarity %': match_result['similarity'],
                'Matched Words': match_result['matched_words'],
                'Missing Words': match_result['missing_words'],
                'Catalog ID': match_result['catalog_id'],
                'Categoria': match_result['categoria'],
                'Variedad': match_result['variedad'],
                'Color': match_result['color'],
                'Grado': match_result['grado'],
                'updated_at': pd.Timestamp.now()
            })
            
            self.logger.info(f"Successfully reprocessed row with {match_result['similarity']}% similarity")
            return True, updated_row
            
        except Exception as e:
            self.logger.error(f"Error reprocessing row: {str(e)}")
            return False, row_data
    
    def _update_synonyms_blacklist_from_row(self, row_data: Dict[str, Any]) -> bool:
        """Update synonyms and blacklist based on row action and word fields"""
        try:
            action = str(row_data.get('Action', '')).strip().lower()
            word = str(row_data.get('Word', '')).strip()
            
            if not action or not word:
                return True  # No action needed
            
            # Get current synonyms and blacklist
            current_data = self.db.get_synonyms_blacklist()
            synonyms_list = []
            blacklist_list = list(current_data.get('blacklist', {}).get('input', []))
            
            # Convert current synonyms dict to list of dicts
            for orig, repl in current_data.get('synonyms', {}).items():
                synonyms_list.append({orig: repl})
            
            # Process based on action
            if action == 'synonym':
                # Expected format: "original_word":"replacement_word"
                if ':' in word:
                    parts = word.split(':', 1)
                    original = parts[0].strip().strip('"')
                    replacement = parts[1].strip().strip('"')
                    
                    if original and replacement:
                        # Add new synonym (remove existing if present)
                        synonyms_list = [s for s in synonyms_list if list(s.keys())[0] != original]
                        synonyms_list.append({original: replacement})
                        self.logger.info(f"Added synonym: {original} → {replacement}")
            
            elif action == 'blacklist':
                # Add word to blacklist if not already present
                if word not in blacklist_list:
                    blacklist_list.append(word)
                    self.logger.info(f"Added to blacklist: {word}")
            
            # Update database
            success, message = self.db.update_synonyms_blacklist(synonyms_list, blacklist_list)
            if success:
                self.logger.info(f"Updated synonyms/blacklist: {message}")
            else:
                self.logger.error(f"Failed to update synonyms/blacklist: {message}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating synonyms/blacklist from row: {str(e)}")
            return False
    
    def _get_combined_catalog_data(self) -> List[Dict[str, Any]]:
        """Get combined catalog data from both master and staging databases"""
        try:
            catalog_data = []
            
            # 1. Get master catalog data
            master_data = self._get_master_catalog_data()
            catalog_data.extend(master_data)
            
            # 2. Get staging catalog data
            staging_data = self._get_staging_catalog_data()
            catalog_data.extend(staging_data)
            
            self.logger.info(f"Retrieved {len(catalog_data)} total catalog entries ({len(master_data)} master + {len(staging_data)} staging)")
            return catalog_data
            
        except Exception as e:
            self.logger.error(f"Error getting combined catalog data: {str(e)}")
            return []
    
    def _get_master_catalog_data(self) -> List[Dict[str, Any]]:
        """Get data from master product catalog"""
        try:
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("product_catalog")
            
            import mysql.connector
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT categoria, variedad, color, grado, catalog_id, search_key
                FROM product_catalog 
                WHERE client_id = %s
            """, (self.client_id,))
            
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Format for fuzzy matching
            formatted_data = []
            for row in results:
                search_key = row.get('search_key') or f"{row['categoria']} {row['variedad']} {row['color']} {row['grado']}".strip()
                formatted_data.append({
                    'search_key': clean_text(search_key),
                    'categoria': row['categoria'] or '',
                    'variedad': row['variedad'] or '',
                    'color': row['color'] or '',
                    'grado': row['grado'] or '',
                    'catalog_id': row['catalog_id'] or '',
                    'source': 'master'
                })
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Error getting master catalog data: {str(e)}")
            return []
    
    def _get_staging_catalog_data(self) -> List[Dict[str, Any]]:
        """Get data from staging products catalog"""
        try:
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("staging_products")
            
            import mysql.connector
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT categoria, variedad, color, grado, catalog_id, search_key
                FROM staging_products_to_create 
                WHERE client_id = %s AND status = 'pending'
            """, (self.client_id,))
            
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Format for fuzzy matching
            formatted_data = []
            for row in results:
                search_key = row.get('search_key') or f"{row['categoria']} {row['variedad']} {row['color']} {row['grado']}".strip()
                formatted_data.append({
                    'search_key': clean_text(search_key),
                    'categoria': row['categoria'] or '',
                    'variedad': row['variedad'] or '',
                    'color': row['color'] or '',
                    'grado': row['grado'] or '',
                    'catalog_id': row['catalog_id'] or '111111',  # Always 111111 for staging
                    'source': 'staging'
                })
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Error getting staging catalog data: {str(e)}")
            return []
    
    def _perform_fuzzy_matching(self, cleaned_input: str, catalog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Perform fuzzy matching against combined catalog"""
        try:
            if not cleaned_input.strip():
                return self._empty_match_result()
            
            # Extract search keys for fuzzy matching
            search_keys = [item['search_key'] for item in catalog_data if item['search_key']]
            
            if not search_keys:
                return self._empty_match_result()
            
            # Perform fuzzy matching
            best_match, similarity = process.extractOne(
                cleaned_input, search_keys, scorer=fuzz.token_sort_ratio
            )
            
            # Find the corresponding catalog item
            matched_item = None
            for item in catalog_data:
                if item['search_key'] == best_match:
                    matched_item = item
                    break
            
            if not matched_item:
                return self._empty_match_result()
            
            # Calculate matched and missing words
            input_words = set(extract_words(cleaned_input))
            match_words = set(extract_words(best_match))
            matched_words = input_words.intersection(match_words)
            missing_words = input_words.difference(match_words)
            
            return {
                'best_match': best_match,
                'similarity': similarity,
                'matched_words': ' '.join(matched_words),
                'missing_words': ' '.join(missing_words),
                'catalog_id': matched_item['catalog_id'],
                'categoria': matched_item['categoria'],
                'variedad': matched_item['variedad'],
                'color': matched_item['color'],
                'grado': matched_item['grado'],
                'source': matched_item['source']
            }
            
        except Exception as e:
            self.logger.error(f"Error in fuzzy matching: {str(e)}")
            return self._empty_match_result()
    
    def _empty_match_result(self) -> Dict[str, Any]:
        """Return empty match result"""
        return {
            'best_match': '',
            'similarity': 0,
            'matched_words': '',
            'missing_words': '',
            'catalog_id': '',
            'categoria': '',
            'variedad': '',
            'color': '',
            'grado': '',
            'source': 'none'
        }
    
    def save_row_as_new_product(self, row_data: Dict[str, Any], 
                               categoria: str, variedad: str, color: str, grado: str,
                               created_by: str = None) -> Tuple[bool, str]:
        """Save modified row data as new product in staging"""
        try:
            row_id = row_data.get('id', 0)
            original_input = str(row_data.get('Vendor Product Description', ''))
            
            success, message = self.db.save_product_to_staging(
                categoria=categoria,
                variedad=variedad,
                color=color,
                grado=grado,
                original_row_id=row_id,
                original_input=original_input,
                created_by=created_by or 'system'
            )
            
            if success:
                self.logger.info(f"Saved new product to staging: {categoria}, {variedad}, {color}, {grado}")
            else:
                self.logger.error(f"Failed to save new product: {message}")
            
            return success, message
            
        except Exception as e:
            error_msg = f"Error saving new product: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def update_row_in_database(self, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Update a specific row in the main mapping database"""
        try:
            # Connect to main database
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("main")
            
            import mysql.connector
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # Build update query for allowed fields
            allowed_fields = [
                'cleaned_input', 'applied_synonyms', 'removed_blacklist_words',
                'best_match', 'similarity_percentage', 'matched_words', 'missing_words',
                'catalog_id', 'categoria', 'variedad', 'color', 'grado',
                'accept_map', 'action', 'word'
            ]
            
            update_fields = []
            update_values = []
            
            for field, value in updated_data.items():
                # Convert field names to database column names
                db_field = self._convert_field_name(field)
                if db_field in allowed_fields:
                    update_fields.append(f"{db_field} = %s")
                    update_values.append(str(value) if value is not None else '')
            
            if not update_fields:
                return False, "No valid fields to update"
            
            # Add updated timestamp
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            update_query = f"""
                UPDATE processed_mappings 
                SET {', '.join(update_fields)}
                WHERE id = %s AND client_id = %s
            """
            
            update_values.extend([row_id, self.client_id])
            
            cursor.execute(update_query, tuple(update_values))
            affected_rows = cursor.rowcount
            
            cursor.close()
            connection.close()
            
            if affected_rows > 0:
                success_msg = f"Successfully updated row ID {row_id}"
                self.logger.info(success_msg)
                return True, success_msg
            else:
                return False, f"No row found with ID {row_id} for client {self.client_id}"
                
        except Exception as e:
            error_msg = f"Error updating row in database: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def _convert_field_name(self, field_name: str) -> str:
        """Convert display field names to database column names"""
        field_mapping = {
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
            'Action': 'action',
            'Word': 'word'
        }
        
        return field_mapping.get(field_name, field_name.lower().replace(' ', '_'))


# Convenience functions for easy integration
def reprocess_row(client_id: str, row_data: Dict[str, Any], 
                 update_synonyms: bool = True) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function to reprocess a single row"""
    processor = RowLevelProcessor(client_id)
    return processor.reprocess_single_row(row_data, update_synonyms)

def save_new_product(client_id: str, row_data: Dict[str, Any], 
                    categoria: str, variedad: str, color: str, grado: str,
                    created_by: str = None) -> Tuple[bool, str]:
    """Convenience function to save new product"""
    processor = RowLevelProcessor(client_id)
    return processor.save_row_as_new_product(row_data, categoria, variedad, color, grado, created_by)

def update_row_in_main_db(client_id: str, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Convenience function to update row in main database"""
    processor = RowLevelProcessor(client_id)
    return processor.update_row_in_database(row_id, updated_data)


if __name__ == "__main__":
    # Test row-level processing
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Testing Row-Level Processing...")
    
    test_client = "test_row_client"
    
    # Create test processor
    processor = RowLevelProcessor(test_client)
    
    # Test row data
    test_row = {
        'id': 1,
        'Vendor Product Description': 'red roses premium grade',
        'Vendor Name': 'Test Vendor',
        'Cleaned input': 'red roses premium grade',
        'Best match': 'roses red premium',
        'Similarity %': 85,
        'Categoria': 'Flowers',
        'Variedad': 'Roses',
        'Color': 'Red',
        'Grado': 'Premium',
        'Action': 'synonym',
        'Word': '"premium":"high quality"'
    }
    
    print(f"\n1. Testing row reprocessing...")
    success, updated_row = processor.reprocess_single_row(test_row, True)
    print(f"Reprocess result: {success}")
    if success:
        print(f"Updated similarity: {updated_row.get('Similarity %')}")
        print(f"Updated best match: {updated_row.get('Best match')}")
    
    print(f"\n2. Testing save new product...")
    success, message = processor.save_row_as_new_product(
        test_row, "Custom Category", "Custom Variety", "Custom Color", "Custom Grade", "test_user"
    )
    print(f"Save result: {success} - {message}")
    
    print("\n✅ Row-level processing testing completed!")