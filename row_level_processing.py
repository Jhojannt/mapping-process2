# enhanced_row_level_processing.py - Updated for Enhanced Multi-Client System
"""
Enhanced Row-Level Processing Module

Handles individual row processing, fuzzy matching, and product creation
with full integration to the enhanced multi-client database system.

Key Features:
- Individual row reprocessing with updated synonyms/blacklist
- Combined catalog fuzzy matching (master + staging)
- Direct database updates with client isolation
- Enhanced error handling and logging
- Optimized database operations
"""

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from typing import Dict, Any, Tuple, List, Optional
import logging
import mysql.connector
from datetime import datetime

# Import enhanced database system
from Enhanced_MultiClient_Database import (
    EnhancedMultiClientDatabase,
    get_client_synonyms_blacklist,
    update_client_synonyms_blacklist,
    save_new_product_to_staging
)
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words

class EnhancedRowLevelProcessor:
    """
    Enhanced row processor with full multi-client database integration
    """
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.db = EnhancedMultiClientDatabase(client_id)
        self.logger = logging.getLogger(f"EnhancedRowProcessor_{client_id}")
        
        # Cache for performance optimization
        self._catalog_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes cache TTL
    
    def reprocess_single_row(self, row_data: Dict[str, Any], 
                           update_synonyms_blacklist: bool = True,
                           force_catalog_refresh: bool = False) -> Tuple[bool, Dict[str, Any]]:
        """
        Enhanced row reprocessing with optimized database operations
        
        Args:
            row_data: Dictionary containing row data
            update_synonyms_blacklist: Whether to update synonyms/blacklist before processing
            force_catalog_refresh: Force refresh of catalog cache
            
        Returns:
            (success: bool, updated_row_data: Dict[str, Any])
        """
        try:
            self.logger.info(f"Starting enhanced reprocessing for row ID: {row_data.get('id', 'unknown')}")
            
            # 1. Update synonyms and blacklist if requested
            if update_synonyms_blacklist:
                success = self._update_synonyms_blacklist_from_row(row_data)
                if not success:
                    self.logger.warning("Failed to update synonyms/blacklist, continuing with existing data")
            
            # 2. Get current synonyms and blacklist using enhanced system
            synonyms_blacklist = get_client_synonyms_blacklist(self.client_id)
            
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
            
            # 7. Get combined catalog for fuzzy matching (with caching)
            combined_catalog = self._get_combined_catalog_data(force_catalog_refresh)
            
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
        """Enhanced synonyms/blacklist update using new system"""
        try:
            action = str(row_data.get('Action', '')).strip().lower()
            word = str(row_data.get('Word', '')).strip()
            
            if not action or not word:
                return True  # No action needed
            
            # Get current data using enhanced system
            current_data = get_client_synonyms_blacklist(self.client_id)
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
            
            # Update database using enhanced system
            success, message = update_client_synonyms_blacklist(
                self.client_id, synonyms_list, blacklist_list
            )
            
            if success:
                self.logger.info(f"Updated synonyms/blacklist: {message}")
                # Clear cache to force refresh
                self._catalog_cache = None
            else:
                self.logger.error(f"Failed to update synonyms/blacklist: {message}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error updating synonyms/blacklist from row: {str(e)}")
            return False
    
    def _get_combined_catalog_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get combined catalog data with intelligent caching"""
        try:
            # Check cache validity
            current_time = datetime.now()
            if (not force_refresh and 
                self._catalog_cache is not None and 
                self._cache_timestamp is not None and
                (current_time - self._cache_timestamp).seconds < self._cache_ttl):
                
                self.logger.debug("Using cached catalog data")
                return self._catalog_cache
            
            self.logger.info("Refreshing catalog data from database")
            catalog_data = []
            
            # 1. Get master catalog data
            master_data = self._get_master_catalog_data()
            catalog_data.extend(master_data)
            
            # 2. Get staging catalog data
            staging_data = self._get_staging_catalog_data()
            catalog_data.extend(staging_data)
            
            # Update cache
            self._catalog_cache = catalog_data
            self._cache_timestamp = current_time
            
            self.logger.info(f"Retrieved {len(catalog_data)} total catalog entries ({len(master_data)} master + {len(staging_data)} staging)")
            return catalog_data
            
        except Exception as e:
            self.logger.error(f"Error getting combined catalog data: {str(e)}")
            return self._catalog_cache or []  # Return cached data if available
    
    def _get_master_catalog_data(self) -> List[Dict[str, Any]]:
        """Get data from master product catalog with optimized query"""
        try:
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("product_catalog")
            
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT categoria, variedad, color, grado, catalog_id, search_key
                FROM product_catalog 
                WHERE client_id = %s AND is_active = TRUE
                ORDER BY created_at DESC
            """, (self.client_id,))
            
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Format for fuzzy matching
            formatted_data = []
            for row in results:
                search_key = row.get('search_key')
                if not search_key:
                    search_key = f"{row['categoria']} {row['variedad']} {row['color']} {row['grado']}".strip()
                
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
        """Get data from staging products catalog with status filtering"""
        try:
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("staging_products")
            
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT categoria, variedad, color, grado, catalog_id, search_key, status
                FROM staging_products_to_create 
                WHERE client_id = %s AND status IN ('pending', 'approved')
                ORDER BY created_at DESC
            """, (self.client_id,))
            
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Format for fuzzy matching
            formatted_data = []
            for row in results:
                search_key = row.get('search_key')
                if not search_key:
                    search_key = f"{row['categoria']} {row['variedad']} {row['color']} {row['grado']}".strip()
                
                formatted_data.append({
                    'search_key': clean_text(search_key),
                    'categoria': row['categoria'] or '',
                    'variedad': row['variedad'] or '',
                    'color': row['color'] or '',
                    'grado': row['grado'] or '',
                    'catalog_id': row['catalog_id'] or '111111',  # Always 111111 for staging
                    'source': 'staging',
                    'status': row.get('status', 'pending')
                })
            
            return formatted_data
            
        except Exception as e:
            self.logger.error(f"Error getting staging catalog data: {str(e)}")
            return []
    
    def _perform_fuzzy_matching(self, cleaned_input: str, catalog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Enhanced fuzzy matching with better scoring and fallbacks"""
        try:
            if not cleaned_input.strip():
                return self._empty_match_result()
            
            # Extract search keys for fuzzy matching
            search_keys = [item['search_key'] for item in catalog_data if item['search_key']]
            
            if not search_keys:
                return self._empty_match_result()
            
            # Perform fuzzy matching with multiple algorithms
            best_match, similarity = process.extractOne(
                cleaned_input, search_keys, scorer=fuzz.token_sort_ratio
            )
            
            # Try alternative scoring if similarity is low
            if similarity < 70:
                alt_match, alt_similarity = process.extractOne(
                    cleaned_input, search_keys, scorer=fuzz.partial_ratio
                )
                if alt_similarity > similarity:
                    best_match, similarity = alt_match, alt_similarity
            
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
                'matched_words': ' '.join(sorted(matched_words)),
                'missing_words': ' '.join(sorted(missing_words)),
                'catalog_id': matched_item['catalog_id'],
                'categoria': matched_item['categoria'],
                'variedad': matched_item['variedad'],
                'color': matched_item['color'],
                'grado': matched_item['grado'],
                'source': matched_item['source'],
                'status': matched_item.get('status', 'active')
            }
            
        except Exception as e:
            self.logger.error(f"Error in fuzzy matching: {str(e)}")
            return self._empty_match_result()
    
    def _empty_match_result(self) -> Dict[str, Any]:
        """Return empty match result with consistent structure"""
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
            'source': 'none',
            'status': 'none'
        }
    
    def save_row_as_new_product(self, row_data: Dict[str, Any], 
                               categoria: str, variedad: str, color: str, grado: str,
                               created_by: str = None) -> Tuple[bool, str]:
        """Enhanced product saving using new staging system"""
        try:
            row_id = row_data.get('id', 0) 
            original_input = str(row_data.get('Vendor Product Description', ''))
            
            # Use enhanced staging system
            success, message = save_new_product_to_staging(
                self.client_id,
                categoria, variedad, color, grado,
                row_id, original_input, created_by or 'enhanced_system'
            )
            
            if success:
                self.logger.info(f"Saved new product to staging: {categoria}, {variedad}, {color}, {grado}")
                # Clear catalog cache to include new staging product
                self._catalog_cache = None
            else:
                self.logger.error(f"Failed to save new product: {message}")
            
            return success, message
            
        except Exception as e:
            error_msg = f"Error saving new product: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def update_row_in_database(self, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Enhanced database update with better error handling"""
        try:
            # Connect to main database
            config = self.db.connection_config.copy()
            config['database'] = self.db.get_client_database_name("main")
            
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()
            
            # Build update query for allowed fields
            allowed_fields = {
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
            
            update_fields = []
            update_values = []
            
            for field, value in updated_data.items():
                db_field = allowed_fields.get(field, field.lower().replace(' ', '_'))
                if db_field in allowed_fields.values():
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
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics for the current client"""
        try:
            stats = {
                'client_id': self.client_id,
                'catalog_cache_status': 'active' if self._catalog_cache else 'empty',
                'catalog_entries': len(self._catalog_cache) if self._catalog_cache else 0,
                'cache_age_seconds': (datetime.now() - self._cache_timestamp).seconds if self._cache_timestamp else 0
            }
            
            # Get database statistics
            try:
                from Enhanced_MultiClient_Database import get_client_statistics
                db_stats = get_client_statistics(self.client_id)
                stats.update(db_stats)
            except Exception as e:
                stats['db_error'] = str(e)
            
            return stats
            
        except Exception as e:
            return {'error': f"Failed to get statistics: {str(e)}"}
    
    def clear_cache(self):
        """Clear internal caches"""
        self._catalog_cache = None
        self._cache_timestamp = None
        self.logger.info("Cleared catalog cache")


# Enhanced convenience functions with better error handling
def enhanced_reprocess_row(client_id: str, row_data: Dict[str, Any], 
                          update_synonyms: bool = True,
                          force_catalog_refresh: bool = False) -> Tuple[bool, Dict[str, Any]]:
    """Enhanced convenience function to reprocess a single row"""
    processor = EnhancedRowLevelProcessor(client_id)
    return processor.reprocess_single_row(row_data, update_synonyms, force_catalog_refresh)

def enhanced_save_new_product(client_id: str, row_data: Dict[str, Any], 
                             categoria: str, variedad: str, color: str, grado: str,
                             created_by: str = None) -> Tuple[bool, str]:
    """Enhanced convenience function to save new product"""
    processor = EnhancedRowLevelProcessor(client_id)
    return processor.save_row_as_new_product(row_data, categoria, variedad, color, grado, created_by)

def enhanced_update_row_in_main_db(client_id: str, row_id: int, 
                                  updated_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Enhanced convenience function to update row in main database"""
    processor = EnhancedRowLevelProcessor(client_id)
    return processor.update_row_in_database(row_id, updated_data)

def get_row_processing_stats(client_id: str) -> Dict[str, Any]:
    """Get processing statistics for a client"""
    processor = EnhancedRowLevelProcessor(client_id)
    return processor.get_processing_statistics()

# Backward compatibility - keeping original function names
reprocess_row = enhanced_reprocess_row
save_new_product = enhanced_save_new_product  
update_row_in_main_db = enhanced_update_row_in_main_db


if __name__ == "__main__":
    # Enhanced testing
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Testing Enhanced Row-Level Processing...")
    
    test_client = "bill_doran"  # Using your actual client
    
    # Create test processor
    processor = EnhancedRowLevelProcessor(test_client)
    
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
    
    print(f"\n1. Testing enhanced row reprocessing...")
    success, updated_row = processor.reprocess_single_row(test_row, True)
    print(f"Reprocess result: {success}")
    if success:
        print(f"Updated similarity: {updated_row.get('Similarity %')}")
        print(f"Updated best match: {updated_row.get('Best match')}")
    
    print(f"\n2. Testing enhanced save new product...")
    success, message = processor.save_row_as_new_product(
        test_row, "Custom Category", "Custom Variety", "Custom Color", "Custom Grade", "enhanced_test_user"
    )
    print(f"Save result: {success} - {message}")
    
    print(f"\n3. Testing processing statistics...")
    stats = processor.get_processing_statistics()
    print(f"Stats: {stats}")
    
    print("\n✅ Enhanced row-level processing testing completed!")