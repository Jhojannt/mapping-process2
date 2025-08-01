# streamlit_app.py - Enhanced Multi-Client Data Mapping Validation System
"""
Enhanced Multi-Client Streamlit Application

A comprehensive data mapping validation system that orchestrates the symphony of:
- Multi-client database architecture with complete isolation
- Row-level processing with dynamic fuzzy matching
- Staging products creation and management
- Real-time synonyms and blacklist management
- Advanced filtering and exclusion capabilities
- Bulk operations with intelligent progress tracking

This application serves as the conductor's baton, guiding the harmonious 
interaction between user interface and sophisticated backend processing.
"""

import streamlit as st
import pandas as pd
import json
from io import BytesIO
import time
import logging
import asyncio
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from pathlib import Path

# Import enhanced backend modules
from logic import process_files
from ulits import classify_missing_words
from storage import save_output_to_disk, load_output_from_disk
from Enhanced_MultiClient_Database import (
    EnhancedMultiClientDatabase,
    create_enhanced_client_databases,
    get_client_staging_products,
    update_client_synonyms_blacklist,
    get_client_synonyms_blacklist,
    load_client_processed_data,  # Nueva función
    test_client_database_connection  # Nueva función
)
from row_level_processing import (
    RowLevelProcessor,
    reprocess_row,
    save_new_product,
    update_row_in_main_db
)

# Configure logging with poetic precision
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Streamlit page with aesthetic sensibility
st.set_page_config(
    page_title="Enhanced Multi-Client Data Mapping Validation",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

def initialize_session_state():
    """Initialize the symphony of session state variables with robust defaults"""
    session_vars = {
        # Client orchestration
        'current_client_id': None,
        'available_clients': [],
        'client_batches': [],
        'selected_batch': None,
        'show_client_setup': False,
        'new_client_id': '',
        
        # Data choreography
        'processed_data': None,
        'form_data': {},
        'dark_mode': False,
        'db_connection_status': None,
        
        # Enhanced modal symphonies
        'show_edit_product_modal': False,
        'edit_product_row_data': None,
        'edit_product_row_index': None,
        'edit_categoria': '',
        'edit_variedad': '',
        'edit_color': '',
        'edit_grado': '',
        'edit_action': '',
        'edit_word': '',
        
        # Confirmation modal for database operations
        'show_confirmation_modal': False,
        'row_to_insert': None,
        'show_db_columns': False,
        'inserted_rows': set(),
        'verification_results': {},
        'pending_insert_row': None,
        'pending_insert_data': None,
        
        # Progress tracking rhythms
        'reviewed_count': 0,
        'total_rows': 0,
        'show_progress': True,
        'progress_percentage': 0.0,
        'save_progress': 0,
        
        # File processing crescendos
        'uploaded_files': {},
        'processing_status': 'ready',
        
        # Filtering melodies
        'current_page': 1,
        'search_text': '',
        'similarity_range': (1, 100),
        'exclusion_filters': {},
        'selected_exclusion_column': 'None',
        'exclusion_filter_value': '',
        'rows_per_page': 50,
        'filter_column': 'None',
        'filter_column_index': 0,
        'filter_value': '',
        
        # Bulk operations orchestrations
        'show_bulk_save_modal': False,
        'bulk_save_progress': 0,
        'bulk_save_status': 'ready',
        'bulk_save_current_batch': 0,
        'bulk_save_total_batches': 0,
        'bulk_save_success_count': 0,
        'bulk_save_failed_count': 0,
        'bulk_save_results': [],
        'bulk_save_in_progress': False,
        
        # Message harmonics
        'success_message': '',
        'error_message': '',
        'info_message': '',
        'warning_message': ''
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

# Initialize session state
initialize_session_state()

def load_available_clients() -> List[str]:
    """Discover the chorus of available clients in the database constellation"""
    try:
        # Connect to base configuration to discover client databases
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
                if client_id and client_id not in ['db', 'test']:  # Exclude base databases
                    clients.append(client_id)
        
        cursor.close()
        connection.close()
        
        # Sort clients alphabetically for consistent presentation
        clients = sorted(list(set(clients)))
        st.session_state.available_clients = clients
        
        logger.info(f"Discovered {len(clients)} clients: {clients}")
        return clients
        
    except Exception as e:
        logger.error(f"Error discovering clients: {str(e)}")
        # Fallback to demo clients if database discovery fails
        demo_clients = ["demo_client", "test_company", "sample_client"]
        st.session_state.available_clients = demo_clients
        return demo_clients

def check_database_connection():
    """Test database connection using enhanced multi-client system"""
    try:
        success, message = test_client_database_connection()
        if success:
            st.session_state.db_connection_status = "connected"
            return True
        else:
            st.session_state.db_connection_status = f"failed: {message}"
            return False
    except Exception as e:
        st.session_state.db_connection_status = f"error: {str(e)}"
        return False

def database_status_widget():
    """Display database connection status widget with enhanced styling"""
    if st.session_state.db_connection_status is None:
        status_class = "background: linear-gradient(135deg, #ffc10722, #fd7e1422); border: 1px solid #ffc107; color: #ffc107;"
        status_text = "Database Status: Not Tested"
        icon = "🔍"
    elif st.session_state.db_connection_status == "connected":
        status_class = "background: linear-gradient(135deg, #28a74522, #20c99722); border: 1px solid #28a745; color: #28a745;"
        status_text = "Database Status: Connected"
        icon = "✅"
    elif st.session_state.db_connection_status.startswith("failed"):
        status_class = "background: linear-gradient(135deg, #dc354522, #c8211022); border: 1px solid #dc3545; color: #dc3545;"
        status_text = "Database Status: Connection Failed"
        icon = "❌"
    else:
        status_class = "background: linear-gradient(135deg, #dc354522, #c8211022); border: 1px solid #dc3545; color: #dc3545;"
        status_text = f"Database Status: {st.session_state.db_connection_status}"
        icon = "⚠️"
    
    st.sidebar.markdown(
        f"""
        <div style="padding: 0.5rem; border-radius: 6px; margin-bottom: 1rem; font-weight: bold; text-align: center; transition: all 0.3s ease; {status_class}">
            {icon} {status_text}
        </div>
        """,
        unsafe_allow_html=True
    )

def create_liquid_progress_bar(progress, text="Processing..."):
    """Create animated liquid gradient progress bar with enhanced visual effects"""
    progress_html = f"""
    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #2596be22, #ff7f0022); border-radius: 15px; border: 2px solid #2596be; margin: 20px 0;">
        <h3>🌊 {text}</h3>
        <div class="liquid-progress">
            <div class="liquid-fill" style="width: {progress}%"></div>
            <div class="progress-text">{progress:.1f}%</div>
        </div>
    </div>
    """
    return progress_html

def apply_custom_css():
    """Apply the aesthetic foundation with carefully crafted styles"""
    theme = "dark" if st.session_state.dark_mode else "light"
    
    css = f"""
    <style>
    /* Main container styling */
    .main-header {{
        text-align: center;
        padding: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }}
    
    /* Liquid gradient progress bar */
    .liquid-progress {{
        width: 100%;
        height: 40px;
        background: linear-gradient(45deg, #1a1a1a, #2a2a2a);
        border-radius: 20px;
        overflow: hidden;
        position: relative;
        box-shadow: inset 0 2px 10px rgba(0,0,0,0.3);
    }}
    
    .liquid-fill {{
        height: 100%;
        background: linear-gradient(45deg, #2596be, #ff7f00, #2596be, #ff7f00);
        background-size: 400% 400%;
        animation: liquid-flow 3s ease-in-out infinite;
        border-radius: 20px;
        position: relative;
        overflow: hidden;
        transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
    }}
    
    .liquid-fill::before {{
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
        animation: liquid-shine 2s linear infinite;
    }}
    
    .progress-text {{
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        color: white;
        font-weight: bold;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.7);
        z-index: 10;
    }}
    
    @keyframes liquid-flow {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}
    
    @keyframes liquid-shine {{
        0% {{ left: -100%; }}
        100% {{ left: 100%; }}
    }}
    
    /* Enhanced modal styling */
    .edit-modal-container {{
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border: 2px solid #007bff;
        border-radius: 15px;
        padding: 25px;
        margin: 20px 0;
        box-shadow: 0 8px 25px rgba(0, 123, 255, 0.15);
        animation: modal-fade-in 0.3s ease-out;
    }}
    
    @keyframes modal-fade-in {{
        from {{ opacity: 0; transform: translateY(-20px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    
    /* Progress container styling */
    .progress-container {{
        background: {'#1e1e1e' if theme == 'dark' else '#f8f9fa'};
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border-left: 4px solid #28a745;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }}
    
    /* Enhanced button styling */
    .stButton > button {{
        border-radius: 10px;
        font-weight: bold;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        border: none;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }}
    
    .stButton > button:hover {{
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.2);
    }}
    
    /* Inline table row styling */
    .inline-row {{
        border-bottom: 1px solid #eee;
        padding: 8px 0;
        transition: all 0.2s ease;
    }}
    
    .inline-row:hover {{
        background-color: rgba(0, 123, 255, 0.05);
        transform: translateX(2px);
    }}
    
    /* Highlight cells */
    .highlight-cell {{
        background: linear-gradient(135deg, #fffec6, #fff9c4) !important;
        border: 1px solid #ffc107;
        border-radius: 6px;
        padding: 8px;
        font-weight: bold;
        box-shadow: 0 2px 4px rgba(255, 193, 7, 0.2);
    }}
    
    /* Status indicators */
    .status-indicator {{
        display: inline-block;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: bold;
        margin: 2px;
        animation: status-pulse 2s ease-in-out infinite;
    }}
    
    @keyframes status-pulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.7; }}
    }}
    
    .status-inserted {{
        background: linear-gradient(135deg, #28a745, #20c997);
        color: white;
    }}
    
    .status-verified {{
        background: linear-gradient(135deg, #17a2b8, #138496);
        color: white;
    }}
    
    /* Bulk action containers */
    .bulk-action-container {{
        background: linear-gradient(135deg, #f1f3f4, #e8eaed);
        border: 2px solid #1976d2;
        border-radius: 12px;
        padding: 20px;
        margin: 20px 0;
        box-shadow: 0 4px 12px rgba(25, 118, 210, 0.15);
    }}
    
    .bulk-action-header {{
        text-align: center;
        color: #1976d2;
        font-weight: bold;
        margin-bottom: 15px;
        font-size: 1.1em;
    }}
    
    /* Bulk save container */
    .bulk-save-container {{
        background: linear-gradient(135deg, #e8f5e8, #f0fff0);
        border: 3px solid #28a745;
        border-radius: 15px;
        padding: 25px;
        margin: 25px 0;
        box-shadow: 0 6px 20px rgba(40, 167, 69, 0.2);
        text-align: center;
    }}
    
    .bulk-save-header {{
        color: #28a745;
        font-weight: bold;
        font-size: 1.3em;
        margin-bottom: 15px;
    }}
    
    /* Responsive design */
    @media (max-width: 768px) {{
        .main-header {{ padding: 1rem; }}
        .edit-modal-container {{ padding: 15px; margin: 10px 0; }}
        .bulk-action-container {{ padding: 15px; margin: 15px 0; }}
        .bulk-save-container {{ padding: 20px; margin: 20px 0; }}
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def load_processed_data_from_database():
    """Load processed data from the enhanced multi-client database based on current client"""
    if not st.session_state.current_client_id:
        return None
    
    try:
        # Use the enhanced multi-client database system
        db = EnhancedMultiClientDatabase(st.session_state.current_client_id)
        
        # Connect to the client-specific main database
        if not db.connect_to_database("main"):
            logger.error(f"Failed to connect to main database for client {st.session_state.current_client_id}")
            return None
        
        # Query the client-specific processed_mappings table
        import mysql.connector
        config = db.connection_config.copy()
        config['database'] = db.get_client_database_name("main")
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)
        
        # Get all processed mappings for this client
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
        
        cursor.execute(query, (st.session_state.current_client_id,))
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        if results:
            df = pd.DataFrame(results)
            logger.info(f"Retrieved {len(df)} records from database for client {st.session_state.current_client_id}")
            return df
        else:
            logger.info(f"No records found for client {st.session_state.current_client_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error loading data for client {st.session_state.current_client_id}: {str(e)}")
        return None

def safe_float_conversion(series, default_value=0):
    """Safely convert pandas series to float with proper error handling"""
    try:
        # Replace empty strings and None values with default
        cleaned_series = series.fillna(default_value)
        cleaned_series = cleaned_series.replace('', default_value)
        # Convert to numeric, coercing errors to NaN, then fill NaN with default
        return pd.to_numeric(cleaned_series, errors='coerce').fillna(default_value)
    except Exception as e:
        logger.error(f"Error in safe float conversion: {str(e)}")
        # Return series of default values with same index
        return pd.Series([default_value] * len(series), index=series.index)


def sidebar_controls():
    """Enhanced sidebar with all controls and database integration"""
    st.sidebar.header("📁 File Upload & Database")
    
    # Database status section
    st.sidebar.markdown("### 🗄️ Database Connection")
    database_status_widget()
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔍 Test Connection", use_container_width=True):
            with st.spinner("Testing database connection..."):
                check_database_connection()
            st.rerun()
    
    with col2:
        if st.button("📊 Load from DB", use_container_width=True):
            if st.session_state.current_client_id:
                try:
                    with st.spinner(f"Loading data for client {st.session_state.current_client_id}..."):
                        # Usar la función específica del cliente
                        db_data = load_processed_data_from_database()
                        if db_data is not None and len(db_data) > 0:
                            st.session_state.processed_data = db_data
                            st.session_state.total_rows = len(db_data)
                            st.session_state.db_connection_status = "connected"
                            st.sidebar.success(f"✅ Loaded {len(db_data)} records for {st.session_state.current_client_id}")
                            st.rerun()
                        else:
                            st.sidebar.warning(f"⚠️ No data found for client {st.session_state.current_client_id}")
                except Exception as e:
                    st.sidebar.error(f"❌ Error loading from database: {str(e)}")
                    st.sidebar.info("💡 Try creating databases for this client first")
            else:
                st.sidebar.error("❌ Please select a client first")
        
    st.sidebar.divider()
    
    # Client selection
    st.sidebar.header("🏢 Client Management")
    
    available_clients = load_available_clients()
    
    if available_clients:
        client_options = ["-- Select Client --"] + available_clients
        current_index = 0
        
        if st.session_state.current_client_id in available_clients:
            current_index = available_clients.index(st.session_state.current_client_id) + 1
        
        selected_client = st.sidebar.selectbox(
            "Select Client:",
            client_options,
            index=current_index,
            key="client_selector"
        )
        
        if selected_client != "-- Select Client --":
            if st.session_state.current_client_id != selected_client:
                st.session_state.current_client_id = selected_client
                st.session_state.processed_data = None
                st.session_state.form_data = {}
                st.session_state.exclusion_filters = {}
                st.rerun()
        else:
            st.session_state.current_client_id = None
    
    # Client management buttons
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("➕ New Client", use_container_width=True):
            st.session_state.show_client_setup = True
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            load_available_clients()
            st.rerun()
    
    # Display current client status
    if st.session_state.current_client_id:
        st.sidebar.success(f"📋 Active: **{st.session_state.current_client_id}**")
    else:
        st.sidebar.info("Please select a client first")
        return "", 1, 100, "None", ""
    
    st.sidebar.divider()
    
    # File upload section
    st.sidebar.markdown("**Required Files:**")
    file1 = st.sidebar.file_uploader("📄 Main TSV File", type=['tsv'], key="file1")
    file2 = st.sidebar.file_uploader("📊 Catalog TSV File", type=['tsv'], key="file2")
    dictionary = st.sidebar.file_uploader("📝 Dictionary JSON", type=['json'], key="dict")
    
    if file1 and file2 and dictionary:
        if st.sidebar.button("🚀 Process Files", type="primary", use_container_width=True):
            try:
                df1 = pd.read_csv(file1, delimiter="\t", dtype=str)
                df2 = pd.read_csv(file2, delimiter="\t", dtype=str)
                dict_data = json.load(dictionary)
                
                progress_container = st.empty()
                
                def progress_callback(progress_pct, message):
                    progress_container.markdown(
                        create_liquid_progress_bar(progress_pct, message), 
                        unsafe_allow_html=True
                    )
                
                result_df = process_files(df1, df2, dict_data, progress_callback)
                st.session_state.processed_data = result_df
                st.session_state.total_rows = len(result_df)
                
                output = BytesIO()
                result_df.to_csv(output, sep=";", index=False, encoding="utf-8")
                save_output_to_disk(output)
                
                progress_container.empty()
                st.sidebar.success("✅ Files processed successfully!")
                st.rerun()
                
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
    
    # Filter controls with persistent state
    st.sidebar.divider()
    st.sidebar.header("🎯 Filters & Search")

    # Search text
    search_text = st.sidebar.text_input(
        "🔍 Search",
        value=st.session_state.get("search_text", ""),
        placeholder="Search in all columns..."
    )
    st.session_state.search_text = search_text

    # Similarity slider
    similarity_range = st.sidebar.slider(
        "Similarity %",
        min_value=1,
        max_value=100,
        value=st.session_state.get("similarity_range", (1, 100))
    )
    st.session_state.similarity_range = similarity_range

    # Filter column
    filter_column = st.sidebar.selectbox(
        "Filter Column",
        ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"],
        index=st.session_state.get("filter_column_index", 0)
    )
    st.session_state.filter_column = filter_column
    st.session_state.filter_column_index = ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"].index(filter_column)

    # Filter value
    filter_value = ""
    if filter_column != "None":
        filter_value = st.sidebar.text_input(
            "Filter Value",
            value=st.session_state.get("filter_value", ""),
            placeholder="Value to exclude..."
        )
        st.session_state.filter_value = filter_value
    else:
        st.session_state.filter_value = ""
    
    return search_text, similarity_range[0], similarity_range[1], filter_column, filter_value

def apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value):
    """Apply filters to the dataframe with enhanced sorting and safe numeric conversion"""
    if df is None or len(df) == 0:
        return df
    
    filtered_df = df.copy()
    
    # Similarity filter with safe conversion
    if "Similarity %" in filtered_df.columns:
        # Safe conversion to numeric, handling empty strings and non-numeric values
        filtered_df["Similarity %"] = safe_float_conversion(filtered_df["Similarity %"], 0)
        filtered_df = filtered_df[
            (filtered_df["Similarity %"] >= min_sim) & 
            (filtered_df["Similarity %"] <= max_sim)
        ]
        
        # Enhanced sorting: first by Similarity % descending, then by Vendor Product Description
        sort_columns = ["Similarity %"]
        sort_ascending = [False]
        
        if "Vendor Product Description" in filtered_df.columns:
            sort_columns.append("Vendor Product Description")
            sort_ascending.append(False)
        elif len(filtered_df.columns) > 0:
            sort_columns.append(filtered_df.columns[0])
            sort_ascending.append(False)
        
        filtered_df = filtered_df.sort_values(by=sort_columns, ascending=sort_ascending)
    
    # Search filter
    if search_text:
        mask = filtered_df.astype(str).apply(
            lambda row: search_text.lower() in row.to_string().lower(), axis=1
        )
        filtered_df = filtered_df[mask]
    
    # Column filter
    if filter_column != "None" and filter_value:
        if filter_column in filtered_df.columns:
            mask = ~filtered_df[filter_column].astype(str).str.contains(
                filter_value, case=False, na=False
            )
            filtered_df = filtered_df[mask]
    
    return filtered_df

def create_client_setup_modal():
    """Compose the new client creation modal with elegant precision"""
    if not st.session_state.show_client_setup:
        return
    
    st.markdown("""
        <div style="background: linear-gradient(135deg, #e3f2fd, #f1f8e9); border: 3px solid #2196f3; border-radius: 15px; padding: 30px; margin: 25px 0;">
            <h3 style="text-align: center; color: #1976d2; margin-bottom: 25px;">🏢 Create Enhanced Client Database System</h3>
        </div>
        """, unsafe_allow_html=True)
    
    new_client_id = st.text_input(
        "Client ID:",
        value=st.session_state.new_client_id,
        placeholder="e.g., acme_corp, client_001, demo_company"
    )
    st.session_state.new_client_id = new_client_id
    
    if new_client_id:
        valid_id = len(new_client_id) >= 3 and new_client_id.replace('_', '').replace('-', '').isalnum()
        
        if valid_id:
            st.info(f"Will create enhanced database system for: **{new_client_id}**")
        else:
            st.warning("Client ID must be at least 3 characters and contain only letters, numbers, hyphens, and underscores")
    else:
        valid_id = False
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🚀 Create Client", type="primary", use_container_width=True, disabled=not valid_id):
            if valid_id:
                with st.spinner(f"Creating enhanced database system for '{new_client_id}'..."):
                    try:
                        success, message = create_enhanced_client_databases(new_client_id)
                        
                        if success:
                            st.success(f"✅ {message}")
                            st.session_state.current_client_id = new_client_id
                            st.session_state.new_client_id = ''
                            st.session_state.show_client_setup = False
                            load_available_clients()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                    except Exception as e:
                        st.error(f"❌ Error creating client: {str(e)}")
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_client_setup = False
            st.session_state.new_client_id = ''
            st.rerun()

def create_edit_modal():
    """Create modal for editing category, variety, color, grade fields with enhanced functionality"""
    if st.session_state.show_edit_product_modal and st.session_state.edit_product_row_data is not None:
        st.markdown(
            """
            <div class="edit-modal-container">
                <h3 style="text-align: center; color: #007bff; margin-bottom: 20px;">✏️ Edit Row Data</h3>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        row_data = st.session_state.edit_product_row_data
        row_index = st.session_state.edit_product_row_index
        
        # Show current row information
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        vendor_name = str(row_data.get('Vendor Name', 'N/A'))
        similarity = row_data.get('Similarity %', 'N/A')
        missing = row_data.get('Missing Words', '')
        
        st.info(f"**Missing Words:** {missing}")
        
        # Show applied synonyms during processing
        applied_synonyms = row_data.get("Applied Synonyms", "")
        if applied_synonyms:
            st.success(f"🔁 **Synonyms applied:** {applied_synonyms}")
        else:
            st.info("No synonyms were applied.")

        # Show blacklist removed during processing
        removed_blacklist = row_data.get("Removed Blacklist Words", "")
        if removed_blacklist:
            st.warning(f"🛑 **Blacklist words removed:** {removed_blacklist}")
        else:
            st.info("No blacklist words were removed.")
            
        st.info(f"""
        **Editing Row {row_index}**
        
        **Product:** {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}  
        **Vendor:** {vendor_name}  
        **Similarity:** {similarity}%
        """)
        
        # Re-run fuzzy matching button
        if st.button("🔄 Re-run Fuzzy Match", use_container_width=True):
            if st.session_state.current_client_id:
                with st.spinner("Re-processing row..."):
                    try:
                        success, updated_row = reprocess_row(st.session_state.current_client_id, row_data, True)
                        
                        if success:
                            st.session_state.edit_product_row_data = updated_row
                            if st.session_state.processed_data is not None:
                                df = st.session_state.processed_data
                                if row_index in df.index:
                                    for key, value in updated_row.items():
                                        if key in df.columns:
                                            df.loc[row_index, key] = value
                            st.success(f"✅ Row re-processed! New similarity: {updated_row.get('Similarity %', 'N/A')}%")
                            st.rerun()
                        else:
                            st.error("❌ Failed to re-process row")
                    except Exception as e:
                        st.error(f"❌ Error re-processing row: {str(e)}")
            else:
                st.error("❌ No client selected")
        
        # Edit form in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🏷️ Product Classification**")
            
            current_categoria = st.session_state.edit_categoria or str(row_data.get('Categoria', ''))
            current_variedad = st.session_state.edit_variedad or str(row_data.get('Variedad', ''))
            
            categoria = st.text_input(
                "Category:",
                value=current_categoria,
                key="modal_categoria_input",
                help="Edit the product category"
            )
            
            variedad = st.text_input(
                "Variety:",
                value=current_variedad,
                key="modal_variedad_input", 
                help="Edit the product variety"
            )
        
        with col2:
            st.markdown("**🎨 Physical Attributes**")
            
            current_color = st.session_state.edit_color or str(row_data.get('Color', ''))
            current_grado = st.session_state.edit_grado or str(row_data.get('Grado', ''))
            
            color = st.text_input(
                "Color:",
                value=current_color,
                key="modal_color_input",
                help="Edit the product color"
            )
            
            grado = st.text_input(
                "Grade:",
                value=current_grado,
                key="modal_grado_input",
                help="Edit the product grade"
            )
        
        # Update session state with current values
        st.session_state.edit_categoria = categoria
        st.session_state.edit_variedad = variedad
        st.session_state.edit_color = color
        st.session_state.edit_grado = grado
        
        # Word Action section
        st.markdown("**🛠️ Word Action (Blacklist / Synonym)**")

        action = st.selectbox(
            "Action type:",
            ["", "blacklist", "synonym"],
            index=["", "blacklist", "synonym"].index(row_data.get("Action", "") or ""),
            key="modal_action_select"
        )

        # Dynamic fields based on selected action
        word_input_1 = ""
        word_input_2 = ""

        if action == "blacklist":
            word_input_1 = st.text_input(
                "Blacklist word:",
                value=row_data.get("Word", ""),
                key="modal_word_blacklist"
            )

        elif action == "synonym":
            synonym_from = ""
            synonym_to = ""
            if ":" in str(row_data.get("Word", "")):
                parts = row_data.get("Word", "").split(":", 1)
                synonym_from = parts[0].strip('"')
                synonym_to = parts[1].strip('"')
            word_input_1 = st.text_input(
                "Original word:",
                value=synonym_from,
                key="modal_synonym_from"
            )
            word_input_2 = st.text_input(
                "Synonym for word:",
                value=synonym_to,
                key="modal_synonym_to"
            )
        
        st.markdown("---")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("💾 Update Row", type="primary", use_container_width=True, key="modal_confirm_update_btn"):
                # Update the main dataframe
                if st.session_state.processed_data is not None:
                    df = st.session_state.processed_data
                    if row_index in df.index:
                        df.loc[row_index, 'Categoria'] = categoria
                        df.loc[row_index, 'Variedad'] = variedad
                        df.loc[row_index, 'Color'] = color
                        df.loc[row_index, 'Grado'] = grado
                
                # Get action data
                action = st.session_state.get("modal_action_select", "")
                if action == "blacklist":
                    word_final = st.session_state.get("modal_word_blacklist", "").strip()
                elif action == "synonym":
                    w1 = st.session_state.get("modal_synonym_from", "").strip()
                    w2 = st.session_state.get("modal_synonym_to", "").strip()
                    word_final = f'"{w1}":"{w2}"' if w1 and w2 else ""
                else:
                    word_final = ""

                # Update database if possible
                if st.session_state.current_client_id:
                    try:
                        success, message = update_row_in_main_db(st.session_state.current_client_id, row_index, {
                            'Categoria': categoria, 'Variedad': variedad, 'Color': color, 'Grado': grado,
                            'Action': action, 'Word': word_final
                        })
                        
                        if success:
                            st.success(f"✅ Database updated successfully: {message}")
                        else:
                            st.warning(f"⚠️ Local update successful, database update failed: {message}")
                    except Exception as e:
                        st.warning(f"⚠️ Local update successful, database update failed: {str(e)}")
                else:
                    st.warning("⚠️ Database not connected. Only local data updated.")
                
                # Close modal and clear state
                st.session_state.show_edit_product_modal = False
                st.session_state.edit_product_row_data = None
                st.session_state.edit_product_row_index = None
                st.session_state.edit_categoria = ''
                st.session_state.edit_variedad = ''
                st.session_state.edit_color = ''
                st.session_state.edit_grado = ''
                
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("🆕 Save New Product", use_container_width=True, key="modal_save_new_btn"):
                if st.session_state.current_client_id:
                    try:
                        success, message = save_new_product(
                            st.session_state.current_client_id, row_data, categoria, variedad, color, grado, "streamlit_user"
                        )
                        
                        if success:
                            st.success(f"✅ {message}")
                        else:
                            st.error(f"❌ {message}")
                    except Exception as e:
                        st.error(f"❌ Error saving new product: {str(e)}")
                else:
                    st.error("❌ No client selected")
                
                time.sleep(1)
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset", use_container_width=True, key="modal_reset_btn"):
                st.session_state.edit_categoria = str(row_data.get('Categoria', ''))
                st.session_state.edit_variedad = str(row_data.get('Variedad', ''))
                st.session_state.edit_color = str(row_data.get('Color', ''))
                st.session_state.edit_grado = str(row_data.get('Grado', ''))
                st.rerun()
        
        with col4:
            if st.button("❌ Cancel", use_container_width=True, key="modal_cancel_edit_btn"):
                st.session_state.show_edit_product_modal = False
                st.session_state.edit_product_row_data = None
                st.session_state.edit_product_row_index = None
                st.session_state.edit_categoria = ''
                st.session_state.edit_variedad = ''
                st.session_state.edit_color = ''
                st.session_state.edit_grado = ''
                st.rerun()

def create_streamlit_table_with_actions(df):
    """Create table using Streamlit native components with inline action buttons"""
    
    # Key columns for display
    display_cols = [
        'Cleaned input', 'Best match', 'Similarity %', 'Catalog ID', 
        'Categoria', 'Variedad', 'Color', 'Grado'
    ]
    
    # Filter to only show columns that exist in the dataframe
    display_cols = [col for col in display_cols if col in df.columns]
    
    # Create header
    header_cols = st.columns(len(display_cols) + 4)  # +4 for Accept, Deny, Actions, Status
    
    headers = ["🧹 Cleaned Input", "🎯 Best Match", "📊 Similarity %", "🏷️ Catalog ID", 
               "📂 Category", "🌿 Variety", "🎨 Color", "⭐ Grade", 
               "✅ Accept", "❌ Deny", "⚡ Actions", "📊 Status"]
    
    for i, header in enumerate(headers):
        with header_cols[i]:
            st.markdown(f"**{header}**")
    
    # Add separator
    st.markdown("---")
    
    # Iterate through each row and create the table
    for idx, row in df.iterrows():
        # Create columns for this row
        row_cols = st.columns(len(display_cols) + 4)
        
        # Display data columns
        with row_cols[0]:
            cleaned_input = str(row.get('Cleaned input', ''))
            st.markdown(f"<div class='highlight-cell'>{cleaned_input[:50]}{'...' if len(cleaned_input) > 50 else ''}</div>", unsafe_allow_html=True)
        
        with row_cols[1]:
            best_match = str(row.get('Best match', ''))
            st.markdown(f"<div class='highlight-cell'>{best_match[:50]}{'...' if len(best_match) > 50 else ''}</div>", unsafe_allow_html=True)
        
        with row_cols[2]:
            try:
                similarity = float(str(row.get('Similarity %', 0)).replace('%', ''))
            except (ValueError, TypeError):
                similarity = 0
                
            if similarity >= 90:
                color_class = "background-color: #d4edda; color: #155724;"
            elif similarity >= 70:
                color_class = "background-color: #fff3cd; color: #856404;"
            else:
                color_class = "background-color: #f8d7da; color: #721c24;"
            st.markdown(f"<div style='{color_class} padding: 8px; border-radius: 4px; text-align: center; font-weight: bold;'>{similarity}%</div>", unsafe_allow_html=True)
        
        with row_cols[3]:
            catalog_id = str(row.get('Catalog ID', ''))
            if catalog_id.strip() in ["111111.0", "111111"]:
                st.markdown("<div style='background-color: #ff7f00; color: white; padding: 8px; border-radius: 4px; text-align: center; font-weight: bold;'>needs to create product</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='highlight-cell'>{catalog_id}</div>", unsafe_allow_html=True)
        
        with row_cols[4]:
            st.text(str(row.get('Categoria', '')))
        
        with row_cols[5]:
            st.text(str(row.get('Variedad', '')))
        
        with row_cols[6]:
            st.text(str(row.get('Color', '')))
        
        with row_cols[7]:
            st.text(str(row.get('Grado', '')))
                
        # Accept checkbox
        with row_cols[8]:
            accept_key = f"accept_{idx}"
            # Use database value by default if not in session_state
            db_accept = str(row.get("Accept Map", "")).strip().lower() == "true"
            accept = st.session_state.form_data.get(accept_key, db_accept)
            new_accept = st.checkbox("", value=accept, key=f"accept_cb_inline_{idx}")
            st.session_state.form_data[accept_key] = new_accept

            # Exclusivity: if accept is marked, unmark deny
            if new_accept and st.session_state.form_data.get(f"deny_{idx}", False):
                st.session_state.form_data[f"deny_{idx}"] = False
        
        # Deny checkbox
        with row_cols[9]:
            deny_key = f"deny_{idx}"
            db_deny = str(row.get("Deny Map", "")).strip().lower() == "true"
            deny = st.session_state.form_data.get(deny_key, db_deny)
            new_deny = st.checkbox("", value=deny, key=f"deny_cb_inline_{idx}")
            st.session_state.form_data[deny_key] = new_deny

            # Exclusivity: if deny is marked, unmark accept
            if new_deny and st.session_state.form_data.get(f"accept_{idx}", False):
                st.session_state.form_data[f"accept_{idx}"] = False
        
        # Action buttons
        with row_cols[10]:
            action_col1, action_col2 = st.columns(2)
            
            with action_col1:
                if st.button("✏️", key=f"edit_inline_{idx}", help="Edit and Verify in Database"):
                    st.session_state.show_edit_product_modal = True
                    st.session_state.edit_product_row_data = row.to_dict()
                    st.session_state.edit_product_row_index = idx
                    
                    # Initialize edit values
                    st.session_state.edit_categoria = str(row.get('Categoria', ''))
                    st.session_state.edit_variedad = str(row.get('Variedad', ''))
                    st.session_state.edit_color = str(row.get('Color', ''))
                    st.session_state.edit_grado = str(row.get('Grado', ''))
                    
                    st.rerun()
            
            with action_col2:
                if st.button("🔄", key=f"reprocess_inline_{idx}", help="Re-run fuzzy matching"):
                    if st.session_state.current_client_id:
                        with st.spinner("Re-processing..."):
                            try:
                                success, updated_row = reprocess_row(st.session_state.current_client_id, row.to_dict(), True)
                                
                                if success:
                                    # Update the main dataframe
                                    if st.session_state.processed_data is not None:
                                        for key, value in updated_row.items():
                                            if key in st.session_state.processed_data.columns:
                                                st.session_state.processed_data.loc[idx, key] = value
                                    
                                    st.success(f"✅ Row {idx} re-processed successfully!")
                                    st.rerun()
                                else:
                                    st.error(f"❌ Failed to re-process row {idx}")
                            except Exception as e:
                                st.error(f"❌ Error re-processing row {idx}: {str(e)}")
                    else:
                        st.error("❌ No client selected")
        
        # Status indicators
        with row_cols[11]:
            status_indicators = []
            
            if idx in st.session_state.inserted_rows:
                status_indicators.append("✅")
            
            verification_key = f"verify_{idx}"
            if verification_key in st.session_state.verification_results:
                if st.session_state.verification_results[verification_key]:
                    status_indicators.append("🔍✅")
                else:
                    status_indicators.append("🔍❌")
            
            if status_indicators:
                st.markdown(" ".join(status_indicators))
            else:
                st.markdown("⏳")
        
        # Add a subtle separator between rows
        st.markdown("<hr style='margin: 10px 0; border: 1px solid #eee;'>", unsafe_allow_html=True)

def mark_all_accept(filtered_df):
    """Mark all visible rows as Accept and clear Deny"""
    for idx in filtered_df.index:
        st.session_state.form_data[f"accept_{idx}"] = True
        st.session_state.form_data[f"deny_{idx}"] = False

def mark_all_deny(filtered_df):
    """Mark all visible rows as Deny and clear Accept"""
    for idx in filtered_df.index:
        st.session_state.form_data[f"accept_{idx}"] = False
        st.session_state.form_data[f"deny_{idx}"] = True

def display_messages():
    """Display status messages with elegant presentation"""
    if st.session_state.get('success_message'):
        st.success(st.session_state.success_message)
        st.session_state.success_message = ''
    
    if st.session_state.get('error_message'):
        st.error(st.session_state.error_message)
        st.session_state.error_message = ''
    
    if st.session_state.get('info_message'):
        st.info(st.session_state.info_message)
        st.session_state.info_message = ''
    
    if st.session_state.get('warning_message'):
        st.warning(st.session_state.warning_message)
        st.session_state.warning_message = ''

def main():
    """
    The grand conductor of our application symphony,
    orchestrating all components into a harmonious whole
    """
    
    # Apply the aesthetic foundation
    apply_custom_css()
    
    # Initialize database connection status on first run
    if st.session_state.db_connection_status is None:
        check_database_connection()
    
    # Show modals if needed
    if st.session_state.show_client_setup:
        create_client_setup_modal()
        return
    
    if st.session_state.show_edit_product_modal:
        create_edit_modal()
        return
    
    # Display the application header with poetic grandeur
    st.markdown("""
        <div class="main-header">
            <h1>🔧 Enhanced Multi-Client Data Mapping Validation System</h1>
            <p><strong>Harmonizing Data Processing with Elegant Precision</strong></p>
            <p><em>Where raw data transforms into structured knowledge through sophisticated algorithms</em></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display contextual messages
    display_messages()
    
    # Sidebar controls
    search_text, min_sim, max_sim, filter_column, filter_value = sidebar_controls()
    
    # Main content
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Apply filters
        filtered_df = apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value)
        
        if len(filtered_df) > 0:
            # Progress tracking
            total_rows = len(filtered_df)
            reviewed_rows = sum(
                1 for idx in filtered_df.index 
                if st.session_state.form_data.get(f"accept_{idx}", False) or 
                   st.session_state.form_data.get(f"deny_{idx}", False)
            )
            
            progress_pct = (reviewed_rows / total_rows * 100) if total_rows > 0 else 0
            
            # Progress display with liquid effects
            st.markdown(
                f"""
                <div class="progress-container">
                    <h4>📊 Review Progress</h4>
                    <p><strong>{reviewed_rows}</strong> of <strong>{total_rows}</strong> rows reviewed 
                    (<strong>{progress_pct:.1f}%</strong>)</p>
                    <small>Data sorted by: Similarity % ↓, Vendor Product Description ↓</small>
                </div>
                """, 
                unsafe_allow_html=True
            )
            
            # Liquid progress bar
            progress_html = create_liquid_progress_bar(progress_pct, f"Review Progress: {reviewed_rows}/{total_rows}")
            st.markdown(progress_html, unsafe_allow_html=True)
            
            # Pagination
            rows_per_page = 50
            total_pages = (len(filtered_df) + rows_per_page - 1) // rows_per_page
            
            if total_pages > 1:
                st.write(f"**Total rows:** {len(filtered_df)} | **Pages:** {total_pages}")
                
                current_page = st.selectbox(
                    f"Select Page (1-{total_pages})", 
                    options=list(range(1, total_pages + 1)),
                    index=st.session_state.current_page - 1,
                    key="page_selector_main"
                )
                
                st.session_state.current_page = current_page
                
                start_idx = (current_page - 1) * rows_per_page
                end_idx = start_idx + rows_per_page
                page_df = filtered_df.iloc[start_idx:end_idx].copy()
            else:
                page_df = filtered_df.copy()
                st.session_state.current_page = 1
            
            # Create the enhanced inline table
            create_streamlit_table_with_actions(page_df)
            
            # Add bulk action buttons at the bottom
            st.markdown("---")
            st.markdown(
                """
                <div class="bulk-action-container">
                    <div class="bulk-action-header">⚡ Bulk Actions for Current Page</div>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                if st.button("✅ Accept All Visible", 
                             type="primary", 
                             use_container_width=True, 
                             key="bulk_accept_btn",
                             help="Mark all visible rows as Accept and clear Deny"):
                    mark_all_accept(page_df)
                    st.success(f"✅ Marked {len(page_df)} rows as Accept")
                    st.rerun()
            
            with col2:
                if st.button("❌ Deny All Visible", 
                             use_container_width=True, 
                             key="bulk_deny_btn",
                             help="Mark all visible rows as Deny and clear Accept"):
                    mark_all_deny(page_df)
                    st.success(f"❌ Marked {len(page_df)} rows as Deny")
                    st.rerun()
            
            with col3:
                if st.button("🔄 Clear All Selections", 
                             use_container_width=True, 
                             key="bulk_clear_btn",
                             help="Clear all Accept and Deny selections"):
                    for idx in page_df.index:
                        st.session_state.form_data[f"accept_{idx}"] = False
                        st.session_state.form_data[f"deny_{idx}"] = False
                    st.info(f"🔄 Cleared all selections for {len(page_df)} rows")
                    st.rerun()
            
        else:
            st.warning("🔍 No data matches the current filters")
            st.info("Try adjusting your filter criteria in the sidebar")
    else:
        # Instructions for new users
        st.info("👆 **Get Started:** Upload your files using the sidebar or load existing data from database")
        
        with st.expander("📖 **Enhanced Features Guide**"):
            st.markdown("""
            ### 🚀 **Getting Started:**
            
            1. **Select Client** → Use sidebar to choose existing or create new client
            2. **Load Data** → Either upload files or load from database
            3. **Review Data** → Use enhanced filtering and row-level actions
            4. **Edit Products** → Click edit button to modify and save products
            5. **Bulk Actions** → Use bulk operations for efficient workflow
            
            ### 🔧 **Enhanced Features:**
            - ✅ **Row-level Processing** - Individual row reprocessing with updated synonyms
            - ✅ **Enhanced Filtering** - Advanced search and exclusion capabilities
            - ✅ **Product Staging** - Save new products with catalog_id: 111111
            - ✅ **Real-time Updates** - Immediate feedback and validation
            - ✅ **Client Isolation** - Complete data separation per client
            - ✅ **Liquid Progress Bars** - Beautiful animated progress tracking
            - ✅ **Database Integration** - Full CRUD operations with enhanced database
            
            ### 📊 **Data Processing:**
            - **Similarity Calculation** - Advanced fuzzy matching algorithms
            - **Synonyms Application** - Dynamic text replacement
            - **Blacklist Filtering** - Intelligent word removal
            - **Progress Tracking** - Real-time processing feedback
            """)

if __name__ == "__main__":
    main()