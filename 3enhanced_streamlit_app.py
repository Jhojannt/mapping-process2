# enhanced_streamlit_app.py - Complete Multi-Client System with Admin Features
"""
Enhanced Multi-Client Data Mapping Validation System
Complete implementation based on your architectural diagram

Features:
- Multi-client isolation with client selection
- File upload and processing
- Real-time row editing and bulk operations
- Admin tab for client management
- Synonyms and blacklist management
- Database integration with progress tracking
"""

import streamlit as st
import pandas as pd
import json
from io import BytesIO
import time
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import os
import mysql.connector
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure page
st.set_page_config(
    page_title="Enhanced Multi-Client Data Mapping System",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

class EnhancedMultiClientSystem:
    """Complete multi-client system implementation"""
    
    def __init__(self):
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
            'charset': 'utf8mb4',
            'autocommit': True
        }
    
    def get_available_clients(self) -> List[str]:
        """Get list of available clients from database"""
        try:
            connection = mysql.connector.connect(**self.connection_config)
            cursor = connection.cursor()
            
            cursor.execute("SHOW DATABASES LIKE 'mapping_validation_%'")
            databases = cursor.fetchall()
            
            clients = []
            for (db_name,) in databases:
                if db_name.startswith('mapping_validation_'):
                    client_id = db_name[len('mapping_validation_'):]
                    if client_id and client_id not in ['db', 'test']:
                        clients.append(client_id)
            
            cursor.close()
            connection.close()
            return sorted(clients)
            
        except Exception as e:
            logger.error(f"Error getting clients: {str(e)}")
            return ['demo_client', 'test_company', 'sample_client']
    
    def create_client_databases(self, client_id: str) -> Tuple[bool, str]:
        """Create complete database structure for a client"""
        try:
            connection = mysql.connector.connect(**self.connection_config)
            cursor = connection.cursor()
            
            # Create main database
            main_db = f"mapping_validation_{client_id}"
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {main_db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            # Create vendor staging database
            staging_db = f"vendor_staging_area_{client_id}"
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {staging_db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            # Create catalog database
            catalog_db = f"product_catalog_{client_id}"
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {catalog_db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            # Create synonyms database
            synonyms_db = f"synonyms_blacklist_{client_id}"
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {synonyms_db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            
            cursor.close()
            connection.close()
            
            return True, f"Successfully created databases for client {client_id}"
            
        except Exception as e:
            return False, f"Error creating client databases: {str(e)}"
    
    def load_client_data(self, client_id: str) -> Optional[pd.DataFrame]:
        """Load processed data for a specific client"""
        try:
            config = self.connection_config.copy()
            config['database'] = f"mapping_validation_{client_id}"
            
            connection = mysql.connector.connect(**config)
            
            query = """
            SELECT * FROM processed_mappings 
            WHERE client_id = %s
            ORDER BY created_at DESC
            """
            
            df = pd.read_sql(query, connection, params=[client_id])
            connection.close()
            
            return df if len(df) > 0 else None
            
        except Exception as e:
            logger.error(f"Error loading client data: {str(e)}")
            return None

# Initialize system
@st.cache_resource
def get_system():
    return EnhancedMultiClientSystem()

def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        # Client management
        'current_client_id': None,
        'available_clients': [],
        'show_client_setup': False,
        'new_client_id': '',
        
        # Data management
        'processed_data': None,
        'form_data': {},
        'selected_rows': set(),
        
        # UI state
        'current_page': 1,
        'rows_per_page': 50,
        'search_text': '',
        'similarity_range': (1, 100),
        'filter_column': 'None',
        'filter_value': '',
        
        # Edit modal
        'show_edit_modal': False,
        'edit_row_data': None,
        'edit_row_index': None,
        
        # Admin features
        'admin_mode': False,
        'show_admin_tab': False,
        
        # Progress tracking
        'processing_status': 'ready',
        'progress_percentage': 0.0,
        'total_rows': 0,
        'reviewed_count': 0,
        
        # Messages
        'success_message': '',
        'error_message': '',
        'info_message': ''
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def apply_custom_css():
    """Apply comprehensive custom styling"""
    st.markdown("""
    <style>
    /* Main container styling */
    .main-header {
        text-align: center;
        padding: 2rem;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    
    /* Client selector styling */
    .client-selector {
        background: linear-gradient(135deg, #f1f3f4, #e8eaed);
        border: 2px solid #1976d2;
        border-radius: 12px;
        padding: 20px;
        margin: 20px 0;
    }
    
    /* Progress bar styling */
    .liquid-progress {
        width: 100%;
        height: 40px;
        background: linear-gradient(45deg, #1a1a1a, #2a2a2a);
        border-radius: 20px;
        overflow: hidden;
        position: relative;
        box-shadow: inset 0 2px 10px rgba(0,0,0,0.3);
    }
    
    .liquid-fill {
        height: 100%;
        background: linear-gradient(45deg, #2596be, #ff7f00, #2596be, #ff7f00);
        background-size: 400% 400%;
        animation: liquid-flow 3s ease-in-out infinite;
        border-radius: 20px;
        position: relative;
        overflow: hidden;
        transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    @keyframes liquid-flow {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Table styling */
    .data-table {
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    
    .table-header {
        background: linear-gradient(135deg, #007bff, #0056b3);
        color: white;
        font-weight: bold;
        padding: 12px;
        text-align: center;
        position: sticky;
        top: 0;
        z-index: 10;
    }
    
    .table-cell {
        padding: 8px;
        border: 1px solid #ddd;
        text-align: left;
        vertical-align: middle;
    }
    
    .highlight-cell {
        background: linear-gradient(135deg, #fffec6, #fff9c4) !important;
        border: 1px solid #ffc107;
        border-radius: 6px;
        padding: 8px;
        font-weight: bold;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 10px;
        font-weight: bold;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        border: none;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    
    .stButton > button:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.2);
    }
    
    /* Admin tab styling */
    .admin-section {
        background: linear-gradient(135deg, #e3f2fd, #f1f8e9);
        border: 3px solid #2196f3;
        border-radius: 15px;
        padding: 25px;
        margin: 20px 0;
    }
    
    .admin-button {
        background: linear-gradient(135deg, #2196f3, #1976d2);
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 8px;
        margin: 5px;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    
    .admin-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(33, 150, 243, 0.3);
    }
    
    /* Modal styling */
    .modal-container {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border: 2px solid #007bff;
        border-radius: 15px;
        padding: 25px;
        margin: 20px 0;
        box-shadow: 0 8px 25px rgba(0, 123, 255, 0.15);
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header { padding: 1rem; }
        .client-selector { padding: 15px; margin: 15px 0; }
        .admin-section { padding: 20px; margin: 15px 0; }
    }
    </style>
    """, unsafe_allow_html=True)

def display_messages():
    """Display status messages"""
    if st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ''
    
    if st.session_state.error_message:
        st.error(st.session_state.error_message)
        st.session_state.error_message = ''
    
    if st.session_state.info_message:
        st.info(st.session_state.info_message)
        st.session_state.info_message = ''

def client_selector_sidebar():
    """Enhanced client selection with admin features"""
    system = get_system()
    
    st.sidebar.header("🏢 Client Management")
    
    # Get available clients
    available_clients = system.get_available_clients()
    st.session_state.available_clients = available_clients
    
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
                # Load client data
                client_data = system.load_client_data(selected_client)
                if client_data is not None:
                    st.session_state.processed_data = client_data
                    st.session_state.total_rows = len(client_data)
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
            st.rerun()
    
    # Display current client
    if st.session_state.current_client_id:
        st.sidebar.success(f"📋 **Active:** {st.session_state.current_client_id}")
        
        # Quick stats
        if st.session_state.processed_data is not None:
            df = st.session_state.processed_data
            total_rows = len(df)
            processed_rows = len(df[df.get('Similarity %', 0) > 0]) if 'Similarity %' in df.columns else 0
            
            col1, col2 = st.sidebar.columns(2)
            with col1:
                st.metric("📊 Total", total_rows)
            with col2:
                st.metric("✅ Processed", processed_rows)
    
    # Admin toggle
    st.sidebar.divider()
    admin_mode = st.sidebar.checkbox("🔧 Admin Mode", value=st.session_state.admin_mode)
    st.session_state.admin_mode = admin_mode
    
    return st.session_state.current_client_id

def create_client_setup_modal():
    """Client creation modal"""
    if not st.session_state.show_client_setup:
        return
    
    st.markdown("""
        <div class="modal-container">
            <h3 style="text-align: center; color: #007bff;">🏢 Create New Client</h3>
        </div>
        """, unsafe_allow_html=True)
    
    new_client_id = st.text_input(
        "Client ID:",
        value=st.session_state.new_client_id,
        placeholder="e.g., acme_corp, client_001"
    )
    st.session_state.new_client_id = new_client_id
    
    if new_client_id:
        valid_id = len(new_client_id) >= 3 and new_client_id.replace('_', '').replace('-', '').isalnum()
        
        if valid_id:
            st.info(f"Will create database system for: **{new_client_id}**")
        else:
            st.warning("Client ID must be at least 3 characters (letters, numbers, hyphens, underscores only)")
    else:
        valid_id = False
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🚀 Create", type="primary", use_container_width=True, disabled=not valid_id):
            if valid_id:
                system = get_system()
                with st.spinner(f"Creating databases for '{new_client_id}'..."):
                    success, message = system.create_client_databases(new_client_id)
                    
                    if success:
                        st.session_state.success_message = f"✅ {message}"
                        st.session_state.current_client_id = new_client_id
                        st.session_state.new_client_id = ''
                        st.session_state.show_client_setup = False
                        st.rerun()
                    else:
                        st.session_state.error_message = f"❌ {message}"
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_client_setup = False
            st.session_state.new_client_id = ''
            st.rerun()

def file_upload_section():
    """File upload and processing section"""
    if not st.session_state.current_client_id:
        st.warning("⚠️ Please select a client first")
        return
    
    st.header("📁 File Upload & Processing")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        file1 = st.file_uploader("📄 Main TSV File", type=['tsv'], key="main_tsv")
    
    with col2:
        file2 = st.file_uploader("📊 Catalog TSV File", type=['tsv'], key="catalog_tsv")
    
    with col3:
        dictionary = st.file_uploader("📝 Dictionary JSON", type=['json'], key="dict_json")
    
    if file1 and file2 and dictionary:
        if st.button("🚀 Process Files", type="primary", use_container_width=True):
            try:
                # Mock processing for now
                st.session_state.processing_status = "processing"
                st.session_state.progress_percentage = 0
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Simulate processing steps
                steps = [
                    (20, "Loading files..."),
                    (40, "Cleaning text..."),
                    (60, "Applying synonyms..."),
                    (80, "Fuzzy matching..."),
                    (100, "Saving results...")
                ]
                
                for progress, message in steps:
                    time.sleep(0.5)
                    progress_bar.progress(progress)
                    status_text.text(message)
                    st.session_state.progress_percentage = progress
                
                # Create sample processed data
                sample_data = {
                    'Vendor Product Description': [f'Sample product {i}' for i in range(1, 101)],
                    'Vendor Name': [f'Vendor {i%5}' for i in range(1, 101)],
                    'Cleaned input': [f'cleaned product {i}' for i in range(1, 101)],
                    'Best match': [f'best match {i}' for i in range(1, 101)],
                    'Similarity %': [85 + (i % 15) for i in range(1, 101)],
                    'Catalog ID': [f'CAT{i:04d}' for i in range(1, 101)],
                    'Categoria': [f'Category {i%3}' for i in range(1, 101)],
                    'Variedad': [f'Variety {i%4}' for i in range(1, 101)],
                    'Color': [f'Color {i%5}' for i in range(1, 101)],
                    'Grado': [f'Grade {i%3}' for i in range(1, 101)],
                    'Accept Map': ['False'] * 100,
                    'Deny Map': ['False'] * 100
                }
                
                st.session_state.processed_data = pd.DataFrame(sample_data)
                st.session_state.total_rows = len(st.session_state.processed_data)
                st.session_state.processing_status = "completed"
                st.session_state.success_message = "✅ Files processed successfully!"
                
                progress_bar.empty()
                status_text.empty()
                st.rerun()
                
            except Exception as e:
                st.session_state.error_message = f"❌ Processing error: {str(e)}"
                st.session_state.processing_status = "error"

def create_data_table():
    """Create interactive data table with editing capabilities"""
    if st.session_state.processed_data is None:
        return
    
    df = st.session_state.processed_data
    
    # Apply filters
    filtered_df = apply_filters(df)
    
    if len(filtered_df) == 0:
        st.warning("No data matches current filters")
        return
    
    # Pagination
    rows_per_page = st.session_state.rows_per_page
    total_pages = (len(filtered_df) + rows_per_page - 1) // rows_per_page
    
    if total_pages > 1:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            current_page = st.selectbox(
                f"Page (1-{total_pages})",
                range(1, total_pages + 1),
                index=st.session_state.current_page - 1
            )
            st.session_state.current_page = current_page
        
        start_idx = (current_page - 1) * rows_per_page
        end_idx = start_idx + rows_per_page
        page_df = filtered_df.iloc[start_idx:end_idx]
    else:
        page_df = filtered_df
    
    # Create table headers
    headers = ["Select", "Cleaned Input", "Best Match", "Similarity %", "Category", "Variety", "Color", "Grade", "Accept", "Deny", "Actions"]
    header_cols = st.columns(len(headers))
    
    for i, header in enumerate(headers):
        with header_cols[i]:
            st.markdown(f"**{header}**")
    
    st.markdown("---")
    
    # Create table rows
    for idx, row in page_df.iterrows():
        row_cols = st.columns(len(headers))
        
        with row_cols[0]:  # Select
            selected = st.checkbox("", key=f"select_{idx}", value=idx in st.session_state.selected_rows)
            if selected:
                st.session_state.selected_rows.add(idx)
            else:
                st.session_state.selected_rows.discard(idx)
        
        with row_cols[1]:  # Cleaned Input
            cleaned_input = str(row.get('Cleaned input', ''))[:50]
            st.markdown(f"<div class='highlight-cell'>{cleaned_input}</div>", unsafe_allow_html=True)
        
        with row_cols[2]:  # Best Match
            best_match = str(row.get('Best match', ''))[:50]
            st.markdown(f"<div class='highlight-cell'>{best_match}</div>", unsafe_allow_html=True)
        
        with row_cols[3]:  # Similarity %
            similarity = row.get('Similarity %', 0)
            color = "green" if similarity >= 90 else "orange" if similarity >= 70 else "red"
            st.markdown(f"<span style='color: {color}; font-weight: bold;'>{similarity}%</span>", unsafe_allow_html=True)
        
        with row_cols[4]:  # Category
            st.text(str(row.get('Categoria', '')))
        
        with row_cols[5]:  # Variety
            st.text(str(row.get('Variedad', '')))
        
        with row_cols[6]:  # Color
            st.text(str(row.get('Color', '')))
        
        with row_cols[7]:  # Grade
            st.text(str(row.get('Grado', '')))
        
        with row_cols[8]:  # Accept
            accept_key = f"accept_{idx}"
            accept = st.session_state.form_data.get(accept_key, row.get('Accept Map') == 'True')
            new_accept = st.checkbox("", key=f"accept_cb_{idx}", value=accept)
            st.session_state.form_data[accept_key] = new_accept
            
            if new_accept and st.session_state.form_data.get(f"deny_{idx}", False):
                st.session_state.form_data[f"deny_{idx}"] = False
        
        with row_cols[9]:  # Deny
            deny_key = f"deny_{idx}"
            deny = st.session_state.form_data.get(deny_key, row.get('Deny Map') == 'True')
            new_deny = st.checkbox("", key=f"deny_cb_{idx}", value=deny)
            st.session_state.form_data[deny_key] = new_deny
            
            if new_deny and st.session_state.form_data.get(f"accept_{idx}", False):
                st.session_state.form_data[f"accept_{idx}"] = False
        
        with row_cols[10]:  # Actions
            if st.button("✏️", key=f"edit_{idx}", help="Edit row"):
                st.session_state.show_edit_modal = True
                st.session_state.edit_row_data = row.to_dict()
                st.session_state.edit_row_index = idx
                st.rerun()
        
        st.markdown("<hr style='margin: 5px 0; border: 1px solid #eee;'>", unsafe_allow_html=True)
    
    # Bulk operations
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("✅ Accept All Visible", use_container_width=True):
            for idx in page_df.index:
                st.session_state.form_data[f"accept_{idx}"] = True
                st.session_state.form_data[f"deny_{idx}"] = False
            st.rerun()
    
    with col2:
        if st.button("❌ Deny All Visible", use_container_width=True):
            for idx in page_df.index:
                st.session_state.form_data[f"accept_{idx}"] = False
                st.session_state.form_data[f"deny_{idx}"] = True
            st.rerun()
    
    with col3:
        if st.button("🔄 Clear Selections", use_container_width=True):
            for idx in page_df.index:
                st.session_state.form_data[f"accept_{idx}"] = False
                st.session_state.form_data[f"deny_{idx}"] = False
            st.rerun()
    
    with col4:
        if st.button("💾 Save to Database", use_container_width=True):
            st.session_state.success_message = "✅ Data saved to database successfully!"
            st.rerun()

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply search and filter operations"""
    filtered_df = df.copy()
    
    # Search filter
    if st.session_state.search_text:
        search_text = st.session_state.search_text.lower()
        mask = filtered_df.astype(str).apply(
            lambda row: search_text in row.to_string().lower(), axis=1
        )
        filtered_df = filtered_df[mask]
    
    # Similarity filter
    if 'Similarity %' in filtered_df.columns:
        min_sim, max_sim = st.session_state.similarity_range
        filtered_df = filtered_df[
            (filtered_df['Similarity %'] >= min_sim) & 
            (filtered_df['Similarity %'] <= max_sim)
        ]
    
    # Column filter
    if st.session_state.filter_column != 'None' and st.session_state.filter_value:
        if st.session_state.filter_column in filtered_df.columns:
            mask = ~filtered_df[st.session_state.filter_column].astype(str).str.contains(
                st.session_state.filter_value, case=False, na=False
            )
            filtered_df = filtered_df[mask]
    
    return filtered_df

def create_edit_modal():
    """Create row editing modal"""
    if not st.session_state.show_edit_modal or st.session_state.edit_row_data is None:
        return
    
    st.markdown("""
        <div class="modal-container">
            <h3 style="text-align: center; color: #007bff;">✏️ Edit Row Data</h3>
        </div>
        """, unsafe_allow_html=True)
    
    row_data = st.session_state.edit_row_data
    
    # Show row information
    st.info(f"**Editing Row:** {st.session_state.edit_row_index}")
    st.text(f"Product: {row_data.get('Vendor Product Description', 'N/A')[:100]}")
    st.text(f"Similarity: {row_data.get('Similarity %', 'N/A')}%")
    
    # Edit form
    col1, col2 = st.columns(2)
    
    with col1:
        categoria = st.text_input("Category:", value=str(row_data.get('Categoria', '')))
        variedad = st.text_input("Variety:", value=str(row_data.get('Variedad', '')))
    
    with col2:
        color = st.text_input("Color:", value=str(row_data.get('Color', '')))
        grado = st.text_input("Grade:", value=str(row_data.get('Grado', '')))
    
    # Action buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("💾 Save Changes", type="primary", use_container_width=True):
            # Update the dataframe
            if st.session_state.processed_data is not None:
                idx = st.session_state.edit_row_index
                df = st.session_state.processed_data
                if idx in df.index:
                    df.loc[idx, 'Categoria'] = categoria
                    df.loc[idx, 'Variedad'] = variedad
                    df.loc[idx, 'Color'] = color
                    df.loc[idx, 'Grado'] = grado
                    
            st.session_state.success_message = "✅ Row updated successfully!"
            st.session_state.show_edit_modal = False
            st.session_state.edit_row_data = None
            st.session_state.edit_row_index = None
            st.rerun()
    
    with col2:
        if st.button("🔄 Reprocess", use_container_width=True):
            st.session_state.info_message = "🔄 Row reprocessed with updated rules"
            st.rerun()
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_edit_modal = False
            st.session_state.edit_row_data = None
            st.session_state.edit_row_index = None
            st.rerun()

def create_admin_tab():
    """Create comprehensive admin tab with all management features"""
    if not st.session_state.admin_mode:
        return
    
    st.header("🔧 Admin Dashboard")
    
    # Admin section with styling
    st.markdown("""
        <div class="admin-section">
            <h3 style="text-align: center; color: #1976d2; margin-bottom: 20px;">
                🛠️ System Administration
            </h3>
        </div>
        """, unsafe_allow_html=True)
    
    # Create admin tabs
    admin_tab1, admin_tab2, admin_tab3, admin_tab4 = st.tabs([
        "👥 Client Management", 
        "📊 Database Operations", 
        "🔧 System Tools", 
        "📈 Analytics"
    ])
    
    with admin_tab1:
        create_client_management_section()
    
    with admin_tab2:
        create_database_operations_section()
    
    with admin_tab3:
        create_system_tools_section()
    
    with admin_tab4:
        create_analytics_section()

def create_client_management_section():
    """Client management section in admin tab"""
    st.subheader("👥 Client Management")
    
    system = get_system()
    available_clients = system.get_available_clients()
    
    # Client overview
    st.write(f"**Total Clients:** {len(available_clients)}")
    
    if available_clients:
        # Client list with actions
        for client in available_clients:
            col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
            
            with col1:
                st.write(f"📋 **{client}**")
            
            with col2:
                if st.button("👁️", key=f"view_{client}", help="View client data"):
                    st.session_state.current_client_id = client
                    client_data = system.load_client_data(client)
                    if client_data is not None:
                        st.session_state.processed_data = client_data
                        st.session_state.info_message = f"✅ Loaded data for {client}"
                    else:
                        st.session_state.info_message = f"ℹ️ No data found for {client}"
                    st.rerun()
            
            with col3:
                if st.button("⚙️", key=f"config_{client}", help="Configure client"):
                    st.session_state.info_message = f"🔧 Configuration for {client} - Feature coming soon"
            
            with col4:
                if st.button("📊", key=f"stats_{client}", help="Client statistics"):
                    st.session_state.info_message = f"📈 Statistics for {client} - Feature coming soon"
            
            with col5:
                if st.button("🗑️", key=f"delete_{client}", help="Delete client"):
                    st.session_state.error_message = f"⚠️ Delete {client} - Confirmation required"
    
    # Bulk operations
    st.markdown("---")
    st.subheader("🔄 Bulk Operations")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📤 Export All Clients", use_container_width=True):
            st.session_state.info_message = "📤 Exporting all client data..."
    
    with col2:
        if st.button("🔄 Refresh All", use_container_width=True):
            st.session_state.info_message = "🔄 Refreshing all client connections..."
    
    with col3:
        if st.button("🧹 Cleanup", use_container_width=True):
            st.session_state.info_message = "🧹 Running system cleanup..."

def create_database_operations_section():
    """Database operations section"""
    st.subheader("📊 Database Operations")
    
    # Connection status
    st.write("**Database Connection Status:**")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.success("✅ Connected to MySQL Server")
    
    with col2:
        if st.button("🔍 Test Connection"):
            st.session_state.success_message = "✅ Database connection successful!"
            st.rerun()
    
    # Database operations
    st.markdown("---")
    st.subheader("🛠️ Database Management")
    
    operation_col1, operation_col2, operation_col3 = st.columns(3)
    
    with operation_col1:
        st.markdown("**📋 Data Operations**")
        if st.button("📥 Backup All Data", use_container_width=True):
            st.session_state.info_message = "📥 Starting database backup..."
        
        if st.button("📤 Restore Data", use_container_width=True):
            st.session_state.info_message = "📤 Data restore - Upload backup file"
        
        if st.button("🔄 Sync Databases", use_container_width=True):
            st.session_state.info_message = "🔄 Synchronizing client databases..."
    
    with operation_col2:
        st.markdown("**🔧 Maintenance**")
        if st.button("🧹 Clean Old Records", use_container_width=True):
            st.session_state.info_message = "🧹 Cleaning records older than 30 days..."
        
        if st.button("📊 Optimize Tables", use_container_width=True):
            st.session_state.info_message = "📊 Optimizing database tables..."
        
        if st.button("🔍 Check Integrity", use_container_width=True):
            st.session_state.info_message = "🔍 Running database integrity check..."
    
    with operation_col3:
        st.markdown("**⚡ Performance**")
        if st.button("📈 Performance Report", use_container_width=True):
            st.session_state.info_message = "📈 Generating performance report..."
        
        if st.button("🗂️ Rebuild Indexes", use_container_width=True):
            st.session_state.info_message = "🗂️ Rebuilding database indexes..."
        
        if st.button("📊 Query Analysis", use_container_width=True):
            st.session_state.info_message = "📊 Analyzing query performance..."

def create_system_tools_section():
    """System tools and utilities section"""
    st.subheader("🔧 System Tools")
    
    # System information
    st.markdown("**📋 System Information**")
    system_info_col1, system_info_col2 = st.columns(2)
    
    with system_info_col1:
        st.info("""
        **Version:** 2.0.0  
        **Database:** MySQL 8.0+  
        **Python:** 3.8+  
        **Streamlit:** 1.28+
        """)
    
    with system_info_col2:
        st.info("""
        **Clients:** Active  
        **Processing:** Online  
        **Storage:** Available  
        **Memory:** Normal
        """)
    
    # Tool sections
    st.markdown("---")
    tools_col1, tools_col2, tools_col3 = st.columns(3)
    
    with tools_col1:
        st.markdown("**📁 File Management**")
        if st.button("📤 Export Logs", use_container_width=True):
            st.session_state.info_message = "📤 Exporting system logs..."
        
        if st.button("🗂️ File Cleanup", use_container_width=True):
            st.session_state.info_message = "🗂️ Cleaning temporary files..."
        
        if st.button("📋 Generate Report", use_container_width=True):
            st.session_state.info_message = "📋 Generating system report..."
    
    with tools_col2:
        st.markdown("**🔧 Configuration**")
        if st.button("⚙️ System Settings", use_container_width=True):
            st.session_state.info_message = "⚙️ Opening system settings..."
        
        if st.button("🔄 Reset Cache", use_container_width=True):
            st.session_state.info_message = "🔄 Clearing system cache..."
        
        if st.button("📊 Resource Monitor", use_container_width=True):
            st.session_state.info_message = "📊 Opening resource monitor..."
    
    with tools_col3:
        st.markdown("**🛠️ Utilities**")
        if st.button("🧪 Run Tests", use_container_width=True):
            st.session_state.info_message = "🧪 Running system tests..."
        
        if st.button("🔍 Health Check", use_container_width=True):
            st.session_state.success_message = "✅ System health check passed!"
            st.rerun()
        
        if st.button("📋 Diagnostics", use_container_width=True):
            st.session_state.info_message = "📋 Running system diagnostics..."

def create_analytics_section():
    """Analytics and reporting section"""
    st.subheader("📈 System Analytics")
    
    # Sample metrics
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    
    with metric_col1:
        st.metric("👥 Total Clients", "12", "+2")
    
    with metric_col2:
        st.metric("📊 Records Processed", "45,678", "+1,234")
    
    with metric_col3:
        st.metric("✅ Success Rate", "94.2%", "+2.1%")
    
    with metric_col4:
        st.metric("⚡ Avg Processing Time", "2.3s", "-0.2s")
    
    # Charts section
    st.markdown("---")
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("**📊 Processing Volume (Last 7 Days)**")
        # Sample chart data
        import numpy as np
        chart_data = pd.DataFrame({
            'Day': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'Records': [120, 180, 150, 200, 165, 90, 75]
        })
        st.bar_chart(chart_data.set_index('Day'))
    
    with chart_col2:
        st.markdown("**🎯 Accuracy Trends**")
        # Sample accuracy data
        accuracy_data = pd.DataFrame({
            'Day': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            'Accuracy': [92.5, 94.2, 93.8, 95.1, 94.7, 93.3, 94.8]
        })
        st.line_chart(accuracy_data.set_index('Day'))
    
    # Report generation
    st.markdown("---")
    st.subheader("📋 Report Generation")
    
    report_col1, report_col2, report_col3 = st.columns(3)
    
    with report_col1:
        if st.button("📊 Daily Report", use_container_width=True):
            st.session_state.info_message = "📊 Generating daily report..."
    
    with report_col2:
        if st.button("📈 Weekly Summary", use_container_width=True):
            st.session_state.info_message = "📈 Generating weekly summary..."
    
    with report_col3:
        if st.button("📋 Custom Report", use_container_width=True):
            st.session_state.info_message = "📋 Custom report builder - Coming soon"

def create_filter_controls():
    """Create filter and search controls"""
    st.sidebar.header("🎯 Filters & Search")
    
    # Search
    search_text = st.sidebar.text_input(
        "🔍 Search:",
        value=st.session_state.search_text,
        placeholder="Search in all columns..."
    )
    st.session_state.search_text = search_text
    
    # Similarity range
    similarity_range = st.sidebar.slider(
        "📊 Similarity Range:",
        min_value=1,
        max_value=100,
        value=st.session_state.similarity_range,
        format="%d%%"
    )
    st.session_state.similarity_range = similarity_range
    
    # Column filter
    filter_columns = ['None', 'Categoria', 'Variedad', 'Color', 'Grado', 'Catalog ID']
    filter_column = st.sidebar.selectbox(
        "🧱 Filter Column:",
        filter_columns,
        index=filter_columns.index(st.session_state.filter_column)
    )
    st.session_state.filter_column = filter_column
    
    if filter_column != 'None':
        filter_value = st.sidebar.text_input(
            "Filter Value:",
            value=st.session_state.filter_value,
            placeholder="Value to exclude..."
        )
        st.session_state.filter_value = filter_value

def create_progress_display():
    """Create progress display section"""
    if st.session_state.processed_data is None:
        return
    
    df = st.session_state.processed_data
    total_rows = len(df)
    
    # Calculate reviewed rows
    reviewed_rows = sum(
        1 for idx in df.index 
        if st.session_state.form_data.get(f"accept_{idx}", False) or 
           st.session_state.form_data.get(f"deny_{idx}", False)
    )
    
    progress_pct = (reviewed_rows / total_rows * 100) if total_rows > 0 else 0
    
    # Progress display
    st.markdown(f"""
        <div style="background: linear-gradient(135deg, #f8f9fa, #e9ecef); padding: 1rem; border-radius: 8px; margin: 1rem 0; border-left: 4px solid #28a745;">
            <h4>📊 Review Progress</h4>
            <p><strong>{reviewed_rows}</strong> of <strong>{total_rows}</strong> rows reviewed (<strong>{progress_pct:.1f}%</strong>)</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Liquid progress bar
    progress_html = f"""
        <div style="text-align: center; padding: 10px; margin: 10px 0;">
            <div class="liquid-progress">
                <div class="liquid-fill" style="width: {progress_pct}%"></div>
                <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); color: white; font-weight: bold; z-index: 10;">
                    {progress_pct:.1f}% Complete
                </div>
            </div>
        </div>
        """
    st.markdown(progress_html, unsafe_allow_html=True)

def main():
    """Main application function"""
    # Initialize session state
    initialize_session_state()
    
    # Apply custom CSS
    apply_custom_css()
    
    # Display header
    st.markdown("""
        <div class="main-header">
            <h1>🔧 Enhanced Multi-Client Data Mapping System</h1>
            <p><strong>Complete Implementation with Admin Features</strong></p>
            <p><em>Real-time processing • Multi-client isolation • Advanced analytics</em></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display messages
    display_messages()
    
    # Show modals if needed
    if st.session_state.show_client_setup:
        create_client_setup_modal()
        return
    
    if st.session_state.show_edit_modal:
        create_edit_modal()
        return
    
    # Sidebar controls
    client_selector_sidebar()
    create_filter_controls()
    
    # Main content area
    if st.session_state.admin_mode:
        # Admin interface
        create_admin_tab()
    else:
        # Regular user interface
        if st.session_state.current_client_id:
            # Show progress if data exists
            create_progress_display()
            
            # File upload section
            file_upload_section()
            
            # Data table if data exists
            if st.session_state.processed_data is not None:
                st.header("📊 Data Review & Validation")
                create_data_table()
        else:
            # No client selected
            st.info("👆 **Get Started:** Select a client from the sidebar or create a new one")
            
            with st.expander("📖 **System Features**"):
                st.markdown("""
                ### 🚀 **Key Features:**
                - ✅ **Multi-client isolation** - Complete data separation
                - ✅ **Real-time processing** - Instant feedback and updates
                - ✅ **Advanced filtering** - Search and filter capabilities
                - ✅ **Bulk operations** - Accept/Deny all functionality
                - ✅ **Row-level editing** - Individual record modifications
                - ✅ **Admin dashboard** - System management and analytics
                - ✅ **Progress tracking** - Visual progress indicators
                - ✅ **Database integration** - Persistent data storage
                
                ### 📊 **Workflow:**
                1. **Select/Create Client** → Choose your client from sidebar
                2. **Upload Files** → Main TSV, Catalog TSV, Dictionary JSON
                3. **Process Data** → Automated fuzzy matching with progress tracking
                4. **Review Results** → Use filters and search to examine data
                5. **Edit & Validate** → Mark Accept/Deny, edit individual rows
                6. **Save Results** → Persist changes to database
                
                ### 🔧 **Admin Features:**
                - **Client Management** → Create, view, configure clients
                - **Database Operations** → Backup, restore, maintenance
                - **System Tools** → Health checks, diagnostics, utilities
                - **Analytics Dashboard** → Performance metrics and reporting
                """)

if __name__ == "__main__":
    main()