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
from storage import save_output_to_disk
from enhanced_multi_client_database import (
    EnhancedMultiClientDatabase,
    create_enhanced_client_databases,
    get_client_staging_products,
    update_client_synonyms_blacklist,
    get_client_synonyms_blacklist
)
from row_level_processing import (
    RowLevelProcessor,
    reprocess_row,
    save_new_product,
    update_row_in_main_db
)

# Configure logging with poetic precision
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure Streamlit page with aesthetic sensibility
st.set_page_config(
    page_title="Enhanced Multi-Client Data Mapping Validation",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

class StreamlitState:
    """
    A crystalline container for application state, 
    managing the ephemeral dance of user interactions
    """
    
    @staticmethod
    def initialize():
        """Initialize the symphony of session state variables"""
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
            
            # Row processing harmonies
            'processing_single_row': False,
            'processed_row_id': None,
            'show_reprocess_modal': False,
            'reprocess_row_data': None,
            
            # Filtering melodies
            'current_page': 1,
            'search_text': '',
            'similarity_range': (1, 100),
            'exclusion_filters': {},
            'selected_exclusion_column': 'None',
            'exclusion_filter_value': '',
            'rows_per_page': 50,
            
            # Progress tracking rhythms
            'reviewed_count': 0,
            'total_rows': 0,
            'show_progress': True,
            'progress_percentage': 0.0,
            
            # File processing crescendos
            'uploaded_files': {},
            'processing_status': 'ready',
            
            # Bulk operations orchestrations
            'show_bulk_save_modal': False,
            'bulk_save_progress': 0,
            'bulk_save_status': 'ready',
            'bulk_save_results': {},
            
            # Message harmonics
            'success_message': '',
            'error_message': '',
            'info_message': '',
            'warning_message': ''
        }
        
        for var, default_value in session_vars.items():
            if var not in st.session_state:
                st.session_state[var] = default_value

class ClientManager:
    """
    The maestro of client orchestration, conducting the symphony 
    of multi-client database operations with elegant precision
    """
    
    @staticmethod
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
                    if client_id and client_id != 'db':  # Exclude base database
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
    
    @staticmethod
    def create_new_client(client_id: str) -> Tuple[bool, str]:
        """Birth a new client in the database cosmos with all required structures"""
        try:
            success, message = create_enhanced_client_databases(client_id)
            
            if success:
                # Refresh the available clients list
                ClientManager.load_available_clients()
                logger.info(f"Successfully created client: {client_id}")
                return True, f"Client '{client_id}' created successfully with enhanced database structure"
            else:
                logger.error(f"Failed to create client {client_id}: {message}")
                return False, message
                
        except Exception as e:
            error_msg = f"Error creating client {client_id}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

class DataProcessor:
    """
    The alchemist of data transformation, weaving raw input 
    into structured knowledge through sophisticated processing
    """
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.db = EnhancedMultiClientDatabase(client_id)
        self.row_processor = RowLevelProcessor(client_id)
    
    def process_uploaded_files(self, file1_content: bytes, file2_content: bytes, 
                             dict_content: bytes) -> Tuple[bool, Optional[pd.DataFrame]]:
        """Transform uploaded files into processed data symphony"""
        try:
            st.session_state.processing_status = "processing"
            st.session_state.progress_percentage = 0.0
            
            # Parse uploaded files with gentle error handling
            df1 = pd.read_csv(BytesIO(file1_content), delimiter="\t", dtype=str)
            df2 = pd.read_csv(BytesIO(file2_content), delimiter="\t", dtype=str)
            dict_data = json.loads(dict_content.decode('utf-8'))
            
            st.session_state.progress_percentage = 10.0
            
            # Create progress callback for real-time updates
            def progress_callback(progress_pct: float, message: str):
                st.session_state.progress_percentage = progress_pct
                st.session_state.processing_status = f"processing: {message}"
            
            # Process files with progress tracking
            result_df = process_files(df1, df2, dict_data, progress_callback)
            
            st.session_state.progress_percentage = 100.0
            st.session_state.processing_status = "completed"
            
            # Update session state with results
            st.session_state.processed_data = result_df
            st.session_state.total_rows = len(result_df)
            
            # Save to disk storage as backup
            output = BytesIO()
            result_df.to_csv(output, sep=";", index=False, encoding="utf-8")
            save_output_to_disk(output)
            
            logger.info(f"Successfully processed {len(result_df)} rows for client {self.client_id}")
            return True, result_df
            
        except Exception as e:
            st.session_state.processing_status = f"error: {str(e)}"
            logger.error(f"Error processing files: {str(e)}")
            return False, None
    
    def reprocess_single_row(self, row_data: Dict[str, Any], 
                           update_synonyms: bool = True) -> Tuple[bool, Dict[str, Any]]:
        """Reprocess a single row with renewed vigor and updated context"""
        try:
            success, updated_row = self.row_processor.reprocess_single_row(
                row_data, update_synonyms
            )
            
            if success:
                logger.info(f"Successfully reprocessed row with new similarity: {updated_row.get('Similarity %', 'N/A')}%")
            
            return success, updated_row
            
        except Exception as e:
            logger.error(f"Error reprocessing row: {str(e)}")
            return False, row_data

class UIComponents:
    """
    The artist of user interface, painting interactive elements 
    with careful attention to both form and function
    """
    
    @staticmethod
    def apply_custom_css():
        """Apply the aesthetic foundation with carefully crafted styles"""
        st.markdown("""
        <style>
        /* Main application styling */
        .main-header {
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        }
        
        /* Enhanced progress bars */
        .liquid-progress {
            width: 100%;
            height: 40px;
            background: linear-gradient(45deg, #f0f0f0, #e0e0e0);
            border-radius: 20px;
            overflow: hidden;
            position: relative;
            box-shadow: inset 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .liquid-fill {
            height: 100%;
            background: linear-gradient(45deg, #4caf50, #8bc34a, #4caf50, #8bc34a);
            background-size: 40px 40px;
            animation: liquid-flow 2s ease-in-out infinite;
            border-radius: 20px;
            transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        .progress-text {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            color: white;
            font-weight: bold;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.7);
            z-index: 10;
        }
        
        @keyframes liquid-flow {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        /* Modal styling */
        .modal-container {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border: 3px solid #007bff;
            border-radius: 20px;
            padding: 30px;
            margin: 25px 0;
            box-shadow: 0 15px 35px rgba(0, 123, 255, 0.2);
            animation: modal-fade-in 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        @keyframes modal-fade-in {
            from { opacity: 0; transform: translateY(-30px) scale(0.95); }
            to { opacity: 1; transform: translateY(0) scale(1); }
        }
        
        /* Enhanced button styling */
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
        
        /* Table row highlighting */
        .highlight-row {
            background: linear-gradient(135deg, #fff3e0, #ffe0b2);
            border-left: 4px solid #ff9800;
            border-radius: 8px;
            padding: 10px;
            margin: 5px 0;
            transition: all 0.3s ease;
        }
        
        .highlight-row:hover {
            transform: translateX(5px);
            box-shadow: 0 4px 15px rgba(255, 152, 0, 0.2);
        }
        
        /* Status indicators */
        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-success { background: linear-gradient(135deg, #4caf50, #8bc34a); color: white; }
        .status-warning { background: linear-gradient(135deg, #ff9800, #ffc107); color: white; }
        .status-error { background: linear-gradient(135deg, #f44336, #e57373); color: white; }
        .status-info { background: linear-gradient(135deg, #2196f3, #64b5f6); color: white; }
        
        /* Responsive design harmonics */
        @media (max-width: 768px) {
            .main-header { padding: 1rem; }
            .modal-container { padding: 20px; margin: 15px 0; }
        }
        </style>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def create_progress_bar(progress: float, text: str = "Processing") -> str:
        """Craft an animated progress visualization"""
        return f"""
        <div style="margin: 20px 0;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 10px;">
                <span style="font-weight: bold; color: #333;">{text}</span>
                <span style="color: #666;">{progress:.1f}%</span>
            </div>
            <div class="liquid-progress">
                <div class="liquid-fill" style="width: {progress}%"></div>
                <div class="progress-text">{progress:.1f}%</div>
            </div>
        </div>
        """
    
    @staticmethod
    def display_messages():
        """Present the symphony of user messages with appropriate emotional resonance"""
        if st.session_state.success_message:
            st.success(st.session_state.success_message)
            st.session_state.success_message = ''
        
        if st.session_state.error_message:
            st.error(st.session_state.error_message)
            st.session_state.error_message = ''
        
        if st.session_state.info_message:
            st.info(st.session_state.info_message)
            st.session_state.info_message = ''
        
        if st.session_state.warning_message:
            st.warning(st.session_state.warning_message)
            st.session_state.warning_message = ''

class SidebarOrchestrator:
    """
    The conductor of sidebar interactions, harmonizing client selection,
    file uploads, and filtering operations into a cohesive experience
    """
    
    @staticmethod
    def render_client_selection():
        """Orchestrate the client selection symphony"""
        st.sidebar.header("🏢 Client Management")
        
        # Load and display available clients
        available_clients = ClientManager.load_available_clients()
        
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
        else:
            st.sidebar.warning("No clients available")
        
        # Client management buttons
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            if st.button("➕ New Client", use_container_width=True):
                st.session_state.show_client_setup = True
                st.rerun()
        
        with col2:
            if st.button("🔄 Refresh", use_container_width=True):
                ClientManager.load_available_clients()
                st.rerun()
        
        # Display current client status
        if st.session_state.current_client_id:
            st.sidebar.success(f"📋 Active: **{st.session_state.current_client_id}**")
        else:
            st.sidebar.info("Please select a client to continue")
    
    @staticmethod
    def render_file_upload():
        """Compose the file upload interface with elegant simplicity"""
        if not st.session_state.current_client_id:
            st.sidebar.info("Select a client first")
            return
        
        st.sidebar.divider()
        st.sidebar.header("📁 File Upload")
        
        # File upload widgets
        file1 = st.sidebar.file_uploader("📄 Main TSV File", type=['tsv'], key="main_tsv")
        file2 = st.sidebar.file_uploader("📊 Catalog TSV File", type=['tsv'], key="catalog_tsv")
        dict_file = st.sidebar.file_uploader("📝 Dictionary JSON", type=['json'], key="dict_json")
        
        # Process files button
        if file1 and file2 and dict_file:
            if st.sidebar.button("🚀 Process Files", type="primary", use_container_width=True):
                processor = DataProcessor(st.session_state.current_client_id)
                
                with st.spinner("Processing files..."):
                    success, result_df = processor.process_uploaded_files(
                        file1.read(), file2.read(), dict_file.read()
                    )
                
                if success:
                    st.session_state.success_message = f"Successfully processed {len(result_df)} rows"
                    st.rerun()
                else:
                    st.session_state.error_message = "File processing failed"
        
        # Display processing status
        if st.session_state.processing_status != "ready":
            if st.session_state.processing_status == "processing":
                progress_html = UIComponents.create_progress_bar(
                    st.session_state.progress_percentage, "Processing Files"
                )
                st.sidebar.markdown(progress_html, unsafe_allow_html=True)
            elif st.session_state.processing_status == "completed":
                st.sidebar.success("✅ Processing completed!")
            elif st.session_state.processing_status.startswith("error"):
                st.sidebar.error(f"❌ {st.session_state.processing_status}")
    
    @staticmethod
    def render_advanced_filters():
        """Craft the advanced filtering interface with precision"""
        if not st.session_state.current_client_id:
            return
        
        st.sidebar.divider()
        st.sidebar.header("🎯 Advanced Filters")
        
        # Search text filter
        search_text = st.sidebar.text_input(
            "🔍 Search:",
            value=st.session_state.search_text,
            placeholder="Search across all columns..."
        )
        st.session_state.search_text = search_text
        
        # Similarity range filter
        similarity_range = st.sidebar.slider(
            "📊 Similarity Range:",
            min_value=1,
            max_value=100,
            value=st.session_state.similarity_range,
            help="Filter rows by similarity percentage"
        )
        st.session_state.similarity_range = similarity_range
        
        # Exclusion filters section
        st.sidebar.subheader("🚫 Exclusion Filters")
        
        filterable_columns = [
            "None", "vendor_product_description", "vendor_name", "cleaned_input",
            "best_match", "categoria", "variedad", "color", "grado", "catalog_id"
        ]
        
        exclusion_column = st.sidebar.selectbox(
            "Column:",
            filterable_columns,
            key="exclusion_column_select"
        )
        
        if exclusion_column != "None":
            exclusion_value = st.sidebar.text_input(
                "Value to exclude:",
                placeholder="Rows containing this will be hidden",
                key="exclusion_value_input"
            )
            
            if exclusion_value and st.sidebar.button("➕ Add Filter", use_container_width=True):
                if exclusion_column not in st.session_state.exclusion_filters:
                    st.session_state.exclusion_filters[exclusion_column] = []
                if exclusion_value not in st.session_state.exclusion_filters[exclusion_column]:
                    st.session_state.exclusion_filters[exclusion_column].append(exclusion_value)
                    st.rerun()
        
        # Display active exclusion filters
        if st.session_state.exclusion_filters:
            st.sidebar.subheader("Active Exclusions:")
            for column, values in st.session_state.exclusion_filters.items():
                for value in values:
                    col1, col2 = st.sidebar.columns([3, 1])
                    with col1:
                        st.write(f"🚫 {column}: {value}")
                    with col2:
                        if st.button("❌", key=f"remove_{column}_{value}"):
                            st.session_state.exclusion_filters[column].remove(value)
                            if not st.session_state.exclusion_filters[column]:
                                del st.session_state.exclusion_filters[column]
                            st.rerun()
        
        # Clear all exclusions
        if st.session_state.exclusion_filters:
            if st.sidebar.button("🗑️ Clear All", use_container_width=True):
                st.session_state.exclusion_filters = {}
                st.rerun()

class MainContentOrchestrator:
    """
    The master composer of main content, weaving together data tables,
    modals, and interactive elements into a harmonious user experience
    """
    
    @staticmethod
    def create_client_setup_modal():
        """Compose the new client creation modal with elegant precision"""
        if not st.session_state.show_client_setup:
            return
        
        st.markdown("""
            <div class="modal-container">
                <h3 style="text-align: center; color: #007bff; margin-bottom: 25px;">
                    🏢 Create New Client Database System
                </h3>
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
                        success, message = ClientManager.create_new_client(new_client_id)
                        
                        if success:
                            st.session_state.success_message = f"✅ {message}"
                            st.session_state.current_client_id = new_client_id
                            st.session_state.new_client_id = ''
                            st.session_state.show_client_setup = False
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.session_state.error_message = f"❌ {message}"
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.show_client_setup = False
                st.session_state.new_client_id = ''
                st.rerun()
    
    @staticmethod
    def create_edit_product_modal():
        """Orchestrate the product editing modal with comprehensive functionality"""
        if not st.session_state.show_edit_product_modal or st.session_state.edit_product_row_data is None:
            return
        
        st.markdown("""
            <div class="modal-container">
                <h3 style="text-align: center; color: #007bff; margin-bottom: 25px;">
                    ✏️ Edit Product Information
                </h3>
            </div>
            """, unsafe_allow_html=True)
        
        row_data = st.session_state.edit_product_row_data
        row_index = st.session_state.edit_product_row_index
        
        # Display context information
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        vendor_name = str(row_data.get('Vendor Name', 'N/A'))
        similarity = row_data.get('Similarity %', 'N/A')
        
        st.info(f"""
        **Editing Row {row_index}**
        
        **Product:** {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}  
        **Vendor:** {vendor_name}  
        **Similarity:** {similarity}%
        """)
        
        # Re-run fuzzy matching button
        if st.button("🔄 Re-run Fuzzy Match", use_container_width=True):
            processor = DataProcessor(st.session_state.current_client_id)
            
            with st.spinner("Re-processing row with updated synonyms and catalog..."):
                success, updated_row = processor.reprocess_single_row(row_data, True)
                
                if success:
                    st.session_state.edit_product_row_data = updated_row
                    
                    # Update main dataframe
                    if st.session_state.processed_data is not None:
                        df = st.session_state.processed_data
                        if row_index in df.index:
                            for key, value in updated_row.items():
                                if key in df.columns:
                                    df.loc[row_index, key] = value
                    
                    st.session_state.success_message = f"✅ Row re-processed! New similarity: {updated_row.get('Similarity %', 'N/A')}%"
                    st.rerun()
                else:
                    st.session_state.error_message = "❌ Failed to re-process row"
        
        # Edit form with enhanced layout
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🏷️ Product Classification**")
            
            categoria = st.text_input(
                "Category:",
                value=st.session_state.edit_categoria or str(row_data.get('Categoria', '')),
                key="modal_categoria_input"
            )
            st.session_state.edit_categoria = categoria
            
            variedad = st.text_input(
                "Variety:",
                value=st.session_state.edit_variedad or str(row_data.get('Variedad', '')),
                key="modal_variedad_input"
            )
            st.session_state.edit_variedad = variedad
        
        with col2:
            st.markdown("**🎨 Physical Attributes**")
            
            color = st.text_input(
                "Color:",
                value=st.session_state.edit_color or str(row_data.get('Color', '')),
                key="modal_color_input"
            )
            st.session_state.edit_color = color
            
            grado = st.text_input(
                "Grade:",
                value=st.session_state.edit_grado or str(row_data.get('Grado', '')),
                key="modal_grado_input"
            )
            st.session_state.edit_grado = grado
        
        # Word Action section for synonyms and blacklist
        st.markdown("---")
        st.markdown("**🛠️ Word Action Management**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            action = st.selectbox(
                "Action Type:",
                ["", "blacklist", "synonym"],
                index=["", "blacklist", "synonym"].index(st.session_state.edit_action or row_data.get("Action", "") or ""),
                key="modal_action_select"
            )
            st.session_state.edit_action = action
        
        with col2:
            if action == "blacklist":
                word = st.text_input(
                    "Blacklist Word:",
                    value=st.session_state.edit_word or row_data.get("Word", ""),
                    key="modal_word_input",
                    help="Word to be removed from future processing"
                )
            elif action == "synonym":
                current_word = st.session_state.edit_word or row_data.get("Word", "")
                if ":" in current_word:
                    parts = current_word.split(":", 1)
                    original = parts[0].strip('"')
                    replacement = parts[1].strip('"')
                else:
                    original = ""
                    replacement = ""
                
                synonym_col1, synonym_col2 = st.columns(2)
                with synonym_col1:
                    orig_word = st.text_input("Original:", value=original, key="orig_word")
                with synonym_col2:
                    repl_word = st.text_input("Replacement:", value=replacement, key="repl_word")
                
                word = f'"{orig_word}":"{repl_word}"' if orig_word and repl_word else ""
            else:
                word = ""
            
            st.session_state.edit_word = word
        
        # Action buttons with enhanced functionality
        st.markdown("---")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("💾 Update Row", type="primary", use_container_width=True):
                # Update main dataframe
                if st.session_state.processed_data is not None:
                    df = st.session_state.processed_data
                    if row_index in df.index:
                        df.loc[row_index, 'Categoria'] = categoria
                        df.loc[row_index, 'Variedad'] = variedad
                        df.loc[row_index, 'Color'] = color
                        df.loc[row_index, 'Grado'] = grado
                        df.loc[row_index, 'Action'] = action
                        df.loc[row_index, 'Word'] = word
                
                # Update database if possible
                try:
                    success, message = update_row_in_main_db(
                        st.session_state.current_client_id, 
                        row_index, 
                        {
                            'Categoria': categoria, 
                            'Variedad': variedad, 
                            'Color': color, 
                            'Grado': grado,
                            'Action': action,
                            'Word': word
                        }
                    )
                    
                    if success:
                        st.session_state.success_message = "✅ Row updated successfully!"
                    else:
                        st.session_state.warning_message = f"⚠️ Local update successful, database update failed: {message}"
                except Exception as e:
                    st.session_state.warning_message = f"⚠️ Local update successful, database update failed: {str(e)}"
                
                MainContentOrchestrator.close_edit_modal()
                st.rerun()
        
        with col2:
            if st.button("🆕 Save as New Product", use_container_width=True):
                try:
                    success, message = save_new_product(
                        st.session_state.current_client_id, 
                        row_data, 
                        categoria, 
                        variedad, 
                        color, 
                        grado, 
                        "streamlit_user"
                    )
                    
                    if success:
                        st.session_state.success_message = f"✅ {message}"
                    else:
                        st.session_state.error_message = f"❌ {message}"
                except Exception as e:
                    st.session_state.error_message = f"❌ Error saving new product: {str(e)}"
                
                time.sleep(1)
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset", use_container_width=True):
                st.session_state.edit_categoria = str(row_data.get('Categoria', ''))
                st.session_state.edit_variedad = str(row_data.get('Variedad', ''))
                st.session_state.edit_color = str(row_data.get('Color', ''))
                st.session_state.edit_grado = str(row_data.get('Grado', ''))
                st.session_state.edit_action = str(row_data.get('Action', ''))
                st.session_state.edit_word = str(row_data.get('Word', ''))
                st.rerun()
        
        with col4:
            if st.button("❌ Cancel", use_container_width=True):
                MainContentOrchestrator.close_edit_modal()
                st.rerun()
    
    @staticmethod
    def close_edit_modal():
        """Close the edit modal and reset related state"""
        st.session_state.show_edit_product_modal = False
        st.session_state.edit_product_row_data = None
        st.session_state.edit_product_row_index = None
        st.session_state.edit_categoria = ''
        st.session_state.edit_variedad = ''
        st.session_state.edit_color = ''
        st.session_state.edit_grado = ''
        st.session_state.edit_action = ''
        st.session_state.edit_word = ''
    
    @staticmethod
    def apply_data_filters(df: pd.DataFrame) -> pd.DataFrame:
        """Apply the constellation of filters to transform raw data into refined insights"""
        if df is None or len(df) == 0:
            return df
        
        filtered_df = df.copy()
        
        # Apply similarity filter with numerical conversion
        if "Similarity %" in filtered_df.columns:
            filtered_df["Similarity %"] = pd.to_numeric(filtered_df["Similarity %"], errors="coerce").fillna(0)
            min_sim, max_sim = st.session_state.similarity_range
            filtered_df = filtered_df[
                (filtered_df["Similarity %"] >= min_sim) & 
                (filtered_df["Similarity %"] <= max_sim)
            ]
        
        # Apply search filter across all columns
        if st.session_state.search_text:
            search_lower = st.session_state.search_text.lower()
            mask = filtered_df.astype(str).apply(
                lambda row: search_lower in row.to_string().lower(), axis=1
            )
            filtered_df = filtered_df[mask]
        
        # Apply exclusion filters
        for column, exclude_values in st.session_state.exclusion_filters.items():
            if column in filtered_df.columns:
                for exclude_value in exclude_values:
                    mask = ~filtered_df[column].astype(str).str.contains(
                        exclude_value, case=False, na=False
                    )
                    filtered_df = filtered_df[mask]
        
        # Sort by similarity descending, then by product description
        if "Similarity %" in filtered_df.columns:
            sort_columns = ["Similarity %"]
            sort_ascending = [False]
            
            if "Vendor Product Description" in filtered_df.columns:
                sort_columns.append("Vendor Product Description")
                sort_ascending.append(True)
            
            filtered_df = filtered_df.sort_values(by=sort_columns, ascending=sort_ascending)
        
        return filtered_df
    
    @staticmethod
    def create_data_table(df: pd.DataFrame):
        """Compose the main data table with interactive elements and elegant presentation"""
        if df is None or len(df) == 0:
            st.info("📊 No data available. Upload and process files to begin.")
            return
        
        # Apply filters
        filtered_df = MainContentOrchestrator.apply_data_filters(df)
        
        if len(filtered_df) == 0:
            st.warning("🔍 No data matches the current filters. Try adjusting your filter criteria.")
            return
        
        # Progress tracking
        total_rows = len(filtered_df)
        reviewed_rows = sum(
            1 for idx in filtered_df.index 
            if st.session_state.form_data.get(f"accept_{idx}", False) or 
               st.session_state.form_data.get(f"deny_{idx}", False)
        )
        
        progress_pct = (reviewed_rows / total_rows * 100) if total_rows > 0 else 0
        
        # Display progress information
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Rows", total_rows)
        with col2:
            st.metric("✅ Reviewed", reviewed_rows)
        with col3:
            st.metric("📈 Progress", f"{progress_pct:.1f}%")
        
        # Progress bar
        if total_rows > 0:
            progress_html = UIComponents.create_progress_bar(progress_pct, "Review Progress")
            st.markdown(progress_html, unsafe_allow_html=True)
        
        # Pagination
        rows_per_page = st.session_state.rows_per_page
        total_pages = (len(filtered_df) + rows_per_page - 1) // rows_per_page
        
        if total_pages > 1:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                current_page = st.selectbox(
                    f"Page (1-{total_pages})",
                    options=list(range(1, total_pages + 1)),
                    index=min(st.session_state.current_page - 1, total_pages - 1),
                    key="page_selector"
                )
                st.session_state.current_page = current_page
            
            start_idx = (current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page
            page_df = filtered_df.iloc[start_idx:end_idx].copy()
        else:
            page_df = filtered_df.copy()
        
        # Create the data table with enhanced interaction
        MainContentOrchestrator.render_interactive_table(page_df)
        
        # Bulk actions section
        MainContentOrchestrator.render_bulk_actions(page_df)
    
    @staticmethod
    def render_interactive_table(df: pd.DataFrame):
        """Render the interactive data table with inline editing capabilities"""
        st.markdown("### 📋 Data Review Table")
        
        # Table headers
        header_cols = st.columns([3, 3, 1, 2, 2, 2, 2, 2, 1, 1, 2, 1])
        headers = [
            "🧹 Cleaned Input", "🎯 Best Match", "📊 Sim%", "🏷️ Catalog ID",
            "📂 Category", "🌿 Variety", "🎨 Color", "⭐ Grade",
            "✅ Accept", "❌ Deny", "⚡ Actions", "📊 Status"
        ]
        
        for i, header in enumerate(headers):
            with header_cols[i]:
                st.markdown(f"**{header}**")
        
        st.markdown("---")
        
        # Render each row
        for idx, row in df.iterrows():
            MainContentOrchestrator.render_table_row(idx, row, header_cols)
    
    @staticmethod
    def render_table_row(idx: int, row: pd.Series, header_cols: list):
        """Render a single table row with comprehensive interaction capabilities"""
        row_cols = st.columns([3, 3, 1, 2, 2, 2, 2, 2, 1, 1, 2, 1])
        
        # Data columns with enhanced presentation
        with row_cols[0]:  # Cleaned Input
            cleaned_input = str(row.get('Cleaned input', ''))
            st.markdown(f'<div class="highlight-row">{cleaned_input[:50]}{"..." if len(cleaned_input) > 50 else ""}</div>', 
                       unsafe_allow_html=True)
        
        with row_cols[1]:  # Best Match
            best_match = str(row.get('Best match', ''))
            st.markdown(f'<div class="highlight-row">{best_match[:50]}{"..." if len(best_match) > 50 else ""}</div>', 
                       unsafe_allow_html=True)
        
        with row_cols[2]:  # Similarity
            similarity = pd.to_numeric(row.get('Similarity %', 0), errors="coerce")
            if similarity >= 90:
                badge_class = "status-success"
            elif similarity >= 70:
                badge_class = "status-warning"
            else:
                badge_class = "status-error"
            
            st.markdown(f'<span class="status-badge {badge_class}">{similarity}%</span>', 
                       unsafe_allow_html=True)
        
        with row_cols[3]:  # Catalog ID
            catalog_id = str(row.get('Catalog ID', ''))
            if catalog_id.strip() in ["111111.0", "111111"]:
                st.markdown('<span class="status-badge status-warning">New Product</span>', 
                           unsafe_allow_html=True)
            else:
                st.text(catalog_id)
        
        # Editable fields
        with row_cols[4]:  # Category
            st.text(str(row.get('Categoria', '')))
        
        with row_cols[5]:  # Variety
            st.text(str(row.get('Variedad', '')))
        
        with row_cols[6]:  # Color
            st.text(str(row.get('Color', '')))
        
        with row_cols[7]:  # Grade
            st.text(str(row.get('Grado', '')))
        
        # Action checkboxes with mutual exclusivity
        with row_cols[8]:  # Accept
            accept_key = f"accept_{idx}"
            db_accept = str(row.get("Accept Map", "")).strip().lower() == "true"
            accept = st.session_state.form_data.get(accept_key, db_accept)
            new_accept = st.checkbox("", value=accept, key=f"accept_cb_{idx}")
            st.session_state.form_data[accept_key] = new_accept
            
            # Mutual exclusivity
            if new_accept and st.session_state.form_data.get(f"deny_{idx}", False):
                st.session_state.form_data[f"deny_{idx}"] = False
        
        with row_cols[9]:  # Deny
            deny_key = f"deny_{idx}"
            db_deny = str(row.get("Deny Map", "")).strip().lower() == "true"
            deny = st.session_state.form_data.get(deny_key, db_deny)
            new_deny = st.checkbox("", value=deny, key=f"deny_cb_{idx}")
            st.session_state.form_data[deny_key] = new_deny
            
            # Mutual exclusivity
            if new_deny and st.session_state.form_data.get(f"accept_{idx}", False):
                st.session_state.form_data[f"accept_{idx}"] = False
        
        # Action buttons
        with row_cols[10]:
            action_col1, action_col2 = st.columns(2)
            
            with action_col1:
                if st.button("✏️", key=f"edit_{idx}", help="Edit product details"):
                    st.session_state.show_edit_product_modal = True
                    st.session_state.edit_product_row_data = row.to_dict()
                    st.session_state.edit_product_row_index = idx
                    
                    # Initialize edit values
                    st.session_state.edit_categoria = str(row.get('Categoria', ''))
                    st.session_state.edit_variedad = str(row.get('Variedad', ''))
                    st.session_state.edit_color = str(row.get('Color', ''))
                    st.session_state.edit_grado = str(row.get('Grado', ''))
                    st.session_state.edit_action = str(row.get('Action', ''))
                    st.session_state.edit_word = str(row.get('Word', ''))
                    
                    st.rerun()
            
            with action_col2:
                if st.button("🔄", key=f"reprocess_{idx}", help="Re-run fuzzy matching"):
                    processor = DataProcessor(st.session_state.current_client_id)
                    
                    with st.spinner("Re-processing..."):
                        success, updated_row = processor.reprocess_single_row(row.to_dict(), True)
                    
                    if success:
                        # Update the main dataframe
                        if st.session_state.processed_data is not None:
                            for key, value in updated_row.items():
                                if key in st.session_state.processed_data.columns:
                                    st.session_state.processed_data.loc[idx, key] = value
                        
                        st.session_state.success_message = f"✅ Row {idx} re-processed successfully!"
                        st.rerun()
                    else:
                        st.session_state.error_message = f"❌ Failed to re-process row {idx}"
        
        # Status indicators
        with row_cols[11]:
            status_indicators = []
            
            if st.session_state.form_data.get(f"accept_{idx}", False):
                status_indicators.append("✅")
            elif st.session_state.form_data.get(f"deny_{idx}", False):
                status_indicators.append("❌")
            else:
                status_indicators.append("⏳")
            
            st.markdown(" ".join(status_indicators))
        
        # Row separator
        st.markdown('<hr style="margin: 10px 0; border: 1px solid #eee;">', unsafe_allow_html=True)
    
    @staticmethod
    def render_bulk_actions(df: pd.DataFrame):
        """Orchestrate bulk actions with sophisticated progress tracking"""
        st.markdown("---")
        st.markdown("### ⚡ Bulk Actions")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("✅ Accept All Visible", use_container_width=True):
                for idx in df.index:
                    st.session_state.form_data[f"accept_{idx}"] = True
                    st.session_state.form_data[f"deny_{idx}"] = False
                st.session_state.success_message = f"✅ Marked {len(df)} rows as Accept"
                st.rerun()
        
        with col2:
            if st.button("❌ Deny All Visible", use_container_width=True):
                for idx in df.index:
                    st.session_state.form_data[f"accept_{idx}"] = False
                    st.session_state.form_data[f"deny_{idx}"] = True
                st.session_state.success_message = f"❌ Marked {len(df)} rows as Deny"
                st.rerun()
        
        with col3:
            if st.button("🔄 Clear All Selections", use_container_width=True):
                for idx in df.index:
                    st.session_state.form_data[f"accept_{idx}"] = False
                    st.session_state.form_data[f"deny_{idx}"] = False
                st.session_state.info_message = f"🔄 Cleared all selections for {len(df)} rows"
                st.rerun()
        
        with col4:
            # Count rows ready for save
            ready_rows = sum(
                1 for idx in df.index 
                if st.session_state.form_data.get(f"accept_{idx}", False) or 
                   st.session_state.form_data.get(f"deny_{idx}", False)
            )
            
            if st.button(f"💾 Save {ready_rows} Rows", 
                        type="primary", 
                        use_container_width=True,
                        disabled=ready_rows == 0):
                if ready_rows > 0:
                    st.session_state.show_bulk_save_modal = True
                    st.rerun()

def main():
    """
    The grand conductor of our application symphony,
    orchestrating all components into a harmonious whole
    """
    
    # Initialize the foundational elements
    StreamlitState.initialize()
    UIComponents.apply_custom_css()
    
    # Display the application header with poetic grandeur
    st.markdown("""
        <div class="main-header">
            <h1>🔧 Enhanced Multi-Client Data Mapping Validation System</h1>
            <p><strong>Harmonizing Data Processing with Elegant Precision</strong></p>
            <p><em>Where raw data transforms into structured knowledge through sophisticated algorithms</em></p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display contextual messages
    UIComponents.display_messages()
    
    # Render sidebar controls
    SidebarOrchestrator.render_client_selection()
    SidebarOrchestrator.render_file_upload()
    SidebarOrchestrator.render_advanced_filters()
    
    # Handle modal displays
    MainContentOrchestrator.create_client_setup_modal()
    MainContentOrchestrator.create_edit_product_modal()
    
    # Main content area
    if st.session_state.current_client_id:
        # Create tabbed interface for organized content
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Data Review", 
            "🆕 Staging Products", 
            "📝 Synonyms & Blacklist", 
            "📈 Analytics"
        ])
        
        with tab1:
            if st.session_state.processed_data is not None:
                MainContentOrchestrator.create_data_table(st.session_state.processed_data)
            else:
                st.info("📁 Upload and process files to begin data review")
                
                # Quick start guide
                with st.expander("📖 Quick Start Guide"):
                    st.markdown("""
                    ### 🚀 Getting Started:
                    
                    1. **Upload Files**: Use the sidebar to upload your TSV and JSON files
                    2. **Process Data**: Click "Process Files" to run fuzzy matching
                    3. **Review Results**: Use filters to focus on specific data subsets
                    4. **Edit Products**: Click the edit button to modify product details
                    5. **Bulk Actions**: Use bulk operations for efficient workflow
                    6. **Save Progress**: Use the save functionality to persist changes
                    
                    ### 🔧 Enhanced Features:
                    - **Row-level Processing**: Individual row reprocessing with updated synonyms
                    - **Advanced Filtering**: Exclusion filters on multiple columns
                    - **Product Staging**: Create new products with catalog_id: 111111
                    - **Real-time Updates**: Immediate feedback and validation
                    - **Client Isolation**: Complete data separation per client
                    """)
        
        with tab2:
            st.header("🆕 Staging Products to Create")
            
            try:
                staging_df = get_client_staging_products(st.session_state.current_client_id)
                
                if staging_df is not None and len(staging_df) > 0:
                    st.write(f"**Staging Products:** {len(staging_df)}")
                    st.dataframe(staging_df, use_container_width=True)
                    
                    # Download functionality
                    csv_data = staging_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Staging Products CSV",
                        data=csv_data,
                        file_name=f"staging_products_{st.session_state.current_client_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("No staging products found for this client")
                    
            except Exception as e:
                st.error(f"Error loading staging products: {str(e)}")
        
        with tab3:
            st.header("📝 Synonyms & Blacklist Management")
            
            try:
                current_data = get_client_synonyms_blacklist(st.session_state.current_client_id)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🔁 Current Synonyms")
                    if current_data['synonyms']:
                        for original, replacement in current_data['synonyms'].items():
                            st.write(f"• `{original}` → `{replacement}`")
                    else:
                        st.info("No synonyms configured")
                
                with col2:
                    st.subheader("🛑 Current Blacklist")
                    if current_data['blacklist']['input']:
                        for word in current_data['blacklist']['input']:
                            st.write(f"• `{word}`")
                    else:
                        st.info("No blacklist words configured")
                
                st.info("💡 Use the dedicated Synonyms Manager interface for full editing capabilities")
                
            except Exception as e:
                st.error(f"Error loading synonyms/blacklist: {str(e)}")
        
        with tab4:
            st.header("📈 Analytics Dashboard")
            st.info("Analytics functionality coming soon...")
            
            # Placeholder for future analytics
            if st.session_state.processed_data is not None:
                df = st.session_state.processed_data
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    avg_similarity = df['Similarity %'].astype(float).mean() if 'Similarity %' in df.columns else 0
                    st.metric("📊 Average Similarity", f"{avg_similarity:.1f}%")
                
                with col2:
                    high_similarity = len(df[df['Similarity %'].astype(float) >= 90]) if 'Similarity %' in df.columns else 0
                    st.metric("🎯 High Similarity (≥90%)", high_similarity)
                
                with col3:
                    needs_review = len(df[df['Similarity %'].astype(float) < 70]) if 'Similarity %' in df.columns else 0
                    st.metric("⚠️ Needs Review (<70%)", needs_review)
    
    else:
        # Welcome screen for new users
        st.info("👆 **Get Started:** Select or create a client in the sidebar")
        
        # Feature showcase
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            ### 🏢 Multi-Client Architecture
            - Complete data isolation
            - Enhanced database structure
            - Scalable client management
            """)
        
        with col2:
            st.markdown("""
            ### 🔍 Advanced Processing
            - Row-level fuzzy matching
            - Dynamic synonyms management
            - Staging products creation
            """)
        
        with col3:
            st.markdown("""
            ### ⚡ Powerful Features
            - Real-time filtering
            - Bulk operations
            - Progress tracking
            """)

if __name__ == "__main__":
    main()