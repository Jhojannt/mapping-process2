# enhanced_multi_client_streamlit_app_CORRECTED.py - FIXED VERSION

import streamlit as st
import pandas as pd
import json
import time
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configure page
st.set_page_config(
    page_title="Enhanced Multi-Client Data Mapping",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def initialize_session_state():
    """Initialize enhanced session state variables"""
    session_vars = {
        # Client management
        'current_client_id': None,
        'available_clients': [],
        'client_batches': [],
        'selected_batch': None,
        'show_client_setup': False,
        'new_client_id': '',
        
        # Data management
        'processed_data': None,
        'form_data': {},
        'dark_mode': False,
        'db_connection_status': None,
        
        # Enhanced modals
        'show_edit_product_modal': False,
        'edit_product_row_data': None,
        'edit_product_row_index': None,
        'edit_categoria': '',
        'edit_variedad': '',
        'edit_color': '',
        'edit_grado': '',
        
        # Row processing
        'processing_single_row': False,
        'processed_row_id': None,
        'show_reprocess_modal': False,
        'reprocess_row_data': None,
        
        # Filtering
        'current_page': 1,
        'search_text': '',
        'similarity_range': (1, 100),
        'exclusion_filters': {},
        'selected_exclusion_column': 'None',
        'exclusion_filter_value': '',
        
        # Messages
        'success_message': '',
        'error_message': '',
        'info_message': ''
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

# Initialize session state
initialize_session_state()

# Mock functions to replace imports that might not exist yet
def load_available_clients():
    """Mock function - replace with actual database call"""
    try:
        # Simulated client list
        clients = ["demo_client", "acme_corp", "test_company", "sample_client"]
        st.session_state.available_clients = clients
        return clients
    except Exception as e:
        st.error(f"Error loading clients: {str(e)}")
        return []

def create_enhanced_client_databases(client_id: str) -> Tuple[bool, str]:
    """Mock function - replace with actual database creation"""
    try:
        # Simulate database creation
        time.sleep(2)  # Simulate processing time
        return True, f"Successfully created enhanced databases for {client_id}"
    except Exception as e:
        return False, f"Error creating databases: {str(e)}"

def reprocess_row(client_id: str, row_data: Dict[str, Any], update_synonyms: bool = True) -> Tuple[bool, Dict[str, Any]]:
    """Mock function - replace with actual row processing"""
    try:
        # Simulate row processing
        updated_row = row_data.copy()
        updated_row['Similarity %'] = min(100, int(row_data.get('Similarity %', 0)) + 5)
        updated_row['Best match'] = "Updated match after reprocessing"
        return True, updated_row
    except Exception as e:
        return False, row_data

def save_new_product(client_id: str, row_data: Dict[str, Any], categoria: str, variedad: str, 
                    color: str, grado: str, created_by: str = None) -> Tuple[bool, str]:
    """Mock function - replace with actual product saving"""
    try:
        # Simulate saving product
        return True, f"Product saved to staging: {categoria}, {variedad}, {color}, {grado}"
    except Exception as e:
        return False, f"Error saving product: {str(e)}"

def update_row_in_main_db(client_id: str, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Mock function - replace with actual database update"""
    try:
        # Simulate database update
        return True, f"Row {row_id} updated successfully"
    except Exception as e:
        return False, f"Error updating row: {str(e)}"

def get_client_staging_products(client_id: str):
    """Mock function - replace with actual staging products retrieval"""
    try:
        # Return mock staging products
        return pd.DataFrame({
            'categoria': ['Flowers', 'Plants'],
            'variedad': ['Roses', 'Succulents'],
            'color': ['Red', 'Green'],
            'grado': ['Premium', 'Standard'],
            'catalog_id': ['111111', '111111'],
            'status': ['pending', 'pending'],
            'created_at': ['2024-01-01', '2024-01-02']
        })
    except Exception as e:
        return None

def get_client_synonyms_blacklist(client_id: str) -> Dict[str, Any]:
    """Mock function - replace with actual synonyms/blacklist retrieval"""
    return {
        'synonyms': {"premium": "high quality", "standard": "regular"},
        'blacklist': {'input': ["and", "or", "the"]}
    }

def client_selector_sidebar():
    """Enhanced client selection and management in sidebar"""
    st.sidebar.header("🏢 Enhanced Client Management")
    
    # Load available clients
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
    
    # Show current client info
    if st.session_state.current_client_id:
        st.sidebar.success(f"📋 Active Client: **{st.session_state.current_client_id}**")
        
        # Enhanced filtering controls
        st.sidebar.divider()
        st.sidebar.header("🎯 Enhanced Filters")
        
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
        
        # Exclusion filters
        st.sidebar.subheader("🚫 Exclusion Filters")
        filterable_columns = [
            "None", "vendor_product_description", "vendor_name", "cleaned_input",
            "best_match", "categoria", "variedad", "color", "grado", "catalog_id"
        ]
        
        exclusion_column = st.sidebar.selectbox(
            "Column to filter:",
            filterable_columns,
            index=0,
            key="exclusion_column_select"
        )
        
        if exclusion_column != "None":
            exclusion_value = st.sidebar.text_input(
                "Value to exclude:",
                value="",
                placeholder="Rows containing this value will be hidden",
                key="exclusion_value_input"
            )
            
            if exclusion_value and st.sidebar.button("➕ Add Exclusion Filter", use_container_width=True):
                if exclusion_column not in st.session_state.exclusion_filters:
                    st.session_state.exclusion_filters[exclusion_column] = []
                if exclusion_value not in st.session_state.exclusion_filters[exclusion_column]:
                    st.session_state.exclusion_filters[exclusion_column].append(exclusion_value)
                    st.rerun()
        
        # Show active exclusion filters
        if st.session_state.exclusion_filters:
            st.sidebar.subheader("Active Exclusion Filters:")
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
        
        # Clear all exclusion filters
        if st.session_state.exclusion_filters and st.sidebar.button("🗑️ Clear All Exclusions", use_container_width=True):
            st.session_state.exclusion_filters = {}
            st.rerun()
    
    else:
        st.sidebar.warning("⚠️ Please select a client to continue")

def create_client_setup_modal():
    """Enhanced modal for creating new client"""
    if st.session_state.show_client_setup:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #e3f2fd, #f1f8e9); border: 3px solid #2196f3; border-radius: 15px; padding: 30px; margin: 25px 0;">
                <h3 style="text-align: center; color: #1976d2; margin-bottom: 25px;">🏢 Create Enhanced Client Database System</h3>
            </div>
            """, unsafe_allow_html=True)
        
        new_client_id = st.text_input(
            "Client ID:",
            value=st.session_state.new_client_id,
            placeholder="e.g., client_001, acme_corp, etc."
        )
        st.session_state.new_client_id = new_client_id
        
        if new_client_id:
            st.info(f"Enhanced Database System for Client: {new_client_id}")
            valid_id = len(new_client_id) >= 3 and new_client_id.replace('_', '').replace('-', '').isalnum()
        else:
            valid_id = False
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("🚀 Create Enhanced Client", type="primary", use_container_width=True, disabled=not valid_id):
                if valid_id:
                    with st.spinner(f"Creating enhanced database system for '{new_client_id}'..."):
                        success, message = create_enhanced_client_databases(new_client_id)
                        
                        if success:
                            st.success(f"✅ {message}")
                            st.session_state.current_client_id = new_client_id
                            st.session_state.new_client_id = ''
                            st.session_state.show_client_setup = False
                            load_available_clients()
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.show_client_setup = False
                st.session_state.new_client_id = ''
                st.rerun()

def create_edit_product_modal():
    """Enhanced modal for editing products"""
    if st.session_state.show_edit_product_modal and st.session_state.edit_product_row_data is not None:
        st.markdown("### ✏️ Edit Product Information")
        
        row_data = st.session_state.edit_product_row_data
        row_index = st.session_state.edit_product_row_index
        
        # Show current information
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        st.info(f"Editing: {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}")
        
        # Re-run fuzzy matching button
        if st.button("🔄 Re-run Fuzzy Match", use_container_width=True):
            with st.spinner("Re-processing row..."):
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
        
        # Edit form
        col1, col2 = st.columns(2)
        
        with col1:
            categoria = st.text_input("Category:", value=str(row_data.get('Categoria', '')))
            variedad = st.text_input("Variety:", value=str(row_data.get('Variedad', '')))
        
        with col2:
            color = st.text_input("Color:", value=str(row_data.get('Color', '')))
            grado = st.text_input("Grade:", value=str(row_data.get('Grado', '')))
        
        # Action buttons
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("💾 Update Row", type="primary", use_container_width=True):
                # Update dataframe
                if st.session_state.processed_data is not None:
                    df = st.session_state.processed_data
                    if row_index in df.index:
                        df.loc[row_index, 'Categoria'] = categoria
                        df.loc[row_index, 'Variedad'] = variedad
                        df.loc[row_index, 'Color'] = color
                        df.loc[row_index, 'Grado'] = grado
                
                # Mock database update
                success, message = update_row_in_main_db(st.session_state.current_client_id, row_index, {
                    'Categoria': categoria, 'Variedad': variedad, 'Color': color, 'Grado': grado
                })
                
                if success:
                    st.success("✅ Row updated successfully!")
                else:
                    st.error(f"❌ {message}")
                
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("🆕 Save New Product", use_container_width=True):
                success, message = save_new_product(
                    st.session_state.current_client_id, row_data, categoria, variedad, color, grado, "streamlit_user"
                )
                
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
                
                time.sleep(1)
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset", use_container_width=True):
                st.rerun()
        
        with col4:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.show_edit_product_modal = False
                st.session_state.edit_product_row_data = None
                st.session_state.edit_product_row_index = None
                st.rerun()

def apply_enhanced_filters(df):
    """Apply enhanced filters including exclusion filters"""
    if df is None or len(df) == 0:
        return df
    
    filtered_df = df.copy()
    
    # Apply similarity filter
    if "Similarity %" in filtered_df.columns:
        filtered_df["Similarity %"] = pd.to_numeric(filtered_df["Similarity %"], errors="coerce").fillna(0)
        min_sim, max_sim = st.session_state.similarity_range
        filtered_df = filtered_df[(filtered_df["Similarity %"] >= min_sim) & (filtered_df["Similarity %"] <= max_sim)]
    
    # Apply search filter
    if st.session_state.search_text:
        search_lower = st.session_state.search_text.lower()
        mask = filtered_df.astype(str).apply(lambda row: search_lower in row.to_string().lower(), axis=1)
        filtered_df = filtered_df[mask]
    
    # Apply exclusion filters
    for column, exclude_values in st.session_state.exclusion_filters.items():
        if column in filtered_df.columns:
            for exclude_value in exclude_values:
                mask = ~filtered_df[column].astype(str).str.contains(exclude_value, case=False, na=False)
                filtered_df = filtered_df[mask]
    
    return filtered_df

def create_enhanced_data_table(df):
    """Create enhanced data table with new action buttons"""
    if df is None or len(df) == 0:
        st.info("No data available")
        return
    
    # Display basic table
    for idx, row in df.iterrows():
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            product_desc = str(row.get('Vendor Product Description', ''))
            st.write(f"**Product:** {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}")
            st.write(f"**Similarity:** {row.get('Similarity %', 'N/A')}%")
        
        with col2:
            # Accept checkbox
            accept_key = f"accept_{idx}"
            accept = st.session_state.form_data.get(accept_key, False)
            st.session_state.form_data[accept_key] = st.checkbox("Accept", value=accept, key=f"accept_cb_{idx}")
        
        with col3:
            if st.button("✏️ Edit", key=f"edit_product_{idx}"):
                st.session_state.show_edit_product_modal = True
                st.session_state.edit_product_row_data = row.to_dict()
                st.session_state.edit_product_row_index = idx
                st.rerun()
        
        with col4:
            if st.button("💾 Update", key=f"update_{idx}"):
                st.success(f"✅ Updated row {idx}")
        
        st.markdown("---")

def load_sample_data():
    """Load sample data for testing"""
    sample_data = pd.DataFrame({
        'Vendor Product Description': [
            'Red roses premium grade A Ecuador export quality',
            'White lilies standard fresh cut flowers',
            'Yellow sunflowers large size Netherlands import',
            'Pink carnations grade B Kenya domestic market'
        ],
        'Vendor Name': ['FlowerCorp', 'BloomLtd', 'PetalInc', 'FloraMax'],
        'Cleaned input': ['red roses premium grade', 'white lilies standard', 'yellow sunflowers large', 'pink carnations grade'],
        'Best match': ['roses red premium grade', 'lilies white standard', 'sunflowers yellow large', 'carnations pink grade'],
        'Similarity %': [95, 87, 92, 78],
        'Catalog ID': ['CAT001', 'CAT002', 'CAT003', '111111'],
        'Categoria': ['Flowers', 'Flowers', 'Flowers', 'Flowers'],
        'Variedad': ['Roses', 'Lilies', 'Sunflowers', 'Carnations'],
        'Color': ['Red', 'White', 'Yellow', 'Pink'],
        'Grado': ['Premium', 'Standard', 'Large', 'B'],
        'Accept Map': ['', '', '', ''],
        'Deny Map': ['', '', '', '']
    })
    return sample_data

def main():
    """Enhanced main application function"""
    # Apply custom CSS
    st.markdown("""
    <style>
    .main-header {
        text-align: center;
        padding: 1rem;
        background: linear-gradient(135deg, #1e3c72, #2a5298);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .stButton > button {
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
        <div class="main-header">
            <h1>🔧 Enhanced Multi-Client Data Mapping System</h1>
            <p>CORRECTED VERSION - Ready to use</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display messages
    if st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ''
    
    if st.session_state.error_message:
        st.error(st.session_state.error_message)
        st.session_state.error_message = ''
    
    # Sidebar controls
    client_selector_sidebar()
    
    # Show modals
    if st.session_state.show_client_setup:
        create_client_setup_modal()
        return
    
    if st.session_state.show_edit_product_modal:
        create_edit_product_modal()
        return
    
    # Main content
    if st.session_state.current_client_id:
        st.markdown(f"""
        <div style="background: #f8f9fa; border: 2px solid #007bff; border-radius: 10px; padding: 15px; margin: 10px 0; text-align: center;">
            <h3>🏢 Active Client: {st.session_state.current_client_id}</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Create tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📁 File Upload", "📊 Data Review", "🆕 Staging Products", "📝 Synonyms/Blacklist"])
        
        with tab1:
            st.header("📁 File Upload")
            st.info("File upload functionality - Use existing logic from your current system")
            
            # Load sample data button for testing
            if st.button("📊 Load Sample Data for Testing"):
                st.session_state.processed_data = load_sample_data()
                st.success("✅ Sample data loaded!")
                st.rerun()
        
        with tab2:
            st.header("📊 Data Review")
            
            if st.session_state.processed_data is not None:
                df = st.session_state.processed_data
                filtered_df = apply_enhanced_filters(df)
                
                st.write(f"**Total Records:** {len(df)} | **Filtered:** {len(filtered_df)}")
                
                if len(filtered_df) > 0:
                    create_enhanced_data_table(filtered_df)
                else:
                    st.warning("🔍 No data matches the current filters")
            else:
                st.info("📊 Upload and process files to start reviewing data")
        
        with tab3:
            st.header("🆕 Staging Products to Create")
            staging_df = get_client_staging_products(st.session_state.current_client_id)
            
            if staging_df is not None and len(staging_df) > 0:
                st.write(f"**Staging Products:** {len(staging_df)}")
                st.dataframe(staging_df, use_container_width=True)
            else:
                st.info("No staging products found for this client")
        
        with tab4:
            st.header("📝 Synonyms & Blacklist Management")
            
            current_data = get_client_synonyms_blacklist(st.session_state.current_client_id)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🔁 Synonyms")
                if current_data['synonyms']:
                    for original, replacement in current_data['synonyms'].items():
                        st.write(f"• `{original}` → `{replacement}`")
                else:
                    st.info("No synonyms configured")
            
            with col2:
                st.subheader("🛑 Blacklist")
                if current_data['blacklist']['input']:
                    for word in current_data['blacklist']['input']:
                        st.write(f"• `{word}`")
                else:
                    st.info("No blacklist words configured")
            
            st.info("💡 Use the separate Synonyms Manager interface for full editing capabilities")
    
    else:
        st.info("👆 **Get Started:** Select or create a client in the sidebar")
        
        with st.expander("📖 **Quick Start Guide**"):
            st.markdown("""
            ### 🚀 **Getting Started:**
            
            1. **Select or Create Client** → Use sidebar to choose existing or create new
            2. **Load Sample Data** → Click "Load Sample Data" button for testing
            3. **Review Data** → Use enhanced filtering and row-level actions
            4. **Edit Products** → Click edit button to modify and save products
            5. **Manage Synonyms** → Use separate interface for synonyms/blacklist
            
            ### 🔧 **Enhanced Features:**
            - ✅ **Row-level Processing** - Individual row reprocessing
            - ✅ **Enhanced Filtering** - Exclusion filters on any column
            - ✅ **Product Staging** - Save new products with catalog_id: 111111
            - ✅ **Real-time Updates** - Immediate feedback and validation
            - ✅ **Client Isolation** - Complete data separation per client
            
            ### 📱 **Commands to Run:**
            ```bash
            # Main application
            streamlit run enhanced_multi_client_streamlit_app.py
            
            # Synonyms manager (separate port)
            streamlit run synonyms_blacklist_interface.py --server.port 8502
            ```
            """)

if __name__ == "__main__":
    main()