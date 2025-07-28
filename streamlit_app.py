# streamlit_app.py - Enhanced with individual row database operations and verification
import streamlit as st
import pandas as pd
import json
from io import BytesIO
import time
import logging
from logic import process_files
from ulits import classify_missing_words
from storage import save_output_to_disk, load_output_from_disk
from database_integration import (
    save_processed_data_to_database, 
    load_processed_data_from_database,
    MappingDatabase,
    test_database_connection,
    insert_single_row_to_database,
    verify_row_in_database,
    get_database_table_structure
)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configure page
st.set_page_config(
    page_title="Data Mapping Validation",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'form_data' not in st.session_state:
    st.session_state.form_data = {}
if 'dark_mode' not in st.session_state:
    st.session_state.dark_mode = False
if 'db_connection_status' not in st.session_state:
    st.session_state.db_connection_status = None
if 'save_progress' not in st.session_state:
    st.session_state.save_progress = 0
if 'show_confirmation_modal' not in st.session_state:
    st.session_state.show_confirmation_modal = False
if 'row_to_insert' not in st.session_state:
    st.session_state.row_to_insert = None
if 'show_db_columns' not in st.session_state:
    st.session_state.show_db_columns = False
if 'inserted_rows' not in st.session_state:
    st.session_state.inserted_rows = set()
if 'verification_results' not in st.session_state:
    st.session_state.verification_results = {}
if 'pending_insert_row' not in st.session_state:
    st.session_state.pending_insert_row = None
if 'pending_insert_data' not in st.session_state:
    st.session_state.pending_insert_data = None
if 'pending_verify_row' not in st.session_state:
    st.session_state.pending_verify_row = None
if 'pending_verify_data' not in st.session_state:
    st.session_state.pending_verify_data = None

def check_database_connection():
    """Test database connection and update session state"""
    try:
        success, message = test_database_connection()
        if success:
            st.session_state.db_connection_status = "connected"
            return True
        else:
            st.session_state.db_connection_status = f"failed: {message}"
            return False
    except Exception as e:
        st.session_state.db_connection_status = f"error: {str(e)}"
        return False

def insert_single_row_to_database(row_data, row_index):
    """Insert a single row to the database"""
    try:
        # Remove the _index field that's not part of the actual data
        clean_row_data = {k: v for k, v in row_data.items() if k != '_index'}
        
        # Use the enhanced database function
        from database_integration import insert_single_row_to_database as db_insert_single_row
        success, message = db_insert_single_row(clean_row_data)
        
        if success:
            st.session_state.inserted_rows.add(row_index)
            return True, message
        else:
            return False, message
            
    except Exception as e:
        return False, f"Error inserting row: {str(e)}"

def get_database_columns():
    """Get database table structure"""
    try:
        from database_integration import get_database_table_structure
        return get_database_table_structure()
    except Exception as e:
        st.error(f"Error fetching database columns: {str(e)}")
        return None

def apply_custom_css():
    """Apply custom styling with enhanced modal and button styles"""
    theme = "dark" if st.session_state.dark_mode else "light"
    
    css = f"""
    <style>
    .main-header {{
        text-align: center;
        padding: 1rem;
        background: {'#1e1e1e' if theme == 'dark' else '#f0f8ff'};
        border-radius: 10px;
        margin-bottom: 2rem;
    }}
    
    .filter-container {{
        background: {'#2a2a2a' if theme == 'dark' else '#ffffff'};
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid {'#444' if theme == 'dark' else '#dee2e6'};
        margin-bottom: 1rem;
    }}
    
    .progress-container {{
        background: {'#1e1e1e' if theme == 'dark' else '#f8f9fa'};
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border-left: 4px solid #28a745;
    }}
    
    .database-status {{
        padding: 0.5rem;
        border-radius: 6px;
        margin-bottom: 1rem;
        font-weight: bold;
        text-align: center;
    }}
    
    .db-connected {{
        background: linear-gradient(135deg, #28a74522, #20c99722);
        border: 1px solid #28a745;
        color: #28a745;
    }}
    
    .db-failed {{
        background: linear-gradient(135deg, #dc354522, #c8211022);
        border: 1px solid #dc3545;
        color: #dc3545;
    }}
    
    .db-testing {{
        background: linear-gradient(135deg, #ffc10722, #fd7e1422);
        border: 1px solid #ffc107;
        color: #ffc107;
    }}
    
    .db-columns-container {{
        background: {'#2a2a2a' if theme == 'dark' else '#f8f9fa'};
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border-left: 4px solid #007bff;
    }}
    
    .db-column {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem;
        margin: 0.2rem 0;
        background: {'#333' if theme == 'dark' else '#fff'};
        border-radius: 4px;
        border: 1px solid {'#555' if theme == 'dark' else '#dee2e6'};
    }}
    
    /* Liquid Gradient Progress Bar */
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
    
    .processing-status {{
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, #2596be22, #ff7f0022);
        border-radius: 15px;
        border: 2px solid #2596be;
        margin: 20px 0;
    }}
    
    .save-progress {{
        text-align: center;
        padding: 15px;
        background: linear-gradient(135deg, #28a74522, #20c99722);
        border-radius: 12px;
        border: 2px solid #28a745;
        margin: 15px 0;
    }}
    
    /* Table styling */
    .data-table {{
        border-collapse: collapse;
        width: 100%;
        margin-top: 20px;
    }}
    
    .data-table th {{
        background: linear-gradient(135deg, #2596be, #1976d2);
        color: white;
        padding: 12px 8px;
        text-align: left;
        font-weight: bold;
        border: 1px solid #ddd;
        position: sticky;
        top: 0;
        z-index: 10;
    }}
    
    .data-table td {{
        padding: 8px;
        border: 1px solid #ddd;
        vertical-align: top;
    }}
    
    .data-table tr:nth-child(even) {{
        background-color: {'#2a2a2a' if theme == 'dark' else '#f9f9f9'};
    }}
    
    .data-table tr:hover {{
        background-color: {'#3a3a3a' if theme == 'dark' else '#f5f5f5'};
    }}
    
    .highlight-cell {{
        background-color: {'#3a3a1a' if theme == 'dark' else '#fffec6'} !important;
        font-weight: bold;
    }}
    
    .needs-product {{
        background-color: #ff7f0030 !important;
        color: #ff7f00;
        font-weight: bold;
    }}
    
    .similarity-high {{ color: #28a745; font-weight: bold; }}
    .similarity-medium {{ color: #ffc107; font-weight: bold; }}
    .similarity-low {{ color: #dc3545; font-weight: bold; }}
    
    /* Row action buttons */
    .row-action-btn {{
        padding: 6px 12px;
        border: none;
        border-radius: 6px;
        font-size: 12px;
        font-weight: bold;
        cursor: pointer;
        transition: all 0.3s ease;
        margin: 2px;
    }}
    
    .insert-btn {{
        background: linear-gradient(135deg, #28a745, #20c997);
        color: white;
        box-shadow: 0 2px 4px rgba(40, 167, 69, 0.3);
    }}
    
    .insert-btn:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(40, 167, 69, 0.4);
    }}
    
    .verify-btn {{
        background: linear-gradient(135deg, #007bff, #0056b3);
        color: white;
        box-shadow: 0 2px 4px rgba(0, 123, 255, 0.3);
    }}
    
    .verify-btn:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 123, 255, 0.4);
    }}
    
    .inserted-indicator {{
        background: linear-gradient(135deg, #28a745, #20c997);
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: bold;
    }}
    
    .verified-indicator {{
        background: linear-gradient(135deg, #17a2b8, #138496);
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 11px;
        font-weight: bold;
    }}
    
    /* Action buttons */
    .action-buttons {{
        display: flex;
        gap: 10px;
        margin: 20px 0;
        flex-wrap: wrap;
    }}
    
    .stButton > button {{
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s ease;
    }}
    
    .stButton > button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def create_liquid_progress_bar(progress, text="Processing..."):
    """Create animated liquid gradient progress bar"""
    progress_html = f"""
    <div class="processing-status">
        <h3>🌊 {text}</h3>
        <div class="liquid-progress">
            <div class="liquid-fill" style="width: {progress}%"></div>
            <div class="progress-text">{progress:.1f}%</div>
        </div>
    </div>
    """
    return progress_html

def create_save_progress_bar(progress, text="Saving..."):
    """Create save progress bar with green theme"""
    progress_html = f"""
    <div class="save-progress">
        <h4>💾 {text}</h4>
        <div class="liquid-progress">
            <div class="liquid-fill" style="width: {progress}%; background: linear-gradient(45deg, #28a745, #20c997, #28a745, #20c997);"></div>
            <div class="progress-text">{progress:.1f}%</div>
        </div>
    </div>
    """
    return progress_html

def database_status_widget():
    """Display database connection status widget"""
    if st.session_state.db_connection_status is None:
        status_class = "db-testing"
        status_text = "Database Status: Not Tested"
        icon = "🔍"
    elif st.session_state.db_connection_status == "connected":
        status_class = "db-connected"
        status_text = "Database Status: Connected"
        icon = "✅"
    elif st.session_state.db_connection_status.startswith("failed"):
        status_class = "db-failed"
        status_text = "Database Status: Connection Failed"
        icon = "❌"
    else:
        status_class = "db-failed"
        status_text = f"Database Status: {st.session_state.db_connection_status}"
        icon = "⚠️"
    
    st.markdown(
        f"""
        <div class="database-status {status_class}">
            {icon} {status_text}
        </div>
        """,
        unsafe_allow_html=True
    )

def show_database_columns():
    """Display database table structure"""
    if st.session_state.show_db_columns:
        columns = get_database_columns()
        if columns:
            st.markdown(
                """
                <div class="db-columns-container">
                    <h4>🗄️ Database Table Structure</h4>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            # Create columns display
            for col in columns:
                field_name = col[0]
                field_type = col[1]
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                key_info = col[3] if col[3] else ""
                default_val = col[4] if col[4] else ""
                
                st.markdown(
                    f"""
                    <div class="db-column">
                        <div><strong>{field_name}</strong></div>
                        <div>{field_type} {nullable} {key_info}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            st.error("Unable to fetch database structure. Check connection.")

def show_confirmation_modal():
    """Show confirmation modal for row insertion using Streamlit components"""
    if st.session_state.show_confirmation_modal and st.session_state.row_to_insert is not None:
        # Create a prominent confirmation section
        st.markdown("---")
        st.markdown("### 🗄️ Confirm Database Insert")
        
        # Show row details in an info box
        row_data = st.session_state.row_to_insert
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        vendor_name = str(row_data.get('Vendor Name', 'N/A'))
        best_match = str(row_data.get('Best match', 'N/A'))
        
        st.info(f"""
        **Are you sure you want to insert this row into the database?**
        
        **Product:** {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}  
        **Vendor:** {vendor_name}  
        **Best Match:** {best_match[:100]}{'...' if len(best_match) > 100 else ''}
        """)
        
        # Create confirmation buttons with unique, stable keys
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("✅ Confirm Insert", type="primary", use_container_width=True, key="modal_confirm_insert_btn"):
                # Perform the insertion
                row_index = st.session_state.row_to_insert.get('_index', 0)
                success, message = insert_single_row_to_database(
                    st.session_state.row_to_insert, 
                    row_index
                )
                
                if success:
                    st.success(f"✅ Row inserted successfully: {message}")
                else:
                    st.error(f"❌ Failed to insert row: {message}")
                
                # Close modal and clear state
                st.session_state.show_confirmation_modal = False
                st.session_state.row_to_insert = None
                
                # Wait a moment before rerun to avoid conflicts
                time.sleep(0.5)
                st.rerun()
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True, key="modal_cancel_insert_btn"):
                st.session_state.show_confirmation_modal = False
                st.session_state.row_to_insert = None
                st.rerun()
        
        st.markdown("---")

def process_files_with_progress(df1, df2, dictionary_json):
    """Process files with real-time progress tracking focused on main tqdm loop"""
    
    # Create progress containers
    progress_container = st.empty()
    status_container = st.empty()
    
    def progress_callback(progress_pct, message):
        """Callback function to update the liquid progress bar"""
        progress_container.markdown(
            create_liquid_progress_bar(progress_pct, message), 
            unsafe_allow_html=True
        )
    
    try:
        result_df = process_files(df1, df2, dictionary_json, progress_callback)
        time.sleep(1)
        progress_container.empty()
        status_container.success("✅ Processing completed successfully!")
        return result_df
        
    except Exception as e:
        progress_container.empty()
        status_container.error(f"❌ Error during processing: {str(e)}")
        raise e

def sidebar_controls():
    """Sidebar with upload, filter controls, and database status"""
    st.sidebar.header("📁 File Upload")
    
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
            if st.session_state.db_connection_status == "connected":
                try:
                    with st.spinner("Loading from database..."):
                        db_data = load_processed_data_from_database()
                        if db_data is not None and len(db_data) > 0:
                            st.session_state.processed_data = db_data
                            st.sidebar.success(f"✅ Loaded {len(db_data)} records from database")
                            st.rerun()
                        else:
                            st.sidebar.warning("⚠️ No data found in database")
                except Exception as e:
                    st.sidebar.error(f"❌ Error loading from database: {str(e)}")
            else:
                st.sidebar.error("❌ Database not connected")
    
    # Database columns toggle
    if st.sidebar.button("🗂️ Show/Hide DB Columns", use_container_width=True):
        st.session_state.show_db_columns = not st.session_state.show_db_columns
        st.rerun()
    
    st.sidebar.divider()
    
    # File upload section
    st.sidebar.markdown("**Required Files:**")
    file1 = st.sidebar.file_uploader("📄 Main TSV File", type=['tsv'], key="file1", help="Input data with product descriptions")
    file2 = st.sidebar.file_uploader("📊 Catalog TSV File", type=['tsv'], key="file2", help="Product catalog for matching")
    dictionary = st.sidebar.file_uploader("📝 Dictionary JSON", type=['json'], key="dict", help="Synonyms and blacklist configuration")
    
    # File validation info
    if file1:
        st.sidebar.success(f"✅ Main file: {file1.name}")
    if file2:
        st.sidebar.success(f"✅ Catalog file: {file2.name}")
    if dictionary:
        st.sidebar.success(f"✅ Dictionary: {dictionary.name}")
    
    if st.sidebar.button("🚀 Process Files", type="primary", use_container_width=True):
        if file1 and file2 and dictionary:
            try:
                # Load files
                df1 = pd.read_csv(file1, delimiter="\t", dtype=str)
                df2 = pd.read_csv(file2, delimiter="\t", dtype=str)
                dict_data = json.load(dictionary)
                
                # Validate file structure
                if df1.shape[1] < 13:
                    st.sidebar.error("❌ Main TSV must have at least 13 columns")
                    return None, None, None, None, None
                if df2.shape[1] < 6:
                    st.sidebar.error("❌ Catalog TSV must have at least 6 columns")
                    return None, None, None, None, None
                
                # Process with animated progress
                result_df = process_files_with_progress(df1, df2, dict_data)
                st.session_state.processed_data = result_df
                
                # Save to disk
                output = BytesIO()
                result_df.to_csv(output, sep=";", index=False, encoding="utf-8")
                save_output_to_disk(output)
                
                st.sidebar.success("✅ Files processed successfully!")
                st.rerun()
                
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
                st.sidebar.exception(e)
        else:
            st.sidebar.warning("⚠️ Please upload all required files")
    
    st.sidebar.divider()
    
    # Filter controls
    st.sidebar.header("🎯 Filters & Search")
    
    search_text = st.sidebar.text_input("🔍 Search", placeholder="Search in all columns...", key="search_filter")
    
    # Similarity range with slider
    st.sidebar.markdown("**Similarity Range:**")
    similarity_range = st.sidebar.slider(
        "Similarity %",
        min_value=1,
        max_value=100,
        value=(1, 100),
        key="similarity_range"
    )
    
    # Column filter
    st.sidebar.markdown("**Column Filter:**")
    filter_column = st.sidebar.selectbox(
        "Filter Column",
        ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"],
        key="filter_col"
    )
    
    filter_value = ""
    if filter_column != "None":
        filter_value = st.sidebar.text_input("Filter Value", placeholder="Value to exclude...", key="filter_val")
    
    # Theme toggle
    st.sidebar.divider()
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🌓 Theme", use_container_width=True):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()
    
    with col2:
        st.markdown(f"**{'🌙 Dark' if st.session_state.dark_mode else '☀️ Light'}**")
    
    return search_text, similarity_range[0], similarity_range[1], filter_column, filter_value

def apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value):
    """Apply filters to the dataframe"""
    filtered_df = df.copy()
    
    # Similarity filter
    if "Similarity %" in filtered_df.columns:
        filtered_df["Similarity %"] = pd.to_numeric(filtered_df["Similarity %"], errors="coerce").fillna(0)
        filtered_df = filtered_df[
            (filtered_df["Similarity %"] >= min_sim) & 
            (filtered_df["Similarity %"] <= max_sim)
        ]
        # Sort by similarity descending
        filtered_df = filtered_df.sort_values(by="Similarity %", ascending=False)
    
    # Search filter
    if search_text:
        mask = filtered_df.astype(str).apply(
            lambda row: search_text.lower() in row.to_string().lower(), axis=1
        )
        filtered_df = filtered_df[mask]
    
    # Column filter (exclude rows containing the filter value)
    if filter_column != "None" and filter_value:
        if filter_column in filtered_df.columns:
            mask = ~filtered_df[filter_column].astype(str).str.contains(
                filter_value, case=False, na=False
            )
            filtered_df = filtered_df[mask]
    
    return filtered_df

def export_data_with_progress():
    """Export data with progress tracking and database save option"""
    if st.session_state.processed_data is not None:
        export_df = st.session_state.processed_data.copy()
        
        # Update DataFrame with form data
        for idx in export_df.index:
            export_df.loc[idx, 'Accept Map'] = st.session_state.form_data.get(f"accept_{idx}", False)
            export_df.loc[idx, 'Deny Map'] = st.session_state.form_data.get(f"deny_{idx}", False)
            export_df.loc[idx, 'Action'] = st.session_state.form_data.get(f"action_{idx}", "")
            export_df.loc[idx, 'Word'] = st.session_state.form_data.get(f"word_{idx}", "")
            export_df.loc[idx, 'Categoria'] = st.session_state.form_data.get(f"categoria_{idx}", export_df.loc[idx, 'Categoria'])
            export_df.loc[idx, 'Variedad'] = st.session_state.form_data.get(f"variedad_{idx}", export_df.loc[idx, 'Variedad'])
            export_df.loc[idx, 'Color'] = st.session_state.form_data.get(f"color_{idx}", export_df.loc[idx, 'Color'])
            export_df.loc[idx, 'Grado'] = st.session_state.form_data.get(f"grado_{idx}", export_df.loc[idx, 'Grado'])
        
        # Create columns for export options
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV download
            output = BytesIO()
            export_df.to_csv(output, sep=";", index=False, encoding="utf-8")
            
            st.download_button(
                label="📥 Download CSV",
                data=output.getvalue(),
                file_name="confirmed_mappings.csv",
                mime="text/csv",
                key="download_btn",
                use_container_width=True
            )
        
        with col2:
            # Database save with progress
            if st.button("💾 Save All to Database", type="primary", use_container_width=True):
                if st.session_state.db_connection_status != "connected":
                    st.error("❌ Database not connected. Please test connection first.")
                    return
                
                # Create progress containers
                progress_container = st.empty()
                status_container = st.empty()
                
                try:
                    # Simulate save progress
                    total_rows = len(export_df)
                    batch_size = 100
                    batches = (total_rows + batch_size - 1) // batch_size
                    
                    progress_container.markdown(
                        create_save_progress_bar(0, f"Preparing to save {total_rows} records..."),
                        unsafe_allow_html=True
                    )
                    time.sleep(0.5)
                    
                    # Simulate batch processing with progress
                    for i in range(batches):
                        progress = ((i + 1) / batches) * 90  # 90% for processing
                        progress_container.markdown(
                            create_save_progress_bar(progress, f"Saving batch {i+1}/{batches}..."),
                            unsafe_allow_html=True
                        )
                        time.sleep(0.2)  # Simulate processing time
                    
                    # Actual database save
                    progress_container.markdown(
                        create_save_progress_bar(95, "Finalizing database save..."),
                        unsafe_allow_html=True
                    )
                    
                    success, message = save_processed_data_to_database(export_df)
                    
                    if success:
                        progress_container.markdown(
                            create_save_progress_bar(100, "Save completed successfully!"),
                            unsafe_allow_html=True
                        )
                        time.sleep(1)
                        progress_container.empty()
                        status_container.success(f"✅ Successfully saved {len(export_df)} records to database!")
                        # Mark all rows as inserted
                        for idx in export_df.index:
                            st.session_state.inserted_rows.add(idx)
                    else:
                        progress_container.empty()
                        status_container.error(f"❌ Failed to save to database: {message}")
                
                except Exception as e:
                    progress_container.empty()
                    status_container.error(f"❌ Error saving to database: {str(e)}")
                    logging.error(f"Database save error: {e}")

def create_data_table(df):
    """Create the main data table with individual insert functionality"""
    
    # Columns to hide (from your original views.py)
    columns_to_hide = [
        "Company Location", "Vendor ID", "Quantity", "Stems / Bunch", "Unit Type",
        "Staging ID", "Object Mapping ID", "Company ID", "User ID", "Product Mapping ID", "Email"
    ]
    
    # Get visible columns
    visible_cols = [col for col in df.columns if col not in columns_to_hide]
    
    # Ensure required columns exist
    for col in ["Accept Map", "Deny Map", "Action", "Word"]:
        if col not in df.columns:
            df[col] = ""
    
    # Progress tracking
    total_rows = len(df)
    reviewed_rows = sum(
        1 for idx in df.index 
        if st.session_state.form_data.get(f"accept_{idx}", False) or 
           st.session_state.form_data.get(f"deny_{idx}", False)
    )
    
    progress_pct = (reviewed_rows / total_rows * 100) if total_rows > 0 else 0
    
    # Progress display
    st.markdown(
        f"""
        <div class="progress-container">
            <h4>📊 Review Progress</h4>
            <p><strong>{reviewed_rows}</strong> of <strong>{total_rows}</strong> rows reviewed 
            (<strong>{progress_pct:.1f}%</strong>)</p>
        </div>
        """, 
        unsafe_allow_html=True
    )
    
    # Animated progress bar for review progress
    progress_html = f"""
    <div class="liquid-progress" style="margin-bottom: 20px;">
        <div class="liquid-fill" style="width: {progress_pct}%"></div>
        <div class="progress-text">{progress_pct:.1f}% Reviewed</div>
    </div>
    """
    st.markdown(progress_html, unsafe_allow_html=True)
    
    # Action buttons
    st.markdown('<div class="action-buttons">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([2, 2, 4])
    
    with col1:
        if st.button("✅ Accept All Visible", type="primary"):
            for idx in df.index:
                st.session_state.form_data[f"accept_{idx}"] = True
                st.session_state.form_data[f"deny_{idx}"] = False
            st.rerun()
    
    with col2:
        if st.button("❌ Clear All"):
            for idx in df.index:
                st.session_state.form_data[f"accept_{idx}"] = False
                st.session_state.form_data[f"deny_{idx}"] = False
                st.session_state.form_data[f"action_{idx}"] = ""
                st.session_state.form_data[f"word_{idx}"] = ""
            st.rerun()
    
    # Enhanced Save All button section
    with col3:
        if st.button("💾 Save All", use_container_width=True):
            export_data_with_progress()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Show database columns if toggled
    show_database_columns()
    
    # Pagination
    rows_per_page = 25  # Reduced for better performance and fewer widget conflicts
    total_pages = (len(df) + rows_per_page - 1) // rows_per_page
    
    if total_pages > 1:
        st.write(f"**Total rows:** {len(df)} | **Pages:** {total_pages}")
        
        # Use selectbox instead of number_input to avoid conflicts
        current_page = st.selectbox(
            f"Select Page (1-{total_pages})", 
            options=list(range(1, total_pages + 1)),
            index=st.session_state.get('current_page', 1) - 1,
            key="page_selector"
        )
        
        # Update session state
        st.session_state.current_page = current_page
        
        start_idx = (current_page - 1) * rows_per_page
        end_idx = start_idx + rows_per_page
        page_df = df.iloc[start_idx:end_idx].copy()
    else:
        page_df = df.copy()
        st.session_state.current_page = 1
    
    # Create HTML table with individual actions
    create_enhanced_html_table(page_df, visible_cols)

def create_enhanced_html_table(df, visible_cols):
    """Create HTML table with status indicators (buttons handled separately)"""
    
    # Table header
    header_cells = ''.join([f'<th>{col}</th>' for col in visible_cols if col not in ["Accept Map", "Deny Map", "Action", "Word"]])
    header_cells += '<th>Accept Map</th><th>Deny Map</th><th>Status</th>'
    
    table_html = f"""
    <table class="data-table">
        <thead>
            <tr>{header_cells}</tr>
        </thead>
        <tbody>
    """
    
    # Table rows with status indicators only
    for idx, row in df.iterrows():
        row_cells = ""
        
        for col in visible_cols:
            if col in ["Accept Map", "Deny Map", "Action", "Word"]:
                continue
                
            val = row.get(col, "")
            
            # Special handling for different cell types
            if col == "Catalog ID" and str(val).strip() == "111111.0":
                val = "needs to create product"
                cell_class = "needs-product"
            elif col in ["Cleaned input", "Best match", "Catalog ID"]:
                cell_class = "highlight-cell"
            elif col == "Similarity %":
                similarity = pd.to_numeric(val, errors="coerce")
                if similarity >= 90:
                    cell_class = "similarity-high"
                elif similarity >= 70:
                    cell_class = "similarity-medium"
                else:
                    cell_class = "similarity-low"
            else:
                cell_class = ""
            
            row_cells += f'<td class="{cell_class}">{val}</td>'
        
        # Accept/Deny status
        accept_status = "✅" if st.session_state.form_data.get(f"accept_{idx}", False) else "☐"
        deny_status = "❌" if st.session_state.form_data.get(f"deny_{idx}", False) else "☐"
        row_cells += f'<td>{accept_status}</td><td>{deny_status}</td>'
        
        # Status indicators cell
        status_cell = '<td style="min-width: 150px;">'
        
        # Check if row is already inserted
        if idx in st.session_state.inserted_rows:
            status_cell += '<span class="inserted-indicator">✅ INSERTED</span><br>'
        
        # Check verification status
        verification_key = f"verify_{idx}"
        if verification_key in st.session_state.verification_results:
            if st.session_state.verification_results[verification_key]:
                status_cell += '<span class="verified-indicator">🔍 IN DB</span>'
            else:
                status_cell += '<span style="background: #dc3545; color: white; padding: 4px 8px; border-radius: 4px; font-size: 11px;">🔍 NOT IN DB</span>'
        
        status_cell += '</td>'
        row_cells += status_cell
        
        table_html += f"<tr>{row_cells}</tr>"
    
    table_html += """
        </tbody>
    </table>
    """
    
    st.markdown(table_html, unsafe_allow_html=True)
    
    # Interactive buttons section using Streamlit native components
    st.markdown("### 🔧 Individual Row Actions")
    
    # Create a more organized layout for row actions
    rows_per_batch = 3  # Show 3 rows per horizontal line
    
    # Create a unique batch identifier to avoid key conflicts
    current_page = st.session_state.get('current_page', 1)
    batch_prefix = f"page_{current_page}_batch"
    
    for batch_idx in range(0, len(df), rows_per_batch):
        batch_df = df.iloc[batch_idx:batch_idx+rows_per_batch]
        cols = st.columns(len(batch_df))
        
        for col_idx, (row_idx, row) in enumerate(batch_df.iterrows()):
            with cols[col_idx]:
                # Create stable, unique identifiers
                stable_row_id = f"{batch_prefix}_{batch_idx}_{col_idx}_{row_idx}"
                
                # Row information
                product_desc = str(row.get('Vendor Product Description', 'N/A'))[:40]
                vendor_name = str(row.get('Vendor Name', 'N/A'))[:20]
                similarity = row.get('Similarity %', 'N/A')
                
                st.markdown(f"""
                **Row {row_idx}**  
                **Product:** {product_desc}...  
                **Vendor:** {vendor_name}  
                **Similarity:** {similarity}%
                """)
                
                # Action buttons with unique keys
                button_col1, button_col2 = st.columns(2)
                
                with button_col1:
                    insert_disabled = row_idx in st.session_state.inserted_rows
                    insert_key = f"insert_btn_{stable_row_id}"
                    
                    if st.button(
                        "💾 Insert", 
                        key=insert_key, 
                        help="Insert to Database",
                        disabled=insert_disabled,
                        use_container_width=True
                    ):
                        # Store action in session state instead of immediate rerun
                        st.session_state.pending_insert_row = row_idx
                        st.session_state.pending_insert_data = row.to_dict()
                        st.session_state.pending_insert_data['_index'] = row_idx
                
                with button_col2:
                    verify_key = f"verify_btn_{stable_row_id}"
                    
                    if st.button(
                        "🔍 Verify", 
                        key=verify_key, 
                        help="Verify in Database",
                        use_container_width=True
                    ):
                        # Store action in session state instead of immediate rerun
                        st.session_state.pending_verify_row = row_idx
                        st.session_state.pending_verify_data = row.to_dict()
                
                # Status display
                status_messages = []
                if row_idx in st.session_state.inserted_rows:
                    status_messages.append("✅ Inserted")
                
                verification_key = f"verify_{row_idx}"
                if verification_key in st.session_state.verification_results:
                    if st.session_state.verification_results[verification_key]:
                        status_messages.append("🔍 In DB")
                    else:
                        status_messages.append("🔍 Not in DB")
                
                if status_messages:
                    for msg in status_messages:
                        if "Inserted" in msg:
                            st.success(msg)
                        elif "In DB" in msg:
                            st.info(msg)
                        else:
                            st.error(msg)
                
                st.markdown("---")
    
def handle_pending_actions():
    """Handle pending insert and verify actions to avoid widget conflicts"""
    
    # Handle pending insert action
    if hasattr(st.session_state, 'pending_insert_row') and st.session_state.pending_insert_row is not None:
        row_idx = st.session_state.pending_insert_row
        row_data = st.session_state.pending_insert_data
        
        # Clear pending state
        st.session_state.pending_insert_row = None
        st.session_state.pending_insert_data = None
        
        # Show confirmation modal
        st.session_state.show_confirmation_modal = True
        st.session_state.row_to_insert = row_data
        st.rerun()
    
    # Handle pending verify action
    if hasattr(st.session_state, 'pending_verify_row') and st.session_state.pending_verify_row is not None:
        row_idx = st.session_state.pending_verify_row
        row_data = st.session_state.pending_verify_data
        
        # Clear pending state
        st.session_state.pending_verify_row = None
        st.session_state.pending_verify_data = None
        
        # Verify if row exists in database
        exists = verify_row_in_database(row_data)
        st.session_state.verification_results[f"verify_{row_idx}"] = exists
        
        if exists:
            st.success(f"✅ Row {row_idx} found in database")
        else:
            st.warning(f"⚠️ Row {row_idx} not found in database")
        
        st.rerun()

def create_row_form(idx, row):
    """Create form for individual row (similar to views.py)"""
    col1, col2, col3 = st.columns([4, 4, 4])
    
    with col1:
        st.markdown("**📝 Input Data**")
        st.text_area("Cleaned Input:", value=str(row.get('Cleaned input', '')), height=60, disabled=True, key=f"input_{idx}")
        st.text_area("Best Match:", value=str(row.get('Best match', '')), height=60, disabled=True, key=f"match_{idx}")
        
        similarity = pd.to_numeric(row.get('Similarity %', 0), errors="coerce")
        if similarity >= 90:
            st.success(f"🎯 Similarity: {similarity}%")
        elif similarity >= 70:
            st.warning(f"⚠️ Similarity: {similarity}%")
        else:
            st.error(f"❌ Similarity: {similarity}%")
    
    with col2:
        st.markdown("**📊 Catalog Info**")
        catalog_id = row.get('Catalog ID', '')
        if str(catalog_id).strip() == "111111.0":
            st.error("⚠️ **Needs to create product**")
        else:
            st.info(f"**ID:** {catalog_id}")
        
        # Editable fields
        deny_enabled = st.session_state.form_data.get(f"deny_{idx}", False)
        
        categoria = st.text_input(
            "Categoria:", 
            value=str(row.get('Categoria', '')), 
            disabled=not deny_enabled,
            key=f"categoria_{idx}"
        )
        
        variedad = st.text_input(
            "Variedad:", 
            value=str(row.get('Variedad', '')), 
            disabled=not deny_enabled,
            key=f"variedad_{idx}"
        )
        
        color = st.text_input(
            "Color:", 
            value=str(row.get('Color', '')), 
            disabled=not deny_enabled,
            key=f"color_{idx}"
        )
        
        grado = st.text_input(
            "Grado:", 
            value=str(row.get('Grado', '')), 
            disabled=not deny_enabled,
            key=f"grado_{idx}"
        )
    
    with col3:
        st.markdown("**⚡ Actions**")
        
        # Accept/Deny checkboxes (mutually exclusive)
        accept_key = f"accept_{idx}"
        deny_key = f"deny_{idx}"
        
        current_accept = st.session_state.form_data.get(accept_key, False)
        current_deny = st.session_state.form_data.get(deny_key, False)
        
        accept = st.checkbox(
            "✅ Accept Mapping", 
            value=current_accept,
            key=f"accept_cb_{idx}"
        )
        
        deny = st.checkbox(
            "❌ Deny Mapping", 
            value=current_deny,
            key=f"deny_cb_{idx}"
        )
        
        # Handle mutual exclusivity
        if accept and deny:
            st.error("⚠️ Cannot accept and deny simultaneously")
            accept = False
            deny = False
        elif accept and current_deny:
            deny = False
        elif deny and current_accept:
            accept = False
        
        st.session_state.form_data[accept_key] = accept
        st.session_state.form_data[deny_key] = deny
        
        # Additional fields for denied mappings
        if deny:
            st.markdown("**🔧 Denial Details**")
            action_key = f"action_{idx}"
            word_key = f"word_{idx}"
            
            action = st.selectbox(
                "Action Type:",
                ["", "blacklist", "synonym"],
                index=["", "blacklist", "synonym"].index(
                    st.session_state.form_data.get(action_key, "")
                ) if st.session_state.form_data.get(action_key, "") in ["", "blacklist", "synonym"] else 0,
                key=f"action_sel_{idx}"
            )
            
            if action in ["blacklist", "synonym"]:
                word = st.text_input(
                    f"Word for {action}:",
                    value=st.session_state.form_data.get(word_key, ""),
                    key=f"word_input_{idx}"
                )
                st.session_state.form_data[word_key] = word
            
            st.session_state.form_data[action_key] = action
        
        # Database actions for this row
        st.markdown("**🗄️ Database Actions**")
        
        col_a, col_b = st.columns(2)
        with col_a:
            insert_key = f"form_db_insert_{idx}_{hash(str(row.get('Vendor Product Description', ''))[:10])}"
            if st.button(f"💾 Insert", key=insert_key, 
                        disabled=(idx in st.session_state.inserted_rows),
                        help="Insert this row to database"):
                row_data = row.to_dict()
                row_data['_index'] = idx
                st.session_state.show_confirmation_modal = True
                st.session_state.row_to_insert = row_data
                st.rerun()
        
        with col_b:
            verify_key = f"form_db_verify_{idx}_{hash(str(row.get('Vendor Product Description', ''))[:10])}"
            if st.button(f"🔍 Verify", key=verify_key, 
                        help="Check if row exists in database"):
                row_data = row.to_dict()
                exists = verify_row_in_database(row_data)
                st.session_state.verification_results[f"verify_{idx}"] = exists
                
                if exists:
                    st.success("✅ Found in DB")
                else:
                    st.warning("⚠️ Not in DB")
                st.rerun()
        
        # Show insertion status
        if idx in st.session_state.inserted_rows:
            st.success("✅ **INSERTED TO DB**")
        
        # Show verification status
        verification_key = f"verify_{idx}"
        if verification_key in st.session_state.verification_results:
            if st.session_state.verification_results[verification_key]:
                st.info("🔍 **VERIFIED IN DB**")
            else:
                st.error("🔍 **NOT FOUND IN DB**")
        
        # Update form fields in session state
        st.session_state.form_data[f"categoria_{idx}"] = categoria
        st.session_state.form_data[f"variedad_{idx}"] = variedad  
        st.session_state.form_data[f"color_{idx}"] = color
        st.session_state.form_data[f"grado_{idx}"] = grado

def main():
    """Main application function"""
    apply_custom_css()
    
    # Initialize database connection status on first run
    if st.session_state.db_connection_status is None:
        check_database_connection()
    
    # Handle any pending actions first, before rendering new widgets
    if (hasattr(st.session_state, 'pending_insert_row') and st.session_state.pending_insert_row is not None) or \
       (hasattr(st.session_state, 'pending_verify_row') and st.session_state.pending_verify_row is not None):
        handle_pending_actions()
        return  # Exit early to avoid widget conflicts
    
    # Show confirmation modal if needed
    if st.session_state.show_confirmation_modal:
        show_confirmation_modal()
        return  # Exit early to avoid widget conflicts
    
    # Header
    st.markdown(
        """
        <div class="main-header">
            <h1>🔍 Data Mapping Validation System</h1>
            <p>Enhanced with individual row database operations and verification</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Sidebar controls
    search_text, min_sim, max_sim, filter_column, filter_value = sidebar_controls()
    
    # Main content
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Apply filters
        filtered_df = apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value)
        
        if len(filtered_df) > 0:
            create_data_table(filtered_df)
            
            # Only show detailed forms if no modal is active
            if not st.session_state.show_confirmation_modal:
                # Interactive form section
                st.markdown("---")
                st.markdown("### 📝 Detailed Row Forms")
                
                # Show forms for current page
                rows_per_page = 25
                current_page = st.session_state.get('current_page', 1)
                start_idx = (current_page - 1) * rows_per_page
                end_idx = start_idx + rows_per_page
                page_df = filtered_df.iloc[start_idx:end_idx]
                
                for idx, row in page_df.iterrows():
                    # Create stable key for expander
                    expander_key = f"expander_{current_page}_{idx}_{hash(str(row.get('Vendor Product Description', ''))[:20])}"
                    with st.expander(f"Row {idx}: {str(row.get('Vendor Product Description', 'N/A'))[:50]}...", key=expander_key):
                        create_row_form(idx, row)
        else:
            st.warning("🔍 No data matches the current filters")
            st.info("Try adjusting your filter criteria in the sidebar")
    else:
        # Try to load from disk
        try:
            saved_data = load_output_from_disk()
            if saved_data:
                df = pd.read_csv(saved_data, sep=";", dtype=str)
                st.session_state.processed_data = df
                st.info("📂 Loaded previously processed data from disk")
                st.rerun()
        except Exception as e:
            pass
        
        # Instructions for first-time users
        st.info("👆 **Get Started:** Upload your files using the sidebar")
        
        with st.expander("📖 **How to use this application**"):
            st.markdown("""
            ### File Requirements:
            1. **Main TSV File:** Your input data with product descriptions (minimum 13 columns)
            2. **Catalog TSV File:** Your product catalog for matching (minimum 6 columns)  
            3. **Dictionary JSON:** Synonyms and blacklist configuration
            
            ### Enhanced Features:
            - **Individual Row Operations**: Insert specific rows to database with confirmation
            - **Row Verification**: Check if individual rows exist in the database
            - **Database Column Display**: View the complete database table structure
            - **Status Tracking**: Visual indicators for inserted and verified rows
            - **Confirmation Modals**: Safe insertion with user confirmation
            
            ### Process:
            1. Upload all three files in the sidebar
            2. Test database connection using sidebar controls
            3. Process files and review mappings
            4. Use individual row "Insert" buttons for selective database operations
            5. Use "Verify" buttons to check database presence
            6. View database structure using "Show/Hide DB Columns"
            7. Bulk operations available via "Save All" button
            
            ### Database Operations:
            - **💾 Insert Button**: Insert individual rows with confirmation modal
            - **🔍 Verify Button**: Check if row exists in database
            - **✅ INSERTED**: Green indicator for successfully inserted rows
            - **🔍 IN DB / NOT IN DB**: Verification status indicators
            """)

if __name__ == "__main__":
    main()