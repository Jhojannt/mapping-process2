# streamlit_app.py
import streamlit as st
import pandas as pd
import json
from io import BytesIO
import time
from logic import process_files
from ulits import classify_missing_words
from storage import save_output_to_disk, load_output_from_disk
from database_integration import save_processed_data_to_database, load_processed_data_from_database

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

def apply_custom_css():
    """Apply custom styling with liquid gradient progress bar"""
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
        # Call the enhanced process_files with progress callback
        # This will handle 90% of progress during "Procesando coincidencias"
        result_df = process_files(df1, df2, dictionary_json, progress_callback)
        
        # Brief pause to show completion
        time.sleep(1)
        
        # Clear progress and show success
        progress_container.empty()
        status_container.success("✅ Procesamiento completado exitosamente!")
        
        return result_df
        
    except Exception as e:
        progress_container.empty()
        status_container.error(f"❌ Error durante el procesamiento: {str(e)}")
        raise e

def sidebar_controls():
    """Sidebar with upload and filter controls"""
    st.sidebar.header("📁 File Upload")
    
    # File upload section with better layout
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

def create_data_table(df):
    """Create the main data table similar to views.py"""
    
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
    col1, col2, col3, col4 = st.columns([2, 2, 2, 4])
    
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
    
    with col3:
        if st.button("💾 Save All"):
            export_data()
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Pagination
    rows_per_page = 100
    total_pages = (len(df) + rows_per_page - 1) // rows_per_page
    
    if total_pages > 1:
        st.write(f"**Total rows:** {len(df)} | **Pages:** {total_pages}")
        page = st.number_input(
            f"Page (1-{total_pages})", 
            min_value=1, 
            max_value=total_pages, 
            value=1,
            key="current_page"
        )
        
        start_idx = (page - 1) * rows_per_page
        end_idx = start_idx + rows_per_page
        page_df = df.iloc[start_idx:end_idx].copy()
    else:
        page_df = df.copy()
    
    # Create HTML table similar to your views.py
    create_html_table(page_df, visible_cols)

def create_html_table(df, visible_cols):
    """Create HTML table similar to views.py interface"""
    
    # Table header
    header_cells = ''.join([f'<th>{col}</th>' for col in visible_cols if col not in ["Accept Map", "Deny Map", "Action", "Word"]])
    header_cells += '<th>Accept Map</th><th>Deny Map</th><th>Details</th>'
    
    table_html = f"""
    <table class="data-table">
        <thead>
            <tr>{header_cells}</tr>
        </thead>
        <tbody>
    """
    
    # Table rows
    for idx, row in df.iterrows():
        row_cells = ""
        
        for col in visible_cols:
            if col in ["Accept Map", "Deny Map", "Action", "Word"]:
                continue
                
            val = row.get(col, "")
            
            # Special handling for Catalog ID
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
        
        table_html += f"<tr>{row_cells}</tr>"
    
    table_html += """
        </tbody>
    </table>
    """
    
    st.markdown(table_html, unsafe_allow_html=True)
    
    # Interactive form below table for each row
    st.markdown("### 📝 Row Actions")
    
    for idx, row in df.iterrows():
        with st.expander(f"Row {idx}: {str(row.get('Cleaned input', 'N/A'))[:50]}..."):
            create_row_form(idx, row)

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
        
        # Editable fields (like in views.py)
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
        
        # Additional fields for denied mappings (like in views.py)
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
        
        # Update form fields in session state
        st.session_state.form_data[f"categoria_{idx}"] = categoria
        st.session_state.form_data[f"variedad_{idx}"] = variedad  
        st.session_state.form_data[f"color_{idx}"] = color
        st.session_state.form_data[f"grado_{idx}"] = grado

def export_data():
    """Export the current mappings to CSV"""
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
        
        # Create download
        output = BytesIO()
        export_df.to_csv(output, sep=";", index=False, encoding="utf-8")
        
        st.download_button(
            label="📥 Download Confirmed Mappings CSV",
            data=output.getvalue(),
            file_name="confirmed_mappings.csv",
            mime="text/csv",
            key="download_btn",
            use_container_width=True
        )

def main():
    """Main application function"""
    apply_custom_css()
    
    # Header
    st.markdown(
        """
        <div class="main-header">
            <h1>🔍 Data Mapping Validation System</h1>
            <p>Upload your TSV files and JSON dictionary to begin the mapping process</p>
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
            
            ### Process:
            1. Upload all three files in the sidebar
            2. Click "🚀 Process Files" to run fuzzy matching
            3. Watch the beautiful liquid progress bar during "Procesando coincidencias"
            4. Review and validate the mappings using the table interface
            5. Accept/deny mappings and edit catalog fields
            6. Add synonyms/blacklist entries for denied mappings
            7. Export your results as CSV
            
            ### Features:
            - **Liquid Progress Bar**: Beautiful animated progress during processing
            - **Table View**: Similar to your original views.py interface  
            - **Filtering**: Similarity range, text search, column exclusions
            - **Pagination**: Handle large datasets efficiently
            - **Form Persistence**: Your changes are saved as you work
            - **Dark/Light Theme**: Toggle between themes for comfort
            """)

if __name__ == "__main__":
    main()