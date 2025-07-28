# Updated streamlit_app.py with inline action integration

import streamlit as st
import pandas as pd
import json
from io import BytesIO
import uuid
import time
import logging
from logic import process_files
from ulits import classify_missing_words
from storage import save_output_to_disk
from database_integration import (
    load_processed_data_from_database,
    MappingDatabase,
    test_database_connection
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

# Initialize session state variables
def initialize_session_state():
    """Initialize all session state variables"""
    session_vars = {
        'processed_data': None,
        'form_data': {},
        'dark_mode': False,
        'db_connection_status': None,
        'save_progress': 0,
        'show_confirmation_modal': False,
        'row_to_insert': None,
        'show_db_columns': False,
        'inserted_rows': set(),
        'verification_results': {},
        'pending_insert_row': None,
        'pending_insert_data': None,
        'show_edit_modal': False,
        'edit_row_data': None,
        'edit_row_index': None,
        'edit_categoria': '',
        'edit_variedad': '',
        'edit_color': '',
        'edit_grado': '',
        'current_page': 1
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

# Initialize session state
initialize_session_state()

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

def insert_single_row_to_database_app(row_data, row_index):
    """Insert a single row to the database with app-specific handling"""
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

def apply_custom_css():
    """Apply comprehensive custom styling"""
    theme = "dark" if st.session_state.dark_mode else "light"
    
    css = f"""
    <style>
    /* Main container styling */
    .main-header {{
        text-align: center;
        padding: 1rem;
        background: {'#1e1e1e' if theme == 'dark' else '#f0f8ff'};
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
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
    
    /* Database status styling */
    .database-status {{
        padding: 0.5rem;
        border-radius: 6px;
        margin-bottom: 1rem;
        font-weight: bold;
        text-align: center;
        transition: all 0.3s ease;
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
    
    /* Edit modal styling */
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
    
    /* Button styling */
    .stButton > button {{
        border-radius: 8px;
        font-weight: bold;
        transition: all 0.3s ease;
        border: none;
    }}
    
    .stButton > button:hover {{
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
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
    
    /* Action buttons specific styling */
    .action-buttons {{
        display: flex;
        gap: 10px;
        margin: 20px 0;
        flex-wrap: wrap;
    }}
    
    /* Responsive design */
    @media (max-width: 768px) {{
        .main-header {{
            padding: 0.5rem;
        }}
        
        .progress-container {{
            padding: 0.5rem;
        }}
        
        .edit-modal-container {{
            padding: 15px;
            margin: 10px 0;
        }}
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def create_liquid_progress_bar(progress, text="Processing..."):
    """Create animated liquid gradient progress bar"""
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

def database_status_widget():
    """Display database connection status widget"""
    if st.session_state.db_connection_status is None:
        status_class = "background: linear-gradient(135deg, #ffc10722, #fd7e1422); border: 1px solid #ffc107; color: #ffc107;"
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
        <div class="database-status" style="{status_class}">
            {icon} {status_text}
        </div>
        """,
        unsafe_allow_html=True
    )

def show_confirmation_modal():
    """Show confirmation modal for row insertion"""
    if st.session_state.show_confirmation_modal and st.session_state.row_to_insert is not None:
        st.markdown("---")
        st.markdown("### 💾 Confirm Database Insert")
        
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
        
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("✅ Confirm Insert", type="primary", use_container_width=True, key="modal_confirm_insert_btn"):
                row_index = st.session_state.row_to_insert.get('_index', 0)
                success, message = insert_single_row_to_database_app(
                    st.session_state.row_to_insert, 
                    row_index
                )
                
                if success:
                    st.success(f"✅ Row inserted successfully: {message}")
                else:
                    st.error(f"❌ Failed to insert row: {message}")
                
                st.session_state.show_confirmation_modal = False
                st.session_state.row_to_insert = None
                time.sleep(0.5)
                st.rerun()
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True, key="modal_cancel_insert_btn"):
                st.session_state.show_confirmation_modal = False
                st.session_state.row_to_insert = None
                st.rerun()
        
        st.markdown("---")

def create_edit_modal():
    """Create modal for editing category, variety, color, grade fields"""
    if st.session_state.show_edit_modal and st.session_state.edit_row_data is not None:
        st.markdown(
            """
            <div class="edit-modal-container">
                <h3 style="text-align: center; color: #007bff; margin-bottom: 20px;">✏️ Edit Row Data</h3>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        row_data = st.session_state.edit_row_data
        row_index = st.session_state.edit_row_index
        
        # Show current row information
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        vendor_name = str(row_data.get('Vendor Name', 'N/A'))
        similarity = row_data.get('Similarity %', 'N/A')
        missing = row_data.get('Missing Words', '')
        st.info(f"**Missing Words:** {missing}")
        # Mostrar sinónimos aplicados durante el procesamiento
        applied_synonyms = row_data.get("Applied Synonyms", "")
        if applied_synonyms:
            st.success(f"🔁 **Synonyms applied:** {applied_synonyms}")
        else:
            st.info("No synonyms were applied.")

        # Mostrar blacklist eliminadas durante el procesamiento
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
        
        # Create edit form in columns
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
        
        # Action buttons
        # Selector de acción: blacklist o synonym
        st.markdown("**🛠️ Word Action (Blacklist / Synonym)**")

        action = st.selectbox(
            "Action type:",
            ["", "blacklist", "synonym"],
            index=["", "blacklist", "synonym"].index(row_data.get("Action", "") or ""),
            key="modal_action_select"
        )

        # Campos dinámicos según la acción seleccionada
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
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("💾 Confirm and Update Database", type="primary", use_container_width=True, key="modal_confirm_update_btn"):
                # Prepare updated data
                updated_data = {
                    'categoria': categoria,
                    'variedad': variedad,
                    'color': color,
                    'grado': grado
                }
                # Obtener datos del select y campos de palabra
                action = st.session_state.get("modal_action_select", "")
                if action == "blacklist":
                    word_final = st.session_state.get("modal_word_blacklist", "").strip()
                elif action == "synonym":
                    w1 = st.session_state.get("modal_synonym_from", "").strip()
                    w2 = st.session_state.get("modal_synonym_to", "").strip()
                    word_final = f'"{w1}":"{w2}"' if w1 and w2 else ""
                else:
                    word_final = ""

                # Agregar a datos actualizados
                updated_data["action"] = action
                updated_data["word"] = word_final
                
                # Update the main dataframe
                if st.session_state.processed_data is not None:
                    df = st.session_state.processed_data
                    if row_index in df.index:
                        df.loc[row_index, 'Categoria'] = categoria
                        df.loc[row_index, 'Variedad'] = variedad
                        df.loc[row_index, 'Color'] = color
                        df.loc[row_index, 'Grado'] = grado
                
                # Update database if connected
                if st.session_state.db_connection_status == "connected":
                    try:
                        from database_integration import MappingDatabase
                        db = MappingDatabase()
                        if db.connect():
                            # Check if row exists in database first
                            exists, db_row_id = db.verify_row_exists(row_data)
                            if exists and db_row_id:
                                success, message = db.update_single_row(db_row_id, updated_data)
                                if success:
                                    st.success(f"✅ Database updated successfully: {message}")
                                else:
                                    st.error(f"❌ Failed to update database: {message}")
                            else:
                                st.warning("⚠️ Row not found in database. Cannot update.")
                            db.disconnect()
                    except Exception as e:
                        st.error(f"❌ Database update error: {str(e)}")
                else:
                    st.warning("⚠️ Database not connected. Only local data updated.")
                
                # Close modal and clear state
                st.session_state.show_edit_modal = False
                st.session_state.edit_row_data = None
                st.session_state.edit_row_index = None
                st.session_state.edit_categoria = ''
                st.session_state.edit_variedad = ''
                st.session_state.edit_color = ''
                st.session_state.edit_grado = ''
                
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("🔄 Reset to Original", use_container_width=True, key="modal_reset_btn"):
                st.session_state.edit_categoria = str(row_data.get('Categoria', ''))
                st.session_state.edit_variedad = str(row_data.get('Variedad', ''))
                st.session_state.edit_color = str(row_data.get('Color', ''))
                st.session_state.edit_grado = str(row_data.get('Grado', ''))
                st.rerun()
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True, key="modal_cancel_edit_btn"):
                st.session_state.show_edit_modal = False
                st.session_state.edit_row_data = None
                st.session_state.edit_row_index = None
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
            similarity = pd.to_numeric(row.get('Similarity %', 0), errors="coerce")
            if similarity >= 90:
                color_class = "background-color: #d4edda; color: #155724;"
            elif similarity >= 70:
                color_class = "background-color: #fff3cd; color: #856404;"
            else:
                color_class = "background-color: #f8d7da; color: #721c24;"
            st.markdown(f"<div style='{color_class} padding: 8px; border-radius: 4px; text-align: center; font-weight: bold;'>{similarity}%</div>", unsafe_allow_html=True)
        
        with row_cols[3]:
            catalog_id = str(row.get('Catalog ID', ''))
            if catalog_id.strip() == "111111.0":
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
            # Usa valor de BD por defecto, si no está en session_state
            db_accept = str(row.get("Accept Map", "")).strip().lower() == "true"
            accept = st.session_state.form_data.get(accept_key, db_accept)
            st.session_state.form_data[accept_key] = st.checkbox("", value=accept, key=f"accept_cb_inline_{idx}")

            # Exclusividad: si se marca accept, desmarcar deny
            if st.session_state.form_data[accept_key]:
                st.session_state.form_data[f"deny_{idx}"] = False
            
        
        # Deny checkbox
        with row_cols[9]:
            deny_key = f"deny_{idx}"
            db_deny = str(row.get("Deny Map", "")).strip().lower() == "true"
            deny = st.session_state.form_data.get(deny_key, db_deny)
            st.session_state.form_data[deny_key] = st.checkbox("", value=deny, key=f"deny_cb_inline_{idx}")

            # Exclusividad: si se marca deny, desmarcar accept
            if st.session_state.form_data[deny_key]:
                st.session_state.form_data[f"accept_{idx}"] = False

        
        # Action buttons
        with row_cols[10]:
            action_col1, action_col2 = st.columns(2)
            
        with action_col1:
            if st.button("📝", key=f"update_mapping_{idx}", help="Update accept/deny in DB"):
                # Obtener los valores actuales de los checkboxes desde session_state
                accept = st.session_state.form_data.get(f"accept_{idx}", False)
                deny = st.session_state.form_data.get(f"deny_{idx}", False)

                try:
                    from database_integration import MappingDatabase
                    db = MappingDatabase()
                    if db.connect():
                        # Verificar si la fila existe en la BD
                        exists, db_row_id = db.verify_row_exists(row.to_dict())
                        if exists and db_row_id:
                            update_data = {
                                "accept_map": str(accept),
                                "deny_map": str(deny)
                            }
                            success, msg = db.update_single_row(db_row_id, update_data)
                            if success:
                                st.session_state["last_mapping_update"] = f"✅ Mapping updated for row {idx}"
                            else:
                                st.session_state["last_mapping_update"] = f"❌ Failed to update DB for row {idx}"
                        else:
                            st.session_state["last_mapping_update"] = f"⚠️ Row not found in DB for row {idx}"
                        db.disconnect()
                except Exception as e:
                    st.session_state["last_mapping_update"] = f"❌ Error updating row: {str(e)}"
                st.rerun()
            
            with action_col2:
                if st.button("✏️", key=f"edit_inline_{idx}", 
                           help="Edit and Verify in Database"):
                    st.session_state.show_edit_modal = True
                    st.session_state.edit_row_data = row.to_dict()
                    st.session_state.edit_row_index = idx
                    
                    # Initialize edit values
                    st.session_state.edit_categoria = str(row.get('Categoria', ''))
                    st.session_state.edit_variedad = str(row.get('Variedad', ''))
                    st.session_state.edit_color = str(row.get('Color', ''))
                    st.session_state.edit_grado = str(row.get('Grado', ''))
                    
                    st.rerun()
        
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




def sidebar_controls():
    """Enhanced sidebar with all controls"""
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
                
                output = BytesIO()
                result_df.to_csv(output, sep=";", index=False, encoding="utf-8")
                save_output_to_disk(output)
                
                progress_container.empty()
                st.sidebar.success("✅ Files processed successfully!")
                st.rerun()
                
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
    
    # # Filter controls
    # st.sidebar.divider()
    # st.sidebar.header("🎯 Filters & Search")
    
    # search_text = st.sidebar.text_input("🔍 Search", placeholder="Search in all columns...")
    # similarity_range = st.sidebar.slider("Similarity %", min_value=1, max_value=100, value=(1, 100))
    
    # filter_column = st.sidebar.selectbox(
    #     "Filter Column",
    #     ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"]
    # )
    
    # filter_value = ""
    # if filter_column != "None":
    #     filter_value = st.sidebar.text_input("Filter Value", placeholder="Value to exclude...")
    
    # Divider y encabezado
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
    
    # Theme toggle
    st.sidebar.divider()
    if st.sidebar.button("🌓 Toggle Theme", use_container_width=True):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()
    
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
        filtered_df = filtered_df.sort_values(by="Similarity %", ascending=False)
    
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

def main():
    """Main application function with enhanced inline actions"""
    apply_custom_css()
    
    # Initialize database connection status on first run
    if st.session_state.db_connection_status is None:
        check_database_connection()
    
    # Show modals if needed
    if st.session_state.show_confirmation_modal:
        show_confirmation_modal()
        return
    
    if st.session_state.show_edit_modal:
        create_edit_modal()
        return
    
    # Header
    st.markdown(
        """
        <div class="main-header">
            <h1>🔍 Data Mapping Validation System</h1>
            <p>Enhanced with inline row actions and comprehensive editing capabilities</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    # Mostrar notificación si se actualizó un mapping
    if "last_mapping_update" in st.session_state:
        st.toast(st.session_state["last_mapping_update"])  # Puedes cambiar a st.success(...) si prefieres
        del st.session_state["last_mapping_update"]
        
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
            
            # Progress bar
            progress_html = f"""
            <div class="liquid-progress" style="margin-bottom: 20px;">
                <div class="liquid-fill" style="width: {progress_pct}%"></div>
                <div class="progress-text">{progress_pct:.1f}% Reviewed</div>
            </div>
            """
            st.markdown(progress_html, unsafe_allow_html=True)
            
            # Pagination
            rows_per_page = 10
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
            
        else:
            st.warning("🔍 No data matches the current filters")
            st.info("Try adjusting your filter criteria in the sidebar")
    else:
        # Instructions for new users
        st.info("👆 **Get Started:** Upload your files using the sidebar")
        
        with st.expander("📖 **Enhanced Features Guide**"):
            st.markdown("""
            ### New Inline Action Features:
            - **💾 Insert Button**: Directly insert rows to database with confirmation
            - **✏️ Edit Button**: Edit category, variety, color, grade with database update
            - **Modal Editing**: Safe, guided editing with confirmation dialogs
            - **Status Indicators**: Visual feedback for inserted and verified rows
            
            ### Workflow:
            1. Upload files and process data
            2. Use inline action buttons in each table row
            3. Edit fields directly with the ✏️ button
            4. Confirm changes with database updates
            5. Track progress with visual indicators
            
            ### Database Operations:
            - Real-time connection status monitoring
            - Individual row operations with confirmation
            - Bulk operations for efficiency
            - Comprehensive error handling and feedback
            """)

if __name__ == "__main__":
    main()