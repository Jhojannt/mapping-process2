# synonyms_blacklist_interface_CORRECTED.py - FIXED VERSION

import streamlit as st
import pandas as pd
import json
import time
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Configure page
st.set_page_config(
    page_title="Synonyms & Blacklist Manager - CORRECTED",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def initialize_session_state():
    defaults = {
        'current_client_id': None,
        'available_clients': [],
        'synonyms_data': {},
        'blacklist_data': [],
        'staging_products': None,
        'filter_text': '',
        'show_staging_products': True,
        'new_synonym_original': '',
        'new_synonym_replacement': '',
        'new_blacklist_word': '',
        'show_json_import': False,
        'json_import_text': '',
        'success_message': '',
        'error_message': '',
        'info_message': ''
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session_state()

def load_available_clients():
    """Load available clients (mock implementation)"""
    try:
        clients = ["demo_client", "acme_corp", "test_company", "sample_client"]
        st.session_state.available_clients = clients
        return clients
    except Exception as e:
        st.error(f"Error loading clients: {str(e)}")
        return []

def load_client_data():
    """Load client-specific data (mock implementation)"""
    if not st.session_state.current_client_id:
        return
    
    try:
        client_id = st.session_state.current_client_id
        
        # Mock data - replace with actual database calls
        st.session_state.synonyms_data = {
            "premium": "high quality",
            "standard": "regular", 
            "large": "big",
            "small": "mini",
            "grade a": "top quality"
        }
        
        st.session_state.blacklist_data = [
            "and", "or", "the", "a", "an", "with", "from", "to", "for", "of"
        ]
        
        # Mock staging products
        st.session_state.staging_products = pd.DataFrame({
            'categoria': ['Flowers', 'Plants', 'Tools'],
            'variedad': ['Roses', 'Succulents', 'Pruners'],
            'color': ['Red', 'Green', 'Silver'],
            'grado': ['Premium', 'Standard', 'Professional'],
            'catalog_id': ['111111', '111111', '111111'],
            'status': ['pending', 'pending', 'approved'],
            'created_at': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        
        st.session_state.success_message = f"✅ Loaded data for client: {client_id}"
        
    except Exception as e:
        st.session_state.error_message = f"❌ Error loading client data: {str(e)}"

def save_client_data():
    """Save client data (mock implementation)"""
    if not st.session_state.current_client_id:
        return False, "No client selected"
    
    try:
        client_id = st.session_state.current_client_id
        
        # Convert synonyms to proper format
        synonyms_list = []
        for original, replacement in st.session_state.synonyms_data.items():
            synonyms_list.append({original: replacement})
        
        # Mock save operation - replace with actual database call
        success = True
        message = f"Saved {len(synonyms_list)} synonyms and {len(st.session_state.blacklist_data)} blacklist words for {client_id}"
        
        if success:
            st.session_state.success_message = f"✅ {message}"
            return True, message
        else:
            st.session_state.error_message = f"❌ Save failed"
            return False, "Save operation failed"
            
    except Exception as e:
        error_msg = f"Error saving data: {str(e)}"
        st.session_state.error_message = error_msg
        return False, error_msg

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
    """Client selection sidebar"""
    st.sidebar.header("🏢 Client Selection")
    
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
                load_client_data()
                st.rerun()
        else:
            st.session_state.current_client_id = None
    
    if st.sidebar.button("🔄 Refresh", use_container_width=True):
        load_available_clients()
        st.rerun()
    
    if st.session_state.current_client_id:
        st.sidebar.success(f"📋 **{st.session_state.current_client_id}**")
        
        # Quick stats
        synonym_count = len(st.session_state.synonyms_data)
        blacklist_count = len(st.session_state.blacklist_data)
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("🔁 Synonyms", synonym_count)
        with col2:
            st.metric("🛑 Blacklist", blacklist_count)
        
        if st.session_state.staging_products is not None:
            staging_count = len(st.session_state.staging_products)
            st.sidebar.metric("🆕 Staging", staging_count)
    
    # Tools section
    st.sidebar.divider()
    st.sidebar.header("🛠️ Tools")
    
    # Filter
    filter_text = st.sidebar.text_input(
        "🔍 Filter:",
        value=st.session_state.filter_text,
        placeholder="Search terms..."
    )
    st.session_state.filter_text = filter_text
    
    # Import/Export
    if st.sidebar.button("📥 Import JSON", use_container_width=True):
        st.session_state.show_json_import = True
        st.rerun()
    
    if st.session_state.current_client_id:
        # Export data
        export_data = {
            "client_id": st.session_state.current_client_id,
            "synonyms": st.session_state.synonyms_data,
            "blacklist": {"input": st.session_state.blacklist_data},
            "exported_at": datetime.now().isoformat()
        }
        
        export_json = json.dumps(export_data, indent=2, ensure_ascii=False)
        
        st.sidebar.download_button(
            label="📤 Export JSON",
            data=export_json,
            file_name=f"synonyms_blacklist_{st.session_state.current_client_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )

def json_import_modal():
    """JSON import modal"""
    if not st.session_state.show_json_import:
        return
    
    st.markdown("""
    <div style="background: linear-gradient(135deg, #e3f2fd, #f1f8e9); border: 3px solid #2196f3; border-radius: 15px; padding: 30px; margin: 25px 0;">
        <h3 style="text-align: center; color: #1976d2;">📥 Import JSON Data</h3>
    </div>
    """, unsafe_allow_html=True)
    
    st.info("""
    **Expected JSON Format:**
    ```json
    {
        "synonyms": {"original": "replacement"},
        "blacklist": {"input": ["word1", "word2"]}
    }
    ```
    """)
    
    json_text = st.text_area(
        "JSON Data:",
        value=st.session_state.json_import_text,
        height=200,
        placeholder="Paste your JSON data here..."
    )
    st.session_state.json_import_text = json_text
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("📥 Import", type="primary", use_container_width=True):
            try:
                data = json.loads(json_text)
                
                # Import synonyms
                if 'synonyms' in data and isinstance(data['synonyms'], dict):
                    st.session_state.synonyms_data.update(data['synonyms'])
                
                # Import blacklist
                if 'blacklist' in data and 'input' in data['blacklist']:
                    new_words = data['blacklist']['input']
                    if isinstance(new_words, list):
                        for word in new_words:
                            if word not in st.session_state.blacklist_data:
                                st.session_state.blacklist_data.append(word)
                
                # Save and close
                save_client_data()
                st.session_state.show_json_import = False
                st.session_state.json_import_text = ''
                st.session_state.success_message = "✅ JSON data imported successfully!"
                st.rerun()
                
            except json.JSONDecodeError:
                st.session_state.error_message = "❌ Invalid JSON format"
            except Exception as e:
                st.session_state.error_message = f"❌ Import error: {str(e)}"
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_json_import = False
            st.session_state.json_import_text = ''
            st.rerun()

def synonyms_management_section():
    """Synonyms management interface"""
    st.header("🔁 Synonyms Management")
    
    if not st.session_state.current_client_id:
        st.warning("⚠️ Please select a client to manage synonyms")
        return
    
    # Add new synonym
    st.subheader("➕ Add New Synonym")
    
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        original = st.text_input(
            "Original Word:",
            value=st.session_state.new_synonym_original,
            placeholder="e.g., premium",
            key="new_synonym_original_input"
        )
        st.session_state.new_synonym_original = original
    
    with col2:
        replacement = st.text_input(
            "Replacement Word:", 
            value=st.session_state.new_synonym_replacement,
            placeholder="e.g., high quality",
            key="new_synonym_replacement_input"
        )
        st.session_state.new_synonym_replacement = replacement
    
    with col3:
        if st.button("➕ Add", use_container_width=True, key="add_synonym_btn"):
            if original and replacement:
                original = original.strip()
                replacement = replacement.strip()
                
                if original in st.session_state.synonyms_data:
                    st.session_state.info_message = f"⚠️ Updated existing synonym: {original}"
                
                st.session_state.synonyms_data[original] = replacement
                st.session_state.new_synonym_original = ''
                st.session_state.new_synonym_replacement = ''
                
                save_client_data()
                st.session_state.success_message = f"✅ Synonym added: {original} → {replacement}"
                st.rerun()
            else:
                st.session_state.error_message = "❌ Please enter both original and replacement words"
    
    # Display existing synonyms
    st.subheader("📋 Current Synonyms")
    
    if st.session_state.synonyms_data:
        # Apply filter
        filtered_synonyms = {}
        filter_text = st.session_state.filter_text.lower()
        
        for original, replacement in st.session_state.synonyms_data.items():
            if not filter_text or filter_text in original.lower() or filter_text in replacement.lower():
                filtered_synonyms[original] = replacement
        
        if filtered_synonyms:
            for original, replacement in filtered_synonyms.items():
                col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
                
                with col1:
                    st.write(f"**{original}**")
                
                with col2:
                    st.write(f"→ {replacement}")
                
                with col3:
                    if st.button("✏️", key=f"edit_syn_{original}", help="Edit synonym"):
                        st.session_state.info_message = "Edit functionality - coming soon"
                
                with col4:
                    if st.button("🗑️", key=f"delete_syn_{original}", help="Delete synonym"):
                        del st.session_state.synonyms_data[original]
                        save_client_data()
                        st.session_state.success_message = f"✅ Deleted synonym: {original}"
                        st.rerun()
        else:
            st.info("🔍 No synonyms match the current filter")
    else:
        st.info("📝 No synonyms configured for this client")

def blacklist_management_section():
    """Blacklist management interface"""
    st.header("🛑 Blacklist Management")
    
    if not st.session_state.current_client_id:
        st.warning("⚠️ Please select a client to manage blacklist")
        return
    
    # Add new word
    st.subheader("➕ Add New Blacklist Word")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        new_word = st.text_input(
            "Blacklist Word:",
            value=st.session_state.new_blacklist_word,
            placeholder="e.g., unwanted, remove_this",
            key="new_blacklist_word_input"
        )
        st.session_state.new_blacklist_word = new_word
    
    with col2:
        if st.button("➕ Add", use_container_width=True, key="add_blacklist_btn"):
            if new_word:
                word = new_word.strip()
                if word not in st.session_state.blacklist_data:
                    st.session_state.blacklist_data.append(word)
                    st.session_state.new_blacklist_word = ''
                    
                    save_client_data()
                    st.session_state.success_message = f"✅ Added blacklist word: {word}"
                    st.rerun()
                else:
                    st.session_state.info_message = "⚠️ Word already in blacklist"
            else:
                st.session_state.error_message = "❌ Please enter a word"
    
    # Display existing blacklist
    st.subheader("📋 Current Blacklist")
    
    if st.session_state.blacklist_data:
        # Apply filter
        filter_text = st.session_state.filter_text.lower()
        filtered_blacklist = [word for word in st.session_state.blacklist_data if not filter_text or filter_text in word.lower()]
        
        if filtered_blacklist:
            for word in filtered_blacklist:
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"🛑 **{word}**")
                
                with col2:
                    if st.button("✏️", key=f"edit_bl_{word}", help="Edit word"):
                        st.session_state.info_message = "Edit functionality - coming soon"
                
                with col3:
                    if st.button("🗑️", key=f"delete_bl_{word}", help="Delete word"):
                        st.session_state.blacklist_data.remove(word)
                        save_client_data()
                        st.session_state.success_message = f"✅ Deleted blacklist word: {word}"
                        st.rerun()
        else:
            st.info("🔍 No blacklist words match the current filter")
    else:
        st.info("📝 No blacklist words configured for this client")

def staging_products_section():
    """Staging products display section"""
    st.header("🆕 Staging Products to Create")
    
    if not st.session_state.current_client_id:
        st.warning("⚠️ Please select a client to view staging products")
        return
    
    if st.session_state.staging_products is not None and len(st.session_state.staging_products) > 0:
        df = st.session_state.staging_products
        
        # Apply filter if set
        if st.session_state.filter_text:
            filter_text = st.session_state.filter_text.lower()
            mask = df.astype(str).apply(lambda row: filter_text in row.to_string().lower(), axis=1)
            df = df[mask]
        
        st.write(f"**Staging Products:** {len(df)}")
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # Download button
        if len(df) > 0:
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=f"staging_products_{st.session_state.current_client_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
    else:
        st.info("📝 No staging products found for this client")

def main():
    """Main application function"""
    
    # Apply custom CSS
    st.markdown("""
    <style>
    .main-header {
        text-align: center;
        padding: 1.5rem;
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
    }
    .stButton > button {
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
        <div class="main-header">
            <h1>📝 Synonyms & Blacklist Manager</h1>
            <p><strong>CORRECTED VERSION</strong> - Ready to use without import errors</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Display messages
    display_messages()
    
    # Sidebar
    client_selector_sidebar()
    
    # JSON import modal
    json_import_modal()
    
    # Main content
    if st.session_state.current_client_id:
        # Load data if needed
        if not st.session_state.synonyms_data and not st.session_state.blacklist_data:
            load_client_data()
        
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["🔁 Synonyms", "🛑 Blacklist", "🆕 Staging Products"])
        
        with tab1:
            synonyms_management_section()
        
        with tab2:
            blacklist_management_section()
        
        with tab3:
            staging_products_section()
        
        # Save all button
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            if st.button("💾 Save All Changes", type="primary", use_container_width=True):
                success, message = save_client_data()
                if success:
                    st.session_state.success_message = "✅ All changes saved successfully!"
                else:
                    st.session_state.error_message = f"❌ Save failed: {message}"
                time.sleep(1)
                st.rerun()
    
    else:
        # No client selected
        st.info("👆 **Get Started:** Select a client in the sidebar")

if __name__ == "__main__":
    main()