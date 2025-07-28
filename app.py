# app.py
import reflex as rx
from main import MappingState
from components import (
    theme_button,
    file_upload_section,
    filter_controls,
    progress_bar,
    data_table,
    pagination_controls
)

def index() -> rx.Component:
    """Main page component"""
    return rx.container(
        # Theme toggle button
        theme_button(),
        
        rx.vstack(
            # Header
            rx.heading(
                "Data Mapping Validation System",
                size="xl",
                style={
                    "text_align": "center",
                    "margin_bottom": "20px",
                    "color": rx.cond(MappingState.dark_mode, "#e0e0e0", "#333")
                }
            ),
            
            # Conditional rendering based on processing status
            rx.cond(
                MappingState.processing_status.contains("completed"),
                rx.vstack(
                    # Progress bar (sticky)
                    progress_bar(),
                    
                    # Filter controls
                    filter_controls(),
                    
                    # Data table
                    data_table(),
                    
                    # Pagination
                    pagination_controls(),
                    
                    spacing="4",
                    width="100%"
                ),
                # File upload section (when no data is loaded)
                file_upload_section()
            ),
            
            spacing="6",
            width="100%",
            padding="20px"
        ),
        
        style={
            "background": rx.cond(MappingState.dark_mode, "#121212", "#f0f8ff"),
            "color": rx.cond(MappingState.dark_mode, "#e0e0e0", "#333"),
            "min_height": "100vh",
            "transition": "background-color 0.3s, color 0.3s"
        }
    )

# App configuration
app = rx.App(
    style={
        "font_family": "'Segoe UI', sans-serif",
    }
)

# Add the main page
app.add_page(index, route="/")

# Custom CSS for enhanced styling
app.add_custom_head_code("""
<style>
/* Global styles */
* {
    box-sizing: border-box;
}

/* Sticky table headers */
.sticky-header {
    position: sticky;
    top: 85px;
    z-index: 10;
    background-color: #007ab8 !important;
    color: white !important;
}

/* Dark mode sticky headers */
.dark-mode .sticky-header {
    background-color: #333 !important;
}

/* Table hover effects */
tr:hover td.highlight-cell {
    transform: scale(1.02);
    transition: all 0.2s ease-in-out;
    z-index: 1;
}

/* Light mode highlight */
.light-mode tr:hover td.highlight-cell {
    background-color: #fffec6 !important;
}

/* Dark mode highlight */
.dark-mode tr:hover td.highlight-cell {
    background-color: #3a3a1a !important;
}

/* Progress bar styling */
.progress-bar {
    background: linear-gradient(90deg, #2ecc71 0%, #27ae60 100%);
    transition: width 0.3s ease;
}

/* Button hover effects */
button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    transition: all 0.2s ease;
}

/* Input focus effects */
input:focus, select:focus {
    outline: none;
    border: 2px solid #007ab8;
    box-shadow: 0 0 5px rgba(0,122,185,0.3);
}

/* Responsive design */
@media (max-width: 768px) {
    .container {
        padding: 10px !important;
    }
    
    table {
        font-size: 12px;
    }
    
    .filter-controls {
        flex-direction: column;
        gap: 10px;
    }
}

/* Scrollbar styling */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: #f1f1f1;
}

::-webkit-scrollbar-thumb {
    background: #888;
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: #555;
}

/* Dark mode scrollbar */
.dark-mode ::-webkit-scrollbar-track {
    background: #2a2a2a;
}

.dark-mode ::-webkit-scrollbar-thumb {
    background: #555;
}

.dark-mode ::-webkit-scrollbar-thumb:hover {
    background: #777;
}
</style>
""")

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=3000)