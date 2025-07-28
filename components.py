# components.py
import reflex as rx
from typing import Dict, Any, List
from main import MappingState

def theme_button() -> rx.Component:
    """Theme toggle button"""
    return rx.button(
        "🌓 Toggle Theme",
        on_click=MappingState.toggle_theme,
        style={
            "position": "absolute",
            "top": "10px",
            "right": "10px",
            "background": rx.cond(
                MappingState.dark_mode,
                "#2196f3",
                "#0077cc"
            ),
            "color": "white",
            "border": "none",
            "padding": "8px 12px",
            "border_radius": "4px",
            "cursor": "pointer"
        }
    )

def file_upload_section() -> rx.Component:
    """File upload interface"""
    return rx.vstack(
        rx.heading("File Upload", size="lg"),
        rx.text("Upload 2 TSV files and 1 JSON dictionary file"),
        
        rx.upload(
            rx.vstack(
                rx.button(
                    "Select Files",
                    style={
                        "background": "#0077cc",
                        "color": "white",
                        "padding": "10px 20px",
                        "border": "none",
                        "border_radius": "4px"
                    }
                ),
                rx.text("Drag and drop files here or click to select")
            ),
            id="file_upload",
            multiple=True,
            accept={
                "text/tab-separated-values": [".tsv"],
                "application/json": [".json"]
            },
            max_files=3,
            on_upload=MappingState.upload_files
        ),
        
        rx.cond(
            MappingState.processing_status != "ready",
            rx.vstack(
                rx.text(f"Status: {MappingState.processing_status}"),
                rx.cond(
                    MappingState.processing_status == "processing",
                    rx.progress(value=MappingState.progress_percentage, max=100)
                )
            )
        ),
        
        rx.cond(
            MappingState.processing_status == "ready_to_process",
            rx.button(
                "Process Files",
                on_click=MappingState.process_uploaded_files,
                style={
                    "background": "#28a745",
                    "color": "white",
                    "padding": "10px 20px",
                    "border": "none",
                    "border_radius": "4px"
                }
            )
        ),
        
        spacing="4",
        padding="20px",
        width="100%"
    )

def filter_controls() -> rx.Component:
    """Filter and search controls"""
    return rx.hstack(
        # Search input
        rx.vstack(
            rx.text("🔍 Search:", font_weight="bold"),
            rx.input(
                placeholder="Search text...",
                value=MappingState.search_text,
                on_change=MappingState.update_search,
                style={"width": "200px"}
            ),
            spacing="1"
        ),
        
        # Similarity range
        rx.vstack(
            rx.text("🎯 Similarity:", font_weight="bold"),
            rx.hstack(
                rx.number_input(
                    value=MappingState.min_similarity,
                    min_=1,
                    max_=100,
                    on_change=lambda x: MappingState.update_similarity_range(x, MappingState.max_similarity),
                    style={"width": "80px"}
                ),
                rx.text("-"),
                rx.number_input(
                    value=MappingState.max_similarity,
                    min_=1,
                    max_=100,
                    on_change=lambda x: MappingState.update_similarity_range(MappingState.min_similarity, x),
                    style={"width": "80px"}
                ),
                spacing="2"
            ),
            spacing="1"
        ),
        
        # Column filter
        rx.vstack(
            rx.text("🧱 Filter Column:", font_weight="bold"),
            rx.hstack(
                rx.select(
                    ["", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"],
                    value=MappingState.filter_column,
                    on_change=lambda x: MappingState.update_column_filter(x, MappingState.filter_value),
                    style={"width": "120px"}
                ),
                rx.input(
                    placeholder="Filter value...",
                    value=MappingState.filter_value,
                    on_change=lambda x: MappingState.update_column_filter(MappingState.filter_column, x),
                    style={"width": "120px"}
                ),
                spacing="2"
            ),
            spacing="1"
        ),
        
        spacing="6",
        padding="20px",
        width="100%",
        style={
            "background": rx.cond(MappingState.dark_mode, "#1e1e1e", "#f8f9fa"),
            "border_radius": "8px",
            "border": rx.cond(MappingState.dark_mode, "1px solid #444", "1px solid #dee2e6")
        }
    )

def progress_bar() -> rx.Component:
    """Progress tracking bar"""
    return rx.cond(
        MappingState.show_progress,
        rx.vstack(
            rx.button(
                "⬆️ Hide Progress",
                on_click=MappingState.toggle_progress_visibility,
                size="sm"
            ),
            rx.vstack(
                rx.text(
                    f"Progress: {MappingState.reviewed_count} / {MappingState.total_rows} reviewed",
                    font_weight="bold"
                ),
                rx.progress(
                    value=MappingState.reviewed_count,
                    max=MappingState.total_rows,
                    style={
                        "width": "100%",
                        "height": "25px",
                        "background": "#e74c3c"
                    }
                ),
                spacing="2"
            ),
            spacing="3",
            width="100%",
            style={
                "position": "sticky",
                "top": "0",
                "background": rx.cond(MappingState.dark_mode, "#121212", "#fff"),
                "padding": "15px",
                "border_bottom": rx.cond(MappingState.dark_mode, "1px solid #444", "1px solid #ccc"),
                "z_index": "1000"
            }
        ),
        rx.button(
            "⬇️ Show Progress",
            on_click=MappingState.toggle_progress_visibility,
            size="sm",
            style={
                "position": "sticky",
                "top": "0",
                "z_index": "1000"
            }
        )
    )

def table_row(row: Dict[str, Any]) -> rx.Component:
    """Individual table row component"""
    row_id = row["id"]
    
    return rx.table.row(
        # Main data columns
        rx.table.cell(row["Cleaned input"], style={"background": "#fffec6" if not MappingState.dark_mode else "#3a3a1a"}),
        rx.table.cell(row["Best match"], style={"background": "#fffec6" if not MappingState.dark_mode else "#3a3a1a"}),
        rx.table.cell(f"{row['Similarity %']}%"),
        rx.table.cell(
            "needs to create product" if str(row["Catalog ID"]) == "111111.0" 
            else row["Catalog ID"],
            style={"background": "#fffec6" if not MappingState.dark_mode else "#3a3a1a"}
        ),
        
        # Editable fields
        rx.table.cell(
            rx.input(
                value=row["Categoria"],
                on_change=lambda x: MappingState.update_form_field(row_id, "Categoria", x),
                disabled=not row["deny_map"],
                style={"width": "100%"}
            )
        ),
        rx.table.cell(
            rx.input(
                value=row["Variedad"],
                on_change=lambda x: MappingState.update_form_field(row_id, "Variedad", x),
                disabled=not row["deny_map"],
                style={"width": "100%"}
            )
        ),
        rx.table.cell(
            rx.input(
                value=row["Color"],
                on_change=lambda x: MappingState.update_form_field(row_id, "Color", x),
                disabled=not row["deny_map"],
                style={"width": "100%"}
            )
        ),
        rx.table.cell(
            rx.input(
                value=row["Grado"],
                on_change=lambda x: MappingState.update_form_field(row_id, "Grado", x),
                disabled=not row["deny_map"],
                style={"width": "100%"}
            )
        ),
        
        # Action checkboxes
        rx.table.cell(
            rx.checkbox(
                checked=row["accept_map"],
                on_change=lambda x: MappingState.toggle_mapping(row_id, "accept", x)
            )
        ),
        rx.table.cell(
            rx.checkbox(
                checked=row["deny_map"],
                on_change=lambda x: MappingState.toggle_mapping(row_id, "deny", x)
            )
        ),
        
        # Details section
        rx.table.cell(
            rx.cond(
                row["deny_map"],
                rx.vstack(
                    rx.select(
                        ["", "blacklist", "synonym"],
                        value=row["action"],
                        on_change=lambda x: MappingState.update_form_field(row_id, "action", x),
                        placeholder="-- select --"
                    ),
                    rx.cond(
                        (row["action"] == "blacklist") | (row["action"] == "synonym"),
                        rx.input(
                            placeholder="word",
                            value=row["word"],
                            on_change=lambda x: MappingState.update_form_field(row_id, "word", x)
                        )
                    ),
                    spacing="2",
                    style={
                        "background": rx.cond(MappingState.dark_mode, "#2a2a2a", "#eef"),
                        "padding": "8px",
                        "border_radius": "4px"
                    }
                )
            )
        ),
        
        style={
            "background": rx.cond(
                MappingState.dark_mode,
                "#1e1e1e",
                "white"
            )
        }
    )

def data_table() -> rx.Component:
    """Main data table"""
    return rx.vstack(
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Cleaned Input"),
                    rx.table.column_header_cell("Best Match"),
                    rx.table.column_header_cell("Similarity %"),
                    rx.table.column_header_cell("Catalog ID"),
                    rx.table.column_header_cell("Categoria"),
                    rx.table.column_header_cell("Variedad"),
                    rx.table.column_header_cell("Color"),
                    rx.table.column_header_cell("Grado"),
                    rx.table.column_header_cell("Accept Map"),
                    rx.table.column_header_cell("Deny Map"),
                    rx.table.column_header_cell("Details"),
                    style={
                        "position": "sticky",
                        "top": "85px",
                        "background": rx.cond(MappingState.dark_mode, "#333", "#007ab8"),
                        "color": "white",
                        "z_index": "10"
                    }
                )
            ),
            rx.table.body(
                rx.foreach(MappingState.visible_data, table_row)
            ),
            style={
                "width": "100%",
                "border_collapse": "collapse"
            }
        ),
        
        # Action buttons
        rx.hstack(
            rx.button(
                "💾 Save All",
                on_click=MappingState.export_mappings,
                style={
                    "background": "#28a745",
                    "color": "white",
                    "padding": "10px 20px",
                    "border": "none",
                    "border_radius": "4px"
                }
            ),
            rx.button(
                "✅ Accept All",
                on_click=MappingState.accept_all_visible,
                style={
                    "background": "#17a2b8",
                    "color": "white",
                    "padding": "10px 20px",
                    "border": "none",
                    "border_radius": "4px"
                }
            ),
            spacing="4",
            padding="20px"
        ),
        
        width="100%"
    )

def pagination_controls() -> rx.Component:
    """Pagination navigation"""
    return rx.hstack(
        rx.text(f"Page {MappingState.current_page} of {MappingState.total_pages}"),
        
        rx.cond(
            MappingState.current_page > 1,
            rx.hstack(
                rx.button(
                    "First",
                    on_click=lambda: MappingState.go_to_page(1),
                    size="sm"
                ),
                rx.button(
                    "Previous",
                    on_click=lambda: MappingState.go_to_page(MappingState.current_page - 1),
                    size="sm"
                ),
                spacing="2"
            )
        ),
        
        rx.cond(
            MappingState.current_page < MappingState.total_pages,
            rx.hstack(
                rx.button(
                    "Next",
                    on_click=lambda: MappingState.go_to_page(MappingState.current_page + 1),
                    size="sm"
                ),
                rx.button(
                    "Last",
                    on_click=lambda: MappingState.go_to_page(MappingState.total_pages),
                    size="sm"
                ),
                spacing="2"
            )
        ),
        
        spacing="4",
        justify="center",
        padding="20px"
    )