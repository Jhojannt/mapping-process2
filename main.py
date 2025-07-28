# main.py
import reflex as rx
from typing import Dict, List, Any, Optional
import pandas as pd
from io import BytesIO
import asyncio
from pathlib import Path

# Import your existing logic modules
from logic import process_files
from ulits import classify_missing_words
from storage import save_output_to_disk, load_output_from_disk

class MappingState(rx.State):
    """Main state management for the mapping validation SPA"""
    
    # File processing state
    uploaded_files: Dict[str, bytes] = {}
    processing_status: str = "ready"
    progress_percentage: float = 0.0
    
    # Data state
    df_data: List[Dict[str, Any]] = []
    filtered_data: List[Dict[str, Any]] = []
    total_rows: int = 0
    
    # UI state
    current_page: int = 1
    rows_per_page: int = 100
    total_pages: int = 1
    
    # Filter state
    search_text: str = ""
    min_similarity: int = 1
    max_similarity: int = 100
    filter_column: str = ""
    filter_value: str = ""
    
    # Form state for mappings
    form_data: Dict[str, Any] = {}
    reviewed_count: int = 0
    
    # Theme state
    dark_mode: bool = False
    
    # Progress visibility
    show_progress: bool = True

    @rx.var
    def visible_data(self) -> List[Dict[str, Any]]:
        """Get data for current page"""
        start = (self.current_page - 1) * self.rows_per_page
        end = start + self.rows_per_page
        return self.filtered_data[start:end]
    
    @rx.var
    def progress_width(self) -> str:
        """Calculate progress bar width"""
        if self.total_rows == 0:
            return "0%"
        percentage = (self.reviewed_count / self.total_rows) * 100
        return f"{percentage:.1f}%"

    async def upload_files(self, files: List[rx.UploadFile]):
        """Handle file uploads"""
        self.processing_status = "uploading"
        
        for file in files:
            file_bytes = await file.read()
            self.uploaded_files[file.filename] = file_bytes
        
        self.processing_status = "ready_to_process"

    async def process_uploaded_files(self):
        """Process the uploaded TSV files and dictionary"""
        if len(self.uploaded_files) < 3:
            self.processing_status = "error: Need 3 files (2 TSV + 1 JSON)"
            return
        
        self.processing_status = "processing"
        self.progress_percentage = 0.0
        
        try:
            # Extract files (this is a simplified version)
            file_list = list(self.uploaded_files.items())
            
            # Simulate file processing with your existing logic
            # In real implementation, you'd use your process_files function
            await asyncio.sleep(0.1)  # Simulate processing time
            self.progress_percentage = 50.0
            
            # Mock data for demonstration
            self.df_data = [
                {
                    "id": i,
                    "Cleaned input": f"sample input {i}",
                    "Best match": f"best match {i}",
                    "Similarity %": 85 + (i % 15),
                    "Catalog ID": f"CAT{i:06d}",
                    "Categoria": f"Category {i}",
                    "Variedad": f"Variety {i}",
                    "Color": f"Color {i}",
                    "Grado": f"Grade {i}",
                    "accept_map": False,
                    "deny_map": False,
                    "action": "",
                    "word": ""
                }
                for i in range(1, 501)  # 500 sample rows
            ]
            
            self.total_rows = len(self.df_data)
            self.apply_filters()
            
            self.progress_percentage = 100.0
            self.processing_status = "completed"
            
        except Exception as e:
            self.processing_status = f"error: {str(e)}"

    def apply_filters(self):
        """Apply current filters to the data"""
        filtered = self.df_data.copy()
        
        # Similarity filter
        filtered = [
            row for row in filtered 
            if self.min_similarity <= row["Similarity %"] <= self.max_similarity
        ]
        
        # Search text filter
        if self.search_text:
            search_lower = self.search_text.lower()
            filtered = [
                row for row in filtered
                if any(search_lower in str(value).lower() for value in row.values())
            ]
        
        # Column filter
        if self.filter_column and self.filter_value:
            filter_lower = self.filter_value.lower()
            filtered = [
                row for row in filtered
                if filter_lower not in str(row.get(self.filter_column, "")).lower()
            ]
        
        self.filtered_data = filtered
        self.total_pages = max(1, (len(filtered) + self.rows_per_page - 1) // self.rows_per_page)
        self.current_page = min(self.current_page, self.total_pages)
        
        # Update reviewed count
        self.reviewed_count = sum(
            1 for row in filtered 
            if row.get("accept_map") or row.get("deny_map")
        )

    def update_search(self, value: str):
        """Update search text and apply filters"""
        self.search_text = value
        self.current_page = 1
        self.apply_filters()

    def update_similarity_range(self, min_val: int, max_val: int):
        """Update similarity range and apply filters"""
        self.min_similarity = min_val
        self.max_similarity = max_val
        self.current_page = 1
        self.apply_filters()

    def update_column_filter(self, column: str, value: str):
        """Update column filter and apply filters"""
        self.filter_column = column
        self.filter_value = value
        self.current_page = 1
        self.apply_filters()

    def go_to_page(self, page: int):
        """Navigate to specific page"""
        self.current_page = max(1, min(page, self.total_pages))

    def toggle_mapping(self, row_id: int, mapping_type: str, value: bool):
        """Toggle accept/deny mapping for a row"""
        for row in self.df_data:
            if row["id"] == row_id:
                if mapping_type == "accept":
                    row["accept_map"] = value
                    if value:  # If accepting, clear deny
                        row["deny_map"] = False
                elif mapping_type == "deny":
                    row["deny_map"] = value
                    if value:  # If denying, clear accept
                        row["accept_map"] = False
                break
        
        self.apply_filters()

    def update_form_field(self, row_id: int, field: str, value: str):
        """Update form field for a specific row"""
        for row in self.df_data:
            if row["id"] == row_id:
                row[field] = value
                break

    def accept_all_visible(self):
        """Accept all mappings on current page"""
        for row in self.visible_data:
            row["accept_map"] = True
            row["deny_map"] = False
        
        self.apply_filters()

    def toggle_theme(self):
        """Toggle between light and dark theme"""
        self.dark_mode = not self.dark_mode

    def toggle_progress_visibility(self):
        """Toggle progress bar visibility"""
        self.show_progress = not self.show_progress

    async def export_mappings(self):
        """Export the current mappings to CSV"""
        # Create DataFrame from current data
        df = pd.DataFrame(self.df_data)
        
        # Save to BytesIO
        output = BytesIO()
        df.to_csv(output, sep=";", index=False, encoding="utf-8")
        
        # Save to disk using your existing storage logic
        save_output_to_disk(output)
        
        return output