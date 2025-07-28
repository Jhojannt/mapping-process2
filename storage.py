# storage.py - Updated for cross-platform compatibility
from pathlib import Path
from io import BytesIO
import os

# Use relative path that works anywhere
BASE_DIR = Path(__file__).parent / "output_data"
STORAGE_PATH = BASE_DIR / "output.csv"

def save_output_to_disk(data: BytesIO):
    """Save BytesIO content to disk"""
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    data.seek(0)
    with open(STORAGE_PATH, "wb") as f:
        f.write(data.read())

def load_output_from_disk() -> BytesIO:
    """Load output.csv from disk as BytesIO"""
    if not STORAGE_PATH.exists():
        return None
    
    raw = STORAGE_PATH.read_bytes()
    if not raw:
        return None
    
    buf = BytesIO(raw)
    buf.seek(0)
    return buf