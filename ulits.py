import re
import unicodedata
from io import BytesIO
import os

def clean_text(text: str) -> str:
    """
    Limpia texto: elimina acentos, caracteres especiales, múltiples espacios y lo convierte a minúsculas.
    """
    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore").decode("utf-8")  # elimina acentos
    text = re.sub(r"[^\w\s]", " ", text)  # elimina puntuación
    text = re.sub(r"\s+", " ", text)  # espacios extra
    return text.strip().lower()

def apply_synonyms(text: str, synonyms: dict) -> tuple[str, list[tuple[str, str]]]:
    """
    Reemplaza palabras del texto según el diccionario de sinónimos (no case sensitive).
    Retorna el texto reemplazado y una lista de sinónimos aplicados.
    """
    synonyms_lower = {k.lower(): v for k, v in synonyms.items()}

    words = text.split()
    replaced_words = []
    applied = []

    for word in words:
        key = word.lower()
        if key in synonyms_lower:
            replaced_words.append(synonyms_lower[key])
            applied.append((word, synonyms_lower[key]))
        else:
            replaced_words.append(word)

    return " ".join(replaced_words), applied

def remove_blacklist(text: str, blacklist: list) -> tuple[str, list[str]]:
    """
    Elimina solo frases y palabras completas del texto, ignorando mayúsculas.
    No elimina subcadenas dentro de otras palabras.
    """
    removed = []
    blacklist_sorted = sorted(blacklist, key=lambda x: -len(x))  # frases largas primero

    for phrase in blacklist_sorted:
        pattern = r'\b' + re.escape(phrase.strip()) + r'\b'
        if re.search(pattern, text, flags=re.IGNORECASE):
            removed.append(phrase.strip())
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    # Limpiar espacios extra
    cleaned = " ".join(text.strip().split())
    return cleaned, removed

def extract_words(text: str) -> list:
    """
    Extrae palabras del texto para comparación con fuzzy matching.
    """
    return re.findall(r"\b\w+\b", text.lower())

def classify_missing_words(words_str: str, class_dict: dict) -> str:
    """
    Clasifica cada palabra faltante según el diccionario de categorías proporcionado.
    Retorna una lista de tipos únicos: color, variedad, grado, especie, etc.
    """
    import pandas as pd

    if pd.isna(words_str) or words_str.strip() == "":
        return ""
    
    categories = []
    words = words_str.strip().split()
    
    for word in words:
        matched = False
        for cat, values in class_dict.items():
            if word.lower() in values:
                categories.append(cat)
                matched = True
                break
        if not matched:
            categories.append("sin clasificar")

    return ", ".join(sorted(set(categories)))

