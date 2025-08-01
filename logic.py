# logic.py - Complete Enhanced Multi-Client Processing Logic
"""
Complete Enhanced Multi-Client Data Processing Logic

This module provides comprehensive data processing capabilities with:
- Multi-client database integration
- Enhanced fuzzy matching algorithms
- Progress tracking for Streamlit integration
- Backward compatibility with existing code
- Combined catalog support (master + staging)
- Client-specific synonyms and blacklist management
- Robust error handling and logging

Key Functions:
- process_files(): Original function with multi-client support
- process_files_multiclient(): Full multi-client processing
- process_files_with_tqdm_and_callback(): Console + Streamlit support
"""

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
from typing import Optional, Callable, Tuple, Dict, Any, List
import logging
import warnings

# Core utilities
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words

# Enhanced multi-client database integration
try:
    from Enhanced_MultiClient_Database import (
        save_client_processed_data,
        get_client_synonyms_blacklist,
        EnhancedMultiClientDatabase
    )
    ENHANCED_DB_AVAILABLE = True
except ImportError:
    ENHANCED_DB_AVAILABLE = False
    warnings.warn("Enhanced multi-client database not available, falling back to basic functionality")

# Fallback database integration
try:
    from database_integration import save_processed_data_to_database
    BASIC_DB_AVAILABLE = True
except ImportError:
    BASIC_DB_AVAILABLE = False
    warnings.warn("Basic database integration not available")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_files(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, 
                  progress_callback: Optional[Callable] = None, 
                  client_id: Optional[str] = None) -> pd.DataFrame:
    """
    Enhanced process files function with automatic multi-client support
    
    This is the main entry point that automatically detects if multi-client
    functionality is available and uses it, otherwise falls back to basic processing.
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame  
        dictionary: Synonyms and blacklist dictionary
        progress_callback: Optional function to call with progress updates (progress_pct, message)
        client_id: Optional client identifier for multi-client operations
        
    Returns:
        Processed DataFrame with all matching results
    """
    # Auto-detect multi-client mode
    if client_id and ENHANCED_DB_AVAILABLE:
        logger.info(f"Using enhanced multi-client processing for client: {client_id}")
        return process_files_multiclient(df1, df2, dictionary, client_id, progress_callback)
    else:
        if client_id and not ENHANCED_DB_AVAILABLE:
            logger.warning(f"Client ID provided but enhanced DB not available, using basic processing")
        else:
            logger.info("Using basic processing mode")
        return process_files_basic(df1, df2, dictionary, progress_callback)

def process_files_multiclient(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, 
                             client_id: str, progress_callback: Optional[Callable] = None) -> pd.DataFrame:
    """
    Enhanced multi-client file processing with client-specific database operations
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame  
        dictionary: Synonyms and blacklist dictionary
        client_id: Client identifier for database operations
        progress_callback: Optional function to call with progress updates (progress_pct, message)
        
    Returns:
        Processed DataFrame with all matching results
    """
    if not client_id:
        raise ValueError("client_id is required for multi-client processing")
    
    if not ENHANCED_DB_AVAILABLE:
        raise ImportError("Enhanced multi-client database system not available")
    
    logger.info(f"Starting enhanced multi-client processing for client: {client_id}")
    
    df1 = df1.copy()
    df2 = df2.copy()

    def update_progress(progress_pct: float, message: str = "Processing..."):
        if progress_callback:
            progress_callback(progress_pct, message)

    # Validate input data
    if df1.empty or df2.empty:
        raise ValueError("Input DataFrames cannot be empty")
    
    if len(df1.columns) < 3:
        raise ValueError("df1 must have at least 3 columns (description, location, vendor)")
    
    if len(df2.columns) < 4:
        raise ValueError("df2 must have at least 4 columns (categoria, variedad, color, grado)")

    # Columnas clave
    col_desc = df1.columns[0]
    col_vendor = df1.columns[2]

    update_progress(1, f"Iniciando procesamiento para cliente {client_id}...")

    # Crear clave de deduplicación
    df1["duplicate_key"] = df1[col_desc].str.strip().str.lower() + "|" + df1[col_vendor].str.strip().str.lower()
    seen = set()
    cleaned_inputs_status = []

    for key in df1["duplicate_key"]:
        if key in seen:
            cleaned_inputs_status.append("NN")
        else:
            seen.add(key)
            cleaned_inputs_status.append("PENDING")

    logger.info(f"Found {sum(1 for x in cleaned_inputs_status if x == 'NN')} duplicate entries")
    update_progress(3, "Cargando configuración específica del cliente...")

    # Get client-specific synonyms and blacklist from database
    try:
        client_data = get_client_synonyms_blacklist(client_id)
        client_synonyms = client_data.get('synonyms', {})
        client_blacklist = client_data.get('blacklist', {}).get('input', [])
        
        # Merge with provided dictionary (dictionary takes precedence)
        merged_synonyms = {**client_synonyms, **dictionary.get("synonyms", {})}
        merged_blacklist = list(set(client_blacklist + dictionary.get("blacklist", {}).get("input", [])))
        
        logger.info(f"Using {len(merged_synonyms)} synonyms and {len(merged_blacklist)} blacklist words for client {client_id}")
        
    except Exception as e:
        logger.warning(f"Could not load client-specific data, using provided dictionary: {str(e)}")
        merged_synonyms = dictionary.get("synonyms", {})
        merged_blacklist = dictionary.get("blacklist", {}).get("input", [])

    update_progress(5, "Limpiando texto y aplicando filtros específicos del cliente...")

    # Procesar texto: limpieza, sinónimos, blacklist
    inputs = []
    synonyms_applied = []
    removed_blacklist = []

    for idx, row in df1.iterrows():
        if cleaned_inputs_status[idx] == "NN":
            inputs.append("NN")
            synonyms_applied.append("")
            removed_blacklist.append("")
        else:
            raw_text = str(row[col_desc])
            cleaned = clean_text(raw_text)

            # Aplicar sinónimos (client-specific + provided)
            cleaned, applied = apply_synonyms(cleaned, merged_synonyms)
            formatted_applied = ", ".join([f"{o}→{n}" for o, n in applied])
            synonyms_applied.append(formatted_applied)

            # Aplicar blacklist (client-specific + provided)
            cleaned, removed = remove_blacklist(cleaned, merged_blacklist)
            removed_blacklist.append(" ".join(removed))

            inputs.append(cleaned.strip())

    df1["Cleaned input"] = inputs
    df1["Applied Synonyms"] = synonyms_applied
    df1["Removed Blacklist Words"] = removed_blacklist

    update_progress(8, "Preparando catálogo combinado (master + staging)...")

    # Enhanced catalog preparation - combine with client staging products
    df2_enhanced = prepare_enhanced_catalog(df2, client_id)
    
    # Crear columna search_key en catálogo combinado
    concat_cols = df2_enhanced.columns[:4]
    df2_enhanced["search_key"] = df2_enhanced[concat_cols].fillna("").agg(" ".join, axis=1)
    df2_enhanced["search_key"] = df2_enhanced["search_key"].apply(lambda x: clean_text(x.strip().lower()))

    update_progress(10, "Iniciando procesamiento de coincidencias con catálogo mejorado...")

    # Enhanced matching with combined catalog
    results = perform_enhanced_matching(
        df1["Cleaned input"].tolist(), 
        df2_enhanced, 
        progress_callback, 
        client_id
    )

    # Agregar resultados al DataFrame
    df1["Best match"] = results["best_matches"]
    df1["Similarity %"] = results["similarities"]
    df1["Matched Words"] = results["matched_words"]
    df1["Missing Words"] = results["missing_words"]
    df1["Catalog ID"] = results["catalog_ids"]
    df1["Categoria"] = results["categorias"]
    df1["Variedad"] = results["variedades"]
    df1["Color"] = results["colores"]
    df1["Grado"] = results["grados"]

    # Add client-specific columns for tracking
    df1["Client ID"] = client_id
    df1["Processing Timestamp"] = pd.Timestamp.now()

    # Add empty columns for user actions (if not present)
    if "Accept Map" not in df1.columns:
        df1["Accept Map"] = ""
    if "Deny Map" not in df1.columns:
        df1["Deny Map"] = ""
    if "Action" not in df1.columns:
        df1["Action"] = ""
    if "Word" not in df1.columns:
        df1["Word"] = ""

    df1.drop(columns=["duplicate_key"], inplace=True)
    
    update_progress(95, "Guardando resultados en base de datos específica del cliente...")
    
    # Enhanced database saving with client-specific operation
    success, message = save_processed_data_multiclient(df1, client_id)
    
    if not success:
        logger.error(f"Error al guardar en base de datos para cliente {client_id}: {message}")
        update_progress(98, f"⚠️ Procesamiento completado, error en guardado: {message}")
    else:
        logger.info(f"Guardado completado para cliente {client_id}: {message}")
        update_progress(100, f"✅ Procesamiento y guardado completados para {client_id}!")
        
    return df1

def process_files_basic(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, 
                       progress_callback: Optional[Callable] = None) -> pd.DataFrame:
    """
    Basic file processing without multi-client features (backward compatibility)
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame  
        dictionary: Synonyms and blacklist dictionary
        progress_callback: Optional function to call with progress updates (progress_pct, message)
        
    Returns:
        Processed DataFrame
    """
    logger.info("Starting basic file processing")
    
    df1 = df1.copy()
    df2 = df2.copy()

    def update_progress(progress_pct: float, message: str = "Processing..."):
        if progress_callback:
            progress_callback(progress_pct, message)

    # Columnas clave
    col_desc = df1.columns[0]
    col_vendor = df1.columns[2]

    update_progress(1, "Iniciando procesamiento...")

    # Crear clave de deduplicación
    df1["duplicate_key"] = df1[col_desc].str.strip().str.lower() + "|" + df1[col_vendor].str.strip().str.lower()
    seen = set()
    cleaned_inputs_status = []

    for key in df1["duplicate_key"]:
        if key in seen:
            cleaned_inputs_status.append("NN")
        else:
            seen.add(key)
            cleaned_inputs_status.append("PENDING")

    update_progress(3, "Limpiando texto y aplicando filtros...")

    # Procesar texto: limpieza, sinónimos, blacklist
    inputs = []
    synonyms_applied = []
    removed_blacklist = []

    for idx, row in df1.iterrows():
        if cleaned_inputs_status[idx] == "NN":
            inputs.append("NN")
            synonyms_applied.append("")
            removed_blacklist.append("")
        else:
            raw_text = str(row[col_desc])
            cleaned = clean_text(raw_text)

            # Aplicar sinónimos
            cleaned, applied = apply_synonyms(cleaned, dictionary.get("synonyms", {}))
            formatted_applied = ", ".join([f"{o}→{n}" for o, n in applied])
            synonyms_applied.append(formatted_applied)

            # Aplicar blacklist
            cleaned, removed = remove_blacklist(cleaned, dictionary.get("blacklist", {}).get("input", []))
            removed_blacklist.append(" ".join(removed))

            inputs.append(cleaned.strip())

    df1["Cleaned input"] = inputs
    df1["Applied Synonyms"] = synonyms_applied
    df1["Removed Blacklist Words"] = removed_blacklist

    update_progress(5, "Preparando catálogo para matching...")

    # Crear columna search_key en df2 con columnas 1-4
    concat_cols = df2.columns[:4]
    df2["search_key"] = df2[concat_cols].fillna("").agg(" ".join, axis=1)
    df2["search_key"] = df2["search_key"].apply(lambda x: clean_text(x.strip().lower()))

    update_progress(10, "Iniciando procesamiento de coincidencias...")

    # Matching optimizado con cache - THIS IS THE MAIN 90% PROGRESS SECTION
    best_matches = []
    similarities = []
    matched_words = []
    missing_words = []
    catalog_ids = []
    categorias = []
    variedades = []
    colores = []
    grados = []

    choices = df2["search_key"].tolist()
    cache = {}

    # This is the main loop that takes 90% of the time (from 10% to 100%)
    cleaned_inputs = df1["Cleaned input"].tolist()
    total_inputs = len(cleaned_inputs)

    for i, cleaned in enumerate(cleaned_inputs):
        # Update progress for the main processing loop (10% to 100% = 90% of total progress)
        current_progress = 10 + ((i / total_inputs) * 85)  # Leave 5% for database operations
        update_progress(current_progress, f"Procesando coincidencias... ({i+1}/{total_inputs})")
        
        if cleaned == "NN":
            best_matches.append("NN")
            similarities.append("")
            matched_words.append("")
            missing_words.append("")
            catalog_ids.append("")
            categorias.append("")
            variedades.append("")
            colores.append("")
            grados.append("")
        else:
            if cleaned not in cache:
                match, score = process.extractOne(cleaned, choices, scorer=fuzz.token_sort_ratio)
                input_words = set(extract_words(cleaned))
                match_words = set(extract_words(match))
                matched = input_words.intersection(match_words)
                missing = input_words.difference(match_words)

                idx = df2[df2["search_key"] == match].index
                catalog_id = df2.loc[idx[0], df2.columns[5]] if len(df2.columns) > 5 and not idx.empty else ""
                categoria = df2.loc[idx[0], df2.columns[0]] if not idx.empty else ""
                variedad = df2.loc[idx[0], df2.columns[1]] if not idx.empty else ""
                color = df2.loc[idx[0], df2.columns[2]] if not idx.empty else ""
                grado = df2.loc[idx[0], df2.columns[3]] if not idx.empty else ""

                cache[cleaned] = {
                    "match": match,
                    "score": score,
                    "matched": " ".join(matched),
                    "missing": " ".join(missing),
                    "catalog_id": catalog_id,
                    "categoria": categoria,
                    "variedad": variedad,
                    "color": color,
                    "grado": grado
                }

            result = cache[cleaned]
            best_matches.append(result["match"])
            similarities.append(result["score"])
            matched_words.append(result["matched"])
            missing_words.append(result["missing"])
            catalog_ids.append(result["catalog_id"])
            categorias.append(result["categoria"])
            variedades.append(result["variedad"])
            colores.append(result["color"])
            grados.append(result["grado"])

    # Agregar resultados al DataFrame
    df1["Best match"] = best_matches
    df1["Similarity %"] = similarities
    df1["Matched Words"] = matched_words
    df1["Missing Words"] = missing_words
    df1["Catalog ID"] = catalog_ids
    df1["Categoria"] = categorias
    df1["Variedad"] = variedades
    df1["Color"] = colores
    df1["Grado"] = grados

    # Add empty columns for user actions (if not present)
    if "Accept Map" not in df1.columns:
        df1["Accept Map"] = ""
    if "Deny Map" not in df1.columns:
        df1["Deny Map"] = ""
    if "Action" not in df1.columns:
        df1["Action"] = ""
    if "Word" not in df1.columns:
        df1["Word"] = ""

    df1.drop(columns=["duplicate_key"], inplace=True)
    
    update_progress(95, "Guardando en base de datos...")
    
    # Basic database saving
    if BASIC_DB_AVAILABLE:
        # Eliminar filas con "NN" (duplicados o vacíos)
        df_to_save = df1[df1["Cleaned input"] != "NN"].copy()
        
        # Guardar en base de datos
        success, message = save_processed_data_to_database(df_to_save)
        if not success:
            logger.error(f"Error al guardar en base de datos: {message}")
            update_progress(98, f"⚠️ Error en guardado: {message}")
        else:
            logger.info(f"Guardado en base de datos completado: {message}")
            update_progress(100, "✅ Procesamiento completado!")
    else:
        logger.warning("No database integration available, skipping save")
        update_progress(100, "✅ Procesamiento completado (sin guardar en BD)!")
        
    return df1

def process_files_with_tqdm_and_callback(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, 
                                        progress_callback: Optional[Callable] = None,
                                        client_id: Optional[str] = None) -> pd.DataFrame:
    """
    Version that maintains the original tqdm display while also supporting Streamlit callback
    Can work with or without multi-client features
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame  
        dictionary: Synonyms and blacklist dictionary
        progress_callback: Optional Streamlit progress callback
        client_id: Optional client identifier for multi-client operations
        
    Returns:
        Processed DataFrame
    """
    logger.info(f"Starting tqdm processing" + (f" for client: {client_id}" if client_id else ""))
    
    df1 = df1.copy()
    df2 = df2.copy()

    def update_progress(progress_pct: float, message: str = "Processing..."):
        if progress_callback:
            progress_callback(progress_pct, message)

    # Columnas clave
    col_desc = df1.columns[0]
    col_vendor = df1.columns[2]

    update_progress(1, "Iniciando procesamiento...")

    # Crear clave de deduplicación
    df1["duplicate_key"] = df1[col_desc].str.strip().str.lower() + "|" + df1[col_vendor].str.strip().str.lower()
    seen = set()
    cleaned_inputs_status = []

    for key in df1["duplicate_key"]:
        if key in seen:
            cleaned_inputs_status.append("NN")
        else:
            seen.add(key)
            cleaned_inputs_status.append("PENDING")

    update_progress(3, "Limpiando texto y aplicando filtros...")

    # Get synonyms and blacklist (client-specific if available)
    if client_id and ENHANCED_DB_AVAILABLE:
        try:
            client_data = get_client_synonyms_blacklist(client_id)
            client_synonyms = client_data.get('synonyms', {})
            client_blacklist = client_data.get('blacklist', {}).get('input', [])
            
            merged_synonyms = {**client_synonyms, **dictionary.get("synonyms", {})}
            merged_blacklist = list(set(client_blacklist + dictionary.get("blacklist", {}).get("input", [])))
        except Exception as e:
            logger.warning(f"Could not load client data: {str(e)}")
            merged_synonyms = dictionary.get("synonyms", {})
            merged_blacklist = dictionary.get("blacklist", {}).get("input", [])
    else:
        merged_synonyms = dictionary.get("synonyms", {})
        merged_blacklist = dictionary.get("blacklist", {}).get("input", [])

    # Procesar texto: limpieza, sinónimos, blacklist
    inputs = []
    synonyms_applied = []
    removed_blacklist = []

    for idx, row in df1.iterrows():
        if cleaned_inputs_status[idx] == "NN":
            inputs.append("NN")
            synonyms_applied.append("")
            removed_blacklist.append("")
        else:
            raw_text = str(row[col_desc])
            cleaned = clean_text(raw_text)

            # Aplicar sinónimos
            cleaned, applied = apply_synonyms(cleaned, merged_synonyms)
            formatted_applied = ", ".join([f"{o}→{n}" for o, n in applied])
            synonyms_applied.append(formatted_applied)

            # Aplicar blacklist
            cleaned, removed = remove_blacklist(cleaned, merged_blacklist)
            removed_blacklist.append(" ".join(removed))

            inputs.append(cleaned.strip())

    df1["Cleaned input"] = inputs
    df1["Applied Synonyms"] = synonyms_applied
    df1["Removed Blacklist Words"] = removed_blacklist

    update_progress(5, "Preparando catálogo para matching...")

    # Prepare catalog (enhanced if client_id provided)
    if client_id and ENHANCED_DB_AVAILABLE:
        df2_prepared = prepare_enhanced_catalog(df2, client_id)
    else:
        df2_prepared = df2.copy()

    # Crear columna search_key
    concat_cols = df2_prepared.columns[:4]
    df2_prepared["search_key"] = df2_prepared[concat_cols].fillna("").agg(" ".join, axis=1)
    df2_prepared["search_key"] = df2_prepared["search_key"].apply(lambda x: clean_text(x.strip().lower()))

    update_progress(10, "Iniciando procesamiento de coincidencias...")

    # Matching optimizado con cache
    best_matches = []
    similarities = []
    matched_words = []
    missing_words = []
    catalog_ids = []
    categorias = []
    variedades = []
    colores = []
    grados = []

    choices = df2_prepared["search_key"].tolist()
    cache = {}

    # THIS IS THE ORIGINAL TQDM LOOP - 90% of processing time
    cleaned_inputs = df1["Cleaned input"].tolist()
    total_inputs = len(cleaned_inputs)
    
    # Use tqdm for console output AND callback for Streamlit
    for i, cleaned in enumerate(tqdm(cleaned_inputs, desc="Procesando coincidencias", ncols=80)):
        # Update Streamlit progress (10% to 100% = 90% of total)
        current_progress = 10 + ((i / total_inputs) * 85)
        update_progress(current_progress, f"Procesando coincidencias... ({i+1}/{total_inputs})")
        
        if cleaned == "NN":
            best_matches.append("NN")
            similarities.append("")
            matched_words.append("")
            missing_words.append("")
            catalog_ids.append("")
            categorias.append("")
            variedades.append("")
            colores.append("")
            grados.append("")
        else:
            if cleaned not in cache:
                match, score = process.extractOne(cleaned, choices, scorer=fuzz.token_sort_ratio)
                input_words = set(extract_words(cleaned))
                match_words = set(extract_words(match))
                matched = input_words.intersection(match_words)
                missing = input_words.difference(match_words)

                idx = df2_prepared[df2_prepared["search_key"] == match].index
                catalog_id = df2_prepared.loc[idx[0], df2_prepared.columns[5]] if len(df2_prepared.columns) > 5 and not idx.empty else ""
                categoria = df2_prepared.loc[idx[0], df2_prepared.columns[0]] if not idx.empty else ""
                variedad = df2_prepared.loc[idx[0], df2_prepared.columns[1]] if not idx.empty else ""
                color = df2_prepared.loc[idx[0], df2_prepared.columns[2]] if not idx.empty else ""
                grado = df2_prepared.loc[idx[0], df2_prepared.columns[3]] if not idx.empty else ""

                cache[cleaned] = {
                    "match": match,
                    "score": score,
                    "matched": " ".join(matched),
                    "missing": " ".join(missing),
                    "catalog_id": catalog_id,
                    "categoria": categoria,
                    "variedad": variedad,
                    "color": color,
                    "grado": grado
                }

            result = cache[cleaned]
            best_matches.append(result["match"])
            similarities.append(result["score"])
            matched_words.append(result["matched"])
            missing_words.append(result["missing"])
            catalog_ids.append(result["catalog_id"])
            categorias.append(result["categoria"])
            variedades.append(result["variedad"])
            colores.append(result["color"])
            grados.append(result["grado"])

    # Agregar resultados al DataFrame
    df1["Best match"] = best_matches
    df1["Similarity %"] = similarities
    df1["Matched Words"] = matched_words
    df1["Missing Words"] = missing_words
    df1["Catalog ID"] = catalog_ids
    df1["Categoria"] = categorias
    df1["Variedad"] = variedades
    df1["Color"] = colores
    df1["Grado"] = grados

    # Add empty columns for user actions
    if "Accept Map" not in df1.columns:
        df1["Accept Map"] = ""
    if "Deny Map" not in df1.columns:
        df1["Deny Map"] = ""
    if "Action" not in df1.columns:
        df1["Action"] = ""
    if "Word" not in df1.columns:
        df1["Word"] = ""

    df1.drop(columns=["duplicate_key"], inplace=True)
    
    update_progress(95, "Guardando datos...")
    
    # Save using appropriate method
    if client_id and ENHANCED_DB_AVAILABLE:
        success, message = save_processed_data_multiclient(df1, client_id)
    elif BASIC_DB_AVAILABLE:
        df_to_save = df1[df1["Cleaned input"] != "NN"].copy()
        success, message = save_processed_data_to_database(df_to_save)
    else:
        success, message = False, "No database integration available"
    
    if success:
        update_progress(100, "✅ Procesamiento completado!")
    else:
        logger.error(f"Error saving: {message}")
        update_progress(100, f"⚠️ Procesamiento completado, error guardando: {message}")
    
    return df1

def prepare_enhanced_catalog(df2: pd.DataFrame, client_id: str) -> pd.DataFrame:
    """
    Prepare enhanced catalog combining master catalog with client staging products
    
    Args:
        df2: Original catalog DataFrame
        client_id: Client identifier
        
    Returns:
        Enhanced DataFrame with master + staging products
    """
    try:
        logger.info(f"Preparing enhanced catalog for client {client_id}")
        
        # Start with original catalog
        enhanced_catalog = df2.copy()
        
        # Add source column to track origin
        enhanced_catalog["source"] = "master"
        
        # Get client staging products if enhanced DB is available
        if ENHANCED_DB_AVAILABLE:
            db = EnhancedMultiClientDatabase(client_id)
            staging_df = db.get_staging_products()
            
            if staging_df is not None and len(staging_df) > 0:
                logger.info(f"Found {len(staging_df)} staging products for client {client_id}")
                
                # Ensure we have the required columns in staging data
                required_cols = ['categoria', 'variedad', 'color', 'grado']
                if all(col in staging_df.columns for col in required_cols):
                    # Format staging products to match catalog structure
                    staging_formatted = pd.DataFrame()
                    
                    # Map staging columns to catalog columns
                    staging_formatted[df2.columns[0]] = staging_df['categoria']
                    staging_formatted[df2.columns[1]] = staging_df['variedad']
                    staging_formatted[df2.columns[2]] = staging_df['color']
                    staging_formatted[df2.columns[3]] = staging_df['grado']
                    
                    # Handle additional columns
                    if len(df2.columns) > 4:
                        staging_formatted[df2.columns[4]] = staging_df.get('additional_field_1', '')
                    if len(df2.columns) > 5:
                        staging_formatted[df2.columns[5]] = staging_df.get('catalog_id', '111111')
                    
                    staging_formatted["source"] = "staging"
                    
                    # Combine master and staging
                    enhanced_catalog = pd.concat([enhanced_catalog, staging_formatted], ignore_index=True)
                    logger.info(f"Combined catalog now has {len(enhanced_catalog)} total products")
                else:
                    logger.warning(f"Staging products missing required columns: {required_cols}")
            else:
                logger.info(f"No staging products found for client {client_id}")
        else:
            logger.info("Enhanced database not available, using master catalog only")
        
        return enhanced_catalog
        
    except Exception as e:
        logger.error(f"Error preparing enhanced catalog: {str(e)}")
        # Return original catalog if enhancement fails
        df2_copy = df2.copy()
        df2_copy["source"] = "master"
        return df2_copy

def perform_enhanced_matching(cleaned_inputs: List[str], df2_enhanced: pd.DataFrame, 
                            progress_callback: Optional[Callable], client_id: str) -> Dict[str, List]:
    """
    Perform enhanced fuzzy matching with improved algorithms and caching
    
    Args:
        cleaned_inputs: List of cleaned input strings
        df2_enhanced: Enhanced catalog DataFrame
        progress_callback: Progress callback function
        client_id: Client identifier
        
    Returns:
        Dictionary with matching results
    """
    logger.info(f"Starting enhanced matching for {len(cleaned_inputs)} inputs")
    
    # Initialize result containers
    best_matches = []
    similarities = []
    matched_words = []
    missing_words = []
    catalog_ids = []
    categorias = []
    variedades = []
    colores = []
    grados = []

    choices = df2_enhanced["search_key"].tolist()
    cache = {}
    
    total_inputs = len(cleaned_inputs)

    def update_progress(progress_pct: float, message: str):
        if progress_callback:
            progress_callback(progress_pct, message)

    # Main matching loop (85% of processing time: 10% to 95%)
    for i, cleaned in enumerate(cleaned_inputs):
        current_progress = 10 + ((i / total_inputs) * 85)  # 85% for matching, 5% for saving
        update_progress(current_progress, f"Procesando coincidencias mejoradas... ({i+1}/{total_inputs})")
        
        if cleaned == "NN":
            # Handle duplicates
            best_matches.append("NN")
            similarities.append("")
            matched_words.append("")
            missing_words.append("")
            catalog_ids.append("")
            categorias.append("")
            variedades.append("")
            colores.append("")
            grados.append("")
        else:
            if cleaned not in cache:
                # Enhanced matching with multiple algorithms
                result = enhanced_fuzzy_match(cleaned, choices, df2_enhanced)
                cache[cleaned] = result
            
            result = cache[cleaned]
            best_matches.append(result["match"])
            similarities.append(result["score"])
            matched_words.append(result["matched"])
            missing_words.append(result["missing"])
            catalog_ids.append(result["catalog_id"])
            categorias.append(result["categoria"])
            variedades.append(result["variedad"])
            colores.append(result["color"])
            grados.append(result["grado"])

    logger.info(f"Enhanced matching completed with {len(cache)} unique matches cached")
    
    return {
        "best_matches": best_matches,
        "similarities": similarities,
        "matched_words": matched_words,
        "missing_words": missing_words,
        "catalog_ids": catalog_ids,
        "categorias": categorias,
        "variedades": variedades,
        "colores": colores,
        "grados": grados
    }

def enhanced_fuzzy_match(cleaned_input: str, choices: List[str], df2_enhanced: pd.DataFrame) -> Dict[str, Any]:
    """
    Enhanced fuzzy matching with multiple algorithms and better scoring
    
    Args:
        cleaned_input: Cleaned input string
        choices: List of search keys from catalog
        df2_enhanced: Enhanced catalog DataFrame
        
    Returns:
        Dictionary with match results
    """
    try:
        # Primary matching algorithm
        match, score = process.extractOne(cleaned_input, choices, scorer=fuzz.token_sort_ratio)
        
        # Try alternative algorithm if score is low
        if score < 70:
            alt_match, alt_score = process.extractOne(cleaned_input, choices, scorer=fuzz.partial_ratio)
            if alt_score > score:
                match, score = alt_match, alt_score
                logger.debug(f"Used partial_ratio for better match: {cleaned_input} -> {match} ({score}%)")
        
        # Try token_set_ratio for another perspective
        if score < 80:
            token_match, token_score = process.extractOne(cleaned_input, choices, scorer=fuzz.token_set_ratio)
            if token_score > score:
                match, score = token_match, token_score
                logger.debug(f"Used token_set_ratio for better match: {cleaned_input} -> {match} ({score}%)")
        
        # Calculate word analysis
        input_words = set(extract_words(cleaned_input))
        match_words = set(extract_words(match))
        matched = input_words.intersection(match_words)
        missing = input_words.difference(match_words)

        # Get catalog information
        idx = df2_enhanced[df2_enhanced["search_key"] == match].index
        if not idx.empty:
            row_idx = idx[0]
            catalog_id = df2_enhanced.loc[row_idx, df2_enhanced.columns[5]] if len(df2_enhanced.columns) > 5 else ""
            categoria = df2_enhanced.loc[row_idx, df2_enhanced.columns[0]]
            variedad = df2_enhanced.loc[row_idx, df2_enhanced.columns[1]]
            color = df2_enhanced.loc[row_idx, df2_enhanced.columns[2]]
            grado = df2_enhanced.loc[row_idx, df2_enhanced.columns[3]]
        else:
            catalog_id = categoria = variedad = color = grado = ""

        return {
            "match": match,
            "score": score,
            "matched": " ".join(sorted(matched)),
            "missing": " ".join(sorted(missing)),
            "catalog_id": catalog_id,
            "categoria": categoria,
            "variedad": variedad,
            "color": color,
            "grado": grado
        }
        
    except Exception as e:
        logger.error(f"Error in enhanced fuzzy matching: {str(e)}")
        return {
            "match": "", "score": 0, "matched": "", "missing": "",
            "catalog_id": "", "categoria": "", "variedad": "", "color": "", "grado": ""
        }

def save_processed_data_multiclient(df: pd.DataFrame, client_id: str) -> Tuple[bool, str]:
    """
    Enhanced multi-client data saving with better error handling
    
    Args:
        df: Processed DataFrame to save
        client_id: Client identifier
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Remove duplicates before saving
        df_to_save = df[df["Cleaned input"] != "NN"].copy()
        
        if len(df_to_save) == 0:
            return False, "No valid data to save (all rows were duplicates)"
        
        # Generate batch ID
        batch_id = f"batch_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}_{client_id}"
        
        # Save using enhanced multi-client system
        success, message = save_client_processed_data(client_id, df_to_save, batch_id)
        
        if success:
            logger.info(f"Successfully saved {len(df_to_save)} records for client {client_id}")
            return True, f"Saved {len(df_to_save)} records to database for client {client_id}"
        else:
            logger.error(f"Failed to save data for client {client_id}: {message}")
            return False, f"Database save failed: {message}"
            
    except Exception as e:
        error_msg = f"Error saving processed data for client {client_id}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def get_processing_statistics(client_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get processing statistics and system status
    
    Args:
        client_id: Optional client identifier
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        "enhanced_db_available": ENHANCED_DB_AVAILABLE,
        "basic_db_available": BASIC_DB_AVAILABLE,
        "client_id": client_id,
        "timestamp": pd.Timestamp.now().isoformat()
    }
    
    if client_id and ENHANCED_DB_AVAILABLE:
        try:
            from Enhanced_MultiClient_Database import get_client_statistics
            client_stats = get_client_statistics(client_id)
            stats.update(client_stats)
        except Exception as e:
            stats["client_stats_error"] = str(e)
    
    return stats

def validate_input_data(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict) -> Tuple[bool, str]:
    """
    Validate input data before processing
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame
        dictionary: Synonyms and blacklist dictionary
        
    Returns:
        Tuple of (is_valid: bool, error_message: str)
    """
    try:
        # Check DataFrames are not empty
        if df1.empty:
            return False, "Input DataFrame (df1) is empty"
        
        if df2.empty:
            return False, "Catalog DataFrame (df2) is empty"
        
        # Check minimum column requirements
        if len(df1.columns) < 3:
            return False, f"Input DataFrame must have at least 3 columns, got {len(df1.columns)}"
        
        if len(df2.columns) < 4:
            return False, f"Catalog DataFrame must have at least 4 columns, got {len(df2.columns)}"
        
        # Check dictionary structure
        if not isinstance(dictionary, dict):
            return False, "Dictionary must be a dict object"
        
        # Validate synonyms structure
        synonyms = dictionary.get("synonyms", {})
        if not isinstance(synonyms, dict):
            return False, "Dictionary 'synonyms' must be a dict"
        
        # Validate blacklist structure
        blacklist = dictionary.get("blacklist", {})
        if not isinstance(blacklist, dict):
            return False, "Dictionary 'blacklist' must be a dict"
        
        blacklist_input = blacklist.get("input", [])
        if not isinstance(blacklist_input, list):
            return False, "Dictionary 'blacklist.input' must be a list"
        
        # Check for required columns in df1
        required_columns = [0, 2]  # description, vendor
        for col_idx in required_columns:
            if col_idx >= len(df1.columns):
                return False, f"Input DataFrame missing required column at index {col_idx}"
        
        return True, "Input data validation passed"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def create_processing_summary(df: pd.DataFrame, client_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a summary of processing results
    
    Args:
        df: Processed DataFrame
        client_id: Optional client identifier
        
    Returns:
        Dictionary with processing summary
    """
    try:
        total_rows = len(df)
        valid_rows = len(df[df["Cleaned input"] != "NN"])
        duplicate_rows = total_rows - valid_rows
        
        # Similarity statistics
        similarities = df[df["Similarity %"] != ""]["Similarity %"]
        if len(similarities) > 0:
            similarities_numeric = pd.to_numeric(similarities, errors='coerce')
            avg_similarity = similarities_numeric.mean()
            min_similarity = similarities_numeric.min()
            max_similarity = similarities_numeric.max()
        else:
            avg_similarity = min_similarity = max_similarity = 0
        
        # Count matches that need product creation
        needs_creation = len(df[df["Catalog ID"].astype(str).str.contains("111111", na=False)])
        
        # User action counts
        accepted = len(df[df["Accept Map"].astype(str).str.lower() == "true"])
        denied = len(df[df["Deny Map"].astype(str).str.lower() == "true"])
        pending_review = valid_rows - accepted - denied
        
        summary = {
            "client_id": client_id,
            "processing_timestamp": pd.Timestamp.now().isoformat(),
            "totals": {
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "duplicate_rows": duplicate_rows,
                "needs_product_creation": needs_creation
            },
            "similarity_stats": {
                "average": round(avg_similarity, 2) if not pd.isna(avg_similarity) else 0,
                "minimum": round(min_similarity, 2) if not pd.isna(min_similarity) else 0,
                "maximum": round(max_similarity, 2) if not pd.isna(max_similarity) else 0
            },
            "user_actions": {
                "accepted": accepted,
                "denied": denied,
                "pending_review": pending_review
            },
            "top_vendors": df["Vendor Name"].value_counts().head(5).to_dict() if "Vendor Name" in df.columns else {},
            "top_categories": df["Categoria"].value_counts().head(5).to_dict() if "Categoria" in df.columns else {}
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Error creating processing summary: {str(e)}")
        return {
            "error": str(e),
            "client_id": client_id,
            "processing_timestamp": pd.Timestamp.now().isoformat()
        }

# Export all main functions for easy importing
__all__ = [
    'process_files',
    'process_files_multiclient', 
    'process_files_basic',
    'process_files_with_tqdm_and_callback',
    'prepare_enhanced_catalog',
    'perform_enhanced_matching',
    'enhanced_fuzzy_match',
    'save_processed_data_multiclient',
    'get_processing_statistics',
    'validate_input_data',
    'create_processing_summary'
]

# Module information
__version__ = "2.0.0"
__author__ = "Enhanced Multi-Client Processing System"
__description__ = "Complete data processing logic with multi-client database integration"

if __name__ == "__main__":
    # Module testing and demonstration
    print(f"🔧 Enhanced Logic Module v{__version__}")
    print(f"📊 {__description__}")
    print(f"✅ Enhanced DB Available: {ENHANCED_DB_AVAILABLE}")
    print(f"✅ Basic DB Available: {BASIC_DB_AVAILABLE}")
    
    # Show available functions
    print(f"\n📋 Available Functions:")
    for func_name in __all__:
        print(f"   - {func_name}")
    
    # Basic system check
    stats = get_processing_statistics()
    print(f"\n📈 System Status:")
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print(f"\n✅ Module loaded successfully!")
    print(f"💡 Usage: from logic import process_files")
    print(f"💡 Multi-client: process_files(df1, df2, dict, client_id='your_client')")
    print(f"💡 Basic mode: process_files(df1, df2, dict) # auto-detects capabilities")