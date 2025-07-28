# logic.py - Enhanced with progress callback focused on main tqdm loop
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words
from database_integration import save_processed_data_to_database

def process_files(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, progress_callback=None) -> pd.DataFrame:
    """
    Process files with optional progress callback for Streamlit integration
    Focus 90% of progress on the main "Procesando coincidencias" loop
    
    Args:
        df1: Main input DataFrame
        df2: Catalog DataFrame  
        dictionary: Synonyms and blacklist dictionary
        progress_callback: Optional function to call with progress updates (progress_pct, message)
    """
    df1 = df1.copy()
    df2 = df2.copy()

    def update_progress(progress_pct, message="Processing..."):
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
        current_progress = 10 + ((i / total_inputs) * 90)
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
                catalog_id = df2.loc[idx[0], df2.columns[5]] if not idx.empty else ""
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

    df1.drop(columns=["duplicate_key"], inplace=True)
    
    update_progress(100, "¡Procesamiento completado!")
    
        # Eliminar filas con "NN" (duplicados o vacíos)
    df_to_save = df1[df1["Cleaned input"] != "NN"].copy()
    
    # Guardar en base de datos
    success, message = save_processed_data_to_database(df_to_save)
    if not success:
        print(f"❌ Error al guardar en base de datos: {message}")
    else:
        print(f"✅ Guardado en base de datos completado: {message}")
        
    return df1


# Alternative version that keeps the original tqdm but also supports callback
def process_files_with_tqdm_and_callback(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, progress_callback=None) -> pd.DataFrame:
    """
    Version that maintains the original tqdm display while also supporting Streamlit callback
    """
    df1 = df1.copy()
    df2 = df2.copy()

    def update_progress(progress_pct, message="Processing..."):
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

    choices = df2["search_key"].tolist()
    cache = {}

    # THIS IS THE ORIGINAL TQDM LOOP - 90% of processing time
    cleaned_inputs = df1["Cleaned input"].tolist()
    total_inputs = len(cleaned_inputs)
    
    # Use tqdm for console output AND callback for Streamlit
    for i, cleaned in enumerate(tqdm(cleaned_inputs, desc="Procesando coincidencias", ncols=80)):
        # Update Streamlit progress (10% to 100% = 90% of total)
        current_progress = 10 + ((i / total_inputs) * 90)
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
                catalog_id = df2.loc[idx[0], df2.columns[5]] if not idx.empty else ""
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

    df1.drop(columns=["duplicate_key"], inplace=True)
    
    update_progress(100, "¡Procesamiento completado!")
    
    return df1