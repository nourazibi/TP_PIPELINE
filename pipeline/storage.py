"""Module de stockage des données."""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path

from .config import RAW_DIR, PROCESSED_DIR

def save_raw_json(data: list[dict], name: str) -> Path:
    """Sauvegarde les données brutes en JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = RAW_DIR / f"{name}_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Brut: {filepath.name} ({size_kb:.1f} KB)")

    return filepath


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """Sauvegarde les données transformées en Parquet en gérant correctement les types."""
    # Convertir les colonnes object en numérique si possible
    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        except Exception:
            pass  # laisser les colonnes non numériques inchangées

    # Colonnes catégorielles : ajouter 'unknown' si nécessaire et remplir NaN
    for col in df.select_dtypes(include="category").columns:
        if 'unknown' not in df[col].cat.categories:
            df[col] = df[col].cat.add_categories('unknown')
        df[col].fillna('unknown', inplace=True)

    # Colonnes numériques : remplir NaN par 0
    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        df[col].fillna(0, inplace=True)

    # Créer le nom de fichier
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = PROCESSED_DIR / f"{name}_{timestamp}.parquet"

    # Sauvegarder en Parquet
    df.to_parquet(filepath, index=False, compression="snappy")

    size_kb = filepath.stat().st_size / 1024
    print(f"   💾 Parquet: {filepath.name} ({size_kb:.1f} KB)")

    return filepath


def load_parquet(filepath: str | Path) -> pd.DataFrame:
    """Charge un fichier Parquet et retourne un DataFrame pandas."""
    return pd.read_parquet(filepath)
