import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie et type le DataFrame brut."""
    
    # 1. Normalisation des noms (Title Case)
    df['Name'] = df['Name'].str.title()
    
    # 2. Conversion des dates (String -> Datetime MongoDB compatible)
    date_cols = ['Date of Admission', 'Discharge Date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        
    # 3. Renommage des colonnes pour le format standard (snake_case)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    # 4. Gestion basique des doublons
    initial_count = len(df)
    df = df.drop_duplicates()
    if len(df) < initial_count:
        logger.info(f"Suppression de {initial_count - len(df)} doublons.")

    return df

def load_data(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Chargement de {len(df)} lignes depuis {filepath}")
        return df
    except FileNotFoundError:
        logger.error(f"Fichier non trouvé: {filepath}")
        raise