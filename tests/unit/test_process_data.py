import pandas as pd
from src.pipelines.process_data import clean_dataframe

def test_clean_dataframe_normalization():
    """
    Ce test vérifie la logique de la fonction clean_dataframe de manière ISOLÉE.
    Nous créons un petit jeu de données fictif pour ce test.
    """
    # 1. SETUP : Création de données brutes fictives (comme si elles venaient du CSV)
    data = {
        'Name': ['bobby jackson', 'ALICE smith'],
        'Date of Admission': ['2024-01-31', '2023-12-01'],
        'Discharge Date': ['2024-02-02', '2023-12-05'],
        'Billing Amount': [100.0, 200.0]
    }
    df = pd.DataFrame(data)
    
    # 2. EXECUTION : On appelle la fonction à tester
    cleaned = clean_dataframe(df)
    
    # 3. ASSERTION : On vérifie que le résultat est conforme aux attentes
    # Vérification Title Case
    assert cleaned.iloc[0]['name'] == 'Bobby Jackson'
    assert cleaned.iloc[1]['name'] == 'Alice Smith'
    # Vérification snake_case des colonnes
    assert 'billing_amount' in cleaned.columns
    # Vérification conversion DateTime
    assert pd.api.types.is_datetime64_any_dtype(cleaned['date_of_admission'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned['discharge_date'])