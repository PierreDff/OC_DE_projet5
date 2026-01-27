import pandas as pd
from src.pipelines.process_data import clean_dataframe

def test_clean_dataframe_normalization():
    # Données brutes avec noms mal formatés
    data = {
        'Name': ['bobby jackson', 'ALICE smith'],
        'Date of Admission': ['2024-01-31', '2023-12-01'],
        'Discharge Date': ['2024-02-02', '2023-12-05'],
        'Billing Amount': [100.0, 200.0]
    }
    df = pd.DataFrame(data)
    
    cleaned = clean_dataframe(df)
    
    # Vérification Title Case
    assert cleaned.iloc[0]['name'] == 'Bobby Jackson'
    # Vérification snake_case des colonnes
    assert 'billing_amount' in cleaned.columns
    # Vérification conversion DateTime
    assert pd.api.types.is_datetime64_any_dtype(cleaned['date_of_admission'])