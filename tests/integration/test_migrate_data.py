import mongomock
import pandas as pd
from unittest.mock import patch
from src.pipelines.migrate_data import MongoMigrator
from src.utils.envconf import settings

@patch('src.pipelines.migrate_data.MongoClient')
def test_migration_logic(mock_client):
    # Setup du mock MongoDB
    mock_mongo = mongomock.MongoClient()
    mock_client.return_value = mock_mongo
    
    # Données de test
    df = pd.DataFrame([{'name': 'Test Patient', 'admission_type': 'Emergency'}])
    
    migrator = MongoMigrator()
    migrator.migrate(df)
    
    # Vérification que la donnée est bien "insérée"
    # On utilise settings pour récupérer dynamiquement le nom de la DB et de la collection
    inserted_doc = mock_mongo[settings.MONGO_DB][settings.MONGO_COLLECTION].find_one()
    assert inserted_doc is not None, "Le document n'a pas été inséré dans la base mockée"
    assert inserted_doc['name'] == 'Test Patient'
    assert inserted_doc['admission_type'] == 'Emergency'