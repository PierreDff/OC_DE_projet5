import mongomock
import pandas as pd
from unittest.mock import patch
from src.pipelines.migrate_data import MongoMigrator

@patch('src.pipelines.migrate_data.MongoClient')
def test_migration_logic(mock_client):
    # Setup du mock MongoDB
    mock_mongo = mongomock.MongoClient()
    mock_client.return_value = mock_mongo
    
    # Données de test
    df = pd.DataFrame([{'name': 'Test User', 'age': 30}])
    
    migrator = MongoMigrator()
    migrator.migrate(df)
    
    # Vérification que la donnée est bien "insérée"
    inserted_doc = mock_mongo.healthcare_db.patients.find_one()
    assert inserted_doc['name'] == 'Test User'