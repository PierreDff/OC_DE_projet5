import sys
import os
# Ajout du dossier courant au path pour l'exécution directe
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pipelines.process_data import load_data, clean_dataframe
from src.pipelines.migrate_data import MongoMigrator
from src.utils.envconf import settings

def main():
    csv_path = "data/healthcare_dataset.csv"
    
    # 1. Extraction & Transformation
    raw_df = load_data(csv_path)
    clean_df = clean_dataframe(raw_df)
    
    # 2. Chargement
    migrator = MongoMigrator()
    migrator.migrate(clean_df)
    
    # 3. Post-Optimisation
    migrator.create_indexes()

if __name__ == "__main__":
    main()