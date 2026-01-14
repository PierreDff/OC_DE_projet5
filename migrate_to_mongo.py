import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

def migrate_data(csv_file_path):
    # 1. Connexion à MongoDB
    # On récupère l'URI depuis les variables d'environnement pour plus de sécurité
    mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client[os.getenv("MONGO_DB", "healthcare_db")]
    collection = db["patients"]

    print(f"--- Début de la migration de {csv_file_path} ---")

    try:
        # 2. Lecture du CSV avec Pandas
        df = pd.read_csv(csv_file_path)

        # 3. Nettoyage des données
        # Normalisation des noms (ex: STepHAniE -> Stephanie)
        df['Name'] = df['Name'].str.title()
        
        # Conversion des dates en objets datetime pour MongoDB
        df['Date of Admission'] = pd.to_datetime(df['Date of Admission'])
        df['Discharge Date'] = pd.to_datetime(df['Discharge Date'])

        # 4. Conversion en dictionnaire pour l'insertion
        data_dict = df.to_dict(orient='records')

        # 5. Insertion dans MongoDB
        collection.delete_many({}) # Optionnel : Nettoie la collection avant import
        result = collection.insert_many(data_dict)
        
        print(f"Succès ! {len(result.inserted_ids)} documents ont été insérés.")

    except Exception as e:
        print(f"Erreur lors de la migration : {e}")
    finally:
        client.close()

if __name__ == "__main__":
    migrate_data('healthcare_dataset.csv')