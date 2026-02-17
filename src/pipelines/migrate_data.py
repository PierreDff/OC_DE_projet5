from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from src.utils.envconf import settings
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MongoMigrator:
    def __init__(self):
        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client[settings.MONGO_DB]
        self.collection = self.db[settings.MONGO_COLLECTION]

    def migrate(self, df: pd.DataFrame) -> None:
        """
        Exécute la migration des données du DataFrame vers MongoDB.
        
        Cette méthode est idempotente : elle supprime les données existantes 
        avant d'insérer les nouvelles pour éviter les doublons.
        """
        if df.empty:
            logger.warning("Le DataFrame est vide. Aucune migration effectuée.")
            return
        
        logger.info("NETTOYAGE : Suppression des données existantes")
        self.collection.delete_many({})
        
        # Transformation du DataFrame en liste de dictionnaires (format JSON-like)
        data_dict = df.to_dict("records")
        total_records = len(data_dict)
        
        logger.info(f"Début de la migration de {total_records} documents...")
        
        # Insertion par lot (Batching)
        batch_size = settings.BATCH_SIZE
        for i in range(0, total_records, batch_size):
            batch = data_dict[i : i + batch_size]
            try:
                self.collection.insert_many(batch)
                logger.info(f"Inséré batch {i//batch_size + 1}/{(total_records // batch_size) + 1}")
            except BulkWriteError as bwe:
                logger.error(f"Erreur d'insertion: {bwe.details}")
        
        logger.info("Migration terminée.")

    def create_indexes(self) -> None:
        """
        Crée les index nécessaires pour optimiser les performances de lecture.
        
        Index créés :
        - 'name' : pour la recherche textuelle simple.
        - 'admission_type' + 'date_of_admission' : index composé pour les filtres et tris.
        """
        logger.info("Création des index...")
        self.collection.create_index("name")
        self.collection.create_index([("admission_type", 1), ("date_of_admission", -1)])

    def close(self) -> None:
        """Ferme proprement la connexion au client MongoDB."""
        self.client.close()
        logger.info("Connexion MongoDB fermée.")