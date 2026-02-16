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

    # Nettoyage et migration des données : on assure l'idempotence en supprimant les données existantes avant d'insérer les nouvelles.
    def migrate(self, df: pd.DataFrame):
        if df.empty:
            logger.warning("Le DataFrame est vide. Aucune migration effectuée.")
            return
        
        logger.info("NETTOYAGE : Suppression des données existantes")
        self.collection.delete_many({})
        
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

    # Création d'index pour optimiser les recherches futures
    def create_indexes(self):
        """Création d'index pour optimiser les recherches futures."""
        logger.info("Création des index...")
        # Index sur le nom pour recherche rapide
        self.collection.create_index("name")
        # Index sur le type d'admission et la date pour les stats
        self.collection.create_index([("admission_type", 1), ("date_of_admission", -1)])

    # Fermeture de la connexion au client MongoDB
    def close(self):
        """Ferme proprement la connexion au client MongoDB."""
        self.client.close()
        logger.info("Connexion MongoDB fermée.")