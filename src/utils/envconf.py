from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Gestion centralisée de la configuration via Pydantic.
    Pydantic va lire les variables d'environnement (système ou fichier .env),
    En héritant de BaseSettings, cette classe charge automatiquement les variables d'environnement (système ou fichier .env),
    les valider selon les types spécifiés, et lever une erreur si une variable obligatoire manque.
    """

    # Champs Obligatoires
    # Si ces variables ne sont pas trouvées dans le .env, l'application s'arrête immédiatement.
    # Pydantic vérifie et tente de convertir (caster) la valeur vers le type demandé.
    APP_USER: str
    APP_PASSWORD: str
    MONGO_HOST: str

    # Champs Optionnels (avec valeurs par défaut)
    # Si la variable n'est pas dans le .env, la valeur par défaut est utilisée.
    # Pydantic convertit automatiquement le texte du .env en entier (int) pour MONGO_PORT et BATCH_SIZE.
    MONGO_PORT: int = 27017
    MONGO_DB: str = "healthcare_db"
    MONGO_COLLECTION: str = "patients"
    BATCH_SIZE: int = 1000

    @property
    def MONGO_URI(self) -> str:
        """Reconstruction dynamique de l'URI de connexion MongoDB."""
        return f"mongodb://{self.APP_USER}:{self.APP_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/{self.MONGO_DB}"
    
    # Configuration du modèle (model_config) via SettingsConfigDict :
    # C'est cet objet qui donne les instructions techniques à la classe BaseSettings.
    # - env_file=".env" : L'instruction de charger les variables depuis ce fichier.
    # - extra="ignore" : L'instruction d'ignorer les variables du .env qui ne sont pas définies dans la classe.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()