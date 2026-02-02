from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_USER: str
    APP_PASSWORD: str
    MONGO_HOST: str
    MONGO_PORT: int = 27017
    MONGO_DB: str = "healthcare_db"
    MONGO_COLLECTION: str = "patients"
    BATCH_SIZE: int = 1000

    # Reconstruction de l'URI
    @property
    def MONGO_URI(self) -> str:
        return f"mongodb://{self.APP_USER}:{self.APP_PASSWORD}@{self.MONGO_HOST}:{self.MONGO_PORT}/{self.MONGO_DB}"
    
    # Pour charger depuis le .env si présent
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()