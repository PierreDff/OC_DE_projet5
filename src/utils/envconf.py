from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://admin:password@localhost:27017"
    MONGO_DB: str = "healthcare_db"
    MONGO_COLLECTION: str = "patients"
    BATCH_SIZE: int = 1000
    
    # Pour charger depuis le .env si présent
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()