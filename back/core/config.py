import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ML_FLOW_URL: str = os.getenv("ML_FLOW_URL", "")
    MODEL_NAME: str = os.getenv("MODEL_NAME", "")
    MODEL_VERSION: str = os.getenv("MODEL_VERSION", "")
    MODEL_URL: str = f"models:/{MODEL_NAME}/{MODEL_VERSION}"

    class Config:
        env_file = ".env" 

settings = Settings()