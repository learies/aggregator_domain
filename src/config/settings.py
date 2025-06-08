import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    DEBUG: bool = os.getenv("DEBUG", "False") == "True"
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")


settings = Settings()
