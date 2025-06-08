import logging

from src.config.settings import settings

log_level = logging.DEBUG if settings.DEBUG else logging.INFO

logging.basicConfig(
    level=log_level,
    format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
)
