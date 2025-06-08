from datetime import datetime

from psycopg.rows import scalar_row

import src.meta.sql as sql
from src.config.logging import logging
from src.config.repository import RepositoryBase

from .models import SocialMedia

logger = logging.getLogger(__name__)


class MetaRepository(RepositoryBase):
    def get_social_medias(self) -> list[SocialMedia]:
        logger.info("Getting social medias")
        start_time = datetime.now()

        with self.db.connect() as conn:
            with conn.cursor(row_factory=self.row_factory(SocialMedia)) as cursor:
                cursor.execute(sql.GET_SOCIAL_MEDIAS)
                social_medias = cursor.fetchall()

        logger.info(
            f"Got {len(social_medias)} social medias in {datetime.now() - start_time}"
        )
        return social_medias

    def get_stop_words(self) -> list[str]:
        logger.info("Getting stop words")
        start_time = datetime.now()

        with self.db.connect() as conn:
            with conn.cursor(row_factory=scalar_row) as cursor:
                cursor.execute(sql.GET_STOP_WORDS)
                stop_words = cursor.fetchall()

        logger.info(
            f"Got {len(stop_words)} stop words in {datetime.now() - start_time}"
        )
        return stop_words
