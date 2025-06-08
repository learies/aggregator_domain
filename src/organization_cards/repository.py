from datetime import datetime
from typing import Generator, NamedTuple

from psycopg.rows import scalar_row

import src.organization_cards.sql as sql
from src.config.logging import logging
from src.config.repository import RepositoryBase

from .models import OrganizationCard

logger = logging.getLogger(__name__)


class OrganizationCardRepository(RepositoryBase):
    def stream_organization_cards(
        self, fetch_size: int = 1000000
    ) -> Generator[OrganizationCard, None, None]:
        logger.info("Streaming organization cards")
        start_time = datetime.now()

        with self.db.connect() as conn:
            with conn.cursor(
                name="org_stream", row_factory=self.row_factory(OrganizationCard)
            ) as cursor:
                cursor.execute(sql.GET_ORGANIZATION_CARDS_STREAM)
                while True:
                    stream_organization_cards = cursor.fetchmany(fetch_size)
                    if not stream_organization_cards:
                        break
                    for organization_cards in stream_organization_cards:
                        yield organization_cards

        logger.info(f"Streamed organization cards in {datetime.now() - start_time}")

    def get_top_source_categories(self) -> list[str]:
        logger.info("Getting top source categories")
        start_time = datetime.now()

        with self.db.connect() as conn:
            with conn.cursor(row_factory=scalar_row) as cursor:
                cursor.execute(sql.GET_TOP_SOURCE_CATEGORIES)
                top_source_categories = cursor.fetchall()

        logger.info(
            f"Got {len(top_source_categories)} top source categories in {datetime.now() - start_time}"
        )
        return top_source_categories

    def create_table_aggregate_domains(self) -> None:
        with self.db.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.CREATE_TABLE_AGGREGATE_DOMAINS)

    def insert_aggregate_domains(self, aggregate_domains: list[NamedTuple]) -> None:
        self.create_table_aggregate_domains()
        logger.info(f"Inserting {len(aggregate_domains)} aggregate domains")
        start_time = datetime.now()

        with self.db.connect_with_transaction() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql.INSERT_AGGREGATE_DOMAINS, aggregate_domains)

        logger.info(
            f"Inserted {len(aggregate_domains)} aggregate domains in {datetime.now() - start_time}"
        )
