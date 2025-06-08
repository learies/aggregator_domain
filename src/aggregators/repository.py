from typing import Generator, NamedTuple

from src.config.database import DatabaseConnection, db
from src.config.repository import RepositoryProtocol
from src.meta.models import SocialMedia
from src.meta.repository import MetaRepository
from src.organization_cards.models import OrganizationCard
from src.organization_cards.repository import OrganizationCardRepository


class AggregatorRepository(RepositoryProtocol):
    """
    Repository for the aggregator service.
    """

    def __init__(self, db: DatabaseConnection) -> None:
        self.db = db
        self.organization_card_repository = OrganizationCardRepository(db=self.db)
        self.meta_repository = MetaRepository(db=self.db)

    def stream_organization_cards(self) -> Generator[OrganizationCard, None, None]:
        return self.organization_card_repository.stream_organization_cards()

    def get_social_medias(self) -> list[SocialMedia]:
        return self.meta_repository.get_social_medias()

    def get_top_source_categories(self) -> list[str]:
        return self.organization_card_repository.get_top_source_categories()

    def get_stop_words(self) -> list[str]:
        return self.meta_repository.get_stop_words()

    def insert_aggregate_domains(self, aggregate_domains: list[NamedTuple]):
        return self.organization_card_repository.insert_aggregate_domains(
            aggregate_domains
        )


aggregator_repository = AggregatorRepository(db=db)
