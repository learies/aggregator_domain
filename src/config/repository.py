from typing import Any, Generator, NamedTuple, Protocol, Type, TypeVar

from src.meta.models import SocialMedia
from src.organization_cards.models import OrganizationCard

from .database import DatabaseConnection

T = TypeVar("T")


class RepositoryBase:
    """Base Repository."""

    def __init__(self, db: DatabaseConnection) -> None:
        self.db = db

    @classmethod
    def row_factory(cls, model_class: Type[T]) -> Any:
        def make_row(cursor):
            def make_model(values: Any) -> T:
                return model_class(*values)

            return make_model

        return make_row


class RepositoryProtocol(Protocol):
    """Base class for all repositories."""

    def stream_organization_cards(self) -> Generator[OrganizationCard, None, None]:
        raise NotImplementedError

    def get_social_medias(self) -> list[SocialMedia]:
        raise NotImplementedError

    def get_top_source_categories(self) -> list[str]:
        raise NotImplementedError

    def get_stop_words(self) -> list[str]:
        raise NotImplementedError

    def insert_aggregate_domains(self, aggregate_domains: list[NamedTuple]) -> None:
        raise NotImplementedError
