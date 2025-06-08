from typing import NamedTuple


class OrganizationNamesByDomain(NamedTuple):
    domain: str
    organization_names: list[str]
    organization_names_count: int
    organization_names_unique: list[str]
    organization_names_unique_count: int
    first_word_of_names: list[str]
    first_word_of_names_count: int
    first_word_of_names_unique: list[str]
    first_word_of_names_unique_count: int
    unique_names_ratio: float
    unique_first_words_ratio: float
    keywords: list[str]
    keyword_score: float
    is_aggregator: bool
