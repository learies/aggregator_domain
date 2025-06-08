import re
from collections import Counter, defaultdict
from typing import DefaultDict

from tldextract import extract

from src.aggregators.models import OrganizationNamesByDomain
from src.aggregators.repository import aggregator_repository
from src.config.logging import logging
from src.config.repository import RepositoryProtocol
from src.meta.models import SocialMedia

logger = logging.getLogger(__name__)

VariantNamesByDomain = DefaultDict[str, list[str]]
OrganizationIdsByDomains = DefaultDict[str, set[int]]


class CalculateRatio:
    def get_keywords(self, word_counts: Counter, names_count: int) -> list[str]:
        keywords: list[str] = [
            word for word, count in word_counts.items() if count / names_count >= 0.3
        ]
        return keywords

    def get_keyword_score(self, word_counts: Counter, names_count: int) -> float:
        keyword_score: float = (
            max(word_counts.values()) / names_count if word_counts else 0.0
        )
        return keyword_score

    def calculate_unique_ratio(
        self, unique_names_count: int, names_count: int
    ) -> float:
        unique_names_ratio: float = (
            unique_names_count / names_count if names_count > 0 else 0.0
        )
        return unique_names_ratio


class AggregatorService(CalculateRatio):
    def __init__(self, repository: RepositoryProtocol):
        self.repository = repository

    def clear_variant_names(
        self, variant_names: list[str], top_source_categories: set[str]
    ) -> list[str]:
        cleaned_variant_names: list[str] = []
        for variant_name in variant_names:
            if not variant_name:
                continue
            if variant_name in top_source_categories:
                continue
            cleaned_variant_names.append(variant_name)
        return cleaned_variant_names

    def get_words_out_names(
        self,
        cleaned_variant_names: list[str],
        top_source_categories: set[str],
        stop_words: set[str],
    ):
        pattern = re.compile(r"[^\w']+")
        first_word_of_names: list[str] = []
        words: list[str] = []

        for variant_name in cleaned_variant_names:
            if not variant_name:
                continue

            if variant_name in top_source_categories:
                continue

            variant_words: list[str] = variant_name.split()
            for index, variant_word in enumerate(variant_words):
                if not variant_word:
                    continue

                cleaned_word = pattern.sub("", variant_word)
                if not cleaned_word:
                    continue

                if cleaned_word in stop_words:
                    continue

                words.append(cleaned_word)

                if index == 0:
                    first_word_of_names.append(cleaned_word)

        return first_word_of_names, words

    def is_aggregator(
        self,
        cleaned_variant_names_count: int,
        unique_names_ratio: float,
        unique_first_words_ratio: float,
        keyword_score: float,
    ) -> bool:
        return (
            (cleaned_variant_names_count > 10)
            and (unique_names_ratio > 0.7)
            and ((unique_first_words_ratio > 0.4) or (keyword_score < 0.3))
        )

    def adding_in_models(
        self,
        organization_names_by_domains: VariantNamesByDomain,
        top_source_categories: set[str],
        stop_words: set[str],
    ) -> list[OrganizationNamesByDomain]:
        models: list[OrganizationNamesByDomain] = []

        for domain, variant_names in organization_names_by_domains.items():
            cleaned_variant_names: list[str] = self.clear_variant_names(
                variant_names, top_source_categories
            )
            if not cleaned_variant_names:
                continue

            first_word_of_names, words = self.get_words_out_names(
                cleaned_variant_names, top_source_categories, stop_words
            )

            # 1. Количество названий организаций после фильтрации по топ-категориям
            cleaned_variant_names_count: int = len(cleaned_variant_names)
            # 2. Количество уникальных названий организаций после фильтрации по топ-категориям
            cleaned_variant_names_unique_count: int = len(
                list(set(cleaned_variant_names))
            )

            # 3. Количество первых слов в названиях организаций
            first_word_of_names_count: int = len(first_word_of_names)
            # 4. Количество уникальных первых слов в названиях организаций
            first_word_of_names_unique_count: int = len(list(set(first_word_of_names)))

            # 5. Подсчет слов, которые встречаются в ≥30% названий
            word_counts = Counter(words)
            keywords: list[str] = self.get_keywords(
                word_counts, cleaned_variant_names_count
            )
            keyword_score: float = self.get_keyword_score(
                word_counts, cleaned_variant_names_count
            )

            # 6. Подсчет уникальных названий организаций
            unique_names_ratio: float = self.calculate_unique_ratio(
                cleaned_variant_names_unique_count, cleaned_variant_names_count
            )
            # 7. Подсчет уникальных первых слов в названиях организаций
            unique_first_words_ratio: float = self.calculate_unique_ratio(
                first_word_of_names_unique_count, first_word_of_names_count
            )

            is_aggregator = self.is_aggregator(
                cleaned_variant_names_count,
                unique_names_ratio,
                unique_first_words_ratio,
                keyword_score,
            )

            model = OrganizationNamesByDomain(
                domain=domain,
                organization_names=cleaned_variant_names,
                organization_names_count=len(cleaned_variant_names),
                organization_names_unique=list(set(cleaned_variant_names)),
                organization_names_unique_count=len(list(set(cleaned_variant_names))),
                first_word_of_names=first_word_of_names,
                first_word_of_names_count=len(first_word_of_names),
                first_word_of_names_unique=list(set(first_word_of_names)),
                first_word_of_names_unique_count=len(list(set(first_word_of_names))),
                unique_names_ratio=unique_names_ratio,
                unique_first_words_ratio=unique_first_words_ratio,
                keywords=keywords,
                keyword_score=keyword_score,
                is_aggregator=is_aggregator,
            )
            models.append(model)

        return models

    def get_domain(self, link: str, social_media_domains: set[str]) -> str | None:
        extract_link = extract(link)
        domain = extract_link.registered_domain.lower()
        subdomain = extract_link.subdomain.lower()
        if not domain:
            return
        if domain in social_media_domains:
            return
        if subdomain:
            domain = f"{subdomain}.{domain}"
        return domain

    def clear_domain(self, domain: str) -> str | None:
        domain_cleaned = re.sub(r"^www[1-9]?\.?", "", domain, flags=re.IGNORECASE)
        if not domain_cleaned:
            return
        return domain_cleaned

    def grouping_organization_names_by_domain(
        self, social_media_domains: set[str]
    ) -> VariantNamesByDomain:
        organization_names_by_domains: VariantNamesByDomain = defaultdict(list)
        organization_ids_by_domains: OrganizationIdsByDomains = defaultdict(set)

        for organization_card in self.repository.stream_organization_cards():
            domain = self.get_domain(organization_card.link, social_media_domains)
            if not domain:
                continue

            domain_cleaned = self.clear_domain(domain)
            if not domain_cleaned:
                continue

            if organization_card.id in organization_ids_by_domains[domain]:
                continue

            organization_names_by_domains[domain].append(
                organization_card.organization_name.lower()
            )
            organization_ids_by_domains[domain].add(organization_card.id)

        return organization_names_by_domains

    def get_aggregate_domains(self):
        social_medias: list[SocialMedia] = self.repository.get_social_medias()
        social_media_domains: set[str] = set(
            social_media.primary_domain
            for social_media in social_medias
            if social_media.primary_domain
        )
        top_source_categories: list[str] = self.repository.get_top_source_categories()
        stop_words: list[str] = self.repository.get_stop_words()
        organization_names_by_domains: VariantNamesByDomain = (
            self.grouping_organization_names_by_domain(social_media_domains)
        )
        models: list[OrganizationNamesByDomain] = self.adding_in_models(
            organization_names_by_domains, set(top_source_categories), set(stop_words)
        )
        self.repository.insert_aggregate_domains(models)


aggregator_service = AggregatorService(repository=aggregator_repository)
