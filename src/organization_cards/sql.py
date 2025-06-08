GET_ORGANIZATION_CARDS_STREAM = """
SELECT
    oc.id,
    oc.organization_name,
    ou.link
FROM
    cards.organization_cards oc
JOIN
    cards.organization_cards_original_urls ocou
    ON ocou.organization_card_id = oc.id
JOIN
    cards.organization_cards_country_code occc
    ON occc.organization_card_id = oc.id
JOIN
    meta.original_urls ou
    ON ou.id = ocou.original_url_id
WHERE 
    oc.source_id = 20
    -- Если нужно разметить только по странам
    -- AND
    -- occc.country_code_id = 140
ORDER BY
    oc.id
"""

GET_TOP_SOURCE_CATEGORIES = """
WITH CategoryFrequency AS (
    SELECT
        source_category_id AS category,
        COUNT(*) OVER () AS total_count
    FROM
        cards.organization_cards_source_categories TABLESAMPLE SYSTEM(10)
),
CategoryStats AS (
    SELECT
        category,
        COUNT(*) AS count,
        COUNT(*)::float / MAX(total_count) OVER () AS freq
    FROM CategoryFrequency
    GROUP BY category, total_count
),
CumulativeStats AS (
    SELECT
        category,
        freq,
        SUM(freq) OVER (ORDER BY freq DESC) AS cumulative_freq
    FROM CategoryStats
),
FilteredCategories AS (
    SELECT DISTINCT category
    FROM CumulativeStats
    WHERE cumulative_freq < 0.95
)
SELECT name
FROM FilteredCategories
JOIN meta.source_categories ON category = meta.source_categories.id
ORDER BY category;
"""

TABLE_NAME = "aggregate_domains_by_source_20"

CREATE_TABLE_AGGREGATE_DOMAINS = f"""
CREATE TABLE IF NOT EXISTS brandmatch.{TABLE_NAME} (
    domain VARCHAR(255) PRIMARY KEY,
    organization_names TEXT[],
    organization_names_count INTEGER,
    organization_names_unique TEXT[],
    organization_names_unique_count INTEGER,
    first_word_of_names TEXT[],
    first_word_of_names_count INTEGER,
    first_word_of_names_unique TEXT[],
    first_word_of_names_unique_count INTEGER,
    unique_names_ratio FLOAT,  -- Отношение количества уникальных названий к общему количеству названий (x2/x1). 
                               -- Близко к 1 → много дубликатов (агрегатор), близко к 0 → много уникальных (бренд).
    unique_first_words_ratio FLOAT,  -- Отношение уникальных первых слов названий к общему количеству первых слов (y2/y1).
                                     -- Близко к 1 → первые слова часто повторяются (агрегатор), близко к 0 → разнообразие (бренд).
    keywords TEXT[],
    keyword_score FLOAT,
    is_aggregator BOOLEAN DEFAULT FALSE  -- True, если домен является агрегатором (близость к 1 в unique_names_ratio и unique_first_words_ratio).
);
"""

INSERT_AGGREGATE_DOMAINS = f"""
INSERT INTO brandmatch.{TABLE_NAME} (
    domain,
    organization_names,
    organization_names_count,
    organization_names_unique,
    organization_names_unique_count,
    first_word_of_names,
    first_word_of_names_count,
    first_word_of_names_unique,
    first_word_of_names_unique_count,
    unique_names_ratio,
    unique_first_words_ratio,
    keywords,
    keyword_score,
    is_aggregator
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
