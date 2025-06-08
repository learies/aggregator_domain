GET_SOCIAL_MEDIAS = """
SELECT
    sm.id,
    sm."name",
    sm.primary_domain
FROM
    meta.social_medias sm
WHERE
    sm.primary_domain IS NOT NULL
"""

GET_STOP_WORDS = """
SELECT
    sw.word
FROM
    meta.stop_words sw
"""
