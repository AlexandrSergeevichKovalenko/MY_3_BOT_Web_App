"""
Curated genuinely two-gender German nouns for Artikel Sprint.

These are the words whose article depends on the MEANING (not region): der See
(озеро) vs die See (море). The generator deliberately rejects them (the article
isn't decidable from the word alone), so they are seeded here instead, each sense
as its own row with a Russian meaning. The game shows that meaning during play so
the asked article is fair.

Only well-known, clearly meaning-distinct pairs a B1–B2 learner benefits from.
Both senses live in ONE theme so they can surface together in a daily set and
teach the contrast directly.
"""
from __future__ import annotations

TWO_GENDER_NOUNS: list[dict] = [
    {"word": "See", "theme_key": "natur_landschaft", "senses": [
        {"article": "der", "meaning_ru": "озеро"},
        {"article": "die", "meaning_ru": "море"}]},
    {"word": "Band", "theme_key": "kunst_kultur", "senses": [
        {"article": "das", "meaning_ru": "лента; тесьма"},
        {"article": "der", "meaning_ru": "том (книги)"},
        {"article": "die", "meaning_ru": "музыкальная группа"}]},
    {"word": "Leiter", "theme_key": "beruf_arbeit", "senses": [
        {"article": "der", "meaning_ru": "руководитель"},
        {"article": "die", "meaning_ru": "лестница-стремянка"}]},
    {"word": "Kiefer", "theme_key": "koerper_gesundheit", "senses": [
        {"article": "der", "meaning_ru": "челюсть"},
        {"article": "die", "meaning_ru": "сосна"}]},
    {"word": "Steuer", "theme_key": "verkehr_reisen", "senses": [
        {"article": "das", "meaning_ru": "руль; штурвал"},
        {"article": "die", "meaning_ru": "налог"}]},
    {"word": "Tor", "theme_key": "sport_freizeit", "senses": [
        {"article": "das", "meaning_ru": "ворота; гол"},
        {"article": "der", "meaning_ru": "глупец (устар.)"}]},
    {"word": "Erbe", "theme_key": "familie_menschen", "senses": [
        {"article": "der", "meaning_ru": "наследник"},
        {"article": "das", "meaning_ru": "наследство"}]},
    {"word": "Gehalt", "theme_key": "beruf_arbeit", "senses": [
        {"article": "das", "meaning_ru": "зарплата, оклад"},
        {"article": "der", "meaning_ru": "содержание, доля (напр. Alkoholgehalt)"}]},
    {"word": "Schild", "theme_key": "stadt_gebaeude", "senses": [
        {"article": "das", "meaning_ru": "вывеска; табличка"},
        {"article": "der", "meaning_ru": "щит"}]},
    {"word": "Otter", "theme_key": "tiere", "senses": [
        {"article": "der", "meaning_ru": "выдра"},
        {"article": "die", "meaning_ru": "гадюка"}]},
    {"word": "Verdienst", "theme_key": "beruf_arbeit", "senses": [
        {"article": "der", "meaning_ru": "заработок"},
        {"article": "das", "meaning_ru": "заслуга"}]},
    {"word": "Hut", "theme_key": "kleidung_mode", "senses": [
        {"article": "der", "meaning_ru": "шляпа"},
        {"article": "die", "meaning_ru": "осторожность (auf der Hut sein)"}]},
]


def seed() -> dict:
    """Insert/refresh the curated two-gender nouns. Returns the seeder stats."""
    from backend.database import seed_two_gender_senses
    return seed_two_gender_senses(TWO_GENDER_NOUNS)
