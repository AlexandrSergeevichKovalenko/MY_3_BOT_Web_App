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
    # High-frequency additions (all confirmed two-gender on de.wiktionary).
    {"word": "Junge", "theme_key": "familie_menschen", "senses": [
        {"article": "der", "meaning_ru": "мальчик"},
        {"article": "das", "meaning_ru": "детёныш животного"}]},
    {"word": "Teil", "theme_key": "technik_computer", "senses": [
        {"article": "der", "meaning_ru": "часть (целого)"},
        {"article": "das", "meaning_ru": "деталь; штуковина"}]},
    {"word": "Kunde", "theme_key": "wirtschaft_geld", "senses": [
        {"article": "der", "meaning_ru": "клиент, покупатель"},
        {"article": "die", "meaning_ru": "весть; учение (напр. Erdkunde)"}]},
    {"word": "Laster", "theme_key": "verkehr_reisen", "senses": [
        {"article": "der", "meaning_ru": "грузовик"},
        {"article": "das", "meaning_ru": "порок"}]},
    {"word": "Heide", "theme_key": "natur_landschaft", "senses": [
        {"article": "die", "meaning_ru": "пустошь; вереск"},
        {"article": "der", "meaning_ru": "язычник"}]},
    {"word": "Flur", "theme_key": "haus_wohnen", "senses": [
        {"article": "der", "meaning_ru": "коридор; прихожая"},
        {"article": "die", "meaning_ru": "поле; нива"}]},
    {"word": "Pony", "theme_key": "tiere", "senses": [
        {"article": "das", "meaning_ru": "пони"},
        {"article": "der", "meaning_ru": "чёлка"}]},
    {"word": "Bund", "theme_key": "essen_trinken", "senses": [
        {"article": "der", "meaning_ru": "союз; федерация"},
        {"article": "das", "meaning_ru": "связка, пучок (напр. Bund Radieschen)"}]},
    {"word": "Moment", "theme_key": "gefuehle_charakter", "senses": [
        {"article": "der", "meaning_ru": "миг, мгновение"},
        {"article": "das", "meaning_ru": "фактор; момент (силы)"}]},
    {"word": "Mangel", "theme_key": "haus_wohnen", "senses": [
        {"article": "der", "meaning_ru": "недостаток, нехватка"},
        {"article": "die", "meaning_ru": "каток для белья"}]},
    # Найдены сверкой банка с Wiktionary 26.07.2026: лежали в банке одним артиклем,
    # хотя род зависит от значения. Не убираем из игры — наоборот, это ценная
    # тренировка: показываем русское значение, и человек отвечает осознанно.
    {"word": "Korn", "theme_key": "essen_trinken", "senses": [
        {"article": "das", "meaning_ru": "зерно; зёрнышко"},
        {"article": "der", "meaning_ru": "хлебная водка (шнапс)"}]},
    {"word": "Service", "theme_key": "essen_trinken", "senses": [
        {"article": "der", "meaning_ru": "обслуживание, сервис"},
        {"article": "das", "meaning_ru": "сервиз (посуда)"}]},
    {"word": "Hasel", "theme_key": "natur_landschaft", "senses": [
        {"article": "die", "meaning_ru": "лещина, орешник"},
        {"article": "der", "meaning_ru": "елец (рыба)"}]},
    {"word": "Pik", "theme_key": "kunst_kultur", "senses": [
        {"article": "das", "meaning_ru": "пики (карточная масть)"},
        {"article": "der", "meaning_ru": "зуб на кого-то, затаённая обида"}]},
]


def seed() -> dict:
    """Insert/refresh the curated two-gender nouns. Returns the seeder stats."""
    from backend.database import seed_two_gender_senses
    return seed_two_gender_senses(TWO_GENDER_NOUNS)
