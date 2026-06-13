"""
Artikel Sprint — theme registry (the "code bank" of themes).

Each theme will be filled with 250-300 verified German nouns (der/die/das) for
the article-guessing speed game. `subtopics` drive EXHAUSTIVE generation in the
fill step (Step 2): the generator walks every subtopic so a theme covers its
whole vocabulary (e.g. Körper → external + internal organs, every detail), not a
random sample. subtopics here are seeded empty/minimal and authored in Step 2.

Synced into bt_3_article_sprint_themes by sync_article_sprint_themes_from_code().
"""
from __future__ import annotations

# target_count = how many verified nouns we aim to have per theme.
_DEFAULT_TARGET = 280

# (key, label_de, label_ru)
ARTICLE_SPRINT_THEMES: list[dict] = [
    {"key": "haus_wohnen",          "label_de": "Haus & Wohnen",            "label_ru": "Дом и жильё"},
    {"key": "kueche_geschirr",      "label_de": "Küche & Geschirr",         "label_ru": "Кухня и посуда"},
    {"key": "essen_trinken",        "label_de": "Essen & Trinken",          "label_ru": "Еда и напитки"},
    {"key": "koerper_gesundheit",   "label_de": "Körper & Gesundheit",      "label_ru": "Тело и здоровье"},
    {"key": "kleidung_mode",        "label_de": "Kleidung & Mode",          "label_ru": "Одежда и мода"},
    {"key": "familie_menschen",     "label_de": "Familie & Menschen",       "label_ru": "Семья и люди"},
    {"key": "beruf_arbeit",         "label_de": "Beruf & Arbeit",           "label_ru": "Работа и профессии"},
    {"key": "schule_bildung",       "label_de": "Schule & Bildung",         "label_ru": "Школа и образование"},
    {"key": "stadt_gebaeude",       "label_de": "Stadt & Gebäude",          "label_ru": "Город и здания"},
    {"key": "verkehr_reisen",       "label_de": "Verkehr & Reisen",         "label_ru": "Транспорт и путешествия"},
    {"key": "natur_landschaft",     "label_de": "Natur & Landschaft",       "label_ru": "Природа и ландшафт"},
    {"key": "wetter_jahreszeiten",  "label_de": "Wetter & Jahreszeiten",    "label_ru": "Погода и сезоны"},
    {"key": "tiere",                "label_de": "Tiere",                    "label_ru": "Животные"},
    {"key": "pflanzen_garten",      "label_de": "Pflanzen & Garten",        "label_ru": "Растения и сад"},
    {"key": "technik_computer",     "label_de": "Technik & Computer",       "label_ru": "Техника и компьютеры"},
    {"key": "medien_kommunikation", "label_de": "Medien & Kommunikation",   "label_ru": "Медиа и связь"},
    {"key": "sport_freizeit",       "label_de": "Sport & Freizeit",         "label_ru": "Спорт и досуг"},
    {"key": "kunst_kultur",         "label_de": "Kunst & Kultur",           "label_ru": "Искусство и культура"},
    {"key": "wirtschaft_geld",      "label_de": "Wirtschaft & Geld",        "label_ru": "Экономика и деньги"},
    {"key": "gefuehle_charakter",   "label_de": "Gefühle & Charakter",      "label_ru": "Чувства и характер"},
    {"key": "medizin",              "label_de": "Medizin",                  "label_ru": "Медицина"},
]


def article_sprint_themes() -> list[dict]:
    """Normalized theme rows for syncing (key, label_de, label_ru, target_count)."""
    out: list[dict] = []
    for t in ARTICLE_SPRINT_THEMES:
        key = str(t.get("key") or "").strip()
        if not key:
            continue
        out.append({
            "key": key,
            "label_de": str(t.get("label_de") or "").strip(),
            "label_ru": str(t.get("label_ru") or "").strip(),
            "target_count": int(t.get("target_count") or _DEFAULT_TARGET),
        })
    return out
