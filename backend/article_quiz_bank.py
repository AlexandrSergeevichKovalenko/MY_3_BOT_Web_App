"""
Static bank of ~150 German nouns for the article quiz (der/die/das).

Each entry has a clear, concrete, visually unambiguous meaning so DALL-E
can generate an unambiguous image. Abstract nouns are excluded.

Entry fields:
  id          – unique snake_case identifier
  word        – German noun (capitalised)
  article     – "der" | "die" | "das"
  meaning_ru  – Russian translation
  difficulty  – "A2" | "B1"
  category    – thematic group
  dalle_prompt – English DALL-E image prompt (single object, white bg)
"""
from __future__ import annotations

_S = (
    "children's book illustration style, soft watercolor, vibrant colors, "
    "clean white background, single object centered, no text, no labels, "
    "no other objects, high clarity, detailed"
)

ARTICLE_QUIZ_BANK: list[dict] = [
    # ── Tiere ─────────────────────────────────────────────────────────────────
    {"id": "hund",          "word": "Hund",          "article": "der", "meaning_ru": "собака",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A friendly domestic dog sitting, front view, {_S}"},
    {"id": "katze",         "word": "Katze",         "article": "die", "meaning_ru": "кошка",        "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A cute domestic cat sitting, front view, {_S}"},
    {"id": "pferd",         "word": "Pferd",         "article": "das", "meaning_ru": "лошадь",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A horse standing, side view, {_S}"},
    {"id": "vogel",         "word": "Vogel",         "article": "der", "meaning_ru": "птица",        "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A small colorful bird perched, side view, {_S}"},
    {"id": "fisch",         "word": "Fisch",         "article": "der", "meaning_ru": "рыба",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A single tropical fish swimming, side view, {_S}"},
    {"id": "ente",          "word": "Ente",          "article": "die", "meaning_ru": "утка",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A duck standing, side view, {_S}"},
    {"id": "schaf",         "word": "Schaf",         "article": "das", "meaning_ru": "овца",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A fluffy sheep standing, side view, {_S}"},
    {"id": "kuh",           "word": "Kuh",           "article": "die", "meaning_ru": "корова",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A dairy cow standing, side view, {_S}"},
    {"id": "huhn",          "word": "Huhn",          "article": "das", "meaning_ru": "курица",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A chicken standing, side view, {_S}"},
    {"id": "schwein",       "word": "Schwein",       "article": "das", "meaning_ru": "свинья",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A pink pig standing, side view, {_S}"},
    {"id": "elefant",       "word": "Elefant",       "article": "der", "meaning_ru": "слон",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"An elephant standing, side view, {_S}"},
    {"id": "loewe",         "word": "Löwe",          "article": "der", "meaning_ru": "лев",          "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A lion standing, side view, {_S}"},
    {"id": "giraffe",       "word": "Giraffe",       "article": "die", "meaning_ru": "жираф",        "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A giraffe standing, side view, {_S}"},
    {"id": "maus",          "word": "Maus",          "article": "die", "meaning_ru": "мышь",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A small grey mouse, side view, {_S}"},
    {"id": "kaninchen",     "word": "Kaninchen",     "article": "das", "meaning_ru": "кролик",       "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A fluffy rabbit sitting, front view, {_S}"},
    {"id": "baer",          "word": "Bär",           "article": "der", "meaning_ru": "медведь",      "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A brown bear standing, front view, {_S}"},
    {"id": "affe",          "word": "Affe",          "article": "der", "meaning_ru": "обезьяна",     "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A monkey sitting, front view, {_S}"},
    {"id": "schlange",      "word": "Schlange",      "article": "die", "meaning_ru": "змея",         "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A coiled snake, top view, {_S}"},
    {"id": "schildkroete",  "word": "Schildkröte",   "article": "die", "meaning_ru": "черепаха",     "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A turtle walking, side view, {_S}"},
    {"id": "biene",         "word": "Biene",         "article": "die", "meaning_ru": "пчела",        "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A honey bee, side view, {_S}"},
    {"id": "schmetterling", "word": "Schmetterling", "article": "der", "meaning_ru": "бабочка",      "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A colorful butterfly with wings open, top view, {_S}"},
    {"id": "frosch",        "word": "Frosch",        "article": "der", "meaning_ru": "лягушка",      "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A green frog sitting, front view, {_S}"},
    {"id": "hai",           "word": "Hai",           "article": "der", "meaning_ru": "акула",        "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A shark swimming, side view, {_S}"},
    {"id": "tiger",         "word": "Tiger",         "article": "der", "meaning_ru": "тигр",         "difficulty": "A2", "category": "Tiere",   "dalle_prompt": f"A tiger walking, side view, {_S}"},
    {"id": "wolf",          "word": "Wolf",          "article": "der", "meaning_ru": "волк",         "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A wolf standing, side view, {_S}"},

    # ── Essen & Trinken ───────────────────────────────────────────────────────
    {"id": "apfel",         "word": "Apfel",         "article": "der", "meaning_ru": "яблоко",       "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single red apple, {_S}"},
    {"id": "banane",        "word": "Banane",        "article": "die", "meaning_ru": "банан",        "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single yellow banana, {_S}"},
    {"id": "erdbeere",      "word": "Erdbeere",      "article": "die", "meaning_ru": "клубника",     "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single ripe strawberry, {_S}"},
    {"id": "tomate",        "word": "Tomate",        "article": "die", "meaning_ru": "помидор",      "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single red tomato, {_S}"},
    {"id": "kartoffel",     "word": "Kartoffel",     "article": "die", "meaning_ru": "картофель",    "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single raw potato, {_S}"},
    {"id": "zwiebel",       "word": "Zwiebel",       "article": "die", "meaning_ru": "лук",          "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single whole onion, {_S}"},
    {"id": "brot",          "word": "Brot",          "article": "das", "meaning_ru": "хлеб",         "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A whole loaf of bread, {_S}"},
    {"id": "ei",            "word": "Ei",            "article": "das", "meaning_ru": "яйцо",         "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single white egg, {_S}"},
    {"id": "kaese",         "word": "Käse",          "article": "der", "meaning_ru": "сыр",          "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A wedge of yellow cheese, {_S}"},
    {"id": "kuchen",        "word": "Kuchen",        "article": "der", "meaning_ru": "торт/пирог",   "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A whole round cake, {_S}"},
    {"id": "pizza",         "word": "Pizza",         "article": "die", "meaning_ru": "пицца",        "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A whole round pizza, top view, {_S}"},
    {"id": "eis_speise",    "word": "Eis",           "article": "das", "meaning_ru": "мороженое",    "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single scoop ice cream cone, {_S}"},
    {"id": "kaffee",        "word": "Kaffee",        "article": "der", "meaning_ru": "кофе",         "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A cup of black coffee with steam, {_S}"},
    {"id": "milch",         "word": "Milch",         "article": "die", "meaning_ru": "молоко",       "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A glass of white milk, {_S}"},
    {"id": "saft",          "word": "Saft",          "article": "der", "meaning_ru": "сок",          "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A glass of orange juice, {_S}"},
    {"id": "suppe",         "word": "Suppe",         "article": "die", "meaning_ru": "суп",          "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A bowl of vegetable soup, {_S}"},
    {"id": "orange",        "word": "Orange",        "article": "die", "meaning_ru": "апельсин",     "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single whole orange, {_S}"},
    {"id": "zitrone",       "word": "Zitrone",       "article": "die", "meaning_ru": "лимон",        "difficulty": "A2", "category": "Essen",   "dalle_prompt": f"A single whole lemon, {_S}"},
    {"id": "trauben",       "word": "Trauben",       "article": "die", "meaning_ru": "виноград",     "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A bunch of purple grapes, {_S}"},
    {"id": "pilz",          "word": "Pilz",          "article": "der", "meaning_ru": "гриб",         "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A single mushroom, side view, {_S}"},
    {"id": "mais",          "word": "Mais",          "article": "der", "meaning_ru": "кукуруза",     "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A single ear of corn, {_S}"},
    {"id": "kuerubus",      "word": "Kürbis",        "article": "der", "meaning_ru": "тыква",        "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A single orange pumpkin, {_S}"},

    # ── Kleidung ──────────────────────────────────────────────────────────────
    {"id": "schuh",         "word": "Schuh",         "article": "der", "meaning_ru": "ботинок",      "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A single classic leather shoe, side view, {_S}"},
    {"id": "jacke",         "word": "Jacke",         "article": "die", "meaning_ru": "куртка",       "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A jacket laid flat, front view, {_S}"},
    {"id": "hose",          "word": "Hose",          "article": "die", "meaning_ru": "брюки",        "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A pair of jeans laid flat, front view, {_S}"},
    {"id": "hemd",          "word": "Hemd",          "article": "das", "meaning_ru": "рубашка",      "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A shirt laid flat, front view, {_S}"},
    {"id": "kleid",         "word": "Kleid",         "article": "das", "meaning_ru": "платье",       "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A dress laid flat, front view, {_S}"},
    {"id": "muetze",        "word": "Mütze",         "article": "die", "meaning_ru": "шапка",        "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A knitted winter hat, {_S}"},
    {"id": "schal",         "word": "Schal",         "article": "der", "meaning_ru": "шарф",         "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A woolen scarf, {_S}"},
    {"id": "brille",        "word": "Brille",        "article": "die", "meaning_ru": "очки",         "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A pair of glasses, front view, {_S}"},
    {"id": "handschuh_kl",  "word": "Handschuh",     "article": "der", "meaning_ru": "перчатка",     "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A single leather glove, {_S}"},
    {"id": "socke",         "word": "Socke",         "article": "die", "meaning_ru": "носок",        "difficulty": "A2", "category": "Kleidung","dalle_prompt": f"A single colorful sock, {_S}"},
    {"id": "mantel",        "word": "Mantel",        "article": "der", "meaning_ru": "пальто",       "difficulty": "B1", "category": "Kleidung","dalle_prompt": f"A long coat laid flat, front view, {_S}"},
    {"id": "krawatte",      "word": "Krawatte",      "article": "die", "meaning_ru": "галстук",      "difficulty": "B1", "category": "Kleidung","dalle_prompt": f"A necktie, {_S}"},
    {"id": "stiefel",       "word": "Stiefel",       "article": "der", "meaning_ru": "сапог",        "difficulty": "B1", "category": "Kleidung","dalle_prompt": f"A single knee-high boot, side view, {_S}"},

    # ── Möbel & Haus ──────────────────────────────────────────────────────────
    {"id": "tisch",         "word": "Tisch",         "article": "der", "meaning_ru": "стол",         "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A wooden dining table, side view, {_S}"},
    {"id": "stuhl",         "word": "Stuhl",         "article": "der", "meaning_ru": "стул",         "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A wooden chair, side view, {_S}"},
    {"id": "bett",          "word": "Bett",          "article": "das", "meaning_ru": "кровать",      "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A single bed with pillow and blanket, side view, {_S}"},
    {"id": "sofa",          "word": "Sofa",          "article": "das", "meaning_ru": "диван",        "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A comfortable sofa, side view, {_S}"},
    {"id": "lampe",         "word": "Lampe",         "article": "die", "meaning_ru": "лампа",        "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A floor lamp, {_S}"},
    {"id": "fenster",       "word": "Fenster",       "article": "das", "meaning_ru": "окно",         "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A single window frame, front view, {_S}"},
    {"id": "tuer",          "word": "Tür",           "article": "die", "meaning_ru": "дверь",        "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A wooden door, front view, {_S}"},
    {"id": "schrank",       "word": "Schrank",       "article": "der", "meaning_ru": "шкаф",         "difficulty": "A2", "category": "Möbel",   "dalle_prompt": f"A tall wooden wardrobe, front view, {_S}"},

    # ── Gegenstände ───────────────────────────────────────────────────────────
    {"id": "buch",          "word": "Buch",          "article": "das", "meaning_ru": "книга",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single closed book, {_S}"},
    {"id": "schluessel",    "word": "Schlüssel",     "article": "der", "meaning_ru": "ключ",         "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single door key, {_S}"},
    {"id": "ball",          "word": "Ball",          "article": "der", "meaning_ru": "мяч",          "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single soccer ball, {_S}"},
    {"id": "tasche",        "word": "Tasche",        "article": "die", "meaning_ru": "сумка",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A handbag, {_S}"},
    {"id": "uhr",           "word": "Uhr",           "article": "die", "meaning_ru": "часы",         "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A round analog wall clock, {_S}"},
    {"id": "telefon",       "word": "Telefon",       "article": "das", "meaning_ru": "телефон",      "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A smartphone, front view, {_S}"},
    {"id": "teller",        "word": "Teller",        "article": "der", "meaning_ru": "тарелка",      "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single round plate, top view, {_S}"},
    {"id": "gabel",         "word": "Gabel",         "article": "die", "meaning_ru": "вилка",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single metal fork, {_S}"},
    {"id": "loeffel",       "word": "Löffel",        "article": "der", "meaning_ru": "ложка",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single spoon, {_S}"},
    {"id": "messer",        "word": "Messer",        "article": "das", "meaning_ru": "нож",          "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single kitchen knife, {_S}"},
    {"id": "glas",          "word": "Glas",          "article": "das", "meaning_ru": "стакан",       "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single empty drinking glass, {_S}"},
    {"id": "tasse",         "word": "Tasse",         "article": "die", "meaning_ru": "чашка",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single coffee cup with saucer, {_S}"},
    {"id": "rucksack",      "word": "Rucksack",      "article": "der", "meaning_ru": "рюкзак",       "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A school backpack, {_S}"},
    {"id": "regenschirm",   "word": "Regenschirm",   "article": "der", "meaning_ru": "зонт",         "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"An open umbrella, top view, {_S}"},
    {"id": "kerze",         "word": "Kerze",         "article": "die", "meaning_ru": "свеча",        "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A lit candle with flame, {_S}"},
    {"id": "flasche",       "word": "Flasche",       "article": "die", "meaning_ru": "бутылка",      "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single glass bottle, {_S}"},
    {"id": "schere",        "word": "Schere",        "article": "die", "meaning_ru": "ножницы",      "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single pair of scissors, {_S}"},
    {"id": "hammer",        "word": "Hammer",        "article": "der", "meaning_ru": "молоток",      "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A wooden hammer, side view, {_S}"},
    {"id": "saege",         "word": "Säge",          "article": "die", "meaning_ru": "пила",         "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A hand saw, side view, {_S}"},
    {"id": "ring",          "word": "Ring",          "article": "der", "meaning_ru": "кольцо",       "difficulty": "A2", "category": "Objekte", "dalle_prompt": f"A single golden ring, {_S}"},
    {"id": "koffer",        "word": "Koffer",        "article": "der", "meaning_ru": "чемодан",      "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A single suitcase with handle, {_S}"},
    {"id": "gitarre",       "word": "Gitarre",       "article": "die", "meaning_ru": "гитара",       "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"An acoustic guitar, front view, {_S}"},
    {"id": "trompete",      "word": "Trompete",      "article": "die", "meaning_ru": "труба",        "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A brass trumpet, side view, {_S}"},
    {"id": "pfeife",        "word": "Pfeife",        "article": "die", "meaning_ru": "свисток",      "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A small metal whistle, {_S}"},

    # ── Natur ─────────────────────────────────────────────────────────────────
    {"id": "baum",          "word": "Baum",          "article": "der", "meaning_ru": "дерево",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single oak tree, {_S}"},
    {"id": "blume",         "word": "Blume",         "article": "die", "meaning_ru": "цветок",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single red rose, {_S}"},
    {"id": "stein",         "word": "Stein",         "article": "der", "meaning_ru": "камень",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single grey rock, {_S}"},
    {"id": "berg",          "word": "Berg",          "article": "der", "meaning_ru": "гора",         "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single mountain peak with snow, {_S}"},
    {"id": "wolke",         "word": "Wolke",         "article": "die", "meaning_ru": "облако",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single fluffy white cloud, {_S}"},
    {"id": "sonne",         "word": "Sonne",         "article": "die", "meaning_ru": "солнце",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A bright yellow sun with rays, {_S}"},
    {"id": "mond",          "word": "Mond",          "article": "der", "meaning_ru": "луна",         "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A full moon, {_S}"},
    {"id": "stern",         "word": "Stern",         "article": "der", "meaning_ru": "звезда",       "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single golden five-pointed star, {_S}"},
    {"id": "blatt",         "word": "Blatt",         "article": "das", "meaning_ru": "лист",         "difficulty": "A2", "category": "Natur",   "dalle_prompt": f"A single green maple leaf, {_S}"},
    {"id": "kaktus",        "word": "Kaktus",        "article": "der", "meaning_ru": "кактус",       "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A single potted cactus, {_S}"},
    {"id": "nest",          "word": "Nest",          "article": "das", "meaning_ru": "гнездо",       "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A bird's nest with eggs, top view, {_S}"},
    {"id": "see",           "word": "See",           "article": "der", "meaning_ru": "озеро",        "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A calm lake surrounded by trees, aerial view, {_S}"},

    # ── Körper ────────────────────────────────────────────────────────────────
    {"id": "hand",          "word": "Hand",          "article": "die", "meaning_ru": "рука (кисть)", "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"An open human hand, palm facing viewer, fingers spread, {_S}"},
    {"id": "fuss",          "word": "Fuß",           "article": "der", "meaning_ru": "нога (ступня)","difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single human foot, top view, {_S}"},
    {"id": "auge",          "word": "Auge",          "article": "das", "meaning_ru": "глаз",         "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single human eye, open, front view, {_S}"},
    {"id": "ohr",           "word": "Ohr",           "article": "das", "meaning_ru": "ухо",          "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single human ear, side view, {_S}"},
    {"id": "nase",          "word": "Nase",          "article": "die", "meaning_ru": "нос",          "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A human nose, front view, {_S}"},
    {"id": "mund",          "word": "Mund",          "article": "der", "meaning_ru": "рот",          "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A smiling human mouth with lips, front view, {_S}"},
    {"id": "zahn",          "word": "Zahn",          "article": "der", "meaning_ru": "зуб",          "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single white tooth, {_S}"},
    {"id": "finger",        "word": "Finger",        "article": "der", "meaning_ru": "палец",        "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single human index finger pointing up, {_S}"},
    {"id": "haar",          "word": "Haar",          "article": "das", "meaning_ru": "волос",        "difficulty": "A2", "category": "Körper",  "dalle_prompt": f"A single long golden hair strand, {_S}"},

    # ── Transport ─────────────────────────────────────────────────────────────
    {"id": "auto",          "word": "Auto",          "article": "das", "meaning_ru": "автомобиль",   "difficulty": "A2", "category": "Transport","dalle_prompt": f"A small red car, side view, {_S}"},
    {"id": "fahrrad",       "word": "Fahrrad",       "article": "das", "meaning_ru": "велосипед",    "difficulty": "A2", "category": "Transport","dalle_prompt": f"A bicycle, side view, {_S}"},
    {"id": "bus",           "word": "Bus",           "article": "der", "meaning_ru": "автобус",      "difficulty": "A2", "category": "Transport","dalle_prompt": f"A yellow school bus, side view, {_S}"},
    {"id": "zug",           "word": "Zug",           "article": "der", "meaning_ru": "поезд",        "difficulty": "A2", "category": "Transport","dalle_prompt": f"A passenger train, side view, {_S}"},
    {"id": "flugzeug",      "word": "Flugzeug",      "article": "das", "meaning_ru": "самолёт",      "difficulty": "A2", "category": "Transport","dalle_prompt": f"A passenger airplane, side view, {_S}"},
    {"id": "schiff",        "word": "Schiff",        "article": "das", "meaning_ru": "корабль",      "difficulty": "A2", "category": "Transport","dalle_prompt": f"A large cargo ship, side view, {_S}"},
    {"id": "boot",          "word": "Boot",          "article": "das", "meaning_ru": "лодка",        "difficulty": "A2", "category": "Transport","dalle_prompt": f"A small wooden rowing boat, {_S}"},
    {"id": "motorrad",      "word": "Motorrad",      "article": "das", "meaning_ru": "мотоцикл",     "difficulty": "B1", "category": "Transport","dalle_prompt": f"A motorcycle, side view, {_S}"},
    {"id": "hubschrauber",  "word": "Hubschrauber",  "article": "der", "meaning_ru": "вертолёт",     "difficulty": "B1", "category": "Transport","dalle_prompt": f"A helicopter, side view, {_S}"},
    {"id": "faehre",        "word": "Fähre",         "article": "die", "meaning_ru": "паром",        "difficulty": "B1", "category": "Transport","dalle_prompt": f"A ferry boat, side view, {_S}"},

    # ── Gebäude & Orte ────────────────────────────────────────────────────────
    {"id": "haus",          "word": "Haus",          "article": "das", "meaning_ru": "дом",          "difficulty": "A2", "category": "Gebäude", "dalle_prompt": f"A single family house, front view, {_S}"},
    {"id": "kirche",        "word": "Kirche",        "article": "die", "meaning_ru": "церковь",      "difficulty": "A2", "category": "Gebäude", "dalle_prompt": f"A small church with steeple, front view, {_S}"},
    {"id": "schule",        "word": "Schule",        "article": "die", "meaning_ru": "школа",        "difficulty": "A2", "category": "Gebäude", "dalle_prompt": f"A school building, front view, {_S}"},
    {"id": "schloss",       "word": "Schloss",       "article": "das", "meaning_ru": "замок",        "difficulty": "B1", "category": "Gebäude", "dalle_prompt": f"A medieval castle, front view, {_S}"},
    {"id": "turm",          "word": "Turm",          "article": "der", "meaning_ru": "башня",        "difficulty": "B1", "category": "Gebäude", "dalle_prompt": f"A tall stone tower, {_S}"},
    {"id": "bruecke",       "word": "Brücke",        "article": "die", "meaning_ru": "мост",         "difficulty": "B1", "category": "Gebäude", "dalle_prompt": f"A stone arch bridge over water, {_S}"},
    {"id": "leuchtturm",    "word": "Leuchtturm",    "article": "der", "meaning_ru": "маяк",         "difficulty": "B1", "category": "Gebäude", "dalle_prompt": f"A lighthouse by the sea, {_S}"},
    {"id": "zelt",          "word": "Zelt",          "article": "das", "meaning_ru": "палатка",      "difficulty": "B1", "category": "Gebäude", "dalle_prompt": f"A camping tent, {_S}"},

    # ── B1 Erweiterung ────────────────────────────────────────────────────────
    # Tiere B1
    {"id": "adler",        "word": "Adler",         "article": "der", "meaning_ru": "орёл",         "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A bald eagle with wings slightly spread, front view, {_S}"},
    {"id": "fuchs",        "word": "Fuchs",         "article": "der", "meaning_ru": "лиса",         "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A red fox sitting, side view, {_S}"},
    {"id": "moewe",        "word": "Möwe",          "article": "die", "meaning_ru": "чайка",        "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A seagull in flight, side view, {_S}"},
    {"id": "ameise",       "word": "Ameise",        "article": "die", "meaning_ru": "муравей",      "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A single ant, top view magnified, {_S}"},
    {"id": "libelle",      "word": "Libelle",       "article": "die", "meaning_ru": "стрекоза",     "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A dragonfly with wings spread, top view, {_S}"},
    {"id": "wal",          "word": "Wal",           "article": "der", "meaning_ru": "кит",          "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A blue whale, side view, {_S}"},
    {"id": "pinguin",      "word": "Pinguin",       "article": "der", "meaning_ru": "пингвин",      "difficulty": "B1", "category": "Tiere",   "dalle_prompt": f"A penguin standing, front view, {_S}"},
    # Essen B1
    {"id": "avocado",      "word": "Avocado",       "article": "die", "meaning_ru": "авокадо",      "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A whole avocado, {_S}"},
    {"id": "gurke",        "word": "Gurke",         "article": "die", "meaning_ru": "огурец",       "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A single whole cucumber, {_S}"},
    {"id": "knoblauch",    "word": "Knoblauch",     "article": "der", "meaning_ru": "чеснок",       "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A whole garlic bulb, {_S}"},
    {"id": "kirsche",      "word": "Kirsche",       "article": "die", "meaning_ru": "вишня",        "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A single ripe cherry with stem, {_S}"},
    {"id": "ananas",       "word": "Ananas",        "article": "die", "meaning_ru": "ананас",       "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A whole pineapple, {_S}"},
    {"id": "honig",        "word": "Honig",         "article": "der", "meaning_ru": "мёд",          "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A jar of golden honey with a honey dipper, {_S}"},
    {"id": "essig",        "word": "Essig",         "article": "der", "meaning_ru": "уксус",        "difficulty": "B1", "category": "Essen",   "dalle_prompt": f"A bottle of vinegar, {_S}"},
    # Natur B1
    {"id": "vulkan",       "word": "Vulkan",        "article": "der", "meaning_ru": "вулкан",       "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"An erupting volcano, side view, {_S}"},
    {"id": "gletscher",    "word": "Gletscher",     "article": "der", "meaning_ru": "ледник",       "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A glacier in a mountain valley, {_S}"},
    {"id": "welle",        "word": "Welle",         "article": "die", "meaning_ru": "волна",        "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A large ocean wave curling, side view, {_S}"},
    {"id": "klippe",       "word": "Klippe",        "article": "die", "meaning_ru": "скала/утёс",   "difficulty": "B1", "category": "Natur",   "dalle_prompt": f"A rocky cliff by the sea, {_S}"},
    # Objekte B1
    {"id": "kompass",      "word": "Kompass",       "article": "der", "meaning_ru": "компас",       "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A brass compass open, top view, {_S}"},
    {"id": "rucksack",     "word": "Rucksack",      "article": "der", "meaning_ru": "рюкзак",       "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A hiking backpack, front view, {_S}"},
    {"id": "pinsel",       "word": "Pinsel",        "article": "der", "meaning_ru": "кисть",        "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A single paintbrush, {_S}"},
    {"id": "kerze",        "word": "Kerze",         "article": "die", "meaning_ru": "свеча",        "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A single white candle with flame, {_S}"},
    {"id": "lupe",         "word": "Lupe",          "article": "die", "meaning_ru": "лупа",         "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A magnifying glass, {_S}"},
    {"id": "mikroskop",    "word": "Mikroskop",     "article": "das", "meaning_ru": "микроскоп",    "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A lab microscope, side view, {_S}"},
    {"id": "teleskop",     "word": "Teleskop",      "article": "das", "meaning_ru": "телескоп",     "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"An astronomical telescope on a tripod, {_S}"},
    {"id": "schirm",       "word": "Schirm",        "article": "der", "meaning_ru": "зонт",         "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"An open umbrella, top view, {_S}"},
    {"id": "kissen",       "word": "Kissen",        "article": "das", "meaning_ru": "подушка",      "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A single decorative pillow, {_S}"},
    {"id": "nadel",        "word": "Nadel",         "article": "die", "meaning_ru": "игла/иголка",  "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A sewing needle with thread, {_S}"},
    {"id": "nagel",        "word": "Nagel",         "article": "der", "meaning_ru": "гвоздь",       "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A single metal nail, side view, {_S}"},
    {"id": "kette",        "word": "Kette",         "article": "die", "meaning_ru": "цепочка/цепь", "difficulty": "B1", "category": "Objekte", "dalle_prompt": f"A gold necklace chain, {_S}"},

    # ── B2 Standard ───────────────────────────────────────────────────────────
    {"id": "rakete",       "word": "Rakete",        "article": "die", "meaning_ru": "ракета",       "difficulty": "B2", "category": "Technik", "dalle_prompt": f"A rocket launching, side view, {_S}"},
    {"id": "satellit",     "word": "Satellit",      "article": "der", "meaning_ru": "спутник",      "difficulty": "B2", "category": "Technik", "dalle_prompt": f"A satellite in space, {_S}"},
    {"id": "magnet",       "word": "Magnet",        "article": "der", "meaning_ru": "магнит",       "difficulty": "B2", "category": "Technik", "dalle_prompt": f"A horseshoe magnet, {_S}"},
    {"id": "prisma",       "word": "Prisma",        "article": "das", "meaning_ru": "призма",       "difficulty": "B2", "category": "Technik", "dalle_prompt": f"A glass prism splitting white light into rainbow, {_S}"},
    {"id": "fossil",       "word": "Fossil",        "article": "das", "meaning_ru": "окаменелость", "difficulty": "B2", "category": "Natur",   "dalle_prompt": f"A dinosaur fossil embedded in rock, {_S}"},
    {"id": "kristall",     "word": "Kristall",      "article": "der", "meaning_ru": "кристалл",     "difficulty": "B2", "category": "Natur",   "dalle_prompt": f"A clear quartz crystal cluster, {_S}"},
    {"id": "denkmal",      "word": "Denkmal",       "article": "das", "meaning_ru": "памятник",     "difficulty": "B2", "category": "Gebäude", "dalle_prompt": f"A stone monument statue on a pedestal, {_S}"},
    {"id": "archiv",       "word": "Archiv",        "article": "das", "meaning_ru": "архив",        "difficulty": "B2", "category": "Gebäude", "dalle_prompt": f"Old archive shelves filled with folders and documents, {_S}"},
    {"id": "tablett",      "word": "Tablett",       "article": "das", "meaning_ru": "поднос",       "difficulty": "B2", "category": "Objekte", "dalle_prompt": f"A wooden serving tray, top view, {_S}"},
    {"id": "stempel",      "word": "Stempel",       "article": "der", "meaning_ru": "штамп/печать", "difficulty": "B2", "category": "Objekte", "dalle_prompt": f"A rubber stamp, side view, {_S}"},

    # ── B2 Ausnahmen — слова-исключения, где артикль неочевиден ───────────────
    # das Sofa — многие ожидают der (мужское), но нейтральный
    {"id": "sofa",         "word": "Sofa",          "article": "das", "meaning_ru": "диван",        "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A modern three-seat sofa, front view, {_S}"},
    # das Café — многие говорят der, но нейтральный (французское)
    {"id": "cafe",         "word": "Café",          "article": "das", "meaning_ru": "кафе",         "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A cozy street café exterior with tables outside, {_S}"},
    # das Büro — нейтральный, окончание не подсказывает
    {"id": "buero",        "word": "Büro",          "article": "das", "meaning_ru": "офис",         "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A tidy office desk with computer and chair, {_S}"},
    # das Handy — немецкое слово для мобильника, нейтральный
    {"id": "handy",        "word": "Handy",         "article": "das", "meaning_ru": "мобильный телефон","difficulty": "B2","category": "Ausnahme","dalle_prompt": f"A modern smartphone, front view, {_S}"},
    # das Virus — латинское -us обычно der, но здесь das
    {"id": "virus",        "word": "Virus",         "article": "das", "meaning_ru": "вирус",        "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A colorful 3D illustration of a coronavirus particle, {_S}"},
    # das Ticket — английское заимствование, нейтральный
    {"id": "ticket",       "word": "Ticket",        "article": "das", "meaning_ru": "билет",        "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A single paper event ticket, {_S}"},
    # das Regal — нейтральный, окончание -al обычно das, но не очевидно
    {"id": "regal",        "word": "Regal",         "article": "das", "meaning_ru": "полка/стеллаж","difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A wooden bookshelf with books, side view, {_S}"},
    # das Kabel — нейтральный (-el чаще der: Schlüssel, Würfel)
    {"id": "kabel",        "word": "Kabel",         "article": "das", "meaning_ru": "кабель",       "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A coiled electrical cable, {_S}"},
    # das Team — английское, нейтральный (ожидают der)
    {"id": "team",         "word": "Team",          "article": "das", "meaning_ru": "команда",      "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A group of diverse people putting hands together in a circle, {_S}"},
    # das Hobby — нейтральный (ожидают der)
    {"id": "hobby",        "word": "Hobby",         "article": "das", "meaning_ru": "хобби",        "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"Various hobby items: paintbrush, book, tennis racket arranged together, {_S}"},
    # der Joghurt — мужской, многие ожидают das
    {"id": "joghurt",      "word": "Joghurt",       "article": "der", "meaning_ru": "йогурт",       "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A single yogurt cup with a spoon, {_S}"},
    # die Couch — женский (многие ожидают das)
    {"id": "couch",        "word": "Couch",         "article": "die", "meaning_ru": "кушетка/диван","difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A modern couch sofa, side view, {_S}"},
    # der Kanal — мужской (Canal), окончание -al часто das
    {"id": "kanal",        "word": "Kanal",         "article": "der", "meaning_ru": "канал",        "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A water canal with stone walls and a boat, {_S}"},
    # der Gipfel — мужской (mountaintop), -el чаще der но не всегда
    {"id": "gipfel",       "word": "Gipfel",        "article": "der", "meaning_ru": "вершина горы", "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A snowy mountain peak, {_S}"},
    # das Signal — нейтральный (-al чаще das: Signal, Portal, Lokal, но не всегда)
    {"id": "signal",       "word": "Signal",        "article": "das", "meaning_ru": "сигнал",       "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A traffic light showing green, {_S}"},
    # das Pedal — нейтральный (bicycle pedal)
    {"id": "pedal",        "word": "Pedal",         "article": "das", "meaning_ru": "педаль",       "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A bicycle pedal, {_S}"},
    # das Labor — нейтральный (-or обычно der: Motor, Traktor, но Labor — das)
    {"id": "labor",        "word": "Labor",         "article": "das", "meaning_ru": "лаборатория",  "difficulty": "B2", "category": "Ausnahme","dalle_prompt": f"A science laboratory with flasks and equipment, {_S}"},
    # das Podium — нейтральный (греческое -ium → das)
    {"id": "podium",       "word": "Podium",        "article": "das", "meaning_ru": "подиум/пьедестал","difficulty": "B2","category": "Ausnahme","dalle_prompt": f"A winners podium with three levels (1st, 2nd, 3rd), {_S}"},
]


def get_article_quiz_bank_stats() -> dict:
    from collections import Counter
    articles = Counter(e["article"] for e in ARTICLE_QUIZ_BANK)
    diff = Counter(e["difficulty"] for e in ARTICLE_QUIZ_BANK)
    cats = Counter(e["category"] for e in ARTICLE_QUIZ_BANK)
    return {
        "total": len(ARTICLE_QUIZ_BANK),
        "der": articles["der"],
        "die": articles["die"],
        "das": articles["das"],
        "A2": diff["A2"],
        "B1": diff["B1"],
        "B2": diff["B2"],
        "Ausnahme": cats["Ausnahme"],
    }
