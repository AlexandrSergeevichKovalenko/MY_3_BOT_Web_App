"""
Профили ежедневной видеорубрики: «Новость дня» и «Стендап дня».

Механизм у обеих рубрик ОДИН (см. backend/world_news_generator.py): вечером выбирается
ролик на завтра, к нему тянутся немецкие субтитры, один запрос к модели строит разбор
слов и тест, владелец одобряет кнопкой, утром одобренное уходит людям. Различаются они
только тем, что описано здесь: откуда брать ролик, как его выбирать, каким заданием
разбирать и как подписывать карточку.

Почему профиль, а не вторая копия генератора: копия означала бы, что любая будущая
починка делается дважды и один раз забывается.

── Чередование ────────────────────────────────────────────────────────────────
Строго через день, по порядковому номеру даты относительно якоря. Никакого хранимого
состояния: после перезапуска, пропущенного дня или сбоя расписание не разъезжается, и
31-е число или граница года ничего не ломают.

── Откуда взялся набор стендап-каналов ────────────────────────────────────────
Замер 20.08.2026 по живому YouTube Data API: обойдены до 400 последних загрузок у
каждого канала, всего 3756 роликов, из них 646 попадают в 4–15 минут. Числа в скобках
у каждого канала — сколько его роликов попало в диапазон. Решение владельца от
20.08.2026: рубрика — чистый стендап со сцены, телевизионная сатира в неё не входит.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime

RUBRIC_NEWS = "news"
RUBRIC_STANDUP = "standup"

# Якорь чередования: 21.08.2026 — день новостей, и это не выбор, а факт. Запись на 21-е
# собралась ночью 20-го, ещё до выката чередования, и лежит в базе новостью. Якорь на неё
# и указывает, чтобы расписание совпадало с тем, что вправду в базе, а не спорило с ним.
# Отсюда: 22.08 — стендап, 23.08 — новости, и так через день.
#
# Сдвинут на сутки 21.08.2026 по решению владельца. При прежнем якоре (20.08) первый
# стендап выпадал на 23-е: 21-е уже было занято новостью, а вечерняя подготовка 21-го
# делает 22-е, которое по тому якорю тоже было новостным. Владелец ждал бы рубрику два дня.
_ALTERNATION_ANCHOR = date(2026, 8, 21)


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _as_date(value) -> date:
    """Дата из строки YYYY-MM-DD или date. Мусор — это ошибка вызова, а не повод угадывать."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("rubric_for_date: дата не задана")
    return datetime.strptime(text, "%Y-%m-%d").date()


def alternation_enabled() -> bool:
    """Выключатель чередования. Выключенное чередование = каждый день новости, как было
    до 20.08.2026. Нужен, чтобы можно было мгновенно откатить рубрику без выката кода."""
    return _env_flag("DAILY_VIDEO_ALTERNATION_ENABLED", True)


def rubric_for_date(value) -> str:
    """Чья сегодня очередь. Считается от якоря, поэтому пропуск дня ничего не сдвигает."""
    if not alternation_enabled():
        return RUBRIC_NEWS
    forced = (os.getenv("DAILY_VIDEO_FORCE_RUBRIC") or "").strip().lower()
    if forced in (RUBRIC_NEWS, RUBRIC_STANDUP):
        return forced
    delta = (_as_date(value) - _ALTERNATION_ANCHOR).days
    return RUBRIC_NEWS if delta % 2 == 0 else RUBRIC_STANDUP


# ── Задание модели для стендапа ────────────────────────────────────────────────
# Отличается от новостного принципиально, и вот почему (решение владельца 20.08.2026):
# в новостях `die Regierung` — это правительство и точка, а в стендапе `Bock` — не козёл,
# `Alter` — не старик, `krass` — не грубый. Сухой перевод в языковом приложении означает,
# что человек заучит неверное значение. Поэтому карточка слова здесь разбирается как в
# словаре: помета регистра, значение ИМЕННО ЗДЕСЬ, обычное значение (если оно вправду
# другое), дословная цитата из ролика с переводом, откуда взялся образ, и с кем такое
# уместно говорить.
_STANDUP_LLM_SYSTEM = """\
Du bist Sprachwissenschaftler und Deutschlehrer. Du aufbereitest einen deutschen
Stand-up-Comedy-Auftritt für russischsprachige Deutschlernende (B1–B2).

Du bekommst das Transkript des Auftritts. Erstelle daraus ein JSON-Paket:

1) "summary_points": 2–4 sehr kurze Zeilen auf RUSSISCH — worum es in dem Auftritt geht:
   Thema, Figur, Situation. Je 3–9 Wörter, keine Verbindungswörter, kein Wasser.
   Die Pointe NICHT verraten — der Nutzer schaut das Video danach.
   SCHREIBWEISE: Eigennamen behalten ihren GROSSBUCHSTABEN — Länder, Städte, Bundesländer,
   Parteien, Organisationen, Personen (Германия, Саксония-Анхальт, Мекленбург-Передняя
   Померания, АдГ, Бундестаг). Abkürzungen so, wie sie im Russischen üblich sind: AfD → АдГ,
   nicht «афд». «Knapp» heisst OHNE Wasser — NICHT ohne Grossbuchstaben. Jede These beginnt
   mit einem Grossbuchstaben.

2) "phrases": ALLE Einheiten dieses Auftritts, bei denen der KONTEXT die Bedeutung
   bestimmt — Slang, Umgangssprache, Redewendungen, feste Wendungen, Jugendsprache,
   ironische Verwendung, Wortspiele.
   ES GIBT KEINE ZIELZAHL. In einem Auftritt sind es 5, in einem anderen 15 — nimm
   genau so viele, wie es WIRKLICH GIBT. Fülle NIEMALS mit neutralen Alltagswörtern
   auf (arbeiten, das Haus, gut), nur um auf eine Anzahl zu kommen: solche Wörter kennt
   der Nutzer längst und lernt sie in der Nachrichten-Rubrik. Lieber 5 wirklich
   erklärungsbedürftige Einheiten als 14 mit Füllmaterial.
   Der Nutzer dieser Rubrik ist fortgeschritten (B1–B2+): normale Grammatik und
   Alltagslexik beherrscht er, hier geht es um die Tiefe der lebendigen Sprache.
   Jede Einheit:
     - "de": die Einheit in der Form, in der man sie NACHSCHLÄGT — und NICHT mehr.
       Bei einem einzelnen Nomen: MIT Artikel (die Kohle). Bei einem einzelnen Verb:
       Infinitiv (abhauen).
       ABER: eine Redewendung bleibt GANZ. Zerlege sie NICHT in ein Verb und zwinge sie
       NICHT in eine „Wörterbuchnormalform“ — dabei stirbt genau das, was gelernt werden
       soll. Richtig: "Bock haben (auf etwas)", "wie bestellt und nicht abgeholt",
       "null Bock". FALSCH: "haben", "bestellen", "abholen".
       Die Faustregel: Was der Lernende später SAGEN können soll, steht hier — in genau
       der Form, in der man es sagt.
     - "register_ru": Stilmarkierung auf RUSSISCH, kurz: «разговорное», «сленг»,
       «грубое», «молодёжное», «ироничное», «региональное (баварское)», «нейтральное».
       Wenn es derb/vulgär ist, sag das offen — der Lernende muss wissen, wo er das NICHT
       sagen darf.
     - "form_ru": in welcher grammatischen Form "de" dasteht — kurz, auf RUSSISCH, in
       MENSCHLICHER Sprache: «словарная форма», «винительный падеж», «дательный падеж»,
       «инфинитив», «множественное число». Du liest die Form im Transkript ab (Rektion,
       Artikelform) — rate NICHT.
     - "translation_ru": die Bedeutung GENAU HIER, in diesem Auftritt — und in DERSELBEN
       grammatischen Form wie "de". Steht das Deutsche im Akkusativ, steht auch das
       Russische im Akkusativ. Eine Karte, die Akkusativ zeigt und Nominativ übersetzt,
       verwirrt den Lernenden mehr, als sie ihm hilft.
     - "literal_ru": die gewöhnliche/wörtliche Bedeutung — NUR wenn sie sich wirklich von
       der Bedeutung hier unterscheidet, plus ein Satz, wie das eine zum anderen wurde.
       Wenn es keinen Unterschied gibt: LEERER STRING "". ERFINDE KEINE zweite Bedeutung.
     - "quote_de": die Zeile aus dem TRANSKRIPT, in der die Einheit vorkommt — WÖRTLICH
       aus dem Transkript kopiert, 4–20 Wörter. NICHT umformulieren, NICHT ausdenken.
     - "quote_ru": Übersetzung genau dieser Zeile ins Russische, umgangssprachlich, so wie
       ein Mensch das sagen würde.
     - "usage_ru": auf RUSSISCH: Rektion (Kasus/Präposition) und mit WEM man so sprechen
       darf — und womit man es im formellen Umfeld ersetzt.

3) "quiz": GENAU 4 Multiple-Choice-Fragen auf DEUTSCH zum VERSTÄNDNIS des Auftritts:
   Wer macht was, welche Situation wird beschrieben, was meint der Comedian mit einer
   Wendung, worüber lacht das Publikum. KEINE Fragen nach Zahlen und Daten — das ist
   kein Nachrichtenvideo. Jede Frage:
     - "question_de": klar, auf EIN Detail des Auftritts zugespitzt.
     - "options": GENAU 4 Antworten, etwa gleich lang und gleich plausibel, die
       Distraktoren nah an der richtigen — keine offensichtlich absurde Antwort.
     - "correct_index": Index (0–3) der einzig richtigen, im Transkript belegten Antwort.
     - "explanation_ru": 1 kurzer russischer Satz mit dem Beleg aus dem Auftritt.

Antworte NUR mit validem JSON, ohne Erklärungen drumherum."""

_STANDUP_LLM_USER_TMPL = """\
Titel des Auftritts: {title}

Transkript:
{transcript}

Erzeuge das JSON exakt in diesem Format:
{{
  "summary_points": ["…", "…"],
  "phrases": [
    {{"de": "…", "register_ru": "…", "form_ru": "…", "translation_ru": "…",
      "literal_ru": "…", "quote_de": "…", "quote_ru": "…", "usage_ru": "…"}}
  ],
  "quiz": [
    {{"question_de": "…", "options": ["…","…","…","…"], "correct_index": 0, "explanation_ru": "…"}}
  ]
}}"""


@dataclass(frozen=True)
class RubricProfile:
    key: str
    title_ru: str                 # как называется рубрика в тексте владельцу и на карточке
    channel_ids: tuple            # каналы-источники (uploads-плейлисты берутся из них)
    pick_strategy: str            # "newest" — свежие загрузки · "archive" — весь архив
    prefer_manual_captions: bool  # ролики с положенными руками субтитрами идут первыми
    min_seconds: int
    max_seconds: int
    pref_min_seconds: int
    pref_max_seconds: int
    min_phrases: int              # меньше — пакет бракуется, а не показывается урезанным
    max_phrases: int
    archive_pages: int            # сколько страниц по 50 роликов обходить (только archive)
    llm_system: str = ""
    llm_user_tmpl: str = ""
    # Цитата обязана дословно найтись в субтитрах, а карточка — назвать форму, в которой
    # стоит единица. Обязательно у ОБЕИХ рубрик с 21.08.2026: корректор больше не правит
    # текст при сохранении, значит показанное уезжает человеку в словарь дословно.
    requires_quote: bool = True
    requires_register: bool = False  # помета регистра (сленг/грубое) — только у стендапа
    # Берёт ли рубрика ролик С ПОЛКИ (заранее отобранное и уже со скачанными субтитрами)
    # вместо похода в YouTube в момент выпуска. Решение владельца 21.08.2026 — см.
    # backend/standup_shelf.py. У новостей полки нет и быть не может: им нужна свежесть.
    uses_shelf: bool = False
    env_channels_var: str = ""


# Новости: набор каналов и пороги те же, что работали до появления чередования, —
# переезд рубрики на профили не должен менять её поведение.
NEWS_PROFILE = RubricProfile(
    key=RUBRIC_NEWS,
    title_ru="Новость дня",
    channel_ids=(
        "UC5NOEUbkLheQcaaRldYW5GA",  # tagesschau
        "UCeqKIgPQfNInOswGRWt48kQ",  # ZDFheute Nachrichten
        "UCMIgOXM2JEQ2Pv2d0_PVfcg",  # DW Deutsch
        "UCxUWIEL-USsiPak0Qy6_vVg",  # Deutsch lernen mit der DW
        "UCkCab7liRnZSZsN8YqzhuuA",  # Deutschlandfunk
    ),
    pick_strategy="newest",
    prefer_manual_captions=False,
    min_seconds=40,
    max_seconds=900,
    pref_min_seconds=300,
    pref_max_seconds=420,
    min_phrases=6,
    max_phrases=18,
    archive_pages=1,
    env_channels_var="WORLD_NEWS_CHANNEL_IDS",
)

# Стендап: числа в скобках — сколько роликов канала попало в 4–15 минут при замере
# 20.08.2026 (до 400 последних загрузок на канал).
STANDUP_PROFILE = RubricProfile(
    key=RUBRIC_STANDUP,
    title_ru="Стендап дня",
    channel_ids=(
        "UCdoEOMoNFwsCAABHXmxNHtA",  # NightWash club (157)
        "UCfycaisLl4Bgpo3N7CvgvmA",  # Comedy Central Deutschland (117)
        "UCueJOpg1SG9mahPjhALlvJw",  # ARD Stand-Up (116, из них 101 с ручными субтитрами)
        "UCGcheBSVngQt09ubb0BZyJw",  # MySpass Stand-up (108)
        "UCbGevuq9xAqqBGW_2ginhrQ",  # Hazel Brugger (78)
        "UCEMG-07-mvy3e1FiOus3VPg",  # TUTTY TRAN (17)
        "UCJuFbn9jOAxi_XQ6aeUPXpg",  # RebellComedy (16)
        "UC94wFyzOpaxrwtkoRe1FVfw",  # Quatsch Comedy Club (16)
        "UCUZt4aGPkirYE-CBZm-Wk6w",  # Comedy Kollektiv (8, зато 57 роликов с ручными)
        "UC5p4K_i9NpbglbPVKlDapnA",  # Martin Frank (7)
        "UCZqY_CyWJPLjK3xzqWiA9NA",  # Comedyflash (4)
        "UCR65_tsXxQkWLUbjlfmwsDw",  # Nikita Miller (2)
    ),
    # Стендап вечнозелёный: свежесть значения не имеет, важно не повториться. Поэтому
    # обходится архив целиком, а показанное вычитается по вечному реестру.
    pick_strategy="archive",
    # Решение владельца 20.08.2026: сначала ролики с субтитрами, положенными руками
    # (в замере таких 111 в нужной длине — больше семи месяцев вещания через день),
    # машинная расшифровка — второй эшелон. Причина: под стендап машина пишет без знаков
    # препинания и угадывает слова на слух, а человек читает субтитры и заучивает их.
    prefer_manual_captions=True,
    uses_shelf=True,
    min_seconds=240,
    max_seconds=900,
    pref_min_seconds=300,
    pref_max_seconds=600,
    # Плана по количеству НЕТ — решение владельца 20.08.2026: сколько в ролике вправду
    # есть единиц, требующих объяснения, столько и показываем; в одном номере их 5, в
    # другом 15. Фиксированная вилка заставляла бы модель добирать до числа нейтральными
    # словами — ровно тем мусором, которого мы не хотим.
    # min_phrases — не цель, а порог годности РОЛИКА: если сленга и оборотов в нём меньше
    # четырёх, это не материал для рубрики, и генератор честно берёт следующий ролик,
    # а не показывает пустой разбор. max_phrases бережёт экран.
    min_phrases=4,
    max_phrases=18,
    archive_pages=8,  # до 400 роликов на канал — ровно та глубина, что измерена
    llm_system=_STANDUP_LLM_SYSTEM,
    llm_user_tmpl=_STANDUP_LLM_USER_TMPL,
    requires_quote=True,
    requires_register=True,
    env_channels_var="STANDUP_CHANNEL_IDS",
)

_PROFILES = {RUBRIC_NEWS: NEWS_PROFILE, RUBRIC_STANDUP: STANDUP_PROFILE}


def get_profile(rubric: str) -> RubricProfile:
    key = str(rubric or "").strip().lower()
    if key not in _PROFILES:
        raise ValueError(f"неизвестная рубрика: {rubric!r}")
    return _PROFILES[key]


def profile_channel_ids(profile: RubricProfile) -> list:
    """Каналы профиля с возможностью подменить их переменной окружения (без выката кода)."""
    raw = (os.getenv(profile.env_channels_var) or "").strip() if profile.env_channels_var else ""
    if raw:
        return [c.strip() for c in raw.split(",") if c.strip()]
    return list(profile.channel_ids)
