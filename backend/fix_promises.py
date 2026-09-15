# -*- coding: utf-8 -*-
"""Реестр обещаний: каждая починка оставляет проверку, которую система гоняет сама.

┌─ ПОВОД, 04.09.2026. «ЭТО УЖЕ 20-Е ПОДРЯД ЗАДАНИЕ, КОТОРОЕ ТЫ ГОВОРИШЬ, ЧТО ИСПРАВИЛ». ─┐
│ 01.09 я закрыл дверь, через которую прогоны тестов клеймили живые слова, снял 162     │
│ ложные метки, написал тест и сказал «готово». 04.09 владелец открыл тот же экран и    │
│ увидел те же слова с той же подписью. Дверь была закрыта честно, а обещание «меток   │
│ больше не появится» жило только в тексте коммита: никто не перепроверял его назавтра,│
│ и владелец узнал о нарушении сам, через три дня, разозлившись.                       │
│                                                                                      │
│ Решение владельца 04.09.2026: «готово» без зарегистрированного обещания не готово.   │
│ Каждая починка записывает сюда, ЧТО она обещает измеримо: запрос, ожидаемое число,   │
│ дата, как перемерить руками. Каждое утро все обещания проверяются, итог идёт строкой │
│ в утренний отчёт, нарушенное приходит отдельным письмом с кнопками. Владелец ничего  │
│ не вызывает и не помнит. Вопрос агенту в моменте один: «какое обещание ты           │
│ зарегистрировал?». Нет обещания — работа не принята.                                 │
└──────────────────────────────────────────────────────────────────────────────────────┘

Три исхода проверки, и путать их нельзя:
  держится    — измерили, число совпало с обещанным;
  нарушено    — измерили, число НЕ совпало: починка откатилась или не работала никогда;
  не измерено — проверка сама не отработала (база, исключение). Это НЕ «держится» и
                НЕ «нарушено», это отдельный исход, и он тоже идёт владельцу вслух.

Обещание снимается только владельцем, кнопкой. Снятое не проверяется и не показывается
числом «держится»: оно просто не входит в реестр. Не снятое и нарушенное приходит
каждое утро, пока держится или не снято, — молчание не согласие.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

HELD, BROKEN, UNMEASURED = "held", "broken", "unmeasured"


@dataclass(frozen=True)
class Promise:
    key: str                    # короткий ключ, уникален, идёт в callback кнопки
    title: str                  # что обещано, человеческими словами
    since: str                  # дата обещания, ДД.ММ.ГГГГ
    expected: int               # обещанное число
    measure: Callable[[], int]  # как система его считает
    how: str                    # как перемерить руками, чтобы не верить на слово
    # Экран владельца «после» — тот самый текст, который он видит на месте жалобы (отчёт,
    # ответ команды). Первые SCREEN_DAYS дней после починки утро присылает его САМО.
    # Владелец 06.09.2026: «ну я забуду, я же нормальный человек обычный. Всё, что можно
    # сделать автоматически, делается автоматически». Ручной «отправь команду и пришли
    # ответ» — это и есть то, чего быть не должно.
    screen: Callable[[], str] | None = None


# Сколько утр подряд после починки экран «после» приходит сам. Три — чтобы застать и
# первую ночь, когда данные ещё старые, и первую, когда они уже пересчитаны.
SCREEN_DAYS = 3


# ── измерители ────────────────────────────────────────────────────────────────────────

_OLD_BANK_TRACE_KEYS = ("enrich_attempts", "enrich_last_reason", "quarantine_releases",
                        "quarantine_released_at", "quarantine_owner_keep",
                        "quarantine_owner_keep_at")


def _old_bank_quarantine_traces() -> int:
    """Сколько строк старого банка словаря несут след карантина. Обещано: 0.

    Клеймо ставили только прогоны тестов (01.09, 04.09), и в проде его ставить нечем с
    04.09.2026 — функция снесена. Появится хоть одна строка, значит либо метку вернул
    новый код, либо деплой откатился на старый."""
    from backend.database import get_db_connection_context
    условие = " OR ".join(f"response_json ? '{k}'" for k in _OLD_BANK_TRACE_KEYS)
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM bt_3_dictionary_entries WHERE {условие}")
            return int((cursor.fetchone() or [0])[0] or 0)


def _night_enrichment_runs_in_units_mode() -> int:
    """Последний ночной добор шёл по слою слов (mode=units). Обещано: 1.

    Это и есть гарантия, что ветка по старому банку в проде не выполняется. Если журнал
    прогона пуст, измерить нечего — это исход «не измерено», а не «нарушено»."""
    from backend.database import get_latest_scheduler_run_guard
    row = get_latest_scheduler_run_guard(job_key="pool_night_enrichment")
    if not row:
        raise RuntimeError("журнал ночного добора пуст: прогона не было")
    режим = str(((row or {}).get("metadata") or {}).get("mode") or "")
    return 1 if режим == "units" else 0


def _standup_pool_snapshot_stale() -> int:
    """Снимок пула стендапа старше трёх дней. Обещано: 0.

    06.09.2026 отчёт писал «пора добавить каналы» при 90 годных у каналов: запас считался
    по аварийному складу на семь роликов. Теперь запас берётся из снимка пула, а снимок
    пишет каждый обход каналов — вечерний поиск ролика (через день) и ночное пополнение.
    Значит ложная тревога может прийти только по устаревшему снимку: обход не случился
    (квота, сеть) или справка о роликах пришла не вся и снимок честно не записался.
    Первая версия обещания сравнивала текст отчёта с его же числами — тавтология,
    снятая проверяющим агентом 06.09.2026. Снимка нет вовсе — измерить нечего."""
    from datetime import datetime, timedelta, timezone
    from backend.daily_video_rubrics import STANDUP_PROFILE
    from backend.database import get_daily_video_pool_snapshot
    snap = get_daily_video_pool_snapshot(STANDUP_PROFILE.key)
    if not snap or not snap.get("updated_at"):
        raise RuntimeError("снимка пула стендапа ещё нет: обход каналов не доходил до записи")
    age = datetime.now(timezone.utc) - snap["updated_at"]
    return 1 if age > timedelta(days=3) else 0


def _sent_without_shown_mark() -> int:
    """Выпусков, ушедших людям без пометки «показано» в вечном реестре. Обещано: 0.

    До 06.09.2026 сбой этой пометки глушился на уровне debug: ролик мог выйти второй раз,
    и никто бы не узнал. Считается с 07.09.2026 — первого утра после починки."""
    from backend.database import count_sent_daily_videos_without_shown_mark
    return count_sent_daily_videos_without_shown_mark("2026-09-07")


def _shelf_holds_a_draft() -> int:
    """Роликов, положенных на полку ПОСЛЕ того, как их занял черновик дня. Обещано: 0.

    Ночной добор не знал про ролик, который вечер уже выбрал на завтра, и мог положить
    его же на полку: субтитры скачаны зря, место занято (трассировка 06.09.2026).
    Обратный порядок (сначала полка, потом выпуск с неё) — устройство, не считается."""
    from backend.database import count_shelf_items_holding_a_draft
    return count_shelf_items_holding_a_draft()


def _control_extra_calls() -> int:
    """Выпусков с 07.09.2026, где контроль карточек сделал больше обращений к модели, чем
    2 + число сомнений. Обещано: 0.

    Судья с тремя проходами и правом переписывать снят 06.09.2026 по решению владельца
    («зачем три прохода?»); теперь контроль — проход, ответ автора на каждое сомнение и
    повтор по исправленным. Считается по числу обращений, записанному при работе."""
    from backend.database import count_daily_video_issues_with_extra_calls
    return count_daily_video_issues_with_extra_calls("2026-09-07")


def _standup_pool_screen() -> str:
    """Тот же текст, что приходит в воскресенье и по /standup_pool, — экран владельца."""
    from backend.standup_pool_report import format_standup_pool_report, standup_pool_state
    return format_standup_pool_report(standup_pool_state())


_WN_OLD_LOOK = ((".worldnews-card-de", "Georgia"), (".worldnews-step", "clip-path"))


def _count_worldnews_old_look_rules(css: str) -> int:
    """Сколько правил СТАРОГО вида карточки слова «Новость дня» осталось в CSS.

    Старый вид (владелец 04.09.2026: «бедный, блеклый, некачественный») узнаётся по
    двум приметам: газетная Georgia у заголовка .worldnews-card-de и стрелки-шевроны
    (clip-path) у шагов .worldnews-step. Нет самого селектора заголовка — это не тот
    файл, и считать нечего: исход «не измерено», а не «0»."""
    import re
    if not re.search(r"\.worldnews-card-de\s*\{", css):
        raise LookupError("в CSS нет .worldnews-card-de — это не собранный фронт")
    n = 0
    for selector, marker in _WN_OLD_LOOK:
        for m in re.finditer(re.escape(selector) + r"\s*\{([^}]*)\}", css):
            if marker in m.group(1):
                n += 1
    return n


def _access_period_night_sweep() -> int:
    """Сколько начал отсчёта бесплатного месяца за сутки поставила ночная страховка, а
    не дверь записи. Обещано: 0.

    Дверей четыре: /start, первое сообщение в боте, самозапись по ссылке, открытие
    приложения. Строка от страховки значит, что человек прошёл мимо всех четырёх —
    искать, какая молчит (source в bt_3_access_period у соседей подскажет)."""
    from backend.database import count_access_periods_created_by_night_sweep
    return count_access_periods_created_by_night_sweep(days=1)


def _access_period_missing() -> int:
    """Сколько известных боту людей без начала отсчёта бесплатного месяца. Обещано: 0.

    Меряется утром, после ночной страховки: не ноль — значит страховка не отработала, и
    у этих людей право доступа стоит в состоянии unknown (замка нет, но и срока нет)."""
    from backend.database import list_known_user_ids_without_access_period
    return len(list_known_user_ids_without_access_period())


def _locked_users_got_learning_content() -> int:
    """Сколько заданий за сутки ушло людям, у которых бесплатный месяц кончился и подписки
    нет. Обещано: 0. Двери — общий сборщик рассылок и обработчик сообщений бота
    (docs/tasks/light_tier_strategy.md §5.2–5.3).

    ⚠ 05.09.2026: считается от момента, когда человек стал заперт ПО-НАСТОЯЩЕМУ. Сдвиг
    даты рукой на тестовом аккаунте больше не превращает нормальные утренние выдачи в
    «утечку» — разбор в docstring самой функции подсчёта."""
    from backend.database import count_learning_content_sent_to_locked_users
    return count_learning_content_sent_to_locked_users(days=1)


def _access_reminders_over_cadence() -> int:
    """Напоминаний запертым за сутки, ушедших раньше положенного промежутка (7 дней
    первые 8 раз, дальше 30). Обещано: 0."""
    from backend.database import count_access_reminders_over_cadence
    return count_access_reminders_over_cadence(days=1)


def _served_webapp_css() -> str:
    """CSS ЖИВОЙ страницы веб-приложения, скачанный по сети, — то, что получает телефон.

    Обещания проверяет сервис бота, у которого фронта на диске нет (Dockerfile.bot), поэтому
    единственный честный источник — сам сайт по WEB_APP_URL. Vite режет стили по экранам:
    в index.html подключён только общий CSS, остальные .css названы строками внутри
    скриптов страницы — собираем оба слоя. Нет адреса, нет сети, нет ни одного .css —
    исключение, и проверка получает исход «не измерено», а не «0»."""
    import os
    import re
    from urllib.parse import urljoin
    from urllib.request import Request, urlopen
    base = str(os.getenv("WEB_APP_URL") or "").strip()
    if not base:
        raise LookupError("WEB_APP_URL не задан: где живёт веб-приложение, бот не знает")
    if not base.endswith("/"):
        base += "/"

    def _get(url: str) -> str:
        # Веб-сервис засыпает без трафика и просыпается ~35 с (замер 05.09.2026), поэтому
        # ожидание длинное: короткое дало бы «не измерено» каждое тихое утро.
        with urlopen(Request(url, headers={"User-Agent": "fix-promises/1"}), timeout=120) as r:
            return r.read().decode("utf-8", errors="replace")

    html = _get(base)
    # Vite режет стили по экранам: в index.html подключён только общий CSS, а стили
    # карточки лежат в App-*.css, имя которого записано строкой внутри входного скрипта.
    # Поэтому собираем ОБА слоя: CSS из <link> и CSS, упомянутые в скриптах страницы.
    css_paths = set(re.findall(r'<link[^>]+href="(/[^"]+\.css[^"]*)"', html))
    for js in re.findall(r'<script[^>]+src="(/[^"]+\.js)"', html):
        css_paths.update("/" + m for m in re.findall(r'assets/[A-Za-z0-9_.-]+\.css', _get(urljoin(base, js))))
    if not css_paths:
        raise LookupError(f"на странице {base} не нашлось ни одного .css")
    css = "".join(_get(urljoin(base, path)) for path in sorted(css_paths))
    return css


def _served_webapp_chunk(имя: str) -> str:
    """Один собранный кусок ЖИВОГО фронта (Vite-чанк) по его имени, скачанный по сети.

    Экраны интерактивов лежат не во входном скрипте, а в отдельном куске: его имя
    записано строкой внутри входного скрипта — оттуда и берём. Тот же приём, что у
    _served_webapp_css, и та же причина: у сервиса бота фронта на диске нет
    (Dockerfile.bot), честный источник — сам сайт по WEB_APP_URL. Нет адреса, нет сети,
    нет такого куска — исключение, и проверка получает «не измерено», а не «0»."""
    import os
    import re
    from urllib.parse import urljoin
    from urllib.request import Request, urlopen
    base = str(os.getenv("WEB_APP_URL") or "").strip()
    if not base:
        raise LookupError("WEB_APP_URL не задан: где живёт веб-приложение, бот не знает")
    if not base.endswith("/"):
        base += "/"

    def _get(url: str) -> str:
        # Веб-сервис засыпает без трафика и просыпается ~35 с (замер 05.09.2026).
        with urlopen(Request(url, headers={"User-Agent": "fix-promises/1"}), timeout=120) as r:
            return r.read().decode("utf-8", errors="replace")

    html = _get(base)
    входные = re.findall(r'<script[^>]+src="(/[^"]+\.js)"', html)
    if not входные:
        raise LookupError(f"на странице {base} не нашлось ни одного скрипта")
    образец = re.compile(r"assets/" + re.escape(имя) + r"-[A-Za-z0-9_-]+\.js")
    for js in входные:
        найдено = образец.findall(_get(urljoin(base, js)))
        if найдено:
            return _get(urljoin(base, "/" + найдено[0]))
    raise LookupError(f"во входных скриптах {base} нет куска {имя}-*.js — это не собранный фронт")


# Строки-источники, которые экраны «Работа над ошибками» передают дискетке сохранения.
# В собранном коде имена переменных перемолоты, а строковые литералы — нет: именно они
# и доказывают, что дискетка на экране стоит и подписана своим источником.
_REVIEW_SAVE_ORIGINS = ("artikel_review_save", "wofrage_review_save")


def _review_origins_missing_from(js: str) -> list[str]:
    """Какие из источников дискетки НЕ дошли до телефона в этом куске фронта."""
    return [o for o in _REVIEW_SAVE_ORIGINS
            if f'"{o}"' not in js and f"'{o}'" not in js]


def _review_screens_without_save_chip() -> int:
    """Сколько экранов «Работа над ошибками» пришли на телефон БЕЗ дискетки. Обещано: 0.

    Повод (владелец, 10.09.2026, экран «🔁 Работа над ошибками · der Auspuff»): «А где
    пропала иконка сохранения тут в интерактиве с Артиклями?!» Она не пропадала — её там
    не было никогда: экраны повторов собрали по шаблону тренажёров (05.08.2026) ДО того,
    как дискетка появилась в самих тренажёрах, и копия за оригиналом не пошла.

    Меряем не исходник, а то, что реально отдано телефону: в куске AnswerOverlay живого
    сайта должны стоять оба источника сохранения. Нет куска или сеть молчит — «не
    измерено», а не «держится»."""
    return len(_review_origins_missing_from(_served_webapp_chunk("AnswerOverlay")))


def _review_save_chip_screen() -> str:
    """Экран «после»: что по этому поводу отдаёт сайт прямо сейчас."""
    нет = _review_origins_missing_from(_served_webapp_chunk("AnswerOverlay"))
    подпись = {"artikel_review_save": "Артикли", "wofrage_review_save": "Wo-Fragen"}
    if not нет:
        return ("💾 «Работа над ошибками» → Артикли и Wo-Fragen: дискетка в углу слова "
                "стоит на обоих экранах. Ответил — и слово забирается в словарь одним "
                "нажатием, как в тренажёре.")
    return ("💾 «Работа над ошибками»: дискетки НЕТ на экранах — "
            + ", ".join(подпись.get(o, o) for o in нет)
            + ". Слово, на котором человек ошибся, забрать в словарь неоткуда.")


def _worldnews_card_old_look_rules() -> int:
    """Читает CSS ЖИВОЙ страницы веб-приложения по сети. Обещано: 0.

    ┌─ ПОЧИНЕНО 05.09.2026. Проверка читала frontend/dist/assets на диске — и 05.09 ─────┐
    │ пришла «не измерено: собранного CSS нет». Обещания проверяет сервис бота, а фронт   │
    │ собирает и держит только веб-сервис (Dockerfile.backend); в образ бота каталог      │
    │ frontend/ не попадает вовсе (Dockerfile.bot). Файла там нет и не будет.             │
    │ Поэтому проверка идёт туда же, куда идёт человек: скачивает страницу по WEB_APP_URL, │
    │ находит в ней подключённые .css и считает старые правила в них. Это и есть экран    │
    │ владельца, а не копия исходника. Нет адреса или сеть не ответила — «не измерено».  │
    └────────────────────────────────────────────────────────────────────────────────────┘
    """
    return _count_worldnews_old_look_rules(_served_webapp_css())


_HINT_MODAL_ROOT = ".word-hint-overlay"


def _count_hint_modal_own_box_sizing(css: str) -> int:
    """Сколько правил в CSS задают box-sizing: border-box ВСЕМУ поддереву окна подсказки
    по слову (селектор «.word-hint-overlay *»). Ожидается ровно 1.

    Окно живёт в двух хозяевах: в приложении (App.css со сбросом на всё) и в интерактиве
    «Слова со вчерашних тренировок», который приходит в личку и App.css не грузит. Окно
    порталом уходит в <body>, мимо сбросов интерактива, — и 06.09.2026 считалось там по
    content-box: кнопка «Понятно» уходила под нижний край экрана на всех десяти телефонах
    матрицы. Починка — сброс у самого окна (WordHintModal.css). Нет самого селектора
    .word-hint-overlay — это не собранный фронт, считать нечего: «не измерено»."""
    import re
    if not re.search(re.escape(_HINT_MODAL_ROOT) + r"\s*[{,]", css):
        raise LookupError(f"в CSS нет {_HINT_MODAL_ROOT} — это не собранный фронт")
    n = 0
    for m in re.finditer(r"([^{}]+)\{([^}]*)\}", css):
        selectors = [x.strip() for x in m.group(1).split(",")]
        if f"{_HINT_MODAL_ROOT} *" in selectors and re.search(r"box-sizing\s*:\s*border-box", m.group(2)):
            n += 1
    return n


def _hint_modal_box_sizing_missing() -> int:
    """1, если в отданном телефону CSS нет собственного сброса окна подсказки; иначе 0."""
    return 0 if _count_hint_modal_own_box_sizing(_served_webapp_css()) >= 1 else 1


# ── Итоги спринта: шапка поджата, слова получают экран (14.09.2026) ──────────────────
# Жалоба владельца (скриншот Artikel Sprint · Battle): «окошко со словами очень низкое,
# мало слов помещается, постоянно приходится прокручивать». Замер на стенде с настоящим
# answer.css и настоящей подгонкой fitCard (390×780): грамота 213 + Топ-3 158 + подсказка
# 60 = 431 px шапки, списку оставалось 132 px — 3,2 строки. После правки: 89 + 113 + 32,
# списку 352 px — 9,4 строки. На маленьком телефоне (360×600) 2,8 → 8,4.
#
# Обещание меряет не исходник, а CSS, который получает телефон: правила ниже должны быть
# в нём. Пропадёт хоть одно — список снова сожмётся в две строки, и это придёт владельцу
# письмом, а не всплывёт через месяц на его экране.
_SPRINT_RESULT_COMPACT_RULES = (
    ".ans-card:has(> .as-result-list) .as-cert{display:grid",
    ".ans-card:has(> .as-result-list) .as-cert-medal{",
    ".ans-card:has(> .as-result-list) .sp-rank{",
    ".ans-card:has(> .as-result-list) .as-save-hint{",
)


def _sprint_compact_rules_missing_from(css: str) -> list[str]:
    """Какие из правил компактной шапки НЕ дошли до телефона.

    Сравнение без пробелов: vite их срезает («:has(>.as-result-list)»), и посимвольное
    сравнение с исходником дало бы «нарушено» на ровном месте. Нет самого списка слов в
    CSS — это не собранный фронт, считать нечего: «не измерено», а не «0»."""
    import re
    if not re.search(r"\.as-result-list\s*[{,]", css):
        raise LookupError("в CSS нет .as-result-list — это не собранный фронт")
    сжатый = re.sub(r"\s+", "", css)
    return [r for r in _SPRINT_RESULT_COMPACT_RULES if re.sub(r"\s+", "", r) not in сжатый]


def _sprint_result_header_not_compact() -> int:
    """Сколько правил компактной шапки итогов пропало из живого CSS. Обещано: 0."""
    return len(_sprint_compact_rules_missing_from(_served_webapp_css()))


def _sprint_result_header_screen() -> str:
    """Экран «после»: что по этому поводу отдаёт сайт прямо сейчас."""
    нет = _sprint_compact_rules_missing_from(_served_webapp_css())
    if not нет:
        return ("🏁 Итоги спринта (Artikel · Adjektiv · Wo-Frage): шапка компактная — "
                "медаль стоит в строке с местом, Топ-3 поджат. Список слов получает "
                "9 строк вместо 3 на обычном телефоне и 8 вместо 3 на маленьком: "
                "ошибки видно сразу, без прокрутки.")
    return ("🏁 Итоги спринта: из живого CSS пропало правил компактной шапки — "
            + str(len(нет)) + " из " + str(len(_SPRINT_RESULT_COMPACT_RULES))
            + ". Список слов снова сжат в две-три строки.")


def _hint_modal_screen() -> str:
    """Экран «после» для окна подсказки в интерактиве: что реально отдаёт сайт."""
    n = _count_hint_modal_own_box_sizing(_served_webapp_css())
    return ("🪟 Окно «Подсказка по слову» в интерактиве «Слова со вчерашних тренировок»: "
            + ("сброс box-sizing у самого окна в живом CSS ЕСТЬ — кнопка «Понятно» стоит "
               "внутри экрана на любом телефоне" if n >= 1 else
               "сброса box-sizing в живом CSS НЕТ — кнопка «Понятно» снова под краем экрана"))


_OWN_SHEET_ROOT = ".worldnews-own-overlay"
_OWN_INLINE_FIELD = ".worldnews-card-own-input"


def _count_own_sheet_defects(css: str) -> int:
    """Сколько примет СТАРОГО устройства окна «Сохранить по-своему» осталось в живом CSS.

    Обещано: 0. Приметы считаются отдельно, каждая — свой рубль:
      1) слоя поверх экрана нет или он не прибит (у .worldnews-own-overlay нет
         position: fixed) — значит поля снова живут внутри карточки слова;
      2) в CSS остался класс поля старой встроенной формы (.worldnews-card-own-input).
    Нет самого селектора заголовка карточки — это не собранный фронт, считать нечего:
    исход «не измерено», а не «0»."""
    import re
    if not re.search(r"\.worldnews-card-de\s*\{", css):
        raise LookupError("в CSS нет .worldnews-card-de — это не собранный фронт")
    defects = 0
    pinned = any(
        re.search(r"position\s*:\s*fixed", m.group(1))
        for m in re.finditer(re.escape(_OWN_SHEET_ROOT) + r"\s*\{([^}]*)\}", css)
    )
    if not pinned:
        defects += 1
    if re.search(re.escape(_OWN_INLINE_FIELD) + r"\s*[{,]", css):
        defects += 1
    return defects


def _worldnews_own_sheet_defects() -> int:
    """Правится ли своя версия слова в окне ПОВЕРХ экрана. Обещано: 0.

    Повод 09.09.2026: форма стояла последним блоком ВНУТРИ карточки «Новости дня» и
    стендапа. Карточка вписана в экран и целиком не прокручивается (прокрутка только у
    середины), а с открытой клавиатурой видимая высота падает вдвое — верхнее поле с
    немецкой фразой срезалось верхним краем, и владелец не видел, что правит.
    Вернётся старое устройство (откат деплоя, чужая правка) — число станет не нулём."""
    return _count_own_sheet_defects(_served_webapp_css())


def _worldnews_own_sheet_screen() -> str:
    """Экран «после»: что про это окно говорит CSS, который сайт отдаёт телефону."""
    defects = _count_own_sheet_defects(_served_webapp_css())
    return ("✏️ «Сохранить по-своему» («Новость дня» и стендап): "
            + ("правится в окне ПОВЕРХ экрана — фраза и перевод видны целиком даже с "
               "открытой клавиатурой, кнопки «Сохранить»/«Отмена» стоят над ней"
               if defects == 0 else
               "окна поверх экрана в живом CSS НЕТ — поля снова внутри карточки, и "
               "верхний край срезает немецкую фразу"))


def _daily_video_night_recheck_ran() -> int:
    """Запускалась ли ночная перепроверка карточек «Новости дня» после её снятия. Обещано: 0.

    Снята решением владельца 05.09.2026 (одна проверка на входе; разбор у бывшей
    run_daily_video_recheck в bot_3.py). Её сердцебиение — job_key
    'daily_video_recheck_result'. Запись новее даты снятия значит, что работа вернулась
    в расписание (например, старым деплоем)."""
    from backend.database import get_latest_scheduler_run_guard
    row = get_latest_scheduler_run_guard(job_key="daily_video_recheck_result")
    if not row:
        return 0
    when = row.get("finished_at") or row.get("updated_at")
    if when is None:
        return 0
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return 1 if when > _NIGHT_RECHECK_REMOVED_AT else 0


_NIGHT_RECHECK_REMOVED_AT = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
_SCREEN_PROMISE_USER_CAP = 200


def _people_with_pending_words() -> list[int]:
    """Кому экран проверки слов сегодня что-то покажет. Больше потолка — НЕ измеряем.

    Обещание проверяется той же функцией, что рисует экран (`audit_items`), а она
    работает на одного человека. Пока таких людей единицы, обход честный и полный.
    Станет больше потолка — проверка обязана сказать «не измерено», а не померить
    выборку и выдать её за весь продукт (три исхода, и путать их нельзя).
    """
    from backend.database import get_db_connection_context
    from backend.word_confirm_digest import _BARE, _ТОЛЬКО_СЛОВА
    bare = _BARE.format(col="q.word_de")
    sql = """
        SELECT DISTINCT q.user_id
          FROM bt_3_webapp_dictionary_queries q
          JOIN bt_3_word_check w ON w.asked = {bare}
         WHERE w.status IN ('не подтверждено', 'не слово')
           AND {только_слова}
           AND NOT EXISTS (SELECT 1 FROM bt_3_word_confirm_digest d
                            WHERE d.user_id = q.user_id AND d.word = {bare}
                              AND d.closed_at IS NOT NULL);
    """.format(bare=bare, только_слова=_ТОЛЬКО_СЛОВА.format(bare=bare))
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            люди = [int(r[0]) for r in (cursor.fetchall() or [])]
    if len(люди) > _SCREEN_PROMISE_USER_CAP:
        raise RuntimeError(
            f"людей с ждущими словами {len(люди)}, потолок обхода {_SCREEN_PROMISE_USER_CAP}: "
            "обещание нужно мерить иначе, а не по выборке")
    return люди


def _screen_cards() -> list[dict]:
    """Все карточки СЛОВ, которые экран проверки сегодня покажет живым людям.

    Берётся ровно то, что уходит в браузер (`audit_items`), а не свой похожий запрос:
    обещание, которое смотрит мимо продукта, ничего не стережёт. Фразы отбрасываем —
    они не про написание слова."""
    from backend.word_confirm_digest import audit_items
    карточки: list[dict] = []
    for user_id in _people_with_pending_words():
        карточки += [к for к in audit_items(user_id) if к.get("kind") == "word"]
    return карточки


def _accusations_without_a_variant() -> int:
    """Карточек, где надпись говорит «сохранилось не целиком», а варианта рядом нет.

    Обещано: 0. Это и есть жалоба владельца 04.09.2026 — «а где было вот так,
    предлагаю вот так?». Считается по тому, что вправду уходит на экран: вернут одну
    фразу для всех — число вырастет назавтра, само."""
    return sum(1 for к in _screen_cards()
               if "не целиком" in str(к.get("why") or "")
               and not str(к.get("suggestion") or "").strip())


def _real_words_still_bothering_people() -> int:
    """Карточек «слово настоящее, справочник его не знает» на экранах. Обещано: 0.

    Решение владельца 04.09.2026: настоящее слово, которого нет в справочнике, — наша
    забота, а не вопрос человеку. Замер в день починки: 13 таких из 16 карточек на
    экране владельца. В базе эти слова остаются как были — уходят они только с экрана."""
    return sum(1 for к in _screen_cards()
               if "справочник его не знает" in str(к.get("why") or ""))


def _lost_spellings_from_the_gate() -> int:
    """Новых вердиктов «модель предложила другое написание» БЕЗ сохранённого варианта.

    Обещано: 0. До 04.09.2026 дверь получала от модели готовое написание и выбрасывала
    его, оставляя человеку серую надпись без единой кнопки. Считаем только вердикты от
    05.09.2026 и позже: старые 6 добирает ночь, и мешать одно с другим значит не увидеть
    возврата дефекта."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*) FROM bt_3_word_check w
                    WHERE w.status = 'не подтверждено'
                      AND w.source LIKE 'модель предложила другое%%'
                      AND w.checked_at >= TIMESTAMPTZ '2026-09-05'
                      AND NOT EXISTS (SELECT 1 FROM bt_3_word_suggestion s
                                       WHERE s.asked = w.asked
                                         AND COALESCE(s.suggestion, '') <> '');"""
            )
            return int((cursor.fetchone() or [0])[0] or 0)

def _word_pick_door_misses() -> int:
    """Вчерашних сохранений из интерактивов без отбора на сегодня. Обещано: 0.
    Дверь стоит внутри сохранения слова (05.09.2026); появится строка — дверь обошли
    (новый источник не в WORD_PICK_ORIGINS) или запись упала (exception в логе)."""
    from backend.database import count_word_pick_door_misses
    return count_word_pick_door_misses()


def _weekly_ranking_places_given_wrongly() -> int:
    """Строк снимка недельного рейтинга, где место стоит у нуля или не совпадает с местом
    по баллу «как в спорте». Обещано: 0.

    Решение владельца 06.09.2026: ноль активности — места нет (rank NULL), равный балл —
    одно место. До того «@salesdoc» неделями видел «#8 из 14» по алфавиту имени.
    Снимки чинятся при каждом запуске рейтинга (_repair_weekly_global_ranking_snapshots);
    строка здесь значит, что чинилка не отработала или место снова раздал старый код."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM bt_3_weekly_global_ranking_snapshots
                      WHERE final_score <= 0 AND rank IS NOT NULL)
                  + (SELECT COUNT(*) FROM (
                        SELECT rank, total_users,
                               RANK() OVER (PARTITION BY week_start ORDER BY final_score DESC) AS rk,
                               COUNT(*) OVER (PARTITION BY week_start) AS n
                          FROM bt_3_weekly_global_ranking_snapshots
                         WHERE final_score > 0) r
                      WHERE r.rank IS DISTINCT FROM r.rk OR r.total_users IS DISTINCT FROM r.n);
                """
            )
            return int((cursor.fetchone() or [0])[0] or 0)


def _word_pick_posters_missing() -> int:
    """Людей, кому вчера был положен постер «Слова со вчерашних тренировок» дважды,
    а пришло меньше двух. Обещано: 0."""
    from backend.database import count_word_pick_posters_missing
    return count_word_pick_posters_missing()


def _personal_questions_on_unseen_translation() -> int:
    """Открытых личных вопросов, вынесенных по переводу, которого автор не видит на
    экране. Обещано: 0 — ночь берёт такие карточки первыми (06.09.2026, было 111)."""
    from backend.phrase_panel import count_personal_questions_on_unseen_translation
    return count_personal_questions_on_unseen_translation()


# ── реестр ────────────────────────────────────────────────────────────────────────────
# Добавляя починку — добавляй строку сюда. Ключ не менять после регистрации: по нему
# лежат журнал проверок и решение владельца.

def _db_guardrails_alive() -> int:
    """Обе защиты Postgres от зависшей транзакции на месте. Обещано: 1.

    Повод — остановка базы 05.09.2026 19:48–19:50 UTC: все сервисы разом встали, потому
    что серверные соединения PgBouncer были заняты транзакциями дольше минуты, а у
    Postgres не было ни таймаута на брошенную транзакцию, ни лога медленных запросов —
    поэтому виновника не удалось назвать даже задним числом.

    Настройки живут в postgresql.auto.conf на томе. Ноль здесь означает, что том
    пересоздали, кто-то сделал ALTER SYSTEM RESET или сервис Postgres заменили."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT setting FROM pg_settings WHERE name IN "
                "('idle_in_transaction_session_timeout', 'log_min_duration_statement') "
                "ORDER BY name"
            )
            значения = [str(r[0]) for r in (cursor.fetchall() or [])]
    # порядок по имени: idle_in_transaction_session_timeout, log_min_duration_statement
    return 1 if значения == ["60000", "5000"] else 0


def _db_pool_starvation_today() -> int:
    """Сколько раз за сегодня запрос не дождался соединения. Обещано: 0.

    Считаем ТОЛЬКО настоящий голод (_note_pool_starvation), а не «пик коснулся потолка»:
    пик, равный потолку, — норма, это разобрано на живых данных 28.08.2026. Голод был
    дважды за десять дней и оба раза у MY_3_BOT: 28.08 — 11, 05.09 — 11."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(SUM(hits), 0) FROM bt_3_capacity_daily "
                "WHERE kind = 'db_pool' AND day = CURRENT_DATE"
            )
            return int((cursor.fetchone() or [0])[0] or 0)


def _sprint_accepted_duplicates_or_self() -> int:
    """Записей банка спринта (не снятых), где в accepted одно слово дважды или само целевое
    слово, ЛИБО в примерах тренажёра одно слово дважды. Обещано: 0 (06.09.2026). До двери приёма было 9 + 4 из 56: «die Option» шесть
    раз у Gelegenheit — 5 раундов из 8 у пользователя."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH e AS (
                    SELECT b.sprint_id, lower(trim(b.wort)) AS w, lower(trim(x->>'de')) AS de
                    FROM bt_3_sprint_bank b, jsonb_array_elements(b.accepted) x
                    WHERE NOT b.retired
                )
                , ex AS (
                    SELECT b.sprint_id, lower(trim(x->>'word')) AS wd
                    FROM bt_3_sprint_bank b, jsonb_array_elements(b.trainer_json->'correct_examples') x
                    WHERE NOT b.retired
                )
                SELECT COUNT(*) FROM (
                    SELECT sprint_id FROM e GROUP BY sprint_id, w
                    HAVING COUNT(*) > COUNT(DISTINCT de) OR COUNT(*) FILTER (WHERE de = w) > 0
                    UNION
                    SELECT sprint_id FROM ex GROUP BY sprint_id HAVING COUNT(*) > COUNT(DISTINCT wd)
                ) t
                """
            )
            return int((cursor.fetchone() or [0])[0] or 0)


def _sprint_accepted_article_mismatch() -> int:
    """Существительных-синонимов в банке (не снятых), чей артикль расходится со справочником
    рода. Обещано: 0 (06.09.2026). Было 4: die Potenzial, die Backup, die Beistand, das Konsens."""
    from backend.database import get_db_connection_context
    from backend.sprint_intake import _split_noun
    from backend.article_authority import authoritative_article
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT x->>'de' FROM bt_3_sprint_bank b, jsonb_array_elements(b.accepted) x "
                           "WHERE NOT b.retired")
            words = [str(r[0] or "") for r in cursor.fetchall() or []]
    n = 0
    for de in words:
        noun = _split_noun(de)
        if not noun or not noun[0]:
            continue
        ref, _ = authoritative_article(noun[1])
        if ref and ref != noun[0]:
            n += 1
    return n


def _champion_zero_with_place() -> int:
    """Игроков суточного рейтинга (тот же расчёт, что у вечернего «Чемпиона дня» и
    мини-аппа) с 0 очков и назначенным местом. Обещано: 0 (09.09.2026). Повод —
    «Olga: 1 место, 0 очков, 0/7» над Oleg 45 из-за ворот «половина заданий»."""
    from backend.quiz_leaderboard import get_quiz_leaderboard
    lb = get_quiz_leaderboard(1)
    return sum(1 for l in (lb.get("leaders") or []) if int(l.get("points") or 0) <= 0 and l.get("rank") is not None)


def _champion_day_screen() -> str:
    """Экран владельца «после»: сегодняшний суточный рейтинг тем же расчётом, что плакат
    «Чемпион дня» — место, имя, очки, верно/отвечено. Первые три утра приходит сам."""
    from backend.quiz_leaderboard import get_quiz_leaderboard
    lb = get_quiz_leaderboard(1)
    leaders = lb.get("leaders") or []
    if not leaders:
        return "За сутки никто не отвечал — чемпиона дня нет, плакат не уходит."
    lines = [f"Суточный рейтинг (заданий {lb.get('total_tasks', 0)}):"]
    for l in leaders[:7]:
        place = f"{l['rank']} место" if l.get("rank") else "без места"
        lines.append(f"{place}: {l['name']} — {l['points']} очк., {l['correct']}/{l['answered']} верно")
    noms = []
    for key, label in (("accurate", "самый точный"), ("active", "самый активный"), ("fastest", "самый быстрый")):
        if lb.get(key):
            noms.append(f"{label} — {lb[key]['name']}")
    lines.append("Номинации: " + ("; ".join(noms) if noms else "нет (ни у кого нет очков)"))
    return "\n".join(lines)


def _sprint_review_open_stale() -> int:
    """Кандидатов в очереди синонимов без итогового решения старше двух суток.
    Обещано: 0 (08.09.2026, владелец: «модель ставит итоговую точку»). До 08.09 считалось
    только «без вердикта»: 65 строк с вердиктом «сомневаюсь» ждали владельца по 20 в день
    и в обещание не попадали. Теперь открытая строка — это строка, которую судья ещё не
    решил; судья идёт ночью в 03:10 и после набора в 03:20. Строка старше двух суток
    значит: оба голоса молчат (Gemini без кредитов И GPT не отвечает) либо задача не
    запускается."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM bt_3_sprint_accepted_review "
                           "WHERE status = 'open' AND created_at < NOW() - interval '2 days'")
            return int((cursor.fetchone() or [0])[0] or 0)


_sprint_review_unjudged_stale = _sprint_review_open_stale   # прежнее имя (до 08.09.2026)


def _sprint_accepted_no_dictionary() -> int:
    """Однословных ответов в показе (accepted не снятых слов), которых нет ни в
    Wiktionary (кеш bt_3_wiktionary_synonyms: страницы нет), ни в OpenThesaurus, ни в
    DWDS (кеш bt_3_dwds_lemmas: known=false), и которые не оставил владелец кнопкой.
    Обещано: 0 (08.09.2026). Повод — «befehlsgebunden» у «unabhängig»: судья сказал
    «да», а слова нет ни в Duden, ни в DWDS, ни в Wiktionary. Слова без строки в кеше
    Wiktionary или DWDS не считаются: их существование не проверено, а не опровергнуто
    (их проверит ночная гигиена)."""
    from backend.database import get_db_connection_context
    from backend.synonym_sources import _page_title, term_key
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT b.sprint_id, x->>'de' FROM bt_3_sprint_bank b, jsonb_array_elements(b.accepted) x "
                           "WHERE NOT b.retired")
            rows = [(str(r[0]), str(r[1] or "")) for r in cursor.fetchall() or []]
            cursor.execute("SELECT sprint_id, lower(de) FROM bt_3_sprint_accepted_review "
                           "WHERE status = 'kept' AND decision IN ('keep','der','die','das')")
            owner_kept = {(str(r[0]), str(r[1])) for r in cursor.fetchall() or []}
            n = 0
            for sprint_id, de in rows:
                title = _page_title(de)
                if not title or " " in title or (sprint_id, de.lower()) in owner_kept:
                    continue
                cursor.execute("SELECT missing FROM bt_3_wiktionary_synonyms WHERE title = %s", (title,))
                row = cursor.fetchone()
                if not row or not row[0]:
                    continue
                cursor.execute("SELECT 1 FROM bt_3_openthesaurus_synsets WHERE term_key = %s LIMIT 1",
                               (term_key(de),))
                if cursor.fetchone() is not None:
                    continue
                cursor.execute("SELECT known FROM bt_3_dwds_lemmas WHERE title = %s", (title,))
                row = cursor.fetchone()
                if row is not None and not row[0]:
                    n += 1
            return n


def _sprint_gate_screen() -> str:
    """Экран владельца «после»: те же списки, что видит человек на финале тренажёра
    («Все антонимы» / «Все синонимы») у слов, по которым пришла жалоба 08.09.2026, плюс
    сколько строк очереди ждёт судью. Первые три утра приходит сам."""
    from backend.database import get_db_connection_context
    lines = []
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            for wort, relation in (("unabhängig", "antonym"), ("die Gelegenheit", "synonym")):
                cursor.execute("SELECT accepted, retired FROM bt_3_sprint_bank "
                               "WHERE lower(wort) = lower(%s) AND relation = %s", (wort, relation))
                row = cursor.fetchone()
                if not row:
                    lines.append(f"{wort}: записи в банке нет")
                    continue
                words = [str((a or {}).get("de") or "") for a in (row[0] or [])]
                lines.append(f"{'Все антонимы' if relation == 'antonym' else 'Все синонимы'} «{wort}»"
                             f"{' (снято с показа)' if row[1] else ''}: {len(words)} — " + ", ".join(words))
            cursor.execute("SELECT COUNT(*) FROM bt_3_sprint_accepted_review WHERE status = 'open'")
            open_n = int((cursor.fetchone() or [0])[0] or 0)
    lines.append(f"Ждут судью: {open_n}" + ("" if open_n else " (на согласование никому ничего не уходит)"))
    return "\n".join(lines)


def _sprint_bank_unchecked() -> int:
    """Записей банка спринта без отметки accepted_checked_at — то есть попавших в банк мимо
    двери приёма. Обещано: 0 (06.09.2026): новое слово помечается при вставке, накопленное
    — ночной гигиеной 03:10."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM bt_3_sprint_bank WHERE accepted_checked_at IS NULL")
            return int((cursor.fetchone() or [0])[0] or 0)


# Момент деплоя починки «слово из читалки несёт источник» (UTC). Слова из читалки,
# сохранённые раньше, привязать не к чему — у них нет ни номера книги, ни ролика.
_READER_SOURCE_SINCE = "2026-09-07T06:39:00+00:00"


def _reader_saves_without_source() -> int:
    """Слов, сохранённых из читалки после починки БЕЗ источника. Обещано: 0.

    Владелец 07.09.2026: слово из книги «Текст видео» обязано лечь под название того
    же ролика, что и слова из плеера; книги и статьи — под своё название. Дверь одна
    (/api/webapp/dictionary/save), карточку источника называет сервер при открытии
    книги, фронт прикладывает её к каждому сохранению. Не ноль — карточка не доехала:
    фронт не приложил, сервер не принял или у книги-видео ссылка не нашего вида."""
    from backend.database import count_reader_dictionary_saves_without_source
    return count_reader_dictionary_saves_without_source(_READER_SOURCE_SINCE)


def _reader_sources_screen() -> str:
    """Экран владельца «Откуда» в личном словаре: ролики, книги, статьи с числом слов и
    общие группы. Ровно то, что рисует список в приложении (тот же запрос
    get_dictionary_sources_with_counts), — а не свой похожий подсчёт."""
    from backend.database import get_admin_telegram_ids, get_dictionary_sources_with_counts
    ids = sorted(get_admin_telegram_ids() or [])
    if not ids:
        raise LookupError("не знаем, кто владелец: список админов пуст")
    data = get_dictionary_sources_with_counts(int(ids[0]))
    icons = {"youtube": "🎬", "article": "📰", "book": "📖"}
    lines = ["📚 «Откуда» в личном словаре владельца — как в приложении:"]
    for item in data.get("sources") or []:
        title = str(item.get("title") or "").strip() or "Ролик без названия"
        lines.append(f"{icons.get(item.get('kind'), '📖')} {title} · {int(item.get('word_count') or 0)}")
    for group in data.get("groups") or []:
        lines.append(f"{group.get('icon')} {group.get('name')} · {int(group.get('word_count') or 0)}")
    lines.append(f"🤝 Слов из читалки без источника с 07.09.2026: {_reader_saves_without_source()}")
    return "\n".join(lines)


def _sentence_lookups_without_input_kind() -> int:
    """Сколько разборов ПРЕДЛОЖЕНИЙ с 10.09.2026 ушли к модели без пометки «предложение».
    Обещано: 0.

    Владелец 09.09.2026: «Ich weiß die Antwort nicht, ich rate ins Blaue hinein» после
    «Подробного разбора» показал крупно «угадывать без оснований, наугад» — модель сама
    решила, что перед ней выражение. Теперь форму ввода решает сервер
    (_lookup_input_kind) и сообщает модели фактом; след — metadata.input_kind у события
    dictionary_lookup (provider app_internal, одно на обращение; у строк расхода токенов
    OpenAI своё поле input_kind = fresh|cached, их не считаем). Предложение без пометки
    — значит, разбор прошёл через дверь, где сервер форму ввода не назвал (откат кода
    или новая дверь без штампа). Что модель получила поле — проверяет тест
    test_sentence_headline_stays_the_sentence.py, не этот замер."""
    from backend.backend_server import _looks_like_dictionary_sentence
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT metadata->>'word', metadata->>'input_kind'
                              FROM bt_3_billing_events
                              WHERE action_type = 'dictionary_lookup'
                                AND provider = 'app_internal' AND units_type = 'requests'
                                AND created_at >= '2026-09-10'
                                AND metadata->>'lookup_status' IN ('stream', 'enriching')""")
            rows = cursor.fetchall() or []
    return sum(1 for word, kind in rows
               if _looks_like_dictionary_sentence(word) and (kind or "") != "sentence")


def _sentence_lookups_screen() -> str:
    """Экран «после»: последние разборы многословного ввода и как их назвал сервер, плюс
    строки пула для фразы владельца — ровно то, что стоит крупно в заголовке."""
    from backend.backend_server import _lookup_input_kind
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT created_at::date, metadata->>'word', metadata->>'input_kind'
                              FROM bt_3_billing_events
                              WHERE action_type = 'dictionary_lookup'
                                AND provider = 'app_internal' AND units_type = 'requests'
                                AND created_at >= '2026-09-10'
                                AND metadata->>'lookup_status' IN ('stream', 'enriching')
                                AND metadata->>'word' LIKE '% %'
                              ORDER BY created_at DESC LIMIT 8""")
            recent = cursor.fetchall() or []
            cursor.execute("""SELECT source_text, target_text FROM bt_3_dictionary_entries
                              WHERE source_lang = 'de' AND target_lang = 'ru'
                                AND source_text_norm LIKE '%rate ins blaue%'
                              ORDER BY updated_at DESC LIMIT 4""")
            pool = cursor.fetchall() or []
    lines = ["📖 Разборы многословного ввода с 10.09 — как сервер назвал форму ввода:"]
    if not recent:
        lines.append("  (пока ни одного)")
    for day, word, kind in recent:
        lines.append(f"  {day} · {kind or '— БЕЗ ПОМЕТКИ'} · {str(word)[:70]}")
    lines.append("📌 Фраза владельца в пуле (заголовок = перевод предложения):")
    for src, tgt in pool:
        lines.append(f"  {src[:60]} → {tgt[:60]} · {_lookup_input_kind(src, 'de')}")
    lines.append(f"🤝 Разборов предложений без пометки с 10.09: {_sentence_lookups_without_input_kind()}")
    return "\n".join(lines)


# С какого момента строка пула ОБЯЗАНА быть подписана. Раньше этого времени колонки
# `translator` в базе просто не было, и требовать подпись от старых строк бессмысленно:
# накопленное подняла миграция там, где имя лежало в json, остальное так и осталось
# неизвестным — честно, числом, а не задним числом придуманным автором.
POOL_SIGNATURE_SINCE = "2026-09-11"


def _pool_rows_without_translator() -> int:
    """Новые строки общего пула без подписи «кто дал этот перевод». Обещано: 0.

    09.09.2026 из быстрого перевода убрали MyMemory (замер: 6 ошибок на 20 живых фраз
    против одной у DeepL; именно он выдал владельцу «Я даю совет наугад» вместо «Я гадаю
    наугад»). Накопленные строки перепроверены заново с судьёй, и каждая несёт поле
    translator. Появилась строка без него — значит либо чистка не доехала, либо в пул
    снова пишет путь, не называющий переводчика.

    ┌─ ПРОВЕРЕНО 10.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────┐
    │ Первая версия замера считала ЛЮБУЮ строку без разбора и без следа, и наутро    │
    │ показала 49 «нарушений». Разложение: 49 из 49 — СОХРАНЕНИЯ ЛЮДЕЙ (примеры и    │
    │ синонимы, сохранённые из карточки: «unentschlossen», «Maria ist sehr           │
    │ zielstrebig.»). У них response_json = NULL, потому что дверь сохранения кладёт │
    │ тонкую запись, и переводчика у них не было вовсе — перевод дала модель или сам │
    │ человек. Это устройство системы, а не дефект.                                  │
    │                                                                               │
    │ Заполнять им response_json ради пометки НЕЛЬЗЯ: на «response_json IS NULL»     │
    │ завязаны 17 мест в коде (миграции, поиск карточек без разбора), и подмена NULL │
    │ на объект меняет их поведение молча.                                           │
    │                                                                               │
    │ Поэтому замер сужен до строк, у которых разбор ЕСТЬ как объект (их кладёт путь │
    │ перевода), но следа переводчика нет. На 10.09.2026: 871 строка со следом,      │
    │ 0 без следа, 49 сохранений людей вне замера. Перемерить — запросом ниже.       │
    └───────────────────────────────────────────────────────────────────────────────┘

    ┌─ ДОПОЛНЕНО 10.09.2026 ВЕЧЕРОМ, решение владельца «вариант А». ─────────────────┐
    │ Разбор выше верен наполовину, и вторая половина важнее. Строки БЕЗ разбора —   │
    │ это не только сохранения людей: НОВАЯ строка от машинного переводчика ложится  │
    │ ровно такой же. Пул закрыт для разбора с 05.08.2026, и на дне записи payload   │
    │ выбрасывается целиком (`database.py`, ветка INSERT): замер 10.09 — 01.07–04.08 │
    │ пустых json 0 из 1966, 06.08–31.08 уже 2406 из 2484. Значит сужение замера     │
    │ «только строки с json» делало обещание НЕПРОВЕРЯЕМЫМ: единственные строки с    │
    │ json — те 871, что переписал скрипт 09.09, а всё новое замер не видел вовсе.   │
    │                                                                               │
    │ Поэтому подпись переехала в КОЛОНКУ `translator` (миграция там же, в           │
    │ `ensure_*_schema`), и её ставит КАЖДЫЙ писатель, называя себя: имя машинного   │
    │ переводчика (deepl_free / google_translate / azure_translator), «разбор        │
    │ модели», «обогащение», «сохранение человека», «связывание карточки». Классы    │
    │ теперь различаются в данных, а не рассуждением, и замер снова широкий.        │
    │ Накопленное поднято из json миграцией: 871 имя переводчика, 5 793 вида записи.│
    └───────────────────────────────────────────────────────────────────────────────┘
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM bt_3_dictionary_entries
                WHERE created_at >= %s
                  AND translator IS NULL;
            """, (POOL_SIGNATURE_SINCE,))
            return int((cursor.fetchone() or [0])[0] or 0)


def _pool_signature_screen() -> str:
    """Экран «после»: кто подписал строки пула за последние сутки и что легло последним.

    Владелец видит ровно то, чего не хватало 09.09: у каждой строки назван автор — либо
    машинный переводчик по имени, либо наш путь. Пустая подпись — тоже строка отчёта."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(translator, '— БЕЗ ПОДПИСИ'), COUNT(*)
                  FROM bt_3_dictionary_entries
                 WHERE created_at >= NOW() - INTERVAL '24 hours'
                 GROUP BY 1 ORDER BY 2 DESC;
            """)
            за_сутки = cursor.fetchall() or []
            cursor.execute("""
                SELECT COALESCE(translator, '— БЕЗ ПОДПИСИ'),
                       COALESCE(entry_kind, '—'),
                       left(source_text, 44)
                  FROM bt_3_dictionary_entries
                 ORDER BY created_at DESC LIMIT 6;
            """)
            последние = cursor.fetchall() or []
    строки = ["🖋 Кто завёл строки общего словаря за сутки:"]
    if за_сутки:
        for автор, сколько in за_сутки:
            строки.append(f"  {сколько:4d} · {автор}")
    else:
        строки.append("  (за сутки новых строк не было)")
    строки.append("📌 Последние строки:")
    for автор, вид, текст in последние:
        строки.append(f"  {автор} · {вид} · {текст}")
    строки.append(f"🤝 Строк без подписи с {POOL_SIGNATURE_SINCE}: "
                  f"{_pool_rows_without_translator()}")
    return "\n".join(строки)


def _expression_reference_size() -> int:
    """Сколько устойчивых выражений в справочнике. Обещано: не меньше 2900.

    Справочник (идиомы и пословицы немецкого Викисловаря) отвечает на вопрос «это
    выражение или текст», и от его ответа зависит, будет ли у идиомы из пяти слов
    крупным шрифтом буквальный машинный перевод. Пустой справочник = молчаливый возврат
    к счёту слов."""
    from backend.german_expressions import counters
    return int(counters().get("всего") or 0)


def _expression_reference_screen() -> str:
    """Экран «после»: как система называет форму ввода на живых примерах владельца."""
    from backend.backend_server import _resolve_input_kind
    from backend.german_expressions import counters, expression_of
    примеры = [
        "Ich weiß die Antwort nicht, ich rate ins Blaue hinein",
        "Haare auf den Zähnen haben",
        "die Katze aus dem Sack lassen",
        "jemanden an der Nase herumführen",
        "raten",
    ]
    подписи = {"word": "слово", "phrase": "выражение", "sentence": "предложение"}
    строки = ["📖 Как система называет форму ввода (словарь важнее счёта слов):"]
    for текст in примеры:
        вид = _resolve_input_kind(текст, "de")
        из_справочника = "из справочника" if expression_of(текст) else "по форме"
        строки.append(f"  {подписи.get(вид, вид)} · {из_справочника} · {текст[:52]}")
    ч = counters()
    строки.append(f"📚 В справочнике выражений: {ч.get('всего')} "
                  f"(идиом {ч.get('идиом')}, пословиц {ч.get('пословиц')}, "
                  f"без немецкого значения {ч.get('без_значения')})")
    строки.append(f"🤝 Строк пула без подписи автора: {_pool_rows_without_translator()}")
    return "\n".join(строки)


def _echo_translation_cards() -> int:
    """Карточек, где русский «перевод» дословно повторяет немецкое слово. Обещано: 0.

    Живой случай владельца 13.09.2026: `in Frage kommen` с «переводом» `in Frage kommen`,
    сохранено из читалки 20.08.2026. Замер той же даты: 1 запись на 27 507 — то есть не
    поток, а открытая дверь, и она была открыта в трёх местах сразу (сторожа на обеих
    дверях сохранения не было вовсе, а на экране `pickTargetTranslation` подставлял
    немецкое «лишь бы что-то было»).

    Ноль держится с двух сторон: дверь такое больше не принимает
    (`_drop_echoed_dictionary_translation`), а накопленное забирает ночной перевод
    (`queue_missing_translations` теперь видит эхо наравне с пустым полем).
    Нарушение означает либо откат деплоя, либо новую дверь мимо сторожа."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries "
                "WHERE COALESCE(word_de, '') <> '' "
                "  AND LOWER(TRIM(word_de)) = LOWER(TRIM(COALESCE(translation_ru, '')))"
            )
            return int((cursor.fetchone() or [0])[0] or 0)


# Поля правки, которые ЭКРАН умеет показать кнопкой. Список обязан совпадать с
# `optionsOf` в frontend/src/dictionary/WordIntegrityReview.jsx: ровно на их расхождении
# и сломалось — сервер слал правку, экран её молча выбрасывал.
_SCREEN_RENDERABLE_FIX_KEYS = ("to_display", "to_lemma", "to_word", "to_translation", "to_pos")


def _mywords_answers_not_shown() -> int:
    """Записей в разборе «Мои слова», где ответ у нас ЕСТЬ, а экран его не покажет. Обещано: 0.

    ┌─ ПОВОД 13.09.2026. Владелец: «вот begreifen — где эта опция?! как я могу вписать?! ─┐
    │ на хуя эта статистика тупая?!» Под словом стояло «НЕ ВОССТАНОВИЛИ», а справочник    │
    │ отвечает про `begreifen` → глагол за доли секунды, и сервер эту правку исправно     │
    │ присылал. Терял её ЭКРАН: `optionsOf` признавал вариант существующим только при     │
    │ новом СЛОВЕ или новом ПЕРЕВОДЕ, а правка «неверна пометка части речи» не несёт ни   │
    │ того ни другого. Регрессия коммита 08576136 (27.08.2026).                           │
    │                                                                                     │
    │ Замер живой базы 13.09.2026: 6 записей из 10 имели готовый ответ и НИ ОДНА не       │
    │ показывала кнопку. Тест ловит форму кода, это обещание ловит ЖИВОЙ ОСТАТОК.         │
    └─────────────────────────────────────────────────────────────────────────────────────┘

    Считаем не «сколько мы не знаем» (behest, killjoy, buzzkill — английские слова, и
    молчание немецкого справочника про них честно), а «сколько знаем и прячем». Очереди
    нет вовсе — считать нечего, это 0, а не «не измерено»."""
    from backend.database import users_with_word_issues, list_user_word_issues
    скрыто = 0
    просмотрено = 0
    for user_id, _сколько in users_with_word_issues(limit=50):
        if просмотрено >= 200:
            break
        for строка in list_user_word_issues(int(user_id), limit=30):
            просмотрено += 1
            if просмотрено > 200:
                break
            правка = строка.get("suggestion")
            if not isinstance(правка, dict) or not правка:
                continue
            if not any(str(правка.get(k) or "").strip() for k in _SCREEN_RENDERABLE_FIX_KEYS):
                скрыто += 1
    return скрыто


def _mywords_review_screen() -> str:
    """Экран владельца «после»: что он увидит на «Мои слова», строка за строкой.

    Не «тест зелёный» и не «скрипт посчитал», а ровно тот список, с которого пришла
    жалоба — с тем, что теперь стоит на кнопке у каждой записи."""
    from backend.database import users_with_word_issues, list_user_word_issues, USER_WORD_POS_RU
    строки: list[str] = ["📚 «Мои слова» — что сейчас на экране:"]
    всего = 0
    for user_id, _сколько in users_with_word_issues(limit=5):
        for строка in list_user_word_issues(int(user_id), limit=30):
            всего += 1
            правка = строка.get("suggestion") or {}
            кнопка = (
                str(правка.get("to_display") or правка.get("to_lemma")
                    or правка.get("to_word") or "").strip()
                or str(правка.get("to_translation") or "").strip()
                or (USER_WORD_POS_RU.get(str(правка.get("to_pos") or ""))
                    or str(правка.get("to_pos") or "")).strip()
            )
            строки.append(
                f"• {строка.get('word')} — {строка.get('issue_text')} → "
                + (f"кнопка «{кнопка}»" if кнопка else "кнопок правки нет, только своё/оставить/удалить")
            )
    if всего == 0:
        строки.append("• очередь пуста")
    строки.append(f"🔁 Карточек «перевод повторяет само слово»: {_echo_translation_cards()}")
    return "\n".join(строки)


def _relation_gap_builds_from_bank() -> int:
    """Слов банка, у которых в среду не собралось бы НИ ОДНОГО пропуска. Обещано: 0.

    ┌─ ПРОВЕРЕНО 14.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────┐
    │ Обещано было 3 — verwirren, bemerken, versäumen. Я считал это браком двери     │
    │ приёма: «в примерах стоит не та форма слова». Это оказалось НЕВЕРНО. Примеры   │
    │ там правильные («Plötzlich registrierte sie einen Fehler im Text»), а не       │
    │ находил их МОЙ поиск: он приписывал окончание к полному слову, тогда как       │
    │ немецкий глагол спрягается заменой «-en» (registrieren → registrierte). У всех │
    │ трёх слов синонимы — глаголы, поэтому они и давали ноль.                       │
    │ Поиск получил третий заход по справочнику спряжений, и теперь таких слов НЕТ:  │
    │ заготовок 425 → 435, «слова в предложении нет» 28 → 18, слов с нулём 3 → 0.    │
    │ Перемерить: scripts-прогон build_gap_items по всему банку, с forms_of и без.   │
    └───────────────────────────────────────────────────────────────────────────────┘

    Считаем ТЕМ ЖЕ правилом, по которому строится живое задание (build_gap_items), а
    не его пересказом: иначе обещание стережёт не то, что работает.
    """
    from backend.database import get_db_connection_context
    from backend.relation_gap import build_gap_items
    # Справочник форм подаём ТОТ ЖЕ, что и живая сборка (backend/answer_eval.
    # _gap_forms_lookup): измеритель обязан считать тем же правилом, что работает у
    # человека. Без него 14.09.2026 он показал 3 при живом 0 — мерил другое правило.
    from backend.answer_eval import _gap_forms_lookup
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT wort, accepted, trainer_json FROM bt_3_sprint_bank "
                "WHERE NOT retired AND trainer_ready;"
            )
            rows = cur.fetchall() or []
    empty = 0
    for wort, accepted, trainer_json in rows:
        full = {"wort": wort, "accepted": accepted, "trainer_json": trainer_json or {}}
        items, _skipped = build_gap_items(
            wort=str(wort or ""), accepted=accepted, trainer_json=trainer_json or {},
            forms_of=_gap_forms_lookup(full))
        if not items:
            empty += 1
    return int(empty)


def _relation_answers_broken_rows() -> int:
    """Записей ответов с исходом, которого в продукте нет. Обещано: 0.

    ┌─ ПРОВЕРЕНО 13.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────┐
    │ Первый измеритель этого обещания считал «отправки, по которым нет ни одного    │
    │ ответа», и вечером 13.09 выдал 1 — казалось, что запись сломалась. Разложил:   │
    │ из семи отправок шесть имеют ответы (2, 12, 2, 5, 6, 7 строк), а нулевая —     │
    │ ровно та, что упала с NameError и НЕ ОТКРЫЛАСЬ. То есть измеритель мерил не то:│
    │ он не отличал «ответы не записались» от «человек не ответил или не смог войти».│
    │ Отличить это с сервера нельзя вообще: закрыть задание, не ответив, — законное  │
    │ поведение, и такое обещание краснело бы от нормальной жизни.                   │
    │                                                                               │
    │ Работу самой записи держит тест (backend/tests/test_relation_answers_record).  │
    │ Здесь остаётся то, что живая база вправду доказывает: исход в каждой строке —  │
    │ один из четырёх, которые продукт умеет ставить. Чужое значение означает, что   │
    │ пишет не тот код или сломался проброс, и это настоящий дефект.                 │
    │ Доходимость до людей меряет отдельное обещание relation_gap_reaches_learners.  │
    └───────────────────────────────────────────────────────────────────────────────┘
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bt_3_relation_answers "
                "WHERE outcome NOT IN ('correct','wrong_form','other_synonym','wrong') "
                "   OR kind NOT IN ('lk','tr') OR target_word = '' OR expected = '';"
            )
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


# Дата, с которой рассылка «Подставь синоним» открыта людям: владелец включил
# RELATION_GAP_ENABLED=1 вечером 13.09.2026. До неё слоты молчали намеренно, и считать
# их пропусками нельзя — иначе обещание нарушено с рождения и перестаёт что-то значить.
_RELATION_GAP_LIVE_SINCE = "2026-09-14"


def _relation_gap_silent_days() -> int:
    """Дней, когда задание МОГЛО уйти людям, но не ушло ни одному. Обещано: 0.

    «Могло» — значит выполнены оба условия разом: позавчера тренировка отправила слово
    этого вида И его получил хоть один человек. Если при этом за день нет ни одной
    строки в bt_3_relation_gap_dispatches для не-админа, слот промолчал, и это дефект:
    либо упала сборка заготовок, либо рассылка не добралась до людей.

    Дни, когда тренировки позавчера не было, НЕ считаются: там молчание правильное —
    брать нечего, и подставлять человеку чужое слово мы отказались осознанно.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dni AS (
                    SELECT g::date AS d
                    FROM generate_series(GREATEST(%s::date, CURRENT_DATE - 14),
                                         CURRENT_DATE - 1, '1 day') g
                ),
                moglo AS (
                    -- DISTINCT обязателен: у одного дня бывает НЕСКОЛЬКО слов с той же
                    -- отметкой тренировки, и без него измеритель считал строки банка, а
                    -- не пропущенные слоты. 15.09.2026 он показал 4 там, где пропущено
                    -- было максимум 2 — сырое число ушло бы владельцу завышенным.
                    SELECT DISTINCT d.d, b.relation
                    FROM dni d
                    JOIN bt_3_sprint_bank b
                      ON b.trainer_sent_date = d.d - 2
                     AND NOT b.retired AND b.trainer_ready
                    WHERE EXISTS (
                        SELECT 1 FROM bt_3_trainer_dispatches t
                        WHERE t.sprint_id = b.sprint_id AND t.target_user_id > 0
                    )
                )
                SELECT COUNT(*) FROM moglo m
                WHERE NOT EXISTS (
                    SELECT 1 FROM bt_3_relation_gap_dispatches gd
                    WHERE gd.slot_date = m.d AND gd.relation = m.relation
                      AND gd.target_user_id > 0
                );
                """,
                (_RELATION_GAP_LIVE_SINCE,),
            )
            row = cur.fetchone()
    return int((row or [0])[0] or 0)

def _trainer_bank_thin() -> int:
    """Видов (синонимы/антонимы), у которых СВОБОДНОГО запаса меньше длины рельса.
    Обещано: 0.

    Считаем по ХУДШЕМУ из двух часов: спринт держит отдых на `last_sent_at`, тренировка
    на `trainer_last_sent_at` и берёт только собранные слова. Замер 14.09.2026: у
    синонимов свободно 12 против 1, у антонимов 35 против 6 — ночной добор смотрел
    только на левое число, видел «всё хорошо» и не заказывал ничего, пока капля каждый
    день уходила в запасной ход с кулдауном ноль.

    Порог — расход × 3 дня (длина рельса: тренировка → спринт), тем же правилом, что и
    сам добор (backend/sprint_pool_need.decide_topup), а не его пересказом.

    ВНИМАНИЕ: в первое утро после починки это обещание ЗАКОНОМЕРНО красное — у синонимов
    свободным было одно слово при пороге четыре. Добор берёт не больше шести карточек за
    ночь, поэтому запас закрывается за ночь-две, и обещание обязано позеленеть. Если оно
    красное третье утро подряд — починка не сработала, и это ровно то, что надо увидеть.
    """
    from backend.database import (measure_sprint_bank_pressure, count_available_sprint_items,
                                  count_available_trainer_items)
    from backend.sprint_pool_need import decide_topup
    thin = 0
    for relation in ("synonym", "antonym"):
        pressure = measure_sprint_bank_pressure(relation=relation, window_days=21)
        d = decide_topup(
            bank=int(pressure["bank"]), target=int(pressure["bank"]),  # банк тут не судим
            per_day=float(pressure["per_day"]),
            free_sprint=count_available_sprint_items(relation=relation, cooldown_days=21),
            free_trainer=count_available_trainer_items(relation=relation, cooldown_days=21),
            rail_span_days=3, cap_per_night=6,
        )
        if d.gap_free > 0:
            thin += 1
    return thin

def _single_vote_verdicts() -> int:
    """Вердиктов двери синонимов, вынесенных ОДНИМ голосом и не перепроверенных.
    Обещано: 0.

    ┌─ ПРОВЕРЕНО 14.09.2026. НЕ ПОДНИМАТЬ ЭТО КАК НОВУЮ НАХОДКУ. ────────────────────┐
    │ Замер: одноголосый вердикт НЕ ВОСПРОИЗВОДИТСЯ. Те же кандидаты, тот же промпт, │
    │ та же модель — и 16% отклонённых синонимов и 38% антонимов меняют решение на   │
    │ противоположное. При трёх голосах разногласий 6%.                              │
    │ Причина накопленного: 374 кандидата у 58 слов отсудили одним голосом 06.09 —   │
    │ за два дня до того, как владелец ввёл три голоса.                              │
    │ Сделано 14.09: все 374 пересужены тремя голосами. По решению владельца          │
    │ 36 вернулись в игру, 7 убраны, 331 подтверждён. Проверено поимённо на живой    │
    │ базе: 36 из 36 лежат в accepted, 7 из 7 оттуда ушли.                            │
    │                                                                                │
    │ ЧТО НЕ НАДО ДЕЛАТЬ СНОВА: менять правило судьи. Гипотезу «оно несимметрично»   │
    │ (у антонима два пути принять, у синонима один) проверили сухим прогоном обоих  │
    │ правил на одних кандидатах: прибавка −5 у синонимов и −3 у контрольных          │
    │ антонимов, то есть шум. Перекос 13% против 65% — свойство задачи, а не          │
    │ формулировки: подтвердить синонимию модели труднее, чем противоположность.      │
    │ Перемерить: SELECT по judge_voice, не похожему на три голоса/пересуд/подтвержд. │
    └────────────────────────────────────────────────────────────────────────────────┘
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bt_3_sprint_accepted_review "
                "WHERE judge_verdict IS NOT NULL "
                "  AND judge_voice NOT LIKE %s AND judge_voice NOT LIKE %s "
                "  AND judge_voice NOT LIKE %s;",
                ("%:3 %", "revote%", "confirmed%"),
            )
            row = cur.fetchone()
    return int((row or [0])[0] or 0)

def _relation_gap_screen() -> str:
    """Экран «после» для владельца: что среда реально сделала за неделю.
    Приходит САМ три утра подряд — команду вызывать не нужно."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT slot_date), COUNT(*), COUNT(DISTINCT target_user_id)
                    FROM bt_3_relation_gap_dispatches
                    WHERE sent_at > NOW() - INTERVAL '7 days';
                    """
                )
                days, sends, people = cur.fetchone() or (0, 0, 0)
                cur.execute(
                    """
                    SELECT outcome, COUNT(*) FROM bt_3_relation_answers
                    WHERE kind = 'lk' AND answered_at > NOW() - INTERVAL '7 days'
                    GROUP BY outcome ORDER BY 2 DESC;
                    """
                )
                by_outcome = cur.fetchall() or []
                cur.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT user_id) FROM bt_3_relation_answers
                    WHERE kind = 'tr' AND answered_at > NOW() - INTERVAL '7 days';
                    """
                )
                tr_rows, tr_people = cur.fetchone() or (0, 0)
    except Exception:
        logging.warning("relation_gap screen failed", exc_info=True)
        return "✏️ «Подставь синоним»: экран не собрался — смотри логи."
    names = {"correct": "вписал верно", "wrong_form": "слово то, форма не та",
             "other_synonym": "другой синоним", "wrong": "не то слово"}
    lines = [f"✏️ <b>«Подставь синоним» за 7 дней</b>",
             f"отправлено: {sends} в {days} дн., людям {people}"]
    if by_outcome:
        lines.append("ответы:")
        lines += [f"  · {names.get(str(o), str(o))}: {n}" for o, n in by_outcome]
    else:
        lines.append("ответов пока нет — никто не открывал")
    lines.append(f"тренировка (пн/вт): {tr_rows} ответов у {tr_people} чел.")
    return "\n".join(lines)

# ── анаграммы: слово существует, ходовое и написано верно (13.09.2026) ───────────────
# Условие «противоречит источнику» держится в ОДНОМ месте — в SQL ниже, и оно повторяет
# правило двери (`backend/anagram_word_gate.spelling_by_source`): слово пишется строчными,
# только если ВСЕ записи журнала о нём говорят «глагол/прилагательное/наречие» и ни в
# одной нет артикля. Артикль важнее части речи: у `das Aufstoßen` заглавная верна.
_СЛОВО_ЖУРНАЛА = ("REGEXP_REPLACE(COALESCE(q.response_json->>'word_de',''),"
                  "'^(der|die|das)\\s+','')")


def _anagram_cards_against_source() -> int:
    """Живых карточек анаграмм, где написание противоречит источнику. Обещано: 0.

    Замер 13.09.2026 до починки: 6 живых карточек (Benachteiligen, Hervorrufen,
    Nachstehen, Peinlich, Unsachlich, Vorzeitig) и ещё 28 снятых. Человек видел эту
    заглавную на экране: буквы отдаются как записаны, и слово предлагается сохранить
    в личный словарь.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM bt_3_anagram_cards c
                WHERE NOT c.retired AND c.word ~ '^[A-ZÄÖÜ]'
                  AND EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q
                              WHERE LOWER({_СЛОВО_ЖУРНАЛА}) = LOWER(c.word)
                                AND q.response_json->>'part_of_speech'
                                    IN ('verb','adjective','adverb','participle'))
                  AND NOT EXISTS (SELECT 1 FROM bt_3_webapp_dictionary_queries q
                                  WHERE LOWER({_СЛОВО_ЖУРНАЛА}) = LOWER(c.word)
                                    AND (COALESCE(q.response_json->>'word_de','')
                                             ~* '^(der|die|das)\\s+'
                                         OR q.response_json->>'part_of_speech' = 'noun'));
                """
            )
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _anagram_new_cards_below_threshold() -> int:
    """Карточек, добранных ПОСЛЕ постановки двери, чья частота ниже порога. Обещано: 0.

    Это сторож самой двери: если правило перестанет вызываться, в банк снова пойдут
    слова вроде `Inkelgasse` (0 вхождений на миллиард, ушло трём людям). Считаем только
    те слова, про которые частота у нас ЕСТЬ: «не знаем» — это не «нарушено».
    """
    from backend.database import get_db_connection_context
    from backend.rebus_word_gate import MIN_PER_BILLION
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM bt_3_anagram_cards c
                JOIN bt_3_dwds_frequency f ON f.word = c.word AND f.hits IS NOT NULL
                WHERE c.created_at > TIMESTAMPTZ '2026-09-13 20:00+00'
                  AND f.per_billion < %s;
                """,
                (float(MIN_PER_BILLION),),
            )
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _anagram_bank_screen() -> str:
    """Экран «после»: то же, что владелец видит в банке анаграмм. Приходит САМ."""
    from backend.database import get_db_connection_context
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FILTER (WHERE NOT retired),
                           COUNT(*) FILTER (WHERE NOT retired AND word ~ '^[A-ZÄÖÜ]'),
                           COUNT(*) FILTER (WHERE NOT retired
                                            AND created_at > TIMESTAMPTZ '2026-09-13 20:00+00')
                    FROM bt_3_anagram_cards;
                    """
                )
                выдаются, с_заглавной, новых = cur.fetchone() or (0, 0, 0)
    except Exception:
        logging.warning("anagram bank screen failed", exc_info=True)
        return "🔤 Анаграммы: экран не снялся — база не ответила"
    return (f"🔤 Анаграммы: выдаются {выдаются}, из них с заглавной буквы {с_заглавной}, "
            f"добрано после двери {новых}")


def _form_headwords_unfixed() -> int:
    """Карточек, где заголовком стоит написание, которое СПРАВОЧНИК признал формой другого
    слова. Обещано: 0 (13.09.2026).

    До разовой чистки таких было 118 («Blähungen» вместо «die Blähung», «wirbt» вместо
    «werben»), и они уезжали в общий словарь, отвечающий всем. Дверь закрыта в двух
    местах: кнопка «Разбор» в читалке спрашивает словарную форму (frontend App.jsx), а
    ночная проверка 03:50 (`backend/form_headword_sweep.py`) спрашивает справочник по
    новым написаниям и чинит подтверждённые. Спорные (написание с заглавной, чей строчный
    вариант — законное слово) сюда НЕ входят: их чинить нельзя, они ждут владельца."""
    from backend.form_headword_sweep import unfixed_forms_count
    return unfixed_forms_count()


def _anagram_people_running_out() -> int:
    """Людей, у кого запас непоказанных анаграмм меньше недели. Обещано: 0.

    Считает то же самое, что ночной добор (`backend.database.anagram_unseen_by_person`),
    поэтому отчёт не может разойтись с работой: если добор перестанет просыпаться, это
    число вырастет само. Замер 13.09.2026 до правки: самый бедный — 33 карточки, 16,5
    дня; ночной добор при этом спал, потому что смотрел на размер банка (51 при цели 12).
    """
    from backend.anagram_pool_plan import runway_days
    from backend.database import anagram_unseen_by_person
    люди = anagram_unseen_by_person()
    if not люди:
        return 0
    слотов = 2  # ANAGRAM_SLOT_TIMES: 12:15 и 19:15
    return sum(1 for _uid, n in люди if (runway_days(n, слотов) or 0) < 7)


def _anagram_runway_screen() -> str:
    """Экран «после»: на сколько дней заданий хватит самому бедному человеку."""
    from backend.anagram_pool_plan import MIN_RUNWAY_DAYS, runway_days
    from backend.database import anagram_unseen_by_person
    try:
        люди = anagram_unseen_by_person()
    except Exception:
        logging.warning("anagram runway screen failed", exc_info=True)
        return "🔤 Анаграммы: запас не посчитан — база не ответила"
    if not люди:
        return "🔤 Анаграммы: активных получателей нет, считать запас не для кого"
    худший_id, худший = люди[0]
    дней = runway_days(худший, 2)
    return (f"🔤 Анаграммы: самый маленький запас {худший} карточек = {дней} дней "
            f"(порог добора {MIN_RUNWAY_DAYS} дней, людей в счёте {len(люди)})")
def _listening_repeats_within_30_days() -> int:
    """Сколько раз за последние 30 дней человеку повторили текст аудирования, который он
    уже слышал меньше 30 дней назад. Обещано: 0.

    Повод 13.09.2026: владелец увидел в ленте «📞 Telefonisches Gespräch» и спросил,
    ротируется ли аудирование вообще. Померено: ротация работает, но она держится не на
    отборе, а на РАЗМЕРЕ БАНКА. Пока в банке было 7–23 текста (июнь-июль), тот же отбор
    давал 100 повторов с интервалом 2–19 дней. После добора до 75 текстов (21.08.2026)
    повторов быстрее 30 дней — ноль. Поэтому обещание меряет не «правильно ли выбирает
    запрос» (он не менялся и не сломается сам), а то единственное, что может вернуть
    тесноту: расход обогнал банк. Число поползло вверх — значит пора добирать банк.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH s AS (
                    SELECT slot_date AS d,
                           LAG(slot_date) OVER (PARTITION BY target_user_id, listening_id
                                                ORDER BY slot_date) AS prev
                    FROM bt_3_listening_dispatches
                )
                SELECT COUNT(*) FROM s
                WHERE d > CURRENT_DATE - 30 AND prev IS NOT NULL AND (d - prev) < 30
                """
            )
            return int((cursor.fetchone() or [0])[0] or 0)


def _listening_rotation_screen() -> str:
    """Экран «после»: ротируется ли аудирование — то же, что владелец спросил словами."""
    from backend.database import get_db_connection_context
    строки = ["🎧 Аудирование: ротация"]
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE NOT retired AND audio_status = 'ready'
                                        AND COALESCE(audio_object_key, '') <> ''),
                       COUNT(*) FILTER (WHERE NOT retired AND audio_status = 'ready'
                                        AND (last_sent_at IS NULL
                                             OR last_sent_at < NOW() - INTERVAL '7 days')),
                       COUNT(DISTINCT topic)
                FROM bt_3_listening_bank
                """
            )
            всего, готовых, свободных, тем = cursor.fetchone()
            строки.append(f"  банк: {всего} текстов, {готовых} готовых с озвучкой, "
                          f"{тем} тем; свободных к выдаче сейчас — {свободных}")

            cursor.execute(
                """
                SELECT COUNT(DISTINCT listening_id) FROM bt_3_listening_dispatches
                WHERE slot_date > CURRENT_DATE - 30
                """
            )
            разных_всего = int((cursor.fetchone() or [0])[0] or 0)

            # Считаем только тех, кто за месяц получил хотя бы 10 заданий: у человека,
            # пришедшего вчера, «мало разных текстов» значит «он тут недавно», а не повтор.
            cursor.execute(
                """
                SELECT MIN(разных), MAX(разных) FROM (
                    SELECT COUNT(DISTINCT listening_id) AS разных
                    FROM bt_3_listening_dispatches
                    WHERE slot_date > CURRENT_DATE - 30
                    GROUP BY target_user_id
                    HAVING COUNT(*) >= 10
                ) c
                """
            )
            мин_у_человека, макс_у_человека = cursor.fetchone()
            строки.append(f"  за 30 дней в выдачу ушло разных текстов: {разных_всего}; "
                          f"у активного человека разных текстов: "
                          f"{мин_у_человека}–{макс_у_человека}")

            cursor.execute(
                """
                WITH s AS (
                    SELECT slot_date AS d,
                           LAG(slot_date) OVER (PARTITION BY target_user_id, listening_id
                                                ORDER BY slot_date) AS prev
                    FROM bt_3_listening_dispatches
                )
                SELECT COUNT(*) FILTER (WHERE prev IS NOT NULL), MIN(d - prev)
                FROM s WHERE d > CURRENT_DATE - 30
                """
            )
            повторов, мин_интервал = cursor.fetchone()
    строки.append(f"  повторов за 30 дней: {повторов}; самый короткий интервал возврата: "
                  f"{мин_интервал if мин_интервал is not None else '—'} дн. "
                  f"(ниже 30 — банк пора добирать)")
    return "\n".join(строки)


def _dictionary_headword_case_against_source() -> int:
    """Слов словаря, записанных с заглавной вопреки источнику. Обещано: 0.

    Правило отбора НЕ своё: берётся из продукта — `database.list_headword_case_offenders`,
    тем же вызовом, которым пользуется уборка. Пока правил было два, они разошлись на
    первом прогоне (226 против 230), и разница оказалась содержательной: под ней лежали
    обрубки и неверные пометки части речи (Wehr, Verdeck, Gelass, Umgekehr).

    Замер 13.09.2026 до уборки: 226 слов. Через общую формулу показа на экран с заглавной
    не доходило ни одно, но сырое поле читают игры — так «Behaupten» попало в анаграмму.
    """
    from backend.database import list_headword_case_offenders
    from backend.german_grammar_tables import german_headword_case
    return sum(1 for слово, pos in list_headword_case_offenders()
               if german_headword_case(слово, pos) != слово)


def _pos_gender_conflicts_unattended() -> int:
    """Статей «часть речи спорит с родом», о которых владельцу НЕ сказано. Обещано: 0.

    Род в немецком бывает только у существительного, поэтому «прилагательное с родом
    der» — запись, противоречащая себе: формула показа верит пометке и не поднимает
    заглавную, а человек читает «ruhestände» вместо «der Ruhestand». Замер 13.09.2026:
    21 статья, 36 личных карточек.

    Считаем НЕ «сколько противоречий осталось»: часть из них — обрубки и опечатки,
    решать их должен владелец, и пока он не решил, противоречие законно висит. Считаем
    те, что ни починены, ни отправлены ему: вот это и есть незакрытая работа.
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM bt_3_lex_units u
                WHERE u.lang = 'de' AND u.pos IN ('verb','adjective','adverb')
                  AND u.gender IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM bt_3_card_complaints c
                                   WHERE c.unit_id = u.id AND c.status <> 'решена');
                """)
            row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _newcomers_without_a_letter() -> int:
    """Новичков старше трёх суток, оставшихся без личного письма владельца. Обещано: 0.

    Сторож самой утренней работы (bot_3._welcome_letter_job, 09:30 Вена). Перестанет
    запускаться — число вырастет само и придёт владельцу утром, а не всплывёт через
    месяц жалобой «мне никто не написал». Человек с закрытой личкой сюда НЕ попадает:
    он закрыт статусом 'undeliverable' и виден отдельной строкой отчёта."""
    from backend.database import count_welcome_letter_holes
    return count_welcome_letter_holes()


def _welcome_letter_screen() -> str:
    """Экран «после»: последние личные письма новичкам — кому, когда, чем кончилось."""
    from backend.database import list_recent_welcome_letters, welcome_letter_stats
    строки = ["💌 Последние письма новичкам:"]
    записи = list_recent_welcome_letters(limit=8)
    if not записи:
        строки.append("  (ни одного — с 14.09.2026 новичков ещё не было)")
    for з in записи:
        когда = з["sent_at"] or з["updated_at"]
        подпись = {"sent": "ушло", "undeliverable": "личка закрыта",
                   "pending": "попробуем завтра утром"}.get(з["status"], з["status"])
        хвост = f" · {str(з['last_error'])[:60]}" if з.get("last_error") and з["status"] != "sent" else ""
        имя = з["name"] or f"user_{з['user_id']}"
        строки.append(f"  {когда:%d.%m %H:%M} · {имя} · {подпись} (попыток {з['attempts']}){хвост}")
    s = welcome_letter_stats()
    строки.append(f"Итого: ушло {s['sent_total']} · ждут своего утра {s['waiting']} · "
                  f"не доставлено {s['undeliverable']}")
    return "\n".join(строки)



def _allowed_rows_not_real_people() -> int:
    """Строк в списке доступа, за которыми нет человека. Обещано: 0.

    14.09.2026 их было три (77, 777, 987654321), и каждая рассылка бота стучалась в них,
    а владелец видел это как «🚫 Не дошло». Строки убраны
    (scripts/allowed_users_drop_test_rows.py), правило «кто настоящий человек» поставлено
    в сам источник адресатов. Число ВЫРОСЛО = снова прогон кода по боевой базе записал
    себя в список доступа, и рассылки опять стучатся в пустоту."""
    from backend.database import count_allowed_rows_not_real_people
    return count_allowed_rows_not_real_people()


def _battle_targets_ignoring_choice() -> int:
    """Адресатов батла, которые выключили «Готов к батлам» или закрыли бота. Обещано: 0.

    До 14.09.2026 рассылка «всем» не смотрела кнопку вообще: включивших было 9 из 28, а
    приглашение получали все 30, причём двое выключили её явно. Считается пересечением
    ТОГО ЖЕ списка, который уходит в рассылку, с двумя признаками — то есть меряется
    результат кода, а не его намерение. Число ВЫРОСЛО = появился ещё один путь сборки
    адресатов мимо list_battle_invite_targets."""
    from backend.database import count_battle_targets_ignoring_choice
    return count_battle_targets_ignoring_choice()


def _battle_invite_targets_screen() -> str:
    """Экран владельца «после»: кому уйдёт следующее приглашение на батл и кому нет.

    Это ровно те числа, которые он увидит на подписи своей карточки, — не «тест
    зелёный», а состав рассылки на живой базе."""
    from backend.database import list_battle_invite_targets, list_bot_blocked_allowed_people
    info = list_battle_invite_targets()
    закрыли = list_bot_blocked_allowed_people()
    строки = ["⚔️ Следующий батл — состав рассылки:",
              f"📨 Получат вызов: {len(info.get('targets') or [])}",
              f"🚫 Закрыли бота: {int(info.get('blocked') or 0)}",
              f"🔕 Не готовы к батлам: {int(info.get('opted_out') or 0)}",
              f"🧪 Строк не-людей в списке доступа: {int(info.get('not_real') or 0)}"]
    if закрыли:
        имена = ", ".join(str(p["name"]) for p in закрыли[:10])
        строки.append(f"Закрыли бота: {имена}")
    return "\n".join(строки)


PROMISES: tuple[Promise, ...] = (
    Promise(
        key="allowed_rows_are_real_people",
        title="Строк в списке доступа, за которыми нет человека",
        since="14.09.2026",
        expected=0,
        measure=_allowed_rows_not_real_people,
        how="python3 scripts/allowed_users_drop_test_rows.py --dry-run — ждём 0. До "
            "14.09.2026 было 3 (77, 777, 987654321): у всех трёх getChat отвечает «Chat "
            "not found», в список они попали прогонами по боевой базе 28–30.08. Число "
            "ВЫРОСЛО = дверь мини-аппа опять впустила выдуманный id",
    ),
    Promise(
        key="battle_invites_respect_the_button",
        title="Адресатов батла, которые выключили «Готов к батлам» или закрыли бота",
        since="14.09.2026",
        expected=0,
        measure=_battle_targets_ignoring_choice,
        screen=_battle_invite_targets_screen,
        how="/admin_promises — или backend.database.count_battle_targets_ignoring_choice(). "
            "До 14.09.2026 рассылка «всем» кнопку не смотрела: включивших 9 из 28, "
            "приглашение получили 30, двое выключили её явно. Число ВЫРОСЛО = адресаты "
            "собираются мимо list_battle_invite_targets",
    ),
    Promise(
        key="pos_gender_conflicts_attended",
        title="Статей, где часть речи спорит с родом и владельцу об этом не сказано",
        since="13.09.2026",
        expected=0,
        measure=_pos_gender_conflicts_unattended,
        how="python3 scripts/lex_units_fix_pos_gender_conflict.py — ждём 0. До 13.09.2026 "
            "было 21: 2 оказались существительными (Zeitschrift, Festschreibung), у 10 "
            "снят невозможный род, 9 ушли владельцу с кнопками. Ночью в 03:50 разбор "
            "идёт сам. Число ВЫРОСЛО = ночная задача не работает или DWDS молчит неделями",
    ),
    Promise(
        key="dictionary_headword_case_from_source",
        title="Слов словаря, записанных с заглавной вопреки источнику",
        since="13.09.2026",
        expected=0,
        measure=_dictionary_headword_case_against_source,
        screen=_mywords_review_screen,
        how="python3 scripts/dict_fix_headword_case_from_source.py (сухой прогон) — "
            "ждём 0. До 13.09.2026 было 226 слов, из них 6 легли ПОСЛЕ постановки "
            "правила 19.08, потому что оно стояло у одной двери записи из нескольких. "
            "Число ВЫРОСЛО = появился ещё один вход мимо "
            "_create_or_attach_user_dictionary_entry_with_cursor",
    ),
    Promise(
        key="anagram_nobody_runs_out_of_tasks",
        title="Людей, у кого анаграммы кончатся меньше чем через неделю",
        since="13.09.2026",
        expected=0,
        measure=_anagram_people_running_out,
        screen=_anagram_runway_screen,
        how="/admin_promises — или backend.database.anagram_unseen_by_person(): у "
            "каждого активного получателя должно оставаться больше 14 непоказанных "
            "карточек (2 в день). Ночной добор сам поднимает запас до 14 дней. Число "
            "ВЫРОСЛО = добор не просыпается или дверь приёмки не пропускает слова",
    ),
    Promise(
        key="anagram_cards_spelling_from_source",
        title="Живых карточек анаграмм, где написание противоречит источнику",
        since="13.09.2026",
        expected=0,
        measure=_anagram_cards_against_source,
        screen=_anagram_bank_screen,
        how="python3 scripts/anagram_bank_liveness_audit.py — или SELECT по "
            "bt_3_anagram_cards WHERE NOT retired AND слово с заглавной, а все записи "
            "журнала о нём говорят «глагол/прилагательное/наречие» без артикля: ждём 0. "
            "До 13.09.2026 таких было 6 живых и 28 снятых (Behaupten, Peinlich, "
            "Nachstehen). Число ВЫРОСЛО = дверь приёмки перестала звать "
            "anagram_word_gate.spelling_by_source",
    ),
    Promise(
        key="anagram_new_cards_pass_frequency_gate",
        title="Новых карточек анаграмм с частотой ниже порога 300 на миллиард",
        since="13.09.2026",
        expected=0,
        measure=_anagram_new_cards_below_threshold,
        screen=_anagram_bank_screen,
        how="SELECT count(*) FROM bt_3_anagram_cards c JOIN bt_3_dwds_frequency f "
            "ON f.word=c.word WHERE c.created_at > 13.09.2026 AND f.per_billion < 300 — "
            "ждём 0. Повод: Inkelgasse 0,0 на миллиард ушло трём людям 22.08, 04.09 и "
            "13.09. Число ВЫРОСЛО = judge_anagram_word не вызывается при доборе банка",
    ),
    Promise(
        key="synonym_door_three_votes",
        title="Вердиктов двери синонимов одним голосом, без перепроверки",
        since="14.09.2026",
        expected=0,
        measure=_single_vote_verdicts,
        how="SELECT count(*) FROM bt_3_sprint_accepted_review WHERE judge_verdict IS NOT "
            "NULL AND judge_voice NOT LIKE '%:3 %' AND judge_voice NOT LIKE 'revote%' "
            "AND judge_voice NOT LIKE 'confirmed%' — ждём 0. Одноголосый вердикт не "
            "воспроизводится: на повторе меняется у 16% синонимов и 38% антонимов",
    ),
    Promise(
        key="trainer_bank_has_slack",
        title="Видов, где свободного запаса слов меньше, чем на длину рельса",
        since="14.09.2026",
        expected=0,
        measure=_trainer_bank_thin,
        screen=_relation_gap_screen,
        how="/admin_promises — или сравнить count_available_sprint_items и "
            "count_available_trainer_items по обоим видам: худшее из двух должно быть "
            "не меньше расход×3. 14.09 до починки было синонимы 12 против 1 при пороге 4. "
            "Первое утро после деплоя красное законно — добор берёт 6 карточек за ночь",
    ),
    Promise(
        key="relation_gap_reaches_learners",
        title="Дней, когда «Подставь синоним» могло уйти людям, но не ушло ни одному",
        since="13.09.2026",
        expected=0,
        measure=_relation_gap_silent_days,
        screen=_relation_gap_screen,
        how="/admin_promises — считаются только дни с 14.09.2026 и только те, где "
            "позавчера тренировка отправила слово и его получил хоть один человек. "
            "Ждём 0. Не ноль = сборка заготовок упала или рассылка не дошла",
    ),
    Promise(
        key="relation_gap_builds_from_bank",
        title="Слов банка, у которых «Подставь синоним» не собирает ни одного пропуска",
        since="13.09.2026",
        expected=0,
        measure=_relation_gap_builds_from_bank,
        screen=_relation_gap_screen,
        how="/admin_promises — или python3 -c из backend.relation_gap import build_gap_items "
            "по bt_3_sprint_bank WHERE NOT retired AND trainer_ready: слов с пустым "
            "списком заготовок ждём 0. До 14.09.2026 их было 3 — не из-за двери, а "
            "из-за поиска формы; поиск теперь ходит в справочник спряжений",
    ),
    Promise(
        key="relation_answers_sane",
        title="Записей ответов рельса с исходом, которого в продукте нет",
        since="13.09.2026",
        expected=0,
        measure=_relation_answers_broken_rows,
        screen=_relation_gap_screen,
        how="SELECT count(*) FROM bt_3_relation_answers WHERE outcome NOT IN "
            "('correct','wrong_form','other_synonym','wrong') OR kind NOT IN ('lk','tr') "
            "OR target_word='' OR expected='' — ждём 0. Сколько ответов пришло вообще "
            "видно в экране «после»; «отправок без ответа» НЕ считаем — закрыть задание, "
            "не ответив, это законное поведение (разбор 13.09.2026 в коде измерителя)",
    ),
    Promise(
        key="dictionary_echo_translations",
        title="Карточек, где русский перевод дословно повторяет немецкое слово",
        since="13.09.2026",
        expected=0,
        measure=_echo_translation_cards,
        screen=_mywords_review_screen,
        how="SELECT count(*) FROM bt_3_webapp_dictionary_queries "
            "WHERE LOWER(TRIM(word_de)) = LOWER(TRIM(translation_ru)) — ждём 0",
    ),
    Promise(
        key="mywords_answers_not_shown",
        title="Записей «Мои слова», где ответ у нас есть, а экран его не показывает",
        since="13.09.2026",
        expected=0,
        measure=_mywords_answers_not_shown,
        screen=_mywords_review_screen,
        how="/admin_promises — или открыть «Мои слова» и сверить: у записи, под которой "
            "справочник назвал часть речи, обязана стоять кнопка с этой частью речи "
            "(«begreifen» → «глагол»), а не одни «Оставить»/«Удалить»",
    ),
    Promise(
        key="db_guardrails_alive",
        title="Защиты Postgres от зависшей транзакции на месте (таймаут 60 с + лог медленных запросов)",
        since="06.09.2026",
        expected=1,
        measure=_db_guardrails_alive,
        how="SELECT name, setting FROM pg_settings WHERE name IN "
            "('idle_in_transaction_session_timeout','log_min_duration_statement') "
            "— ждём 60000 и 5000",
    ),
    Promise(
        key="db_pool_starvation_today",
        title="Запросов, не дождавшихся соединения к базе, за сегодня",
        since="06.09.2026",
        expected=0,
        measure=_db_pool_starvation_today,
        how="SELECT day, service, hits FROM bt_3_capacity_daily "
            "WHERE kind='db_pool' AND day=CURRENT_DATE",
    ),
    Promise(
        key="old_bank_quarantine_traces",
        title="Следов карантина в старом банке словаря",
        since="04.09.2026",
        expected=0,
        measure=_old_bank_quarantine_traces,
        how="python3 scripts/pool_quarantine_drop_ghost_marks.py (сухой прогон печатает число)",
    ),
    Promise(
        key="night_enrichment_units_mode",
        title="Ночной добор идёт по слою слов, а не по старому банку",
        since="04.09.2026",
        expected=1,
        measure=_night_enrichment_runs_in_units_mode,
        how="SELECT metadata->>'mode' FROM bt_3_scheduler_run_guards "
            "WHERE job_key='pool_night_enrichment' — ждём units",
    ),
    Promise(
        key="sprint_result_words_get_the_screen",
        title="Итоги спринта: список слов занимает экран, а не две строки под шапкой",
        since="14.09.2026",
        expected=0,
        measure=_sprint_result_header_not_compact,
        screen=_sprint_result_header_screen,
        how="открыть $WEB_APP_URL, скачать подключённые .css; в них должны стоять все "
            "четыре правила '.ans-card:has(>.as-result-list) …' (as-cert / as-cert-medal / "
            "sp-rank / as-save-hint) — ждём 0 пропавших",
    ),
    Promise(
        key="worldnews_card_old_look",
        title="Карточка слова «Новость дня» собрана в новом виде (без Georgia и шевронов)",
        since="04.09.2026",
        expected=0,
        measure=_worldnews_card_old_look_rules,
        how="открыть $WEB_APP_URL, скачать подключённые .css; grep -c 'Georgia' рядом с "
            ".worldnews-card-de и 'clip-path' рядом с .worldnews-step — ждём 0 и 0",
    ),
    Promise(
        key="worldnews_own_variant_sheet",
        title="Своя версия слова в «Новости дня» правится в окне поверх экрана, а не в срезанной карточке",
        since="09.09.2026",
        expected=0,
        measure=_worldnews_own_sheet_defects,
        screen=_worldnews_own_sheet_screen,
        how="открыть $WEB_APP_URL, скачать подключённые .css; в них должно быть правило "
            ".worldnews-own-overlay с position: fixed и НЕ должно быть "
            ".worldnews-card-own-input — ждём 0 дефектов. Глазами: утренняя новость → "
            "«Слова» → «Сохранить по-своему» → окно поверх экрана, обе строки видны целиком",
    ),
    Promise(
        key="daily_video_night_recheck_off",
        title="Ночная перепроверка карточек «Новости дня» (03:20) не запускалась после снятия",
        since="05.09.2026",
        expected=0,
        measure=_daily_video_night_recheck_ran,
        how="SELECT finished_at FROM bt_3_scheduler_run_guards WHERE job_key="
            "'daily_video_recheck_result' ORDER BY finished_at DESC LIMIT 1 — ждём дату "
            "не позже 05.09.2026 12:00 UTC",
    ),
    Promise(
        key="access_period_night_sweep",
        title="Людей, кому начало бесплатного месяца поставила ночная страховка, а не дверь",
        since="04.09.2026",
        expected=0,
        measure=_access_period_night_sweep,
        how="SELECT COUNT(*) FROM bt_3_access_period WHERE source='night_sweep' "
            "AND created_at > NOW() - interval '1 day'",
    ),
    Promise(
        key="access_period_missing",
        title="Известных людей без начала отсчёта бесплатного месяца",
        since="04.09.2026",
        expected=0,
        measure=_access_period_missing,
        how="python3 -c \"from backend.database import list_known_user_ids_without_access_period as f; print(len(f()))\"",
    ),
    Promise(
        key="locked_users_got_learning_content",
        title="Заданий, ушедших запертым (бесплатный месяц кончился, подписки нет) за сутки",
        since="04.09.2026",
        expected=0,
        measure=_locked_users_got_learning_content,
        how="python3 -c \"from backend.database import count_learning_content_sent_to_locked_users as f; print(f())\"",
    ),
    Promise(
        key="access_reminders_over_cadence",
        title="Напоминаний запертым, ушедших чаще положенного (7 дней ×8, потом 30)",
        since="04.09.2026",
        expected=0,
        measure=_access_reminders_over_cadence,
        how="python3 -c \"from backend.database import count_access_reminders_over_cadence as f; print(f())\"",
    ),
    Promise(
        key="audit_accusation_without_variant",
        title="Карточек «слово сохранилось не целиком» без готового варианта",
        since="04.09.2026",
        expected=0,
        measure=_accusations_without_a_variant,
        how="открыть экран проверки слов (startapp=woerter): под каждой надписью про "
            "«не целиком» обязана стоять кнопка «Да, это «…»»",
    ),
    Promise(
        key="audit_real_words_not_asked",
        title="Настоящих слов, которыми экран проверки зря тревожит человека",
        since="04.09.2026",
        expected=0,
        measure=_real_words_still_bothering_people,
        how="открыть экран проверки слов (startapp=woerter): надписи «слово настоящее, "
            "просто редкое — справочник его не знает» там больше нет ни одной. В базе "
            "эти слова остаются, уходят они только с экрана",
    ),
    Promise(
        key="gate_keeps_the_spelling_it_got",
        title="Новых вердиктов «модель предложила другое написание» без варианта",
        since="04.09.2026",
        expected=0,
        measure=_lost_spellings_from_the_gate,
        how="SELECT w.asked FROM bt_3_word_check w LEFT JOIN bt_3_word_suggestion s "
            "ON s.asked=w.asked WHERE w.source LIKE 'модель предложила другое%' "
            "AND w.checked_at >= '2026-09-05' AND COALESCE(s.suggestion,'')=''",
    ),
    Promise(
        key="weekly_ranking_no_place_for_zero",
        title="Строк недельного рейтинга с местом у нуля или не по баллу (1, 2, 2, 4)",
        since="06.09.2026",
        expected=0,
        measure=_weekly_ranking_places_given_wrongly,
        how="SELECT COUNT(*) FROM bt_3_weekly_global_ranking_snapshots WHERE final_score<=0 "
            "AND rank IS NOT NULL — ждём 0; места занимавшихся сверить с RANK() OVER "
            "(PARTITION BY week_start ORDER BY final_score DESC)",
    ),
    Promise(
        key="word_pick_door_writes_every_tap",
        title="Тапов по слову в интерактивах за вчера (по следу двери, с 05.09 14:22) без отбора на сегодня",
        since="05.09.2026",
        expected=0,
        measure=_word_pick_door_misses,
        how="python3 -c \"from backend.database import count_word_pick_door_misses as f; print(f())\" "
            "— тапы bt_3_word_pick_taps за вчера без строки bt_3_word_picks на сегодня",
    ),
    Promise(
        key="standup_pool_snapshot_fresh",
        title="Снимков пула стендапа старше трёх дней (по нему отчёт считает запас)",
        since="06.09.2026",
        expected=0,
        measure=_standup_pool_snapshot_stale,
        how="/standup_pool в боте: дата в строке «Каналы смотрели …» не старше трёх дней; "
            "в базе — SELECT updated_at FROM bt_3_daily_video_pool_snapshot WHERE rubric='standup'",
        screen=_standup_pool_screen,
    ),
    Promise(
        key="daily_video_sent_without_shown_mark",
        title="Выпусков «Новость/Стендап дня», ушедших людям без пометки «показано» (с 07.09)",
        since="06.09.2026",
        expected=0,
        measure=_sent_without_shown_mark,
        how="SELECT news_date, video_id FROM bt_3_world_news_daily d WHERE status='sent' AND "
            "news_date >= '2026-09-07' AND NOT EXISTS (SELECT 1 FROM bt_3_daily_video_shown s "
            "WHERE s.video_id = d.video_id)",
    ),
    Promise(
        key="standup_shelf_holds_no_draft",
        title="Роликов, положенных на полку стендапа уже после того, как их занял выпуск",
        since="06.09.2026",
        expected=0,
        measure=_shelf_holds_a_draft,
        how="SELECT sh.video_id FROM bt_3_standup_shelf sh JOIN bt_3_world_news_daily d "
            "ON d.video_id = sh.video_id WHERE sh.added_at > d.created_at",
    ),
    Promise(
        key="daily_video_control_calls_bounded",
        title="Выпусков «Новость/Стендап дня», где контроль карточек сделал больше обращений, чем 2 + число сомнений (с 07.09)",
        since="06.09.2026",
        expected=0,
        measure=_control_extra_calls,
        how="SELECT news_date, judge_report->>'calls', judge_report->>'doubted' FROM "
            "bt_3_world_news_daily WHERE news_date >= '2026-09-07' AND (judge_report->>'calls')::int "
            "> 2 + COALESCE((judge_report->>'doubted')::int, 0)",
    ),
    Promise(
        key="word_pick_two_posters_per_picker",
        title="Людей с отбором на вчера, кому пришло меньше двух постеров «Слова со вчерашних тренировок»",
        since="05.09.2026",
        expected=0,
        measure=_word_pick_posters_missing,
        how="python3 -c \"from backend.database import count_word_pick_posters_missing as f; print(f())\"",
    ),
    Promise(
        key="personal_questions_on_unseen_translation",
        title="Личных вопросов автору, вынесенных по переводу, которого он не видит",
        since="06.09.2026",
        expected=0,
        measure=_personal_questions_on_unseen_translation,
        how="python3 -c \"from backend.phrase_panel import "
            "count_personal_questions_on_unseen_translation as f; print(f())\"",
    ),
    Promise(
        key="sprint_accepted_duplicates",
        title="Слов спринта с повтором синонима (в списке или в примерах) или самим словом в списке",
        since="06.09.2026",
        expected=0,
        measure=_sprint_accepted_duplicates_or_self,
        how="python3 scripts/sprint_bank_hygiene.py --dry-run (печатает, что дверь сняла бы; 0 строк «ИЗМЕНИТСЯ»)",
    ),
    Promise(
        key="sprint_accepted_article_mismatch",
        title="Синонимов-существительных с артиклем не по справочнику рода",
        since="06.09.2026",
        expected=0,
        measure=_sprint_accepted_article_mismatch,
        how="python3 -c \"from backend.fix_promises import _sprint_accepted_article_mismatch as f; print(f())\"",
    ),
    Promise(
        key="champion_no_place_for_zero",
        title="Игроков суточного рейтинга (чемпион дня / мини-апп) с 0 очков и местом",
        since="09.09.2026",
        expected=0,
        measure=_champion_zero_with_place,
        how="python3 -c \"from backend.quiz_leaderboard import get_quiz_leaderboard as g; "
            "print([(l['name'], l['points'], l['rank']) for l in g(1)['leaders']])\" — у points=0 rank None",
        screen=_champion_day_screen,
    ),
    Promise(
        key="sprint_review_open_stale",
        title="Кандидатов-синонимов без итогового решения судьи старше двух суток (никто никого не ждёт)",
        since="08.09.2026",
        expected=0,
        measure=_sprint_review_open_stale,
        how="SELECT COUNT(*) FROM bt_3_sprint_accepted_review WHERE status='open' "
            "AND created_at < NOW() - interval '2 days'",
        screen=_sprint_gate_screen,
    ),
    Promise(
        key="sprint_accepted_no_dictionary",
        title="Ответов в показе спринта/тренажёра, которых нет ни в Wiktionary, ни в OpenThesaurus",
        since="08.09.2026",
        expected=0,
        measure=_sprint_accepted_no_dictionary,
        how="python3 -c \"from backend.fix_promises import _sprint_accepted_no_dictionary as f; print(f())\" "
            "— однословные ответы не снятых слов: bt_3_wiktionary_synonyms.missing И нет в "
            "bt_3_openthesaurus_synsets, кроме оставленных владельцем кнопкой",
    ),
    Promise(
        key="sprint_bank_unchecked",
        title="Записей банка спринта, попавших мимо двери приёма",
        since="06.09.2026",
        expected=0,
        measure=_sprint_bank_unchecked,
        how="SELECT COUNT(*) FROM bt_3_sprint_bank WHERE accepted_checked_at IS NULL",
    ),
    Promise(
        key="hint_modal_own_box_sizing",
        title="Окно «Подсказка по слову» в интерактиве без собственного сброса box-sizing "
              "(кнопка «Понятно» под краем экрана)",
        since="06.09.2026",
        expected=0,
        measure=_hint_modal_box_sizing_missing,
        how="curl -s $WEB_APP_URL → найти .css из <link> и из строк входного скрипта → "
            "grep '.word-hint-overlay \\*' с box-sizing:border-box; руками — открыть слово "
            "в интерактиве «Слова со вчерашних тренировок», лампочка → кнопка «Понятно» "
            "целиком на экране",
        screen=_hint_modal_screen,
    ),
    Promise(
        key="review_screens_carry_save_chip",
        title="Экранов «Работа над ошибками» (Артикли, Wo-Fragen) без дискетки сохранения",
        since="10.09.2026",
        expected=0,
        measure=_review_screens_without_save_chip,
        how="python3 -c \"from backend.fix_promises import _review_screens_without_save_chip as f; print(f())\" "
            "— скачивает кусок AnswerOverlay живого сайта и ищет в нём источники "
            "artikel_review_save и wofrage_review_save. Руками — бот, «Работа над ошибками» → "
            "Артикли: ответить на слово, в правом верхнем углу блока со словом появляется 💾; "
            "нажать — плашка «в словаре · завтра повторим»",
        screen=_review_save_chip_screen,
    ),
    Promise(
        key="reader_saves_without_source",
        title="Слов из читалки (текст ролика, книга, статья), сохранённых без источника",
        since="07.09.2026",
        expected=0,
        measure=_reader_saves_without_source,
        how="SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries WHERE origin_process='reader' "
            "AND source_id IS NULL AND created_at >= '2026-09-07'; руками — открыть книгу "
            "«Текст видео», сохранить слово, в словаре «Откуда» найти его под названием ролика",
        screen=_reader_sources_screen,
    ),
    Promise(
        key="sentence_lookups_carry_input_kind",
        title="Разбор предложения уходит к модели с пометкой «предложение», заголовок остаётся переводом предложения",
        since="10.09.2026",
        expected=0,
        measure=_sentence_lookups_without_input_kind,
        how="SELECT metadata->>'word', metadata->>'input_kind' FROM bt_3_billing_events WHERE "
            "action_type='dictionary_lookup' AND provider='app_internal' AND units_type='requests' AND "
            "created_at >= '2026-09-10' AND metadata->>'lookup_status' IN ('stream','enriching'); "
            "предложения (5+ слов или 3+ со знаком конца) без input_kind='sentence'. "
            "Руками — быстрый словарь, «Ich weiß die Antwort nicht, ich rate ins Blaue hinein», «Подробный разбор»: "
            "крупно перевод предложения, ниже блок «Выражение в предложении»",
        screen=_sentence_lookups_screen,
    ),
    Promise(
        key="pool_rows_carry_their_translator",
        title="Новых строк общего словаря без подписи «кто дал перевод» не появляется",
        # Дата та же, с которой замер спрашивает подпись (POOL_SIGNATURE_SINCE): колонка
        # появилась вечером 10.09.2026, требовать её от строк того же дня нечестно.
        since="11.09.2026",
        expected=0,
        measure=_pool_rows_without_translator,
        how="SELECT COALESCE(translator,'— БЕЗ ПОДПИСИ'), count(*) FROM bt_3_dictionary_entries "
            "WHERE created_at >= '2026-09-11' GROUP BY 1 ORDER BY 2 DESC; подпись — это КОЛОНКА "
            "translator, а не поле внутри response_json: внутри json она терялась целиком, потому "
            "что пул закрыт для разбора и payload на дне записи выбрасывается. Руками — перевести "
            "фразу в быстром словаре и посмотреть строку пула: в translator должно стоять имя "
            "переводчика (deepl_free / google_translate / azure_translator); у сохранения из "
            "карточки там «сохранение человека», у разбора — «разбор модели»",
        screen=_pool_signature_screen,
    ),
    Promise(
        key="expression_reference_alive",
        title="Справочник устойчивых выражений на месте (идиомы и пословицы, ≥2900)",
        since="10.09.2026",
        expected=2900,
        measure=lambda: min(_expression_reference_size(), 2900),
        how="SELECT count(*) FROM bt_3_german_expressions; руками — быстрый словарь, "
            "«Haare auf den Zähnen haben»: заголовок не должен быть «иметь волосы на зубах», "
            "а разбор должен прийти как про выражение, а не про предложение",
        screen=_expression_reference_screen,
    ),
    Promise(
        key="listening_no_repeat_within_30_days",
        title="Один и тот же текст аудирования не возвращается человеку быстрее чем через 30 дней",
        since="13.09.2026",
        expected=0,
        measure=_listening_repeats_within_30_days,
        how="railway run -s Postgres bash -c 'DATABASE_URL=\"$DATABASE_PUBLIC_URL\" psql -c \""
            "WITH s AS (SELECT target_user_id u, listening_id l, slot_date d, "
            "LAG(slot_date) OVER (PARTITION BY target_user_id, listening_id ORDER BY slot_date) prev "
            "FROM bt_3_listening_dispatches) SELECT COUNT(*) FILTER (WHERE prev IS NOT NULL), "
            "MIN(d-prev) FROM s WHERE d > CURRENT_DATE - 30\"'; лечится это ДОБОРОМ БАНКА "
            "(/admin_ls_pool), а не правкой отбора: отбор берёт самый давно не показанный текст, "
            "и повтор значит, что показывать больше нечего",
        screen=_listening_rotation_screen,
    ),
    Promise(
        key="form_headwords_unfixed",
        title="Карточек, где заголовок — форма слова, а не словарное слово",
        since="13.09.2026",
        expected=0,
        measure=_form_headwords_unfixed,
        how="python3 -c \"from backend.form_headword_sweep import unfixed_forms_count as f; print(f())\"",
    ),
    Promise(
        key="welcome_letter_reaches_newcomers",
        title="Новичков без личного письма владельца (старше трёх суток) не осталось",
        since="14.09.2026",
        expected=0,
        measure=_newcomers_without_a_letter,
        how="SELECT p.user_id FROM bt_3_access_period p LEFT JOIN bt_3_welcome_letters w "
            "ON w.user_id = p.user_id WHERE p.started_at >= '2026-09-14' "
            "AND p.started_at < NOW() - interval '3 days' "
            "AND COALESCE(w.status,'pending') = 'pending'; руками — команда /welcome_letter: "
            "письмо придёт тебе ровно таким, каким его видит новичок",
        screen=_welcome_letter_screen,
    ),
)


def after_screens(*, promises: tuple[Promise, ...] | None = None, muted: set[str] | None = None,
                  today=None) -> list[dict]:
    """Экраны «после» свежих починок — для утренней рассылки владельцу.

    Свежая — та, чьё обещание дано не раньше SCREEN_DAYS дней назад и у которой есть экран.
    Экран не собрался — это говорится словами в том же письме, а не пропускается молча:
    молчащий экран неотличим от «всё хорошо»."""
    from datetime import date, datetime
    реестр = PROMISES if promises is None else promises
    снятые = muted_keys() if muted is None else muted
    сегодня = today or date.today()
    экраны: list[dict] = []
    for p in реестр:
        if p.screen is None or p.key in снятые:
            continue
        с = datetime.strptime(p.since, "%d.%m.%Y").date()
        возраст = (сегодня - с).days
        if возраст < 0 or возраст >= SCREEN_DAYS:
            continue
        запись = {"key": p.key, "title": p.title, "since": p.since, "day": возраст + 1,
                  "text": None, "error": ""}
        try:
            запись["text"] = str(p.screen())
        except Exception as exc:
            logging.warning("экран «после» для %s не собрался: %s", p.key, exc, exc_info=True)
            запись["error"] = str(exc)[:200] or exc.__class__.__name__
        экраны.append(запись)
    return экраны


def screen_message(screen: dict) -> str:
    """Письмо с экраном «после»: заголовок, сам экран как есть, либо честное «не собрался»."""
    шапка = (f"📸 <b>Экран после починки</b> (день {screen['day']} из {SCREEN_DAYS}) — "
             f"{_esc(screen['title'])}, обещание от {_esc(screen['since'])}\n\n")
    if screen.get("text") is None:
        return шапка + f"❓ Экран не собрался: {_esc(screen['error'])}"
    return шапка + screen["text"]


def by_key(key: str) -> Promise | None:
    for p in PROMISES:
        if p.key == key:
            return p
    return None


# ── хранилище: журнал проверок и решения владельца ────────────────────────────────────

def _ensure_tables(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bt_3_fix_promise_checks (
            id          BIGSERIAL PRIMARY KEY,
            promise_key TEXT NOT NULL,
            checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            value       INTEGER,
            status      TEXT NOT NULL,          -- held | broken | unmeasured
            error       TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bt_3_fix_promise_checks_key
            ON bt_3_fix_promise_checks (promise_key, checked_at DESC);
        CREATE TABLE IF NOT EXISTS bt_3_fix_promise_state (
            promise_key TEXT PRIMARY KEY,
            muted_at    TIMESTAMPTZ,
            muted_by    BIGINT
        );
        """
    )


def muted_keys() -> set[str]:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute("SELECT promise_key FROM bt_3_fix_promise_state WHERE muted_at IS NOT NULL")
            return {str(r[0]) for r in (cursor.fetchall() or [])}


def mute(key: str, admin_id: int) -> bool:
    """Решение владельца: обещание снято. Остаётся след — кто и когда."""
    if not by_key(key):
        return False
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute(
                """
                INSERT INTO bt_3_fix_promise_state (promise_key, muted_at, muted_by)
                VALUES (%s, NOW(), %s)
                ON CONFLICT (promise_key) DO UPDATE SET muted_at = NOW(), muted_by = EXCLUDED.muted_by
                """,
                (key, int(admin_id)),
            )
        conn.commit()
    return True


def unmute(key: str) -> bool:
    if not by_key(key):
        return False
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            cursor.execute("DELETE FROM bt_3_fix_promise_state WHERE promise_key = %s", (key,))
        conn.commit()
    return True


def _record(results: list[dict]) -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            _ensure_tables(cursor)
            for r in results:
                cursor.execute(
                    "INSERT INTO bt_3_fix_promise_checks (promise_key, value, status, error) "
                    "VALUES (%s, %s, %s, %s)",
                    (r["key"], r.get("value"), r["status"], (r.get("error") or "")[:300] or None),
                )
        conn.commit()


# ── проверка ──────────────────────────────────────────────────────────────────────────

def check_all(*, record: bool = True, promises: tuple[Promise, ...] | None = None,
              muted: set[str] | None = None) -> list[dict]:
    """Прогнать все обещания. Снятые владельцем не измеряются и не входят в итог.

    Исключение внутри измерителя — исход «не измерено», а не падение всей проверки:
    одно сломанное обещание не имеет права спрятать остальные."""
    реестр = PROMISES if promises is None else promises
    снятые = muted_keys() if muted is None else muted
    итог: list[dict] = []
    for p in реестр:
        if p.key in снятые:
            continue
        запись = {"key": p.key, "title": p.title, "since": p.since,
                  "expected": p.expected, "how": p.how, "value": None, "error": ""}
        try:
            значение = int(p.measure())
            запись["value"] = значение
            запись["status"] = HELD if значение == p.expected else BROKEN
        except Exception as exc:  # исход «не измерено» — отдельный, см. шапку модуля
            logging.warning("обещание %s не измерено: %s", p.key, exc, exc_info=True)
            запись["status"] = UNMEASURED
            запись["error"] = str(exc)[:200] or exc.__class__.__name__
        итог.append(запись)
    if record and итог:
        try:
            _record(итог)
        except Exception:
            # Журнал — не сама проверка: не записалось — сказали в лог, отчёт всё равно уйдёт.
            logging.exception("журнал обещаний не записался")
    return итог


def _esc(text: str) -> str:
    from html import escape
    return escape(str(text or ""))


def _plural(n: int, one: str, few: str, many: str) -> str:
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def report_lines(results: list[dict]) -> list[str]:
    """Строки для утреннего отчёта. Первая — итог одним взглядом, дальше только то,
    что требует человека: нарушенное и не измеренное. Держащееся не перечисляется."""
    if not results:
        return ["🤝 Обещаний в реестре нет."]
    держится = [r for r in results if r["status"] == HELD]
    нарушено = [r for r in results if r["status"] == BROKEN]
    не_измерено = [r for r in results if r["status"] == UNMEASURED]
    части = []
    if держится:
        n = len(держится)
        части.append(f"<b>{n}</b> {_plural(n, 'держится', 'держатся', 'держатся')}")
    if нарушено:
        части.append(f"<b>{len(нарушено)}</b> нарушено")
    if не_измерено:
        части.append(f"<b>{len(не_измерено)}</b> не измерено")
    строки = ["🤝 Обещания: " + ", ".join(части) + ("." if not (нарушено or не_измерено) else ":")]
    for r in нарушено:
        строки.append(
            f"   ⛔ {_esc(r['title'])}: обещано <b>{r['expected']}</b>, сейчас <b>{r['value']}</b> "
            f"(обещание от {_esc(r['since'])})"
        )
    for r in не_измерено:
        строки.append(f"   ❓ {_esc(r['title'])}: проверка не отработала — {_esc(r['error'])}")
    return строки


def full_lines(results: list[dict], muted: set[str]) -> list[str]:
    """Полный список для команды /admin_promises: каждое обещание, число, как перемерить."""
    значок = {HELD: "✅", BROKEN: "⛔", UNMEASURED: "❓"}
    строки = ["🤝 <b>Реестр обещаний</b>", ""]
    for r in results:
        сейчас = r["value"] if r["value"] is not None else "—"
        строки.append(f"{значок[r['status']]} <b>{_esc(r['title'])}</b> — обещано {r['expected']}, "
                      f"сейчас {сейчас} (с {_esc(r['since'])})")
        if r["status"] == UNMEASURED:
            строки.append(f"   не отработало: {_esc(r['error'])}")
        строки.append(f"   перемерить: <code>{_esc(r['how'])}</code>")
    for p in PROMISES:
        if p.key in muted:
            строки.append(f"🔕 <b>{_esc(p.title)}</b> — снято владельцем, не проверяется")
    if not results and not muted:
        строки.append("Реестр пуст.")
    return строки



def broken_alert(r: dict) -> tuple[str, dict]:
    """Письмо владельцу об одном нарушенном или не измеренном обещании — с кнопками.

    Кнопок две: снять обещание (решение: больше не следим) и держать дальше (придёт снова
    завтра). Без нажатия — как «держать»: молчание не снимает обещание."""
    if r["status"] == BROKEN:
        text = (
            f"⛔ <b>Обещание нарушено</b>\n\n"
            f"{_esc(r['title'])}: обещано <b>{r['expected']}</b>, сейчас <b>{r['value']}</b>.\n"
            f"Обещание от {_esc(r['since'])}. Починка либо откатилась, либо не работала.\n\n"
            f"Перемерить: <code>{_esc(r['how'])}</code>\n\n"
            f"<i>Ничего не нажать — тоже ответ: обещание остаётся, завтра проверю снова.</i>"
        )
    else:
        text = (
            f"❓ <b>Обещание не удалось проверить</b>\n\n"
            f"{_esc(r['title'])}: проверка не отработала — {_esc(r['error'])}.\n"
            f"Это не «держится» и не «нарушено», это отдельный исход: проверку надо чинить.\n\n"
            f"<i>Ничего не нажать — тоже ответ: завтра проверю снова.</i>"
        )
    markup = {"inline_keyboard": [[
        {"text": "👀 Держать дальше", "callback_data": f"fp:keep:{r['key']}"},
        {"text": "🔕 Снять обещание", "callback_data": f"fp:mute:{r['key']}"},
    ]]}
    return text, markup
