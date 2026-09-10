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


def _pool_rows_without_translator() -> int:
    """Строки общего пула ОТ ПЕРЕВОДЧИКА без следа «кто перевёл». Обещано: 0.

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
    """
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM bt_3_dictionary_entries
                WHERE source_lang = 'de' AND target_lang = 'ru'
                  AND created_at >= '2026-09-01'
                  AND source_text LIKE '%% %%'
                  AND response_json IS NOT NULL
                  AND NOT (response_json ? 'meanings' OR response_json ? 'translations')
                  AND NOT (response_json ? 'translator');
            """)
            return int((cursor.fetchone() or [0])[0] or 0)


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
    строки.append(f"🤝 Строк пула без следа переводчика: {_pool_rows_without_translator()}")
    return "\n".join(строки)


PROMISES: tuple[Promise, ...] = (
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
        title="Машинных строк пула без следа переводчика (кто перевёл) не осталось",
        since="10.09.2026",
        expected=0,
        measure=_pool_rows_without_translator,
        how="SELECT COUNT(*) FROM bt_3_dictionary_entries WHERE source_lang='de' AND target_lang='ru' "
            "AND created_at >= '2026-09-01' AND source_text LIKE '%% %%' AND response_json IS NOT NULL "
            "AND NOT (response_json ? 'meanings' OR response_json ? 'translations') AND NOT "
            "(response_json ? 'translator'); сохранения людей (response_json IS NULL) сюда НЕ входят — "
            "у них переводчика не было. Руками: перевести фразу в быстром словаре и посмотреть строку "
            "пула — у неё должно быть поле translator",
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
