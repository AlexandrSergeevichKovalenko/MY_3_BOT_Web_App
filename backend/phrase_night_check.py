"""Ночная проверка грамматики фраз общего словаря.

Зачем. Артикль у слова сверяется со справочником бесплатно, а у фразы справочника нет:
«der Titanic rammen ein Eisberg und beginnen zu sinken» ни в одном словаре не лежит.
Грамматику предложения может рассудить только язык, а это деньги — значит партиями,
с потолком и с отчётом.

ОДИН ВЗГЛЯД ОДНОЙ МОДЕЛИ. Решение владельца 04.09.2026: «мне достаточно чтобы один раз
модель посмотрела и всё… это не судьи, это хаотическое распределение случайности».

┌─ ЗДЕСЬ БЫЛИ ДВА СУДЬИ. ЧТО ИМЕННО УБРАНО И ПОЧЕМУ. ─────────────────────────────┐
│ Один и тот же вопрос задавался ДВАЖДЫ, и молча правилось только то, где оба      │
│ ответа совпали дословно. Согласие двух ответов служило страховкой для записи в   │
│ базу без спроса — и эта страховка УЖЕ доказала, что не работает: 19.08.2026 оба  │
│ судьи дословно предложили «in den Taschen» (неверный падеж и другое число), и    │
│ спасла не их сверка, а отдельная проверка самого исправленного текста.           │
│                                                                                 │
│ Замер 04.09.2026 на соседнем механизме (панель, 60 живых карточек): на вопросе   │
│ «правильно ли это?» два голоса дали 31 претензию, настоящей была ОДНА. Дело в    │
│ вопросе, а не в числе голосов: оценка бездонна, улучшить можно любую фразу.      │
│ Закрытый вопрос одной модели дал на тех же карточках одну претензию — ту самую.  │
│                                                                                 │
│ Что теперь держит молчаливую правку вместо согласия двух ответов:                │
│   • вопрос закрытый — только грамматика и понятность, стиль и «лучше бы сказать» │
│     запрещены прямым текстом;                                                    │
│   • модель ОБЯЗАНА процитировать неверный кусок, и цитату мы проверяем своей     │
│     арифметикой, без запросов: нет её в тексте — находки нет;                    │
│   • исправленный текст проходит свою проверку (`_check_fix_once`), а спор с ней  │
│     снимает ПЕЧАТНЫЙ справочник, а не второе мнение;                             │
│   • категория ошибки — из числа тех, что верны или неверны сами по себе.         │
└─────────────────────────────────────────────────────────────────────────────────┘

Молча правим только ошибки, которые не зависят от контекста: опечатка, согласование,
падеж, предлог. ПОРЯДОК СЛОВ — никогда: кусок, вырванный из предложения, законно
выглядит переставленным, и мы не знаем, откуда человек его взял. Такие всегда идут
владельцу. Решение владельца 06.08.2026; если их окажется слишком много для ручного
разбора, правило пересмотрим по числам, а не на ощупь.
"""
from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

from backend.database import (
    count_open_phrase_reviews,
    count_phrases_left_for_grammar_check,
    mark_phrase_checked,
    pick_phrases_for_grammar_check,
    queue_phrase_for_review,
)

# Ошибки, которые верны или неверны сами по себе, независимо от контекста.
SILENT_CATEGORIES = {"rechtschreibung", "kongruenz", "kasus", "praeposition"}
NIGHT_CAP = int(os.getenv("PHRASE_NIGHT_CHECK_CAP", "500") or "500")
WORKERS = int(os.getenv("PHRASE_NIGHT_CHECK_WORKERS", "6") or "6")


def _check_own_fixes(judge: dict, text: str, translation: str) -> dict:
    """Проверить правки судьи ЕГО ЖЕ уровня качества — до того, как их кто-то увидит.

    Судья ошибается двумя способами: выдаёт неграмотный немецкий и молча меняет смысл.
    Живой случай 19.08.2026: «Steck das Portemonnaie in die Tasche» («в карман») оба
    судьи предложили переписать в «in den Taschen» — это и неверный падеж (нужен
    Akkusativ направления), и другое число («в карманы»). Проверять это строками мы
    права не имеем — язык не арифметика, — поэтому спрашиваем модель отдельным вопросом
    про ГОТОВЫЙ текст. Итог кладём рядом с правкой, в `corrected_check` /
    `proposal_check`, и дальше по нему решают и ночь, и экран.
    """
    from backend.openai_manager import run_phrase_fix_check

    for field in ("corrected", "proposal"):
        fix = str(judge.get(field) or "").strip()
        if not fix:
            continue
        try:
            judge[f"{field}_check"] = _check_fix_once(text, translation, fix)
        except Exception as exc:
            logging.debug("проверка правки судьи не прошла: %s", exc)
            judge[f"{field}_check"] = {"checked": False}
    return judge


def _disputed_words(fix: str, corrected: str) -> list[str]:
    """Слова, которые проверка хочет ЗАМЕНИТЬ в правке судьи.

    Берём слова правки, которых нет в предложенном исправлении. Это не разбор языка, а
    сравнение двух списков слов — арифметика, ей верить можно. Знаки препинания по краям
    снимаем: спор идёт про написание слова, а не про точку.
    """
    def words(value: str) -> list[str]:
        return [w.strip(".,;:!?…\"'»«()[]–—") for w in str(value or "").split()]

    theirs = list(words(corrected))
    out = []
    for word in words(fix):
        if word in theirs:
            theirs.remove(word)          # одно вхождение гасит одно
        elif word:
            out.append(word)
    return out


def _reference_confirms_the_wording(fix: str, corrections: list[str]) -> str:
    """Напечатаны ли в справочнике ВСЕ слова, к которым придралась проверка.

    Придирка снимается только целиком: подтвердилось одно слово из двух — значит
    во втором претензия могла быть по делу, и мы её не трогаем. Половинчатых снятий
    здесь нет.

    Возвращает подпись подтверждения («loswerden», «loswerden, aufbleiben») или пустую
    строку — тогда вердикт проверки остаётся в силе.
    """
    from backend.german_verb_paradigms import confirm_form_growing_the_reference

    disputed: list[str] = []
    for corrected in corrections:
        if not str(corrected or "").strip():
            continue
        for word in _disputed_words(fix, corrected):
            if word not in disputed:
                disputed.append(word)
    # Не за что зацепиться: проверка не показала СВОЙ текст либо переписала всё целиком.
    # Спорить со справочником тут не о чем — оставляем вердикт как есть.
    if not disputed or len(disputed) > 3:
        return ""

    confirmed = []
    for word in disputed:
        try:
            verb = confirm_form_growing_the_reference(word, sentence=fix)
        except Exception:
            logging.debug("справочник форм не ответил про %s", word, exc_info=True)
            return ""
        if not verb:
            return ""                    # хоть одно слово не подтвердилось — не снимаем
        if verb not in confirmed:
            confirmed.append(verb)
    return ", ".join(confirmed)


def _check_fix_once(text: str, translation: str, fix: str) -> dict:
    """Спросить проверяющего ОДИН раз про готовый исправленный текст.

    Это НЕ переспрос мнения: проверяется текст, которого раньше не было, — грамотен ли
    он и не уехал ли смысл. Именно эта проверка поймала «in den Taschen», когда сверка
    двух судей не поймала.

    ⚠ ВТОРОЙ ЗАПРОС УБРАН 04.09.2026 вместе со вторым судьёй. Он ловил редкий случай,
    когда проверяющий придирается к верному написанию («losgeworden пишется раздельно»),
    — но ловит это НЕ второе мнение, а печатный справочник ниже, и он остаётся.
    """
    from backend.openai_manager import run_phrase_fix_check

    итог = run_phrase_fix_check(original=text, meaning_ru=translation, fix=fix) or {}
    if not итог.get("checked"):
        return {"checked": False}
    if итог.get("grammar_ok") and итог.get("meaning_kept"):
        return итог
    out = dict(итог)
    # СПРАВОЧНИК СИЛЬНЕЕ МОДЕЛИ.
    #
    # Проверяющий ошибается на трудном месте: правку «…dass er das Schwein losgeworden
    # war» он забраковал со словами «пишется раздельно», хотя `losgeworden` напечатано
    # в таблице `loswerden` именно слитно. Спорить с моделью нечем, а со справочником
    # есть чем: если слово, к которому придрались, напечатано на странице Flexion,
    # придирка снимается. Это не наше правило и не наша догадка — это то, что напечатано
    # в источнике (CLAUDE.md, правило ноль).
    if not out.get("grammar_ok"):
        confirmed_by = _reference_confirms_the_wording(fix, [str(итог.get("fixed") or "")])
        if confirmed_by:
            logging.info("справочник подтвердил написание %s — придирка снята", confirmed_by)
            out["grammar_ok"] = True
            out["reference"] = confirmed_by
            out["why"] = (f"Написание подтверждено справочником ({confirmed_by}) — "
                          f"придирка к орфографии снята.")
    return out


_CYRILLIC = re.compile(r"[а-яёА-ЯЁ]")


def _in_russian(value: str) -> bool:
    """Написано ли объяснение по-русски. Кириллица — единственный надёжный признак."""
    return bool(_CYRILLIC.search(str(value or "")))


# ⛔ ЗДЕСЬ БЫЛ ПЕРЕСПРОС СУДЬИ ИЗ-ЗА ЯЗЫКА ОБЪЯСНЕНИЯ. УБРАН 04.09.2026.
# Если ответ приходил не по-русски, задавался ЕЩЁ ОДИН платный запрос — тот же самый.
# Требование «объясняй по-русски» стоит отдельным правилом в самом вопросе
# (`openai_manager.run_phrase_grammar_verdict`), и платить за переформулирование
# незачем. Пришло не по-русски — показываем как есть: спрятать разбор хуже.


def _что_изменено(исходный: str, правка: str) -> str:
    """Что именно правка меняет в тексте. Пустая строка — не меняет ничего.

    ┌─ ПОЧЕМУ СЧИТАЕМ САМИ, А НЕ СПРАШИВАЕМ. Замер 04.09.2026. ────────────────────┐
    │ Судью попросили цитировать неверное место полем `span`. На 63 живых фразах он │
    │ вернул `null` ВО ВСЕХ случаях, включая контрольные с заведомой ошибкой:       │
    │ «Die Finster der Nacht» он исправил верно, а показать пальцем не смог.        │
    │ Требование, которое модель игнорирует, — не защита, а самообман.               │
    │                                                                              │
    │ Зато исправленный текст она даёт, и разница между ним и исходным И ЕСТЬ то    │
    │ место, о котором речь. Это наша арифметика: она не зависит от послушности     │
    │ модели и ловит ровно тот класс пустых придирок, ради которого затевалась, —   │
    │ «предлог не тот», а в правке дописана только точка.                            │
    └──────────────────────────────────────────────────────────────────────────────┘

    Знаки в конце не считаются изменением: это словарная запись, а не связный текст.
    """
    a = str(исходный or "").strip().rstrip(" .!?…")
    b = str(правка or "").strip().rstrip(" .!?…")
    if not b or a == b:
        return ""
    начало = 0
    while начало < len(a) and начало < len(b) and a[начало] == b[начало]:
        начало += 1
    конец = 0
    while (конец < len(a) - начало and конец < len(b) - начало
           and a[len(a) - 1 - конец] == b[len(b) - 1 - конец]):
        конец += 1
    # Раздвигаем до границ слов: «Finster» → «Finsternis» даёт разницу в три буквы
    # («nis»), а человеку нужно видеть СЛОВО, о котором речь.
    while начало > 0 and not a[начало - 1].isspace():
        начало -= 1
    while конец > 0 and конец < len(a) and not a[len(a) - конец].isspace():
        конец -= 1
    место = a[начало:len(a) - конец].strip()
    # Правка только дописала слова (в исходном на этом месте пусто) — показываем то,
    # что появилось: человеку важно увидеть, ЧТО именно добавили.
    return место or b[начало:len(b) - конец].strip()


def _judge_once(text: str, kind: str, translation: str = "") -> list[dict]:
    """Спросить модель ОДИН раз. Список из одного ответа — форма, которую ждут дальше
    экран и очередь вопросов.

    Перевод передаём обязательно: предлог и падеж в немецком выбираются по смыслу, и
    судья без перевода судит вслепую — на «Wappnen mit» («запастись чем-то») он требовал
    `gegen` и ошибался.

    Каждая предложенная правка сразу проходит проверку на грамотность и на сохранение
    смысла (`_check_own_fixes`): показывать владельцу кнопку с неверным немецким — это
    отдавать ему на проверку то, что должна была проверить система.
    """
    from backend.openai_manager import run_phrase_grammar_verdict

    try:
        verdict = run_phrase_grammar_verdict(
            text=text, kind=kind, translation=translation) or {}
    except Exception as exc:
        logging.debug("судья фраз не ответил: %s", exc)
        return [{}]
    if not verdict:
        return [{}]
    # ⛔ ПРЕТЕНЗИЯ БЕЗ ИЗМЕНЕНИЯ — НЕ ОШИБКА. Считаем САМИ, без запросов.
    if str(verdict.get("verdict") or "") == "error":
        место = _что_изменено(text, str(verdict.get("corrected") or "")) \
            or _что_изменено(text, str(verdict.get("proposal") or ""))
        if not место:
            logging.info("судья не изменил ни слова — придирка снята с %r", str(text)[:60])
            return [{"verdict": "ok", "category": "", "corrected": "", "why": "",
                     "kind": verdict.get("kind") or kind, "span": "",
                     "dropped_no_change": True}]
        verdict["span"] = место
    return [_check_own_fixes(verdict, text, translation)]


def fix_passed_check(judge: dict, field: str) -> bool | None:
    """Прошла ли эта правка проверку. True / False / None («не проверялась»).

    None — честное третье состояние: проверка не отвечала, ответила не той формой или
    два её прогона разошлись. Считать его «всё хорошо» нельзя, но и прятать вариант от
    владельца из-за неуверенности проверки тоже нельзя — он остаётся на экране с
    пометкой, а молча ночью не применяется."""
    check = judge.get(f"{field}_check") if isinstance(judge, dict) else None
    if not isinstance(check, dict) or not check.get("checked"):
        return None
    if not (bool(check.get("grammar_ok")) and bool(check.get("meaning_kept"))):
        return False
    return None if check.get("disputed") else True


def _both_name_the_same_kind(judges: list[dict]) -> str:
    """Вид записи, названный ОБОИМИ судьями одинаково. Иначе пустая строка.

    Судьи спрашиваются независимо и оба видят наш вид лишь подсказкой. Согласие двоих —
    та же планка, по которой ночь молча правит текст: разошлись хоть в чём-то — не наше
    дело решать, вид остаётся прежним и запись просто продолжает жить как жила.

    Пустая строка от судьи («не назвал», ответ не той формы) согласием НЕ считается:
    иначе два молчания сошлись бы в «единогласно» и переписали вид ни на чём.
    """
    if len(judges) != 2:
        return ""
    первый = str((judges[0] or {}).get("kind") or "").strip()
    второй = str((judges[1] or {}).get("kind") or "").strip()
    if not первый or первый != второй:
        return ""
    return первый if первый in ("sentence", "collocation") else ""


def _both_agree(judges: list[dict], text: str = "") -> tuple[bool, str, str]:
    """Можно ли применить правку МОЛЧА. Возвращает (можно, категория, готовый текст).

    ⚠ ИМЯ ОСТАЛОСЬ ПРЕЖНИМ, ЗАКОН ИЗМЕНИЛСЯ 04.09.2026. Раньше «согласны» значило
    «два независимых ответа совпали дословно». Судья теперь один (см. рамку в шапке
    файла), и молчаливую правку держат четыре условия — каждое проверяемо:

      1) вердикт «ошибка» и категория из тех, что верны или неверны сами по себе;
      2) правка непустая и отличается от исходного текста не одной точкой на конце;
      3) МОДЕЛЬ ПРОЦИТИРОВАЛА неверный кусок, и цитата НАЙДЕНА в самом тексте —
         это наша арифметика, без запросов и без веры на слово;
      4) сам исправленный текст прошёл проверку (`_check_fix_once`), а спор с ней
         снимает печатный справочник.

    Смотрим ТОЛЬКО `corrected` — правку без добавления слов. Поле `proposal` (достройка
    неполной фразы: дописанное местоимение, артикль, подлежащее) сюда не допускается
    никогда: дописать слова за человека — это решение о смысле, а его принимает владелец
    в /admin_phrase_review, а не ночь молча.
    """
    if len(judges) != 1:
        return False, "", ""
    a = judges[0]
    if not isinstance(a, dict) or a.get("verdict") != "error":
        return False, "", ""
    fix = str(a.get("corrected") or "").strip()
    if not fix:
        return False, "", ""
    исходный = str(text or "").strip()
    if исходный and fix.rstrip(" .!?") == исходный.rstrip(" .!?"):
        # Правка, отличающаяся одной точкой, — пустая придирка, а не исправление.
        return False, "", ""
    # ⛔ ЦИТАТА ОБЯЗАНА НАЙТИСЬ В САМОМ ТЕКСТЕ. Модель, которая не может показать
    # пальцем на неверное место, не имеет права молча переписать чужую фразу.
    кусок = str(a.get("span") or "").strip()
    if исходный and (not кусок or кусок not in исходный):
        logging.info("молчаливая правка отклонена: цитата %r не найдена в %r",
                     кусок[:40], исходный[:60])
        return False, "", ""
    # Проверка правки — не мнение о фразе, а вопрос о НОВОМ тексте. «Не проверялась»
    # (None) сюда не пускаем: молчание проверки не согласие.
    if fix_passed_check(a, "corrected") is not True:
        return False, "", ""
    return True, str(a.get("category") or ""), fix


def rejudge_phrase_review(review_id: int) -> bool:
    """Переспросить судей по одной уже открытой спорной фразе.

    Зачем. Судьи, отвечавшие до 08.08.2026, не обязаны были показывать готовый вариант:
    могли написать «фраза не полная, нет местоимения» и не дописать НИЧЕГО. Владельцу
    оставалось печатать руками. Промпт починен, но фразы, отложенные раньше, лежат со
    старым вердиктом — эта кнопка спрашивает по ним заново. Один запрос на фразу,
    поштучно и по нажатию, поэтому расход копеечный."""
    from backend.database import get_open_phrase_review, update_phrase_review_judges

    row = get_open_phrase_review(int(review_id))
    if not row:
        return False
    judges = _judge_once(row["text"], row.get("kind") or "collocation",
                          row.get("translation") or "")
    if not any(j for j in judges):
        return False
    update_phrase_review_judges(int(review_id), judges)
    return True


def rejudge_open_phrase_reviews(limit: int = 60) -> dict:
    """Пересудить разом открытые вопросы, которые судились БЕЗ перевода.

    До 08.08.2026 судья видел только немецкую строку и выбирал более частое управление:
    «Wappnen mit» («запастись чем-то») оба судьи независимо потребовали переписать на
    `gegen` — и оба ошиблись. Все накопленные вердикты слепые, и по одному их
    пересуживать — это десятки нажатий. Идём в несколько потоков, как ночью."""
    from backend.database import list_open_phrase_reviews_judged_blind

    ids = list_open_phrase_reviews_judged_blind(int(limit))
    out = {"picked": len(ids), "rejudged": 0, "failed": 0}
    if not ids:
        return out
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for ok in pool.map(rejudge_phrase_review, ids):
            if ok:
                out["rejudged"] += 1
            else:
                out["failed"] += 1
    logging.info("пересуд открытых вопросов: %s", out)
    return out


# ── ТРЕТИЙ СУДЬЯ ЗОВЁТСЯ НОЧЬЮ, А НЕ КНОПКОЙ ────────────────────────────────────
ARBITER_CAP = int(os.getenv("PHRASE_ARBITER_CAP", "60") or "60")
# Потолок применения вердиктов. Каждое применение пересобирает разбор — то есть идёт
# к модели, — поэтому порция ограничена так же, как у третьего судьи. Накопленные 64
# разойдутся за одну ночь, дальше поток — единицы в сутки.
APPLY_CAP = int(os.getenv("PHRASE_APPLY_CAP", "60") or "60")


def _judge_proposals(judges: list) -> list[str]:
    """Все тексты, которые предложили судьи, — включая забракованные проверкой.

    Третьему судье показывают именно ПРЕДМЕТ СПОРА, поэтому отсев здесь не тот, что на
    кнопках владельца: там прячется то, что нельзя нажимать, а здесь нужно всё, о чём
    спорили. Пустое и совпавшее с самой фразой не в счёт — спорить не о чем."""
    out: list[str] = []
    for j in judges or []:
        if not isinstance(j, dict):
            continue
        for field in ("corrected", "proposal"):
            value = str(j.get(field) or "").strip()
            if value and value not in out:
                out.append(value)
    return out


def settle_dispute(review_id: int) -> bool:
    """Позвать третьего судью по одной открытой фразе и положить его вердикт на неё.

    ЗАЧЕМ. Владелец, 26.08.2026, глядя на экран: «какое решение я могу принять?» —
    и он прав. Двое судей объявили ошибку, обе их правки забракованы нашей же
    проверкой, кнопки нет ни одной, правильного немецкого на экране нет вообще.
    Замер того же дня: 106 открытых вопросов из 232 — ровно такие.

    Третий судья уже был написан (`openai_manager.run_phrase_dispute_verdict`) и умеет
    главное: видя оба предложения, дать СВОЙ текст, когда оба мимо. Но звала его только
    кнопка «Пересудить» в мини-аппе — поэтому на 232 открытых вопроса в живой базе не
    было НИ ОДНОГО сохранённого вердикта. Теперь его зовёт ночь, до того как владелец
    откроет экран.

    ЕГО ТЕКСТ ПРОХОДИТ ТУ ЖЕ ПРОВЕРКУ, что и правки судей: он такая же модель и
    ошибается так же. Не прошёл — остаётся на экране с приговором, но без кнопки
    «сохранить» (`database.phrase_review_variants` смотрит на `better_check`).
    """
    from backend.database import get_open_phrase_review, set_phrase_review_arbiter
    from backend.openai_manager import run_phrase_dispute_verdict

    row = get_open_phrase_review(int(review_id))
    if not row:
        return False
    judges = row.get("judges") if isinstance(row.get("judges"), list) else []
    proposals = _judge_proposals(judges)
    if not proposals:
        # Спорить не о чем: судьи не предложили ни одного текста. Это не грамматический
        # спор, а вопрос другого рода (карточка панели) — третьему судье там делать
        # нечего, и деньги за него платить не за что.
        return False
    text = str(row.get("text") or "")
    translation = str(row.get("translation") or "")
    try:
        verdict = run_phrase_dispute_verdict(
            text=text, variants=proposals, translation=translation,
            kind=str(row.get("kind") or "collocation")) or {}
    except Exception as exc:
        logging.warning("третий судья не ответил по #%s: %s", review_id, exc)
        return False
    if not verdict:
        return False
    better = str(verdict.get("better") or "").strip()
    if better:
        # Свой текст третьего судьи — такая же правка, как у первых двух, и проверяется
        # тем же способом. Итог кладём рядом, в `better_check`.
        try:
            verdict["better_check"] = _check_fix_once(text, translation, better)
        except Exception as exc:
            logging.debug("проверка текста третьего судьи не прошла: %s", exc)
            verdict["better_check"] = {"checked": False}
    set_phrase_review_arbiter(int(review_id), verdict)
    return True


def answer_beyond_what_the_owner_saw(*, unit_id: int, text: str, translation: str,
                                    judges: list) -> bool:
    """Повтор — молчать, ЕСЛИ ответить нечем. Иначе это уже другой вопрос.

    ЗАЧЕМ. Защита от круга сама по себе опасна: она может заморозить неверную фразу
    навсегда. Живой случай 26.08.2026: «Der Bus fährt 100 Personen mit» — по-немецки так
    не говорят (`mitfahren` — про пассажира, автобус людей `mitnimmt`), но владелец
    нажал «оставить как есть», потому что ВЕРНОГО варианта на экране не было вообще:
    оба судьи предлагали одно и то же, и проверка их забраковала. С этого момента фраза
    считалась решённой, и защита от круга больше не пустила бы к ней ни одного вопроса.

    Поэтому, наткнувшись на повтор, ночь делает ещё одно движение: спрашивает решающий
    голос, есть ли ВЕРНЫЙ текст, которого владелец не видел. Есть — вопрос ставится с
    ним, и владелец наконец получает то, чего ему не дали в прошлый раз. Нет — молчим,
    это настоящий круг. Случай редкий (повторы штучные), поэтому и денег стоит мало.
    """
    from backend.database import (
        phrase_review_settled_texts, queue_phrase_for_review, set_phrase_review_arbiter,
        _phrase_text_key,
    )
    from backend.openai_manager import run_phrase_dispute_verdict

    proposals = _judge_proposals(judges)
    if not proposals:
        return False
    seen = phrase_review_settled_texts(int(unit_id))
    try:
        verdict = run_phrase_dispute_verdict(
            text=text, variants=proposals, translation=translation) or {}
    except Exception as exc:
        logging.warning("решающий голос по повтору не ответил: %s", exc)
        return False
    better = str(verdict.get("better") or "").strip()
    if not better or _phrase_text_key(better) in seen:
        return False                       # ответить нечем — это круг, молчим
    check = _check_fix_once(text, translation, better)
    if not (check.get("checked") and check.get("grammar_ok") and check.get("meaning_kept")):
        return False                       # свой же текст не прошёл проверку — не несём
    verdict["better_check"] = check
    rid = queue_phrase_for_review(unit_id=int(unit_id), text=text,
                                  translation=translation, judges=judges, force=True)
    if not rid:
        return False
    set_phrase_review_arbiter(int(rid), verdict)
    logging.info("повтор превратился в новый вопрос: %r → %r", text[:50], better[:50])
    return True


def settle_open_disputes(limit: int | None = None) -> dict:
    """Разрешить накопившиеся споры — порцией за ночь.

    Потолок обязателен: третий судья идёт на gpt-4.1, это дороже судей. 106 накопленных
    разойдутся за две ночи, а дальше поток — единицы в сутки, столько же, сколько
    попадает к владельцу."""
    from backend.database import list_open_phrase_reviews_needing_arbiter

    cap = int(limit if limit is not None else ARBITER_CAP)
    ids = list_open_phrase_reviews_needing_arbiter(cap)
    out = {"взято": len(ids), "решено": 0, "не вышло": 0}
    if not ids:
        return out
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for ok in pool.map(settle_dispute, ids):
            out["решено" if ok else "не вышло"] += 1
    logging.info("третий судья за ночь: %s", out)
    return out


def settled_verdict_to_apply(judges: list, arbiter: dict | None) -> tuple[str, str]:
    """Что из вердикта третьего судьи ночь имеет право применить САМА.

    ┌─ ЗАВЕДЕНО 29.08.2026 ПО РЕШЕНИЮ ВЛАДЕЛЬЦА. ──────────────────────────────────┐
    │ Владелец 28.08.2026, глядя на экран: «а если третий судья рассудил спор, то   │
    │ зачем тут я? Давай принимать то, что судья оставил».                          │
    │ Он прав: третьего судью научили только ПОКАЗЫВАТЬ вердикт, а половину         │
    │ «применить» не написал никто. Замер по живой базе 28.08.2026 по 104 открытым  │
    │ вопросам про немецкий — вердикт есть у ВСЕХ 104, и 64 из них выбирают правку  │
    │ судьи, прошедшую нашу проверку. То есть три четверти очереди владельца висели  │
    │ на нём без причины.                                                           │
    └──────────────────────────────────────────────────────────────────────────────┘

    ПЛАНКА ЗДЕСЬ НЕ НИЖЕ, ЧЕМ У МОЛЧАЛИВОЙ ПРАВКИ (`_both_agree`), а по одному
    признаку выше. Там: двое судей сошлись дословно + обе правки прошли проверку.
    Здесь: правку предложил судья + НАША независимая проверка её пропустила + третий
    судья, видевший обе стороны спора, выбрал именно её. Три сигнала против двух.

    ⛔ БЕРЁМ ТОЛЬКО `corrected` — ПРАВКУ ТОГО, ЧТО БЫЛО.
    Поле `proposal` — это ДОСТРОЙКА: судья дописывает подлежащее, местоимение,
    сказуемое, и словарная запись превращается в готовое предложение («Leiche
    verwesen» → «Die Leiche verwest»). Это решение о СМЫСЛЕ, и его принимает владелец
    — правило его же, от 06.08.2026, оно записано в `_both_agree` и здесь не
    отменяется. Замер 28.08.2026: таких вердиктов 22 из 104, они по-прежнему его.

    ⛔ СВОЙ ТЕКСТ ТРЕТЬЕГО СУДЬИ НОЧЬ НЕ ПРИМЕНЯЕТ — ЕГО РЕШАЕТ ВЛАДЕЛЕЦ.
    ┌─ ПРОВЕРЕНО 29.08.2026 НА ЖИВЫХ ДАННЫХ. ДВЕРЬ ЗАКРЫТА ОСОЗНАННО. ─────────────┐
    │ Здесь два РАЗНЫХ случая, и путать их нельзя:                                  │
    │   А. судья ВЫБРАЛ из двух готовых правок — никто ничего не сочинял, он лишь   │
    │      показал на одну из них. Это и применяется (64 записи на 28.08.2026);      │
    │   Б. судья НАПИСАЛ свой текст, потому что обе правки ему не подошли (14-16).   │
    │                                                                              │
    │ Случай Б пробовали открыть. По просьбе владельца третьего судью научили       │
    │ подписывать свой текст самому (`better_kind`: fix / rebuild) — подпись         │
    │ бесплатная, приезжает тем же ответом. Замер сразу после ввода, по восьми       │
    │ живым вердиктам: из семи подписей «исправил» ДВЕ неверные — судья перестроил   │
    │ запись и назвал это исправлением:                                             │
    │   #579 «Die Karriere von Null an aufgebaut» → «Ich baue die Karriere von      │
    │        Null an auf.» — словарная запись стала предложением от первого лица;   │
    │   #595 «Sie war so gut als die Verkäuferin…» → «Sie war so eine gute          │
    │        Verkäuferin, dass sie mich umgestimmt hat.»                            │
    │ 29% брака — не та планка, при которой правят словарь без человека. Рядом уже  │
    │ стоит цена ошибки: «ein Stück rücken» → «ein Stück Rücken» («подвинуться» →   │
    │ «кусок спины») прошло все три сигнала и всё равно было мимо.                   │
    │                                                                              │
    │ Разнобой снял бы второй вопрос — так держатся сами судьи. Но второй вопрос     │
    │ это второй запрос к модели, то есть деньги на каждой спорной фразе.            │
    │ Владелец 29.08.2026: «если дополнительный запрос — то не годится». И его же    │
    │ решение тем же днём: случай Б выключить.                                       │
    │                                                                              │
    │ ПОДПИСЬ ПРИ ЭТОМ НЕ ВЫБРОШЕНА: она бесплатная и теперь печатается на кнопке   │
    │ у владельца («третий судья: исправил» / «переписал заново»), чтобы он видел,  │
    │ что берёт, ДО нажатия. Решение осталось его, но стало зрячим. Цена невелика:   │
    │ 14-16 записей на всю очередь, и каждая приезжает готовой кнопкой.              │
    └──────────────────────────────────────────────────────────────────────────────┘

    Возвращает (текст, почему) — или ("", причина отказа).
    """
    arbiter = arbiter if isinstance(arbiter, dict) else None
    if not arbiter:
        return "", "третий судья не высказался"
    if str(arbiter.get("better") or "").strip():
        return "", "третий судья написал свой текст — решает владелец"
    try:
        winner = int(arbiter.get("winner") or 0)
    except (TypeError, ValueError):
        winner = 0
    proposals = _judge_proposals(judges or [])
    if not (1 <= winner <= len(proposals)):
        return "", "вердикт есть, а выбор не читается"
    chosen = proposals[winner - 1]
    for judge in (judges or []):
        if not isinstance(judge, dict):
            continue
        if str(judge.get("corrected") or "").strip() == chosen:
            if fix_passed_check(judge, "corrected") is not True:
                return "", "наша проверка эту правку не пропустила"
            return chosen, str(arbiter.get("why") or "").strip()
        if str(judge.get("proposal") or "").strip() == chosen:
            return "", "достройка — дописаны слова, решает владелец"
    return "", "выбранный текст не нашёлся у судей"


# ┌─ УБРАНО 29.08.2026. НЕ ЗАВОДИТЬ СНОВА БЕЗ РЕШЕНИЯ ВЛАДЕЛЬЦА. ────────────────┐
# │ Здесь стоял `label_unsigned_arbiter_texts`: он переспрашивал третьего судью по │
# │ старым вердиктам, чтобы те получили подпись «исправил / переписал». Подпись    │
# │ нужна была ровно для одного — чтобы свой текст судьи уходил в автомат. Автомат │
# │ выключен (см. рамку в `settled_verdict_to_apply`), а переспрос — это ЗАПРОС К  │
# │ МОДЕЛИ на каждую запись, то есть деньги за то, что уже никому не нужно.        │
# │ Владелец 29.08.2026: «если дополнительный запрос — то не годится».             │
# │ Новые вердикты подпись получают даром, тем же ответом, и она печатается на     │
# │ кнопке. Старым её не будет — и это не дефект, а осознанная цена.                │
# └──────────────────────────────────────────────────────────────────────────────┘


def apply_settled_disputes(limit: int | None = None) -> dict:
    """Применить вердикты третьего судьи, которые ночь имеет право применить сама.

    Правка идёт ТОЙ ЖЕ дверью, что и решение владельца кнопкой
    (`database.apply_phrase_review_decision`): она переименует запись, разнесёт текст
    по всем местам, снимет метку проверки и соберёт разбор заново. Своя копия этой
    логики означала бы третий путь правки фразы, который завтра разойдётся с двумя
    другими.

    Отчёт возвращается СТРОКАМИ «было → стало»: владелец должен видеть, что ночь
    сделала за него, а не узнавать об этом по исчезнувшей очереди.
    """
    from backend.database import (
        apply_phrase_review_decision, get_db_connection_context,
    )

    cap = int(limit if limit is not None else APPLY_CAP)
    out = {"взято": 0, "применено": 0, "не вышло": 0, "оставлено владельцу": 0,
           "строки": [], "причины": {}}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, btrim(text), judges, arbiter
                     FROM bt_3_phrase_review
                    WHERE status = 'open' AND kind = 'grammar' AND arbiter IS NOT NULL
                    ORDER BY id LIMIT %s;""",
                (cap,))
            rows = cur.fetchall() or []
    out["взято"] = len(rows)
    for review_id, text, judges, arbiter in rows:
        judges = judges if isinstance(judges, list) else []
        chosen, why = settled_verdict_to_apply(judges, arbiter)
        if not chosen:
            out["оставлено владельцу"] += 1
            out["причины"][why] = out["причины"].get(why, 0) + 1
            continue
        try:
            итог = apply_phrase_review_decision(int(review_id), "accept", "", 0, "",
                                                chosen_text=chosen)
        except Exception as exc:                                   # noqa: BLE001
            logging.warning("вердикт третьего судьи по #%s не применён: %s", review_id, exc)
            out["не вышло"] += 1
            continue
        # Пустой текст означает, что применить было нечего (правка совпала с самой
        # фразой либо такая запись уже есть). Это не «применено» — не считаем.
        if not str(итог.get("text") or ""):
            out["не вышло"] += 1
            continue
        out["применено"] += 1
        out["строки"].append({"id": int(review_id), "было": str(text),
                              "стало": str(итог.get("text")), "почему": why})
    logging.info("вердикты третьего судьи за ночь: применено %s, оставлено владельцу %s",
                 out["применено"], out["оставлено владельцу"])
    return out


def _apply_silent_fix(unit_id: int, corrected: str) -> bool:
    """Записать исправленную фразу в слой слов. Ключ поиска пересобираем, старое
    написание оставляем рядом: по нему уже могли сохраниться карточки.

    Переименование идёт через `lex_units.retitle_unit` — ОДНИМ местом на всё приложение.
    Здесь оно раньше делалось своим UPDATE, и вместе с написанием НЕ пересчитывался вид
    записи (`kind`). А ночной добор берёт в работу только `kind = 'word'`: фраза, которую
    ночь свела к одному слову, оставалась «предложением» и разбор не получала уже никогда.
    Дыру нашёл соседний агент, собиравший карту мест записи, 21.08.2026.
    """
    from backend.database import get_db_connection_context
    from backend.lex_units import normalize_query, retitle_unit

    key = normalize_query(corrected)
    if not key:
        return False
    try:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM bt_3_lex_units WHERE lang='de' AND lemma_key=%s AND id<>%s LIMIT 1;",
                    (key, int(unit_id)),
                )
                if cur.fetchone():
                    return False        # такое слово уже есть — сливать надо осознанно
                retitle_unit(cur, int(unit_id), corrected)
                cur.execute(
                    """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
                       VALUES ('de', %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
                    (key, int(unit_id)),
                )
            conn.commit()
        return True
    except Exception as exc:
        logging.warning("правка фразы %s не записалась: %s", unit_id, exc)
        return False


def run_phrase_night_check(*, limit: int | None = None, dry_run: bool = False) -> dict:
    """Одна ночная партия. Возвращает отчёт для утреннего сообщения владельцу."""
    cap = int(limit if limit is not None else NIGHT_CAP)
    report = {"cap": cap, "picked": 0, "checked": 0, "fixed": 0, "doubt": 0, "errors": 0,
              "noise": 0, "by_category": {}, "left": 0, "open_reviews": 0,
              # Повторные вопросы, которые владельцу НЕ задали: он это уже решал.
              "circle_blocked": 0, "reopened_with_answer": 0,
              # Закрыто без него («оба судьи: ошибки нет») и разрешено третьим судьёй.
              "closed_all_ok": 0, "settled": {},
              "dry_run": bool(dry_run)}
    rows = pick_phrases_for_grammar_check(cap)
    report["picked"] = len(rows)
    if not rows:
        report["left"] = count_phrases_left_for_grammar_check()
        report["open_reviews"] = count_open_phrase_reviews()
        return report

    def work(row):
        return row, _judge_once(row["text"], row["kind"], row.get("translation") or "")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for row, judges in pool.map(work, rows):
            report["checked"] += 1
            if not any(j for j in judges):
                report["errors"] += 1
                continue
            # ВИД ЗАПИСИ — ОТ МОДЕЛИ, А НЕ ОТ СЧЁТА СЛОВ (решение владельца 28.08.2026).
            # Спрошено тем же запросом, что и грамматика, поэтому даром. Меняем молча
            # только при согласии ОБОИХ судей — то же правило, что и для правок текста.
            if not dry_run:
                новый_вид = _both_name_the_same_kind(judges)
                if новый_вид and новый_вид != str(row.get("kind") or ""):
                    from backend.lex_units import set_unit_kind
                    if set_unit_kind(row["unit_id"], новый_вид, source="модель"):
                        report["kind_fixed"] = report.get("kind_fixed", 0) + 1
                        logging.info("вид записи %r: %s → %s (оба судьи)",
                                     row["text"][:60], row.get("kind"), новый_вид)
            agreed, category, corrected = _both_agree(judges, row["text"])
            if agreed and category in SILENT_CATEGORIES:
                if dry_run or _apply_silent_fix(row["unit_id"], corrected):
                    if not dry_run:
                        # Текст изменился — разбор к нему собирается ЗАНОВО, а не
                        # латается заменой: замена по строке ломает падеж, если слово
                        # внутри примера стоит в другой форме.
                        #
                        # ⛔ НО ТОЛЬКО НЕ ПРЕДЛОЖЕНИЮ. Решение владельца 27.08.2026:
                        # «это уже предложение, включающее в себя контекст использования
                        # слов… главное — есть немецкий и русский вариант, и больше
                        # ничего не нужно». Здесь была третья дверь ночного обогащения
                        # предложений, и она работала: замер 27.08.2026 — 64 предложения
                        # получили полный СЛОВАРНЫЙ разбор именно этим путём (метка
                        # `пересборка после правки`, час 01 UTC = ночной прогон).
                        # Внутри лежали формы глагола sein у целого предложения и
                        # антонимы к нему же.
                        #
                        # Вид считаем по ИСПРАВЛЕННОМУ тексту, а не по прежнему: правка
                        # могла свести предложение к обороту, и наоборот. Правило одно
                        # на всё приложение — `lex_units._kind_for_text`, тот же, по
                        # которому `retitle_unit` только что пересчитал вид записи.
                        #
                        # ┌─ ПРОВЕРЕНО 28.08.2026. НЕ ПОДНИМАТЬ КАК НОВУЮ НАХОДКУ. ──┐
                        # │ «А если у предложения разбор УЖЕ есть и текст поправили — │
                        # │ разбор ведь останется про старый текст?» Останется. Но во │
                        # │ всей оставшейся очереди ночной проверки таких предложений │
                        # │ ОДИННАДЦАТЬ из 512, и поправлены будут не все одиннадцать  │
                        # │ (нужно согласие двух судей на тихую правку). Накопленное   │
                        # │ владелец решил не трогать (28.08.2026): «уже как есть так  │
                        # │ пусть и будет». Стирать разбор здесь — это чистка          │
                        # │ накопленного, а её он не заказывал.                        │
                        # └───────────────────────────────────────────────────────────┘
                        from backend.lex_units import _kind_for_text
                        if _kind_for_text(corrected) == "sentence":
                            report["разбор не собирали, это предложение"] = (
                                report.get("разбор не собирали, это предложение", 0) + 1)
                        else:
                            try:
                                from backend.database import rebuild_unit_breakdown
                                rebuild_unit_breakdown(row["unit_id"], corrected)
                            except Exception as exc:
                                logging.warning("пересборка после ночной правки не удалась: %s", exc)
                    report["fixed"] += 1
                    report["by_category"][category] = report["by_category"].get(category, 0) + 1
                    if not dry_run:
                        mark_phrase_checked(row["unit_id"], corrected, "fixed")
                    continue
            # Хоть один судья увидел ошибку, но согласия нет или это порядок слов —
            # решает владелец. Сюда же попадает всё «зависит от контекста».
            #
            # Но только если исправлять реально есть что. Судья умеет объявить ошибку и
            # не дать ничего: «лучше 'an mir' заменить на 'an mir'» или правку, которая
            # отличается от исходной фразы одной точкой в конце. Спрашивать по такому
            # владельца — значит отнимать у него время на пустоту; отмечаем фразу
            # проверенной и идём дальше. Замер на живой очереди 08.08.2026.
            from backend.database import phrase_review_is_noise
            if phrase_review_is_noise(judges, row["text"]):
                report["noise"] = int(report.get("noise") or 0) + 1
                if not dry_run:
                    mark_phrase_checked(row["unit_id"], row["text"], "ok")
                continue
            if any(str(j.get("verdict") or "") == "error" for j in judges):
                report["doubt"] += 1
                if not dry_run:
                    # False означает «владелец это уже решал» — вопрос не заводим,
                    # но и не молчим: число уезжает в утренний отчёт.
                    if not queue_phrase_for_review(
                        unit_id=row["unit_id"], text=row["text"],
                        translation=row["translation"], judges=judges,
                    ):
                        # Владелец это уже решал. Прежде чем промолчать — проверяем,
                        # нет ли верного ответа, которого он НЕ видел.
                        if answer_beyond_what_the_owner_saw(
                            unit_id=row["unit_id"], text=row["text"],
                            translation=row["translation"] or "", judges=judges,
                        ):
                            report["reopened_with_answer"] = int(
                                report.get("reopened_with_answer") or 0) + 1
                        else:
                            report["circle_blocked"] += 1
                            report["doubt"] -= 1
                    mark_phrase_checked(row["unit_id"], row["text"], "doubt")
                continue
            if not dry_run:
                mark_phrase_checked(row["unit_id"], row["text"], "ok")

    # ⚠ ХВОСТ ДЕЛАЕТ НАСТОЯЩУЮ РАБОТУ, И ЭТО ЕДИНСТВЕННОЕ МЕСТО, ГДЕ ЕЁ МОЖНО ЗАПРЕТИТЬ.
    #
    # ┌─ ПОЧИНЕНО 29.08.2026. ПРОГОН ТЕСТОВ ПРИМЕНИЛ 64 ПРАВКИ В ЖИВОЙ БАЗЕ. ────────┐
    # │ Ниже закрываются бесспорные вопросы, зовётся третий судья (это деньги) и      │
    # │ применяются решённые споры. Тест `test_sentences_get_no_breakdown` зовёт эту  │
    # │ функцию с подменёнными судьями, а хвост не подменяет — и хвост отработал по   │
    # │ живой базе: 28.08.2026, 22:20 UTC, 64 записи. Итог совпал с тем, что владелец │
    # │ утвердил, но сделал его прогон тестов, а не ночь. Тот же класс, что и 1010    │
    # │ фантомных строк расхода от локального pytest (см. backend/tests/conftest.py). │
    # │ Переменную ставит conftest; в проде её нет, и ночь работает как работала.     │
    # └──────────────────────────────────────────────────────────────────────────────┘
    побочное_запрещено = str(os.getenv("SKIP_NIGHT_SIDE_EFFECTS") or "").strip() == "1"
    if побочное_запрещено:
        report["side_effects_skipped"] = True
    if not dry_run and not побочное_запрещено:
        # Что не вопрос — до владельца не доходит.
        try:
            from backend.database import close_all_ok_phrase_reviews
            report["closed_all_ok"] = close_all_ok_phrase_reviews()
        except Exception as exc:
            logging.warning("не удалось закрыть бесспорные вопросы: %s", exc)
        # …а по тому, что вопрос, ответ должен быть готов ДО того, как владелец
        # откроет экран. Спор двух судей — не его работа.
        try:
            report["settled"] = settle_open_disputes()
        except Exception as exc:
            logging.warning("третий судья за ночь не отработал: %s", exc)
        # …а разрешённый спор — это уже ОТВЕТ, а не вопрос. Держать его в очереди
        # владельца значит спрашивать о том, на что ответ уже получен и оплачен.
        # Решение владельца 28.08.2026: «если третий судья рассудил — принимаем».
        try:
            report["applied"] = apply_settled_disputes()
        except Exception as exc:
            logging.warning("вердикты третьего судьи не применились: %s", exc)

    report["left"] = count_phrases_left_for_grammar_check()
    report["open_reviews"] = count_open_phrase_reviews()
    return report
