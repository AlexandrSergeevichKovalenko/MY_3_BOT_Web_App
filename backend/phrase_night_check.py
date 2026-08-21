"""Ночная проверка грамматики фраз общего словаря.

Зачем. Артикль у слова сверяется со справочником бесплатно, а у фразы справочника нет:
«der Titanic rammen ein Eisberg und beginnen zu sinken» ни в одном словаре не лежит.
Грамматику предложения может рассудить только язык, а это деньги — значит партиями,
с потолком и с отчётом.

Два судьи, и это главное правило. Один и тот же вопрос задаётся ДВАЖДЫ, независимо.
Молча правим ТОЛЬКО когда оба назвали одну категорию и выдали дословно один и тот же
исправленный текст. Разошлись хоть в букве — фраза уходит владельцу на решение, а не
в базу. Проверено на выборке 06.08.2026: расхождение — верный признак того, что модель
не уверена и придумывает («предлог не тот» при том, что в правке дописана только точка).

Молча правим только ошибки, которые не зависят от контекста: опечатка, согласование,
падеж, предлог. ПОРЯДОК СЛОВ — никогда: кусок, вырванный из предложения, законно
выглядит переставленным, и мы не знаем, откуда человек его взял. Такие всегда идут
владельцу. Решение владельца 06.08.2026; если их окажется слишком много для ручного
разбора, правило пересмотрим по числам, а не на ощупь.
"""
from __future__ import annotations

import logging
import os
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
            judge[f"{field}_check"] = _check_fix_twice(text, translation, fix)
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


def _check_fix_twice(text: str, translation: str, fix: str) -> dict:
    """Спросить проверяющего ДВАЖДЫ и забраковать правку только при единогласии.

    Проверяющий — такая же модель, и на трудном месте ошибается: правку «Er war froh,
    dass er das Schwein losgeworden war» один прогон забраковал со словами «слитное
    написание неверно», хотя `losgeworden` — верное причастие от `loswerden`. Это тот же
    приём, которым в этом файле держатся судьи: один голос — мнение, два совпавших —
    основание. Здесь он развёрнут в сторону осторожности, потому что цена ошибок разная:

      • ложно ЗАБРАКОВАТЬ — владелец теряет годную кнопку. Обидно, но текст и причина у
        него перед глазами, и рядом есть поле «впиши свой вариант»;
      • ложно ПРОПУСТИТЬ — неверный немецкий уезжает на кнопку, а при согласии судей и
        молча в общий словарь, к людям.

    Поэтому: забраковали оба — брак; разошлись — правка остаётся на кнопке, но помечена
    спорной, и ночь её молча НЕ применяет (`fix_passed_check` вернёт None).
    """
    from backend.openai_manager import run_phrase_fix_check

    first = run_phrase_fix_check(original=text, meaning_ru=translation, fix=fix) or {}
    if not first.get("checked"):
        return {"checked": False}
    if first.get("grammar_ok") and first.get("meaning_kept"):
        return first          # претензий нет — второй голос не нужен, это лишние деньги
    second = run_phrase_fix_check(original=text, meaning_ru=translation, fix=fix) or {}
    if not second.get("checked"):
        return {"checked": False}
    out = dict(first)
    for key in ("grammar_ok", "meaning_kept"):
        # Брак только тогда, когда его увидели ОБА.
        out[key] = bool(first.get(key)) or bool(second.get(key))
    # СПРАВОЧНИК СИЛЬНЕЕ ДВУХ МОДЕЛЕЙ.
    #
    # Оба голоса могут ошибиться одинаково — это и произошло: правку «…dass er das
    # Schwein losgeworden war» обе проверки забраковали со словами «пишется раздельно»,
    # хотя `losgeworden` напечатано в таблице `loswerden` именно слитно. Спорить с
    # моделью нечем, а со справочником есть чем: если слово, к которому придрались,
    # напечатано на странице Flexion, придирка снимается. Это не наше правило и не наша
    # догадка — это то, что напечатано в источнике (CLAUDE.md, правило ноль).
    if not out["grammar_ok"]:
        confirmed_by = _reference_confirms_the_wording(
            fix, [str(first.get("fixed") or ""), str(second.get("fixed") or "")])
        if confirmed_by:
            logging.info("справочник подтвердил написание %s — придирка снята", confirmed_by)
            out["grammar_ok"] = True
            out["reference"] = confirmed_by
            out["why"] = (f"Написание подтверждено справочником ({confirmed_by}) — "
                          f"придирка к орфографии снята.")
    if (bool(first.get("grammar_ok")) != bool(second.get("grammar_ok"))
            or bool(first.get("meaning_kept")) != bool(second.get("meaning_kept"))):
        out["disputed"] = True
        out["why"] = str(second.get("why") or first.get("why") or "")
    return out


def _judge_twice(text: str, kind: str, translation: str = "") -> list[dict]:
    """Спросить судью дважды, независимо. Перевод передаём ОБОИМ: предлог и падеж в
    немецком выбираются по смыслу, и судья без перевода судит вслепую — на «Wappnen mit»
    («запастись чем-то») оба независимо потребовали `gegen` и оба ошиблись.

    Каждая предложенная правка сразу проходит проверку на грамотность и на сохранение
    смысла (`_check_own_fixes`): показывать владельцу кнопку с неверным немецким —
    это отдавать ему на проверку то, что должна была проверить система."""
    from backend.openai_manager import run_phrase_grammar_verdict

    out = []
    for _ in range(2):
        try:
            verdict = run_phrase_grammar_verdict(
                text=text, kind=kind, translation=translation) or {}
        except Exception as exc:
            logging.debug("судья фраз не ответил: %s", exc)
            verdict = {}
        out.append(_check_own_fixes(verdict, text, translation) if verdict else verdict)
    return out


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


def _both_agree(judges: list[dict]) -> tuple[bool, str, str]:
    """Согласны ли судьи ДОСЛОВНО. Возвращает (согласны, категория, исправленный текст).

    Смотрим ТОЛЬКО `corrected` — правку без добавления слов. Поле `proposal` (достройка
    неполной фразы: дописанное местоимение, артикль, подлежащее) сюда не допускается
    никогда: дописать слова за человека — это решение о смысле, а его принимает владелец
    в /admin_phrase_review, а не ночь молча."""
    if len(judges) != 2:
        return False, "", ""
    a, b = judges
    if a.get("verdict") != "error" or b.get("verdict") != "error":
        return False, "", ""
    if (a.get("category") or "") != (b.get("category") or ""):
        return False, "", ""
    fix_a = str(a.get("corrected") or "").strip()
    fix_b = str(b.get("corrected") or "").strip()
    if not fix_a or fix_a != fix_b:
        return False, "", ""
    # Согласие двух судей — это ещё не правильность. Оба могут ошибиться одинаково:
    # 19.08.2026 оба дословно предложили «in den Taschen» — неверный падеж и другое
    # число. Молча правим ТОЛЬКО то, что вдобавок прошло проверку своей же правки.
    # «Не проверялась» (None) сюда тоже не пускаем: молчание проверки не согласие.
    if fix_passed_check(a, "corrected") is not True:
        return False, "", ""
    if fix_passed_check(b, "corrected") is not True:
        return False, "", ""
    return True, str(a.get("category") or ""), fix_a


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
    judges = _judge_twice(row["text"], row.get("kind") or "collocation",
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
              "dry_run": bool(dry_run)}
    rows = pick_phrases_for_grammar_check(cap)
    report["picked"] = len(rows)
    if not rows:
        report["left"] = count_phrases_left_for_grammar_check()
        report["open_reviews"] = count_open_phrase_reviews()
        return report

    def work(row):
        return row, _judge_twice(row["text"], row["kind"], row.get("translation") or "")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for row, judges in pool.map(work, rows):
            report["checked"] += 1
            if not any(j for j in judges):
                report["errors"] += 1
                continue
            agreed, category, corrected = _both_agree(judges)
            if agreed and category in SILENT_CATEGORIES:
                if dry_run or _apply_silent_fix(row["unit_id"], corrected):
                    if not dry_run:
                        # Текст изменился — разбор к нему собирается ЗАНОВО, а не
                        # латается заменой: замена по строке ломает падеж, если слово
                        # внутри примера стоит в другой форме.
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
                    queue_phrase_for_review(
                        unit_id=row["unit_id"], text=row["text"],
                        translation=row["translation"], judges=judges,
                    )
                    mark_phrase_checked(row["unit_id"], row["text"], "doubt")
                continue
            if not dry_run:
                mark_phrase_checked(row["unit_id"], row["text"], "ok")

    report["left"] = count_phrases_left_for_grammar_check()
    report["open_reviews"] = count_open_phrase_reviews()
    return report
