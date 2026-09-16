# -*- coding: utf-8 -*-
"""Ночная проверка: заголовок карточки — словарное слово, а не форма.

Повод (13.09.2026). В словаре людей заголовками стояли «beruhte» вместо «beruhen»,
«Blähungen» вместо «die Blähung». Разовая чистка починила 118 карточек
(`scripts/dict_fix_form_headwords.py`), но разовая чистка не закрывает дверь: форма
попадает в словарь с любой поверхности, где человек сохраняет слово из текста.

ПОЧЕМУ ЭТО НОЧНАЯ РАБОТА, А НЕ ПРОВЕРКА НА СОХРАНЕНИИ. Ответ «форма это или слово»
знает только справочник de.wiktionary, а это поход в сеть. Сохранение обязано быть
мгновенным и бесплатным (решение записано в `lex_units._word_gate_for_new_unit`),
поэтому вопрос задаётся ночью, а днём карточка живёт как есть.

⛔ ЧТО ЗДЕСЬ НЕЛЬЗЯ СНИМАТЬ (каждый пункт — пойманная порча, не осторожность):

1. Судья — ТОЛЬКО справочник. Наш указатель `bt_3_lex_surfaces` считает формой
   «arbeiten», «schildern», «die Brühe»: на прогоне 13.09.2026 он дал 95 законных слов
   против 127 настоящих форм.
2. Регистр различаем: «anmachen» (глагол «включать») и «Anmachen» (форма от
   «die Anmache») — разные слова.
3. Написание с заглавной, чей СТРОЧНЫЙ вариант — законное слово, не чиним вовсе:
   у нас «Hüten» это «охранять», а справочник знает такое написание только как форму
   от «Hut» (шляпа). 24 карточки были бы испорчены.
4. Спорность — свойство НАПИСАНИЯ: пока хоть одна карточка спорная, общий словарь по
   этому слову тоже не трогаем (иначе «Запрашивать → der Abruf»).
5. Разбор не правим текстом, а снимаем: он куплен для формы. Снимаем штатным
   `_strip_dictionary_card_body` — `'{}'` писать нельзя, такая карточка открывается
   пустым экраном и выпадает из ночного добора.
"""
from __future__ import annotations

import json
import logging

JOB_KEY = "form_headword_sweep"
# Столько РАЗНЫХ написаний спрашиваем у справочника за ночь. Справочник бесплатный и
# отвечает пачками по 45, поэтому упираемся не в деньги, а в вежливость к нему.
# На 13.09.2026 непроверенных написаний 4569 — при 300 за ночь очередь уходит за две недели.
BATCH_WORDS = 300
ARTICLES = ("der ", "die ", "das ")
НЕ_СУЩЕСТВИТЕЛЬНОЕ = {
    "verb", "adverb", "preposition", "participle", "conjunction", "particle",
    "глагол", "наречие", "предлог", "причастие", "союз", "частица", "междометие",
}


def bare(text: str) -> str:
    t = str(text or "").strip()
    return t.split(" ", 1)[1].strip() if t.lower().startswith(ARTICLES) else t


def ensure_schema() -> None:
    """Отметка «это написание уже спрашивали у справочника»: без неё ночь ходила бы за
    одними и теми же словами вечно."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bt_3_form_headword_checked (
                    surface     TEXT PRIMARY KEY,
                    verdict     TEXT NOT NULL,          -- форма | слово | молчит | спорно
                    lemma       TEXT NOT NULL DEFAULT '',
                    reason      TEXT NOT NULL DEFAULT '',
                    fixed_cards INTEGER NOT NULL DEFAULT 0,
                    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
        conn.commit()


def _candidates(cur, limit: int) -> list[str]:
    """Написания-одиночки из личных карточек, которых мы ещё не спрашивали.

    ⛔ ТОЛЬКО НЕМЕЦКИЕ СЛОВА. Эта работа переписывает заголовок по НЕМЕЦКОМУ справочнику
    форм, и до 16.09.2026 брала всё подряд: язык карточки не спрашивался вовсе. Пока
    база одноязычная, это незаметно. В день, когда рядом ляжет английское слово,
    «Gift» (подарок) превратится в «das Gift» (яд) — молча, ночью, без следа на экране.

    Язык спрашивается ОДНИМ ответчиком на всё приложение (backend/lang_of_word.py), а не
    условием «есть ли de в паре»: у пары en→de немецкий — РОДНОЙ язык человека, а слово
    английское, и такое условие пропустило бы его в немецкую машину.

    Замер живой базы 16.09.2026 перед постановкой: кандидатов 6 117, новое условие
    отсекает РОВНО ОДИН — итальянское «aspettiamo» (пара ru→it), которое немецкий
    справочник форм переписывать не должен. Немецкие 6 116 не затронуты.
    """
    from backend.lang_of_word import это_немецкое
    cur.execute("""
        SELECT DISTINCT regexp_replace(q.word_de, '^(der|die|das) ', '') AS surface,
               q.source_lang, q.target_lang, p.native_language
          FROM bt_3_webapp_dictionary_queries q
          LEFT JOIN bt_3_user_language_profile p ON p.user_id = q.user_id
         WHERE q.word_de IS NOT NULL AND q.word_de <> ''
           AND position(' ' in trim(regexp_replace(q.word_de, '^(der|die|das) ', ''))) = 0
           AND NOT EXISTS (SELECT 1 FROM bt_3_form_headword_checked c
                            WHERE c.surface = regexp_replace(q.word_de, '^(der|die|das) ', ''))
         ORDER BY 1
         LIMIT %s
    """, (int(limit) * 2,))
    годные: list[str] = []
    видели: set[str] = set()
    for surface, src, tgt, native in cur.fetchall() or []:
        ключ = str(surface)
        if ключ in видели:
            continue
        видели.add(ключ)
        if not это_немецкое(source_lang=src, target_lang=tgt, native_lang=native):
            continue
        годные.append(ключ)
        if len(годные) >= int(limit):
            break
    return годные


def _ask_reference(surfaces: list[str]) -> dict[str, dict]:
    """{написание: {'класс','база'}} — спрашиваем справочник пачками, ДО работы с базой
    (сеть внутри открытой транзакции рвёт соединение)."""
    from backend.german_form_headword import headword_kinds
    out: dict[str, dict] = {}
    чистые = sorted({s for s in surfaces if s})
    for i in range(0, len(чистые), 45):
        пачка = чистые[i:i + 45]
        ответы = headword_kinds(пачка) or {}
        for w in пачка:
            r = ответы.get(w) or {}
            вид = str(r.get("kind") or "")
            базы = [str(b) for b in (r.get("bases") or []) if str(b or "").strip()]
            if вид == "word":
                out[w] = {"класс": "слово", "база": ""}
            elif вид == "form" and len(базы) == 1:
                out[w] = {"класс": "форма", "база": базы[0]}
            elif вид == "form":
                out[w] = {"класс": "молчит", "база": ""}   # несколько баз — решает человек
            else:
                out[w] = {"класс": "молчит", "база": ""}
    # Строчный вариант заглавного написания: если он сам законное слово, чинить нельзя.
    строчные = sorted({w[:1].lower() + w[1:] for w, r in out.items()
                       if r["класс"] == "форма" and w[:1].isupper()})
    if строчные:
        ответы = {}
        for i in range(0, len(строчные), 45):
            ответы.update(headword_kinds(строчные[i:i + 45]) or {})
        for w, r in out.items():
            if r["класс"] != "форма" or not w[:1].isupper():
                continue
            строчное = w[:1].lower() + w[1:]
            if str((ответы.get(строчное) or {}).get("kind") or "") == "word":
                r["класс"] = "спорно"
                r["почему"] = f"строчное «{строчное}» — законное слово"
    return out


def _fix_surface(cur, surface: str, lemma: str) -> dict:
    """Починить одно написание. Возвращает счётчики и причину, если не чинили."""
    from backend.article_authority import authoritative_article
    from backend.database import _strip_dictionary_card_body
    итог = {"карточек": 0, "словарь": 0, "спорных": 0, "почему": ""}

    голая_лемма = bare(lemma)
    новый = голая_лемма
    if голая_лемма[:1].isupper():
        cur.execute("SELECT tables FROM bt_3_german_noun_declensions WHERE lower(noun)=%s LIMIT 1",
                    (голая_лемма.casefold(),))
        row = cur.fetchone()
        готовое = ""
        if row and isinstance(row[0], dict):
            блок = row[0].get("singular") or row[0].get("Singular") or {}
            if isinstance(блок, dict):
                готовое = str(блок.get("nominativ") or блок.get("Nominativ") or "").strip()
        if готовое.lower().startswith(ARTICLES):
            новый = готовое
        else:
            article, _ = authoritative_article(голая_лемма)
            новый = f"{article} {голая_лемма}" if article else голая_лемма

    # ⛔ Правим ТОЛЬКО немецкие карточки. Одно написание может лежать у разных людей
    # в разных парах («Hand» есть и в немецком, и в английском): отбор кандидатов
    # выше решает, БРАТЬ ли написание в работу, а этот — ЧЬЮ карточку трогать.
    from backend.lang_of_word import это_немецкое
    cur.execute("""
        SELECT q.id, q.user_id, COALESCE(u.pos, ''), COALESCE(q.word_ru, q.translation_ru, ''),
               q.source_lang, q.target_lang, p.native_language
          FROM bt_3_webapp_dictionary_queries q
          LEFT JOIN bt_3_lex_units u ON u.id = q.lex_unit_id
          LEFT JOIN bt_3_user_language_profile p ON p.user_id = q.user_id
         WHERE regexp_replace(q.word_de, '^(der|die|das) ', '') = %s
    """, (surface,))
    карточки = [{"id": r[0], "user": r[1], "pos": str(r[2]).lower(), "ru": r[3]}
                for r in cur.fetchall() or []
                if это_немецкое(source_lang=r[4], target_lang=r[5], native_lang=r[6])]
    if not карточки:
        итог["почему"] = "карточек нет"
        return итог
    # Часть речи спорит с целью — вся работа по этому написанию отменяется (пункт 4).
    for c in карточки:
        if новый[:1].isupper() or bare(новый)[:1].isupper():
            if c["pos"] in НЕ_СУЩЕСТВИТЕЛЬНОЕ:
                итог["спорных"] = len(карточки)
                итог["почему"] = f"у нас это {c['pos']}, а словарное слово — существительное"
                return итог

    cur.execute("SELECT card->>'translation_ru', card->>'word_ru' FROM bt_3_lex_units "
                "WHERE lang='de' AND lemma=%s AND card IS NOT NULL LIMIT 1", (голая_лемма,))
    row = cur.fetchone()
    перевод = str((row[0] or row[1]) if row else "" or "").strip()
    cur.execute("SELECT id FROM bt_3_lex_units WHERE lang='de' AND lower(lemma)=%s LIMIT 1",
                (голая_лемма.casefold(),))
    row = cur.fetchone()
    unit = int(row[0]) if row else None

    for c in карточки:
        cur.execute("SELECT word_de, response_json FROM bt_3_webapp_dictionary_queries WHERE id=%s",
                    (c["id"],))
        row = cur.fetchone()
        if not row or bare(row[0]) != surface:
            continue
        тело = _strip_dictionary_card_body(row[1])
        тело.pop("part_of_speech", None)
        тело["word_de"] = новый
        тело["source_text"] = новый
        if перевод:
            тело["word_ru"] = перевод
            тело["translation_ru"] = перевод
        cur.execute(
            "UPDATE bt_3_webapp_dictionary_queries SET word_de=%s, "
            "word_ru = CASE WHEN %s THEN %s ELSE word_ru END, "
            "translation_ru = CASE WHEN %s THEN %s ELSE translation_ru END, "
            "response_json=%s::jsonb, lex_unit_id=COALESCE(%s, lex_unit_id), "
            "frequency_rank=NULL, updated_at=NOW() WHERE id=%s",
            (новый, bool(перевод), перевод, bool(перевод), перевод,
             json.dumps(тело, ensure_ascii=False), unit, c["id"]),
        )
        итог["карточек"] += 1

    for направление in ("source", "target"):
        поле, ключ = (f"{направление}_text", f"{направление}_lang")
        cur.execute(f"SELECT id FROM bt_3_dictionary_entries WHERE {ключ}='de' "
                    f"AND regexp_replace({поле}, '^(der|die|das) ', '') = %s", (surface,))
        for (eid,) in cur.fetchall() or []:
            cur.execute("SAVEPOINT пул")
            try:
                cur.execute(
                    f"UPDATE bt_3_dictionary_entries SET {поле}=%s, {направление}_text_norm=lower(%s), "
                    f"{направление}_headword_norm=lower(%s), updated_at=NOW() WHERE id=%s",
                    (новый, bare(новый), bare(новый), eid))
                cur.execute("RELEASE SAVEPOINT пул")
                итог["словарь"] += 1
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT пул")
                logging.info("форма-заголовок: запись словаря %s не переименована: %s", eid, exc)
    return итог


def sweep(*, limit: int = BATCH_WORDS, apply: bool = True) -> dict:
    """Ночной проход. Возвращает сводку для утреннего отчёта."""
    from backend.database import get_db_connection_context, dedupe_personal_entry_after_save
    ensure_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            кандидаты = _candidates(cur, limit)
    if not кандидаты:
        return {"спрошено": 0, "форм": 0, "карточек": 0, "словарь": 0, "спорных": 0}

    вердикты = _ask_reference(кандидаты)
    сводка = {"спрошено": len(кандидаты), "форм": 0, "карточек": 0, "словарь": 0,
              "спорных": 0, "слов": 0, "молчит": 0}
    with get_db_connection_context() as conn:
        for surface in кандидаты:
            v = вердикты.get(surface) or {"класс": "молчит", "база": ""}
            запись = {"verdict": v["класс"], "lemma": v.get("база", ""),
                      "reason": v.get("почему", ""), "fixed": 0}
            try:
                if v["класс"] == "форма" and apply:
                    with conn.cursor() as cur:
                        got = _fix_surface(cur, surface, v["база"])
                    conn.commit()
                    сводка["форм"] += 1
                    сводка["карточек"] += got["карточек"]
                    сводка["словарь"] += got["словарь"]
                    сводка["спорных"] += got["спорных"]
                    запись["fixed"] = got["карточек"]
                    if got["спорных"]:
                        запись["verdict"], запись["reason"] = "спорно", got["почему"]
                elif v["класс"] == "слово":
                    сводка["слов"] += 1
                elif v["класс"] == "спорно":
                    сводка["спорных"] += 1
                else:
                    сводка["молчит"] += 1
            except Exception:
                conn.rollback()
                logging.warning("форма-заголовок: %s не починено", surface, exc_info=True)
                continue
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bt_3_form_headword_checked (surface, verdict, lemma, reason, "
                    "fixed_cards, checked_at) VALUES (%s,%s,%s,%s,%s,NOW()) "
                    "ON CONFLICT (surface) DO UPDATE SET verdict=EXCLUDED.verdict, "
                    "lemma=EXCLUDED.lemma, reason=EXCLUDED.reason, "
                    "fixed_cards=EXCLUDED.fixed_cards, checked_at=NOW()",
                    (surface, запись["verdict"], запись["lemma"], запись["reason"], запись["fixed"]))
            conn.commit()
    return сводка


def pending_count() -> int:
    """Сколько написаний ещё ни разу не спрашивали. Для утреннего отчёта."""
    from backend.database import get_db_connection_context
    ensure_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            return len(_candidates(cur, 1_000_000))


def unfixed_forms_count() -> int:
    """Подтверждённых форм, которые ВСЁ ЕЩЁ стоят заголовками. Обещание: 0."""
    from backend.database import get_db_connection_context
    ensure_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM bt_3_webapp_dictionary_queries q
                 JOIN bt_3_form_headword_checked c
                   ON c.surface = regexp_replace(q.word_de, '^(der|die|das) ', '')
                WHERE c.verdict = 'форма'
            """)
            return int((cur.fetchone() or [0])[0] or 0)
