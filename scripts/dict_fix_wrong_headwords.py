"""Починить заголовки, которых в немецком языке НЕТ.

Откуда взялись. До 06.08.2026 вычитка стояла только на сохранении, а платили мы за
разбор раньше — на поиске. Поэтому кривое написание успевало стать заголовком единицы,
и человек видит карточкой слово, которого не существует: «Bestürtz», «DieAuslegung»,
«Abgabenrückständ». С 06.08 вход закрыт (`_dictionary_hit_or_corrected_word`), новых не
появляется — этот скрипт разбирает накопленное.

Почему именно эти 23. Замер 07.08.2026 нашёл 63 единицы, где модель вернула исправление
той же строки, а мы сохранили своё. Они распадаются на три разные кучи, и одинаково
обходиться с ними нельзя:
  A. 23 — такого написания в немецком нет. Это и чиним, список ниже.
  B. 24 — законная форма слова, а «исправление» это словарная форма («wuchsen» →
     «wachsen»). Ошибки правописания нет, вопрос продуктовый — решает владелец.
  C. 16 — «исправление» это просто другая фраза («todsicher wissen» → «todsicher sein»).
     Не трогаем.
Куча A перечислена поимённо, а не вычисляется правилом: отличить несуществующее слово от
законной формы правилом нельзя, это знание языка. Написание рядом с номером — сторож:
если в базе лежит уже другое, единица пропускается.

Почему не применяем сохранённый ответ модели вслепую. Часть этих ответов испорчена сама:
«Gefügig» → «gefűgig» (венгерская «ű»), «Angefordert» → «anforder». Поэтому пишем ТОЛЬКО
там, где два независимых источника сошлись: сохранённый ответ модели и наш нынешний
корректор (`run_quick_correct`) говорят одно и то же. Пишем при этом ответ МОДЕЛИ:
корректор возвращает написание с большой буквы («Bestürzt»), а у прилагательных и
наречий это неверно.

Разбор не трогаем и заново не покупаем: внутри карточки заголовок уже правильный
(`card->>'word_de'` = «bestürzt»), кривая только сама единица — а на экран идёт именно
она (`lex_units._build_item`: `item["word_de"] = german_display`).

Столкновение ключа. Если исправленное написание уже занято другой единицей («ernen» →
«ernten», а «Ernten» у нас есть), ключ не меняем — правим только видимое написание.
Слияние двух живых единиц это отдельная осознанная операция, скрипт её не делает.

По умолчанию НИЧЕГО НЕ ПИШЕТ и НИЧЕГО НЕ СПРАШИВАЕТ у модели.

    python scripts/dict_fix_wrong_headwords.py           # вхолостую
    python scripts/dict_fix_wrong_headwords.py --apply   # спросить корректор и записать
"""

from __future__ import annotations

import argparse
import os
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

import lex_units  # noqa: E402
from database import (  # noqa: E402
    _normalize_dictionary_headword_key,
    _normalize_dictionary_text_key,
    get_db_connection_context,
)
from dictionary_intake import clean_text  # noqa: E402

# (id единицы, написание, каким оно должно лежать в базе сейчас)
BROKEN_HEADWORDS = [
    (364, "ernen"),
    (568, "in heute verlandet Hafen bei Ostia zeigen"),
    (3400, "Das Zwiebeln ruft Weinen hervor."),
    (7990, "ansonst"),
    (15868, "abwert"),
    (16996, "Bestürtz"),
    (17941, "Strangußanlage"),
    (18504, "DieAuslegung"),
    (20728, "Abgabenrückständ"),
    (20908, "Hartnackig"),
    (20961, "Gefügig"),
    (22567, "Zusammenstöß"),
    (23017, "Wiederlege"),
    (23047, "Beschaffu"),
    (23075, "Abwesentheit"),
    (23080, "Dewählt"),
    (23213, "Erneubar"),
    (23601, "Verängstig"),
    (24120, "Umarm"),
    (24474, "DieBeförderung"),
    (24575, "Depremiert"),
    (25053, "DerEinstieg"),
    (25069, "Ausrufezeihen"),
]


CROOKED = dict(BROKEN_HEADWORDS)

# Написания, которые назвал ВЛАДЕЛЕЦ, разобрав список поимённо 07.08.2026. Здесь правило
# «два источника должны сойтись» не действует: оно нужно там, где решает машина, а тут
# решил человек. Часть этих случаев машина закрыть и не могла — «gefűgig» пришло от
# модели с венгерской «ű», а корректор ошибки вовсе не увидел.
OWNER_DECIDED = {
    568: "im heute verlandeten Hafen bei Ostia zeigen",
    3400: "Das Zwiebelschneiden ruft Weinen hervor",
    15868: "abwerten",
    # Машина непрерывного литья — от Strang (ручей заготовки), не от Strand (пляж).
    # Оба машинных ответа были мимо: «die Strandgußanlage» и «Straußanlage».
    17941: "die Stranggußanlage",
    20961: "gefügig",
    22567: "Zusammenstoß",
    23047: "Beschaffung",
    23017: "widerlegen",
    24120: "umarmen",
    25069: "das Ausrufezeichen",
}

# Ждут владельца: 23080 «Dewählt», 23601 «Verängstig» — исходное написание испорчено так,
# что восстановить задуманное слово нельзя ни машиной, ни человеком.


def _same(a: str, b: str) -> bool:
    return str(a or "").strip().casefold() == str(b or "").strip().casefold()


def _load(cur) -> dict:
    cur.execute(
        """SELECT id, lang, kind, pos, gender, lemma, display, lemma_key,
                  card->>'corrected_form', card->>'word_de'
           FROM bt_3_lex_units WHERE id = ANY(%s);""",
        ([uid for uid, _ in BROKEN_HEADWORDS],),
    )
    out = {}
    for row in cur.fetchall():
        out[int(row[0])] = {
            "id": int(row[0]), "lang": row[1], "kind": row[2], "pos": row[3],
            "gender": row[4], "lemma": row[5], "display": row[6],
            "lemma_key": row[7], "corrected_form": row[8], "card_word_de": row[9],
        }
    return out


def _key_taken(cur, unit: dict, new_key: str) -> int | None:
    """Номер ЧУЖОЙ единицы с тем же опознанием, что получится после переименования."""
    cur.execute(
        """SELECT id FROM bt_3_lex_units
           WHERE lang = %s AND kind = %s AND lemma_key = %s
             AND COALESCE(pos, '') = COALESCE(%s, '')
             AND COALESCE(gender, '') = COALESCE(%s, '')
             AND id <> %s
           LIMIT 1;""",
        (unit["lang"], unit["kind"], new_key, unit["pos"], unit["gender"], unit["id"]),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _identity_taken(cur, unit: dict, *, key: str, pos: str | None, gender: str | None) -> bool:
    cur.execute(
        """SELECT 1 FROM bt_3_lex_units
           WHERE lang = %s AND kind = %s AND lemma_key = %s
             AND COALESCE(pos, '') = COALESCE(%s, '')
             AND COALESCE(gender, '') = COALESCE(%s, '')
             AND id <> %s
           LIMIT 1;""",
        (unit["lang"], unit["kind"], key, pos, gender, unit["id"]),
    )
    return bool(cur.fetchone())


def _rewrite_everywhere(cur, unit: dict, new_text: str, *, rename_key: bool, crooked: str = "") -> dict:
    """Одно исправление во ВСЕХ хранилищах сразу: единица, её написания, личные карточки,
    старый общий словарь, кеш ответов. Половина работы тут хуже, чем ничего: пока хоть
    одно место помнит кривое написание, человек продолжит его видеть.

    Можно прогонять повторно: каждый шаг ищет старое написание и молчит, когда его уже
    нет. Так доделываются хвосты, если первый прогон закрыл не всё."""
    # Кривое написание ищем и по тому, что лежит в единице сейчас, и по исходному: после
    # переименования единица о нём уже не помнит, а хвосты в других хранилищах — помнят.
    # Сравнение ТОЧНОЕ, а не по регистру: «Gefügig» и «gefügig» — разные строки для поиска
    # в остальных хранилищах, и на этом одна запись уже уцелела после «готово».
    old_texts = {t for t in (unit["lemma"], unit["display"], crooked) if t and t != new_text}
    touched = {"единица": 0, "написаний": 0, "карточек": 0, "словарь": 0, "кеш": 0, "разбор снят": 0}

    # Артикль в написании не хранится — для него есть своя колонка. Иначе на экран уедет
    # «die die Auslegung»: заголовок берётся из написания, а артикль подставляется рядом.
    # Только у отдельного СЛОВА: у фразы ведущее «Das» — это подлежащее, а не артикль, и
    # снятие превращает «Das Zwiebelschneiden ruft Weinen hervor» в обрубок.
    article = lex_units.article_of(new_text) if unit["kind"] == "word" else ""
    bare_text = new_text[len(article):].strip() if article else new_text
    if not article and unit["kind"] == "word":
        # Артикля в исправлении может не быть, а в самом разборе он есть: «Abwesentheit»
        # лежала прилагательным, хотя её же разбор называет слово «die Abwesenheit».
        card_word = clean_text(unit.get("card_word_de") or "")
        if _same(lex_units.normalize_query(card_word), lex_units.normalize_query(bare_text)):
            article = lex_units.article_of(card_word)
    new_key = lex_units.normalize_query(bare_text)
    gender = unit["gender"]
    pos = unit["pos"]
    if unit["kind"] != "word":
        # У фразы нет ни рода, ни части речи. «Das Zwiebelschneiden…» успело полежать
        # существительным среднего рода — это след того же обрубания.
        gender, pos = None, None
    if article:
        # Артикль означает существительное — значит и часть речи у слова эта. Кривой
        # заголовок «DieAuslegung» выглядел прилагательным, отсюда и метка.
        want_gender, want_pos = article, "noun"
        if not _identity_taken(cur, unit, key=(new_key if rename_key else unit["lemma_key"]),
                               pos=want_pos, gender=want_gender):
            gender, pos = want_gender, want_pos

    if rename_key and new_key:
        cur.execute(
            """UPDATE bt_3_lex_units
               SET lemma = %s, display = %s, lemma_key = %s, gender = %s, pos = %s, updated_at = NOW()
               WHERE id = %s;""",
            (bare_text, bare_text, new_key, gender, pos, unit["id"]),
        )
        touched["единица"] = cur.rowcount or 0
        cur.execute(
            """INSERT INTO bt_3_lex_surfaces (lang, surface_key, unit_id, match_kind)
               VALUES (%s, %s, %s, 'exact') ON CONFLICT DO NOTHING;""",
            (unit["lang"], new_key, unit["id"]),
        )
        touched["написаний"] = cur.rowcount or 0
    else:
        # Ключ занят соседом — правим только видимое написание. Старый ключ остаётся
        # опечаточным входом: человек, набравший «ernen», попадёт сюда и увидит «ernten».
        cur.execute(
            """UPDATE bt_3_lex_units
               SET lemma = %s, display = %s, gender = %s, pos = %s, updated_at = NOW()
               WHERE id = %s;""",
            (bare_text, bare_text, gender, pos, unit["id"]),
        )
        touched["единица"] = cur.rowcount or 0

    # Разбор внутри покупался для КРИВОГО слова. Там, где он описывает уже не то слово,
    # что стоит заголовком («Zusammenstoß» с разбором глагола «zusammenstoßen»), снимаем
    # его — ночной добор соберёт заново. Оставить хуже: человек увидит верный заголовок и
    # чужой к нему разбор. Там, где разбор и заголовок про одно слово, ничего не трогаем и
    # заново не платим.
    card_word = clean_text(unit.get("card_word_de") or "")
    if card_word and lex_units.normalize_query(card_word) != new_key:
        drop_key = new_key if rename_key and new_key else unit["lemma_key"]
        if unit["kind"] == "word" and not _identity_taken(cur, unit, key=drop_key, pos=None, gender=None):
            # Часть речи и род тоже пришли из снятого разбора — пусть ночь поставит заново.
            cur.execute(
                """UPDATE bt_3_lex_units SET card = NULL, card_source = NULL,
                       pos = NULL, gender = NULL, updated_at = NOW() WHERE id = %s;""",
                (unit["id"],),
            )
        else:
            cur.execute(
                "UPDATE bt_3_lex_units SET card = NULL, card_source = NULL, updated_at = NOW() WHERE id = %s;",
                (unit["id"],),
            )
        touched["разбор снят"] = cur.rowcount or 0

    # Личная карточка хранит слово ВМЕСТЕ с артиклем («Die Strangußanlage»), а единица —
    # без него. Искать одну голую форму мало: так десять карточек у восьми человек
    # остались с кривым написанием после первого «готово». Перебираем формы с артиклем и
    # меняем только само слово, оставляя артикль карточки на месте.
    variants: dict[str, str] = {}
    for old in old_texts:
        old_article = lex_units.article_of(old)
        old_bare = old[len(old_article):].strip() if old_article else old
        for prefix in ("", "der ", "die ", "das ", "Der ", "Die ", "Das ",
                       "den ", "dem ", "des ", "Den ", "Dem ", "Des "):
            candidate = (prefix + old_bare) if prefix else old
            replacement = (prefix + bare_text) if prefix else new_text
            if candidate and candidate != replacement:
                variants[candidate] = replacement

    for old, replacement in variants.items():
        # Немецкая сторона карточки живёт в ДВУХ колонках: `word_de` и `translation_de`.
        # Правил только первую — и 22 карточки у восьми человек остались с кривым
        # написанием во второй, хотя отчёт показывал «готово».
        cur.execute(
            """UPDATE bt_3_webapp_dictionary_queries
               SET word_de = CASE WHEN word_de = %(old)s THEN %(new)s ELSE word_de END,
                   translation_de = CASE WHEN translation_de = %(old)s
                                         THEN %(new)s ELSE translation_de END,
                   updated_at = NOW()
               WHERE (word_de = %(old)s OR translation_de = %(old)s)
                 AND (lex_unit_id = %(uid)s OR lex_unit_id IS NULL);""",
            {"old": old, "new": replacement, "uid": unit["id"]},
        )
        touched["карточек"] += cur.rowcount or 0

        # Старый общий словарь. Немецкое слово стоит слева не всегда: половина этих
        # записей заведена запросом по-русски, и кривое написание лежит СПРАВА. Правим
        # обе стороны, иначе половина работы выдаётся за целую.
        cur.execute(
            """SELECT id, source_lang, target_lang FROM bt_3_dictionary_entries
               WHERE (source_lang = 'de' AND source_text = %s)
                  OR (target_lang = 'de' AND target_text = %s)
                  OR word_de = %s;""",
            (old, old, old),
        )
        for entry_id, src_lang, tgt_lang in cur.fetchall() or []:
            if str(src_lang) == "de":
                cur.execute(
                    """SELECT 1 FROM bt_3_dictionary_entries
                       WHERE source_lang = %s AND target_lang = %s AND source_text_norm = %s AND id <> %s
                       LIMIT 1;""",
                    (src_lang, tgt_lang, _normalize_dictionary_text_key(replacement), entry_id),
                )
                if cur.fetchone():
                    continue
                cur.execute(
                    """UPDATE bt_3_dictionary_entries
                       SET source_text = %s, source_text_norm = %s, source_headword_norm = %s,
                           word_de = %s, translation_de = %s, updated_at = NOW()
                       WHERE id = %s;""",
                    (
                        replacement,
                        _normalize_dictionary_text_key(replacement),
                        _normalize_dictionary_headword_key(replacement) or None,
                        replacement, replacement, entry_id,
                    ),
                )
            else:
                # Та же защита, что и на прямой стороне. Без неё исправление упиралось в
                # уже существующую верную запись той же пары, база отвергала весь шаг, и
                # вместе с ним откатывалась правка личной карточки.
                cur.execute(
                    """SELECT 1 FROM bt_3_dictionary_entries e
                       WHERE e.source_lang = %s AND e.target_lang = %s
                         AND e.source_text_norm = (SELECT source_text_norm
                                                   FROM bt_3_dictionary_entries WHERE id = %s)
                         AND e.target_text_norm = %s AND e.id <> %s
                       LIMIT 1;""",
                    (src_lang, tgt_lang, entry_id,
                     _normalize_dictionary_text_key(replacement), entry_id),
                )
                if cur.fetchone():
                    touched["словарь дубликат"] = touched.get("словарь дубликат", 0) + 1
                    continue
                cur.execute(
                    """UPDATE bt_3_dictionary_entries
                       SET target_text = CASE WHEN target_text = %(old)s THEN %(new)s ELSE target_text END,
                           target_text_norm = CASE WHEN target_text = %(old)s
                                                   THEN %(norm)s ELSE target_text_norm END,
                           target_headword_norm = CASE WHEN target_text = %(old)s
                                                       THEN %(head)s ELSE target_headword_norm END,
                           word_de = CASE WHEN word_de = %(old)s THEN %(new)s ELSE word_de END,
                           translation_de = CASE WHEN translation_de = %(old)s
                                                 THEN %(new)s ELSE translation_de END,
                           updated_at = NOW()
                       WHERE id = %(id)s;""",
                    {
                        "old": old, "new": replacement, "id": entry_id,
                        "norm": _normalize_dictionary_text_key(replacement),
                        "head": _normalize_dictionary_headword_key(replacement) or None,
                    },
                )
            touched["словарь"] += cur.rowcount or 0

        # Кеш ответов отдаёт карточку раньше всех остальных — не вычистив его, мы бы
        # правили базу, а человек продолжал бы видеть старое.
        cur.execute(
            "DELETE FROM bt_3_dictionary_lookup_cache WHERE lower(normalized_word) IN (%s, %s);",
            (str(old).strip().casefold(), lex_units.normalize_query(old)),
        )
        touched["кеш"] += cur.rowcount or 0
        cur.execute("DELETE FROM bt_3_dictionary_cache WHERE word_ru = %s;", (old,))
        touched["кеш"] += cur.rowcount or 0

    return touched


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            units = _load(cur)

    def _chosen(unit: dict) -> str:
        """Написание, которым чиним: слово владельца, если он его назвал, иначе ответ
        модели (его ещё предстоит подтвердить корректором)."""
        return clean_text(OWNER_DECIDED.get(unit["id"]) or unit["corrected_form"] or "")

    def _fixed_already(unit: dict) -> bool:
        """Единица уже переименована прошлым прогоном? Тогда спрашивать корректор не о
        чем — остаётся дописать хвосты в остальных хранилищах."""
        target = _chosen(unit)
        if not target:
            return False
        article = lex_units.article_of(target)
        bare = target[len(article):].strip() if article else target
        return _same(unit["lemma"], target) or _same(unit["lemma"], bare)

    missing = [(uid, txt) for uid, txt in BROKEN_HEADWORDS if uid not in units]
    changed = [
        (uid, txt) for uid, txt in BROKEN_HEADWORDS
        if uid in units and not _same(units[uid]["lemma"], txt) and not _fixed_already(units[uid])
    ]
    ready = [
        uid for uid, txt in BROKEN_HEADWORDS
        if uid in units and (_same(units[uid]["lemma"], txt) or _fixed_already(units[uid]))
    ]
    done = [uid for uid in ready if _fixed_already(units[uid])]

    print("КУЧА A (написания, которых в немецком нет): %d" % len(BROKEN_HEADWORDS))
    print("   на месте и готовы к разбору: %d (из них уже переименованы прошлым прогоном: %d)"
          % (len(ready), len(done)))
    for uid, txt in missing:
        print("   %s: единицы больше нет в базе (%r), пропуск" % (uid, txt))
    for uid, txt in changed:
        print("   %s: в базе теперь %r, а не %r — пропуск" % (uid, units[uid]["lemma"], txt))

    if not args.apply:
        print()
        for uid in ready:
            u = units[uid]
            print("   %-6s %r → %s %r"
                  % (uid, u["lemma"][:44],
                     "слово владельца" if uid in OWNER_DECIDED else "ответ модели",
                     _chosen(u)[:44]))
        print()
        print("ВХОЛОСТУЮ. Спросить корректор и записать: --apply")
        return 0

    from openai_manager import run_quick_correct

    applied, for_owner = [], []
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for uid in ready:
                unit = units[uid]
                stored = _chosen(unit)
                if uid not in done and uid not in OWNER_DECIDED:
                    try:
                        door = clean_text(run_quick_correct(text=unit["lemma"], source_lang=unit["lang"]) or "")
                    except Exception as exc:
                        for_owner.append((unit, stored, "", "корректор не ответил (%s)" % type(exc).__name__))
                        continue
                    if not stored:
                        for_owner.append((unit, stored, door, "ответа модели нет"))
                        continue
                    if not door:
                        for_owner.append((unit, stored, door, "корректор ошибки не видит"))
                        continue
                    if not _same(door, stored):
                        for_owner.append((unit, stored, door, "источники не сошлись"))
                        continue

                new_key = lex_units.normalize_query(stored)
                taken = _key_taken(cur, unit, new_key) if new_key and new_key != unit["lemma_key"] else None
                # Каждое слово — своя сделка. Иначе отказ базы на одном (а он был: запись
                # ru→de упёрлась в уже существующую верную) откатывает и остальные, причём
                # молча: в отчёте остаются нули, будто чинить было нечего.
                try:
                    touched = _rewrite_everywhere(
                        cur, unit, stored,
                        rename_key=not taken and bool(new_key),
                        crooked=CROOKED.get(uid, ""),
                    )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    for_owner.append((unit, stored, "", "база отказала: %s" % str(exc).splitlines()[0][:80]))
                    continue
                applied.append((unit, stored, taken, touched))
                print("   %-6s %r → %r%s   %s"
                      % (uid, unit["lemma"][:38], stored[:38],
                         "  (ключ занят единицей %s, оставлен прежним)" % taken if taken else "",
                         touched))

    print()
    print("ИСПРАВЛЕНО: %d" % len(applied))
    print("НА РЕШЕНИЕ ВЛАДЕЛЬЦУ: %d" % (len(for_owner) + len(missing) + len(changed)))
    for unit, stored, door, why in for_owner:
        print("   %-6s %-42r %s" % (unit["id"], unit["lemma"][:40], why))
        print("          ответ модели: %r    корректор: %r" % (stored[:44], door[:44]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
