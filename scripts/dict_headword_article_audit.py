"""Сверить артикль у каждого немецкого заголовка с арбитром рода и починить расхождения.

Зачем. Владелец нашёл карточку «die Titanic» с примером «Der Titanic sank». Проверка
показала, что regex такое не различает: «der Einwohner» → «Die Einwohner tragen…» —
законное множественное число, а «Das Hoden» → «der Hoden» — настоящая ошибка рода.
Отличить одно от другого догадкой нельзя.

Кого спрашиваем. `backend/article_authority.authoritative_article` — тот самый арбитр,
который уже решает род в игре с артиклями. Он знает три вещи, которых не знает голая
сверка со справочником:
  • выверенный список ДВУРОДОВЫХ слов («der Kiefer» челюсть / «die Kiefer» сосна,
    «der Schild» щит / «das Schild» вывеска, «das Gehalt» зарплата / «der Gehalt»
    содержание) — у них оба артикля верны, и трогать их нельзя;
  • правило композита по 19 тысячам родов, а не по списку из шестидесяти;
  • честное «не знаю» вместо догадки.

Правим ТОЛЬКО когда арбитр назвал ровно один род и он не совпадает с записанным.
Молчание арбитра уликой не считается.

Проверяются ОБА места, где человек видит артикль: слово в общем словаре и заголовок
личной карточки. Это разные записи, и в карточке может лежать свой артикль.

К модели не обращаемся ни разу.

По умолчанию НИЧЕГО НЕ ПИШЕТ. Запись — только с --apply.

    python scripts/dict_headword_article_audit.py                  # вхолостую, слова
    python scripts/dict_headword_article_audit.py --cards          # вхолостую, карточки
    python scripts/dict_headword_article_audit.py --apply --cards  # починить карточки
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_here = os.path.dirname(os.path.abspath(globals().get("__file__", ".")))
sys.path.insert(0, os.path.join(_here, ".."))
sys.path.insert(0, os.path.join(_here, "..", "backend"))
sys.path.insert(0, "/app/backend")

from database import get_db_connection_context  # noqa: E402
from article_authority import authoritative_article  # noqa: E402
from article_wiktionary_ref import plurals_by_article_for_titles  # noqa: E402

HEAD_RE = re.compile(r"^\s*(der|die|das)\s+([A-ZÄÖÜ][\wÄÖÜäöüß-]*)\s*$", re.IGNORECASE)

# Субстантивированные прилагательные: род идёт за человеком, а не за словом —
# «der Abgeordnete» (депутат) и «die Abgeordnete» (депутатка) оба верны. Справочник
# держит для них ОДНУ страницу, обычно женскую, и предлагает «исправить» верный
# мужской род на женский. Такие заголовки не трогаем.
NOMINALIZED_ADJECTIVES = {
    "abgeordnete", "angestellte", "beamte", "bekannte", "betreffende", "deutsche",
    "erwachsene", "gefangene", "jugendliche", "kranke", "militärangehörige",
    "reisende", "tote", "verlobte", "verletzte", "verwandte", "vorgesetzte",
    "vorsitzende", "weise", "auszubildende", "obdachlose", "arbeitslose",
}


def collect_units(cur) -> list:
    cur.execute(
        """SELECT id, display FROM bt_3_lex_units
           WHERE lang = 'de' AND kind = 'word' AND display ~* '^(der|die|das) [A-ZÄÖÜ]'
           ORDER BY id;"""
    )
    return _parse(cur.fetchall())


def collect_cards(cur) -> list:
    cur.execute(
        """SELECT id, word_de FROM bt_3_webapp_dictionary_queries
           WHERE word_de ~* '^(der|die|das) [A-ZÄÖÜ][^ ]*$' ORDER BY id;"""
    )
    return _parse(cur.fetchall())


def _parse(rows) -> list:
    out = []
    for row_id, text in rows:
        m = HEAD_RE.match(text or "")
        if m:
            out.append((row_id, text, m.group(1).lower(), m.group(2)))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--cards", action="store_true", help="заголовки карточек, а не слов")
    args = parser.parse_args()

    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            rows = collect_cards(cur) if args.cards else collect_units(cur)
            print("%s С АРТИКЛЕМ: %d"
                  % ("КАРТОЧЕК" if args.cards else "СЛОВ", len(rows)))

            wrong, unknown, ok, guessed = [], 0, 0, 0
            for row_id, text, article, noun in rows:
                right, basis = authoritative_article(noun, allow_network=False)
                if not right:
                    unknown += 1
                    continue
                if right == article:
                    ok += 1
                    continue
                # Правилу композита здесь не доверяем: у субстантивированных
                # прилагательных оно берёт «головой» окончание — «Vorsitzende» → «Ende»
                # → das, и предлагает «das Vorsitzende» вместо «der Vorsitzende».
                # Для заголовков нужна прямая запись справочника, не вывод.
                if basis != "wiktionary":
                    guessed += 1
                    continue
                if noun.lower() in NOMINALIZED_ADJECTIVES:
                    guessed += 1
                    continue
                wrong.append((row_id, text, article, right, noun, basis))

            # Множественное число — не ошибка. «die Reifen», «die Anführungszeichen»,
            # «die Mitfahrer», «die Eichen» — верные заголовки, хотя единственное у них
            # мужское или среднее. Признак «слово помечено как множественное» здесь не
            # работает: у Reifen множественное СОВПАДАЕТ с единственным, и справочник
            # честно отдаёт страницу леммы. Поэтому спрашиваем прямо: какая у слова
            # форма множественного — и если она равна нашему написанию, «die» верно.
            plural_skipped = []
            suspects = [w for w in wrong if w[2] == "die"]
            titles = set()
            for w in suspects:
                noun = w[4]
                titles.add(noun[:1].upper() + noun[1:])
                for cut in (noun[:-1], noun[:-2]):   # Eichen → Eiche, Wagen → Wag
                    if len(cut) > 3:
                        titles.add(cut[:1].upper() + cut[1:])
            plurals = {}
            ordered = sorted(titles)
            for i in range(0, len(ordered), 40):
                try:
                    plurals.update(plurals_by_article_for_titles(ordered[i:i + 40]))
                except Exception as exc:
                    print("   справочник о множественном не ответил: %s" % exc)

            def _is_plural_form(noun: str) -> bool:
                for candidate in (noun, noun[:-1], noun[:-2]):
                    if len(candidate) < 3:
                        continue
                    page = plurals.get(candidate[:1].upper() + candidate[1:]) or {}
                    if any(str(form).lower() == noun.lower() for form in page.values()):
                        return True
                return False

            keep = []
            for item in wrong:
                if item[2] == "die" and _is_plural_form(item[4]):
                    plural_skipped.append(item)
                else:
                    keep.append(item)
            wrong = keep

            print("АРБИТР ПОДТВЕРДИЛ:            %d" % ok)
            print("АРБИТР МОЛЧИТ (не трогаем):   %d" % unknown)
            print("ТОЛЬКО ДОГАДКА (не трогаем):  %d" % guessed)
            print("МНОЖЕСТВЕННОЕ ЧИСЛО (верно):  %d" % len(plural_skipped))
            for row_id, text, _a, _r, _n, _b in plural_skipped[:10]:
                print("      оставили как есть: %s %r" % (row_id, text[:40]))
            print("НЕВЕРНЫЙ АРТИКЛЬ:             %d" % len(wrong))
            for row_id, text, _a, right, noun, basis in wrong[:50]:
                print("   %s  %r → %r   (%s)" % (row_id, text[:40], "%s %s" % (right, noun), basis))

            if not args.apply:
                print()
                print("ВХОЛОСТУЮ. Починить: --apply")
                return 0

            fixed = 0
            for row_id, text, _a, right, noun, _basis in wrong:
                corrected = "%s %s" % (right, noun)
                if args.cards:
                    cur.execute(
                        """UPDATE bt_3_webapp_dictionary_queries
                           SET word_de = %s,
                               response_json = CASE WHEN response_json ? 'article'
                                   THEN jsonb_set(
                                          jsonb_set(response_json, '{article}', to_jsonb(%s::text)),
                                          '{word_de}', to_jsonb(%s::text))
                                   ELSE response_json END
                           WHERE id = %s;""",
                        (corrected, right, corrected, row_id),
                    )
                else:
                    cur.execute(
                        """UPDATE bt_3_lex_units
                           SET display = %s, lemma = %s, gender = %s,
                               card = CASE WHEN card ? 'article'
                                   THEN jsonb_set(
                                          jsonb_set(card, '{article}', to_jsonb(%s::text)),
                                          '{word_de}', to_jsonb(%s::text))
                                   ELSE card END
                           WHERE id = %s;""",
                        (corrected, corrected, right, right, corrected, row_id),
                    )
                    # Карточки, держащие это слово, показывают своё написание — правим и их.
                    cur.execute(
                        """UPDATE bt_3_webapp_dictionary_queries
                           SET word_de = %s
                           WHERE lex_unit_id = %s AND lower(btrim(word_de)) = lower(%s);""",
                        (corrected, row_id, text),
                    )
                fixed += 1
            conn.commit()
    print()
    print("ИСПРАВЛЕНО: %d" % fixed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
