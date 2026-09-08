"""Дверь приёма списка синонимов/антонимов спринта (`bt_3_sprint_bank.accepted`).

Повод (06.09.2026). Пользователь: «из 8 раз в 5 был вариант die Option». Список
`accepted` у «die Gelegenheit» нёс «die Option» шесть раз с разными переводами и само
«die Gelegenheit» дважды; в банке таких записей было 9 из 56, у четырёх существительных
артикль расходился со справочником («die Potenzial»), 23 «синонима» не существовали ни в
одном словаре («rememembern»). Список писала модель по промпту «18–35 синонимов, как можно
больше», и он шёл в базу без единой проверки. Прежний дефект того же класса (самослово)
чинили тремя фильтрами НИЖЕ по течению — `answer_eval.load_trainer_task`,
`TrainerGame.buildRounds` — а дверь так и стояла открытой.

Решение владельца 06.09.2026 (все пункты «ok», стратегия —
docs/tasks/synonym_intake_gate_strategy.md):

1. дубли и самослово убираются;
2. синоним обязан подтвердиться источником (OpenThesaurus или {{Synonyme}} de.wiktionary,
   `synonym_sources.confirm_synonyms`); не подтвердился — не показывается, уходит владельцу
   с кнопками, В ПИСЬМЕ ОБЯЗАТЕЛЬНО артикль, перевод и вердикт справочника;
3. артикль существительного сверяется со справочником рода
   (`article_authority.authoritative_article`); расхождение и «не знаю» — владельцу;
4. слово принимается от ТРЁХ подтверждённых; квота из промпта убрана;
5. накопленное чистится тем же правилом (ночная гигиена + скрипт), а не руками.

Одна функция `clean_accepted` на оба входа — новое слово (`bot_3._sprint_topup`) и
накопленное (`hygiene_pass`). Всё, что дверь не пропустила и что требует решения,
ложится в `bt_3_sprint_accepted_review`; письмо с кнопками — `sprint_accepted_review.py`.

Антонимы (владелец 06.09.2026: «механика та же самая»): дедуп, самослово, артикль,
подтверждение {{Gegenwörter}} de.wiktionary (OpenThesaurus антонимов не знает), порог 3;
неподтверждённое — судье (`synonym_judge.py`), сомнения судьи — владельцу.

СУДЬЯ (владелец 06.09.2026: «если модель говорит да — зачем я? если нет — тоже зачем я?»):
всё, что дверь не пропустила и что требует решения, сперва судит модель подстановкой
(backend/synonym_judge.py). «Да» — входит само, «нет» — снимается само, владельцу
уходит ТОЛЬКО «сомневаюсь» и «да» при неизвестном справочнику артикле.

ВТОРОЙ ЗАХОД (владелец 08.09.2026, «Do it»): человека из цепочки убрать совсем.
«Я человек, не модель, я всё равно пойду советоваться с моделью или со словарём. Модель
ставит итоговую точку.» Что изменилось (стратегия, раздел «Второй заход»):
- генератор даёт ПОЛНЫЙ список, отсев — дверь и судья;
- антонимы получили второй источник — косвенную антонимию из двух словарей
  (`synonym_sources.confirm_relation`, `by='indirect'`);
- слово без страницы в Wiktionary, которого не знает и OpenThesaurus, снимается ДО судьи
  (`NO_DICTIONARY`) — «befehlsgebunden» вошло по «да» судьи, а его нет ни в одном словаре;
- судья без «сомневаюсь», три голоса, большинство; неизвестный артикль после похода в
  Wiktionary — слово снимается и СЧИТАЕТСЯ (`decision='article_unknown'`);
- письмо 12:45 с кнопками отменено; очередь `bt_3_sprint_accepted_review` — след решений,
  а не список для человека;
- дверь помнит принятое (`decided_keep`): перепроверка не снимает слово, которое судья
  или владелец уже впустили.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

MIN_ACCEPTED = {"synonym": 3, "antonym": 3}   # 3 — решение владельца 06.09.2026;
                                               # антонимы: «механика та же самая» (06.09).

_ARTICLES = ("der", "die", "das")

# Причины отказа. Дубль, самослово и «нет ни в одном словаре» — окончательные (решать
# нечего); остальные три уходят судье-модели (backend/synonym_judge.py), который и ставит
# точку. Владельцу с 08.09.2026 не уходит ничего.
DUPLICATE, SELF, NO_DICTIONARY = "duplicate", "self", "no_dictionary"
ARTICLE_MISMATCH, ARTICLE_UNKNOWN, UNCONFIRMED = "article_mismatch", "article_unknown", "unconfirmed"
FOR_JUDGE = (ARTICLE_MISMATCH, ARTICLE_UNKNOWN, UNCONFIRMED)
ASK_OWNER = FOR_JUDGE   # прежнее имя (до 08.09.2026); оставлено для старых вызовов
ALL_REASONS = (DUPLICATE, SELF, NO_DICTIONARY, ARTICLE_MISMATCH, ARTICLE_UNKNOWN, UNCONFIRMED)
# Решения владельца кнопкой — единственные, которые дверь не пересматривает даже по
# правилу «нет в словаре» (он видел слово и сказал «оставить»).
OWNER_DECISIONS = ("keep", "der", "die", "das")


@dataclass
class Rejected:
    de: str
    ru: str
    reason: str                         # главная причина (по ней кнопки)
    reasons: list[str] = field(default_factory=list)
    stored_article: str = ""            # что стояло у модели
    noun: str = ""                      # само существительное без артикля
    reference_article: str = ""         # что говорит справочник
    reference_source: str = ""
    confirmed_by: list[str] = field(default_factory=list)
    ot_knows_candidate: bool | None = None
    wikt_candidate: str = ""
    wikt_target: str = ""
    via: str = ""                       # косвенная антонимия: через какое слово


@dataclass
class GateResult:
    kept: list[dict]                    # [{de, ru}] — то, что можно показывать
    rejected: list[Rejected]
    stats: dict                         # {duplicate, self, article_mismatch, article_unknown, unconfirmed}

    @property
    def enough(self) -> bool:
        return self.min_needed <= len(self.kept)

    min_needed: int = 3


def _split_noun(de: str) -> tuple[str, str] | None:
    """('die', 'Option') для существительного; None — не существительное.

    Существительное — либо артикль + одно слово, либо одно слово с заглавной буквы без
    артикля. Фразы («zur Verfügung geben») и артикль с несколькими словами артиклем не
    проверяются — справочник рода отвечает про слово, а не про оборот."""
    toks = str(de or "").split()
    if len(toks) == 2 and toks[0].lower() in _ARTICLES:
        return toks[0].lower(), toks[1]
    if len(toks) == 1 and toks[0][:1].isupper():
        return "", toks[0]
    return None


def clean_accepted(wort: str, relation: str, pairs: list[dict], *,
                   confirm: Callable[[str, list[str]], dict] | None = None,
                   article: Callable[[str], tuple[str | None, str]] | None = None,
                   decided_keep: dict[str, str] | None = None,
                   ) -> GateResult:
    """Прогнать список через дверь. `confirm` и `article` подставляются в тестах; по
    умолчанию — живые источники (`synonym_sources.confirm_relation`,
    `article_authority.authoritative_article`, без сети у справочника рода).

    `decided_keep` — {de.lower(): decision} того, что уже впущено решением (судья
    `judge_yes`, владелец `keep`/`der`/`die`/`das`): дверь это не пересматривает —
    иначе перепроверка снимала бы принятое слово молча (найдено 08.09.2026). Одно
    исключение: «нет ни в одном словаре» бьёт решение судьи (он смысл судил, а не
    существование), но не решение владельца."""
    from backend.synonym_sources import term_key
    if confirm is None:
        from backend.synonym_sources import confirm_relation
        confirm = lambda t, c, rel: confirm_relation(t, c, relation=rel)   # noqa: E731
    if article is None:
        from backend.article_authority import authoritative_article
        article = lambda n: authoritative_article(n)                 # noqa: E731

    from backend.synonym_sources import _page_title
    stats = {k: 0 for k in ALL_REASONS}
    rejected: list[Rejected] = []
    seen: set[str] = set()
    target_key = term_key(wort)
    survivors: list[dict] = []
    decided = {str(k).lower(): str(v) for k, v in (decided_keep or {}).items()}

    # 1–2. Дубли (первое вхождение остаётся — с ЕГО переводом; решение владельца
    # 06.09.2026: берём первый ответ модели, сами не выбираем) и самослово.
    for p in pairs or []:
        de = str((p or {}).get("de") or "").strip()
        ru = str((p or {}).get("ru") or "").strip()
        de = re.sub(r"\s+", " ", de)
        if not de:
            continue
        key = de.lower()
        if key in seen:
            stats[DUPLICATE] += 1
            rejected.append(Rejected(de, ru, DUPLICATE, [DUPLICATE]))
            continue
        seen.add(key)
        if term_key(de) == target_key:
            stats[SELF] += 1
            rejected.append(Rejected(de, ru, SELF, [SELF]))
            continue
        survivors.append({"de": de, "ru": ru})

    # Подтверждение источником; один запрос на всё слово. Синонимы — OpenThesaurus или
    # Wiktionary {{Synonyme}}; антонимы — Wiktionary {{Gegenwörter}} либо косвенная
    # антонимия через гнездо OpenThesaurus прямой противоположности (08.09.2026).
    # Неподтверждённое дальше судит модель (synonym_judge) — и ставит точку.
    conf = {}
    if survivors:
        conf = confirm(wort, [s["de"] for s in survivors], relation)

    kept: list[dict] = []
    for s in survivors:
        de, ru = s["de"], s["ru"]
        reasons: list[str] = []
        rej = Rejected(de, ru, "", [])
        c = conf.get(de)
        if c is not None:
            rej.confirmed_by = list(c.by)
            rej.ot_knows_candidate = c.ot_knows_candidate
            rej.wikt_candidate, rej.wikt_target = c.wikt_candidate, c.wikt_target
            rej.via = str(getattr(c, "via", "") or "")
        decision = decided.get(de.lower(), "")
        # 3. Слово обязано существовать. Однословный кандидат без страницы в Wiktionary,
        #    которого не знает и OpenThesaurus, — не слово, а догадка модели; снимается
        #    до судьи. «Не удалось проверить» (сеть) — не «нет страницы». Обороты из
        #    нескольких слов существованием не проверяются — их судит судья.
        single_word = " " not in _page_title(de)
        if (c is not None and single_word and getattr(c, "exists_in_dictionaries", True) is False
                and decision not in OWNER_DECISIONS):
            rej.reason, rej.reasons = NO_DICTIONARY, [NO_DICTIONARY]
            stats[NO_DICTIONARY] += 1
            rejected.append(rej)
            continue
        if decision:
            kept.append({"de": de, "ru": ru})
            continue
        # 4. Артикль по справочнику.
        noun = _split_noun(de)
        if noun:
            stored, word = noun
            rej.stored_article, rej.noun = stored, word
            ref, src = article(word)
            rej.reference_article, rej.reference_source = str(ref or ""), str(src or "")
            if ref is None:
                reasons.append(ARTICLE_UNKNOWN)
            elif ref != stored:
                reasons.append(ARTICLE_MISMATCH)
        # 5. Подтверждение источником.
        if c is not None and not c.confirmed:
            reasons.append(UNCONFIRMED)
        if reasons:
            rej.reason, rej.reasons = reasons[0], reasons
            stats[reasons[0]] += 1
            rejected.append(rej)
            continue
        kept.append({"de": de, "ru": ru})
    return GateResult(kept=kept, rejected=rejected, stats=stats,
                      min_needed=MIN_ACCEPTED.get(relation, 3))


# ── хранилище решений владельца ───────────────────────────────────────────────

def ensure_sprint_intake_schema() -> None:
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE bt_3_sprint_bank
                    ADD COLUMN IF NOT EXISTS accepted_checked_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS retired_reason TEXT NOT NULL DEFAULT '';
                CREATE TABLE IF NOT EXISTS bt_3_sprint_accepted_review (
                    id                 BIGSERIAL PRIMARY KEY,
                    sprint_id          TEXT NOT NULL,
                    relation           TEXT NOT NULL,
                    wort               TEXT NOT NULL,
                    hint_ru            TEXT NOT NULL DEFAULT '',
                    de                 TEXT NOT NULL,
                    de_key             TEXT NOT NULL,
                    ru                 TEXT NOT NULL DEFAULT '',
                    reason             TEXT NOT NULL,
                    reasons            JSONB NOT NULL DEFAULT '[]'::jsonb,
                    stored_article     TEXT NOT NULL DEFAULT '',
                    noun               TEXT NOT NULL DEFAULT '',
                    reference_article  TEXT NOT NULL DEFAULT '',
                    reference_source   TEXT NOT NULL DEFAULT '',
                    confirmed_by       JSONB NOT NULL DEFAULT '[]'::jsonb,
                    ot_knows_candidate BOOLEAN,
                    wikt_candidate     TEXT NOT NULL DEFAULT '',
                    wikt_target        TEXT NOT NULL DEFAULT '',
                    status             TEXT NOT NULL DEFAULT 'open',
                    decision           TEXT NOT NULL DEFAULT '',
                    asked_at           TIMESTAMPTZ,
                    decided_at         TIMESTAMPTZ,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (sprint_id, de_key)
                );
                ALTER TABLE bt_3_sprint_accepted_review
                    ADD COLUMN IF NOT EXISTS judge_verdict TEXT,
                    ADD COLUMN IF NOT EXISTS judge_reason TEXT NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS judge_example_target TEXT NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS judge_example_candidate TEXT NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS judge_voice TEXT NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS judged_at TIMESTAMPTZ,
                    ADD COLUMN IF NOT EXISTS via TEXT NOT NULL DEFAULT '';
                """
            )
        conn.commit()


def queue_for_judge(*, sprint_id: str, relation: str, wort: str, hint_ru: str,
                    rejected: list[Rejected]) -> int:
    """Положить в очередь судье то, что требует решения (FOR_JUDGE), и оставить след о
    снятом без словаря (NO_DICTIONARY — сразу status='removed', чтобы в базе было видно,
    почему слова нет). Дубли и самослово не кладём — там решать нечего. Уже лежащее
    (та же пара) не задваивается. Возвращает число строк, ушедших судье."""
    from backend.database import get_db_connection_context
    rows = [r for r in rejected if r.reason in FOR_JUDGE or r.reason == NO_DICTIONARY]
    if not rows:
        return 0
    ensure_sprint_intake_schema()
    n = 0
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            for r in rows:
                final = r.reason == NO_DICTIONARY
                cur.execute(
                    """
                    INSERT INTO bt_3_sprint_accepted_review
                        (sprint_id, relation, wort, hint_ru, de, de_key, ru, reason, reasons,
                         stored_article, noun, reference_article, reference_source,
                         confirmed_by, ot_knows_candidate, wikt_candidate, wikt_target, via,
                         status, decision, decided_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,
                            %s,%s,%s)
                    ON CONFLICT (sprint_id, de_key) DO NOTHING
                    """,
                    (sprint_id, relation, wort, hint_ru, r.de, r.de.lower(), r.ru, r.reason,
                     json.dumps(r.reasons), r.stored_article, r.noun, r.reference_article,
                     r.reference_source, json.dumps(r.confirmed_by), r.ot_knows_candidate,
                     r.wikt_candidate, r.wikt_target, r.via,
                     "removed" if final else "open", NO_DICTIONARY if final else "",
                     datetime.now(timezone.utc) if final else None),
                )
                if not final:
                    n += cur.rowcount
        conn.commit()
    return n


queue_for_owner = queue_for_judge   # прежнее имя (до 08.09.2026)


def load_decided_keep(sprint_id: str) -> dict[str, str]:
    """{de.lower(): decision} всего, что по этому слову уже впущено решением (судьи или
    владельца). Ключ — форма, которая ЛЕЖИТ в accepted (у существительного — с артиклем
    решения), потому что дверь сверяет именно её."""
    from backend.database import get_db_connection_context
    ensure_sprint_intake_schema()
    out: dict[str, str] = {}
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT de, noun, decision FROM bt_3_sprint_accepted_review "
                        "WHERE sprint_id = %s AND status = 'kept'", (sprint_id,))
            for de, noun, decision in cur.fetchall() or []:
                dec = str(decision or "")
                out[str(de or "").lower()] = dec
                if dec in _ARTICLES and noun:
                    out[f"{dec} {noun}".lower()] = dec
    return out


def count_open_reviews(*, judged_only: bool = False) -> int:
    """Сколько открытых строк очереди. С 08.09.2026 открытое ждёт СУДЬЮ (ночь 03:10 и
    сразу после набора), а не владельца; judged_only оставлен для старых вызовов."""
    from backend.database import get_db_connection_context
    ensure_sprint_intake_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bt_3_sprint_accepted_review WHERE status = 'open'"
                        + (" AND judge_verdict IS NOT NULL" if judged_only else ""))
            return int((cur.fetchone() or [0])[0] or 0)


def count_unjudged_reviews() -> int:
    from backend.database import get_db_connection_context
    ensure_sprint_intake_schema()
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bt_3_sprint_accepted_review "
                        "WHERE status = 'open' AND judge_verdict IS NULL")
            return int((cur.fetchone() or [0])[0] or 0)


def list_open_reviews(limit: int) -> list[dict]:
    from backend.database import get_db_connection_context
    ensure_sprint_intake_schema()
    cols = ("id", "sprint_id", "relation", "wort", "hint_ru", "de", "ru", "reason", "reasons",
            "stored_article", "noun", "reference_article", "reference_source", "confirmed_by",
            "ot_knows_candidate", "wikt_candidate", "wikt_target",
            "judge_verdict", "judge_reason", "judge_example_target", "judge_example_candidate", "judge_voice")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            # Владельцу — только то, где судья уже высказался (сомнение либо «да» при
            # неизвестном артикле). Несудимое ждёт судью, а не человека.
            cur.execute(
                f"SELECT {', '.join(cols)} FROM bt_3_sprint_accepted_review "
                "WHERE status = 'open' AND judge_verdict IS NOT NULL ORDER BY wort, id LIMIT %s", (int(limit),),
            )
            rows = cur.fetchall() or []
    return [dict(zip(cols, r)) for r in rows]


def mark_asked(ids: list[int]) -> None:
    from backend.database import get_db_connection_context
    if not ids:
        return
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE bt_3_sprint_accepted_review SET asked_at = NOW() WHERE id = ANY(%s)",
                        ([int(i) for i in ids],))
        conn.commit()


def apply_decision(row_id: int, decision: str, *, by: str = "owner",
                   label: str | None = None) -> dict | None:
    """«keep» / «der|die|das» — кандидат входит в accepted (существительное — с артиклем
    решения или справочника); «drop» — остаётся снятым. None — уже решено.
    `by` — кто решил: 'owner' (кнопка на старом письме) или 'judge' (модель); пишется в
    decision, чтобы в базе было видно, чьё это слово. `label` — своя подпись решения
    (например 'article_unknown': судья сказал «да», а артикля не знает никто)."""
    from backend.database import get_db_connection_context
    dec = str(decision or "").strip().lower()
    if dec not in ("keep", "drop", *_ARTICLES):
        raise ValueError(f"неизвестное решение: {decision!r}")
    who = str(by or "owner")
    decision_label = label or (dec if who == "owner" else f"judge_{'no' if dec == 'drop' else 'yes'}")
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sprint_id, wort, de, ru, noun, stored_article, reference_article, relation "
                "FROM bt_3_sprint_accepted_review WHERE id = %s AND status = 'open' FOR UPDATE",
                (int(row_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            sprint_id, wort, de, ru, noun, stored, ref, relation = row
            final_de = de
            if dec in _ARTICLES:
                if not noun:
                    raise ValueError("артикль можно ставить только существительному")
                final_de = f"{dec} {noun}"
            elif dec == "keep" and noun and ref and ref != stored:
                # Владелец жмёт «оставить» на карточке, где справочник назвал артикль:
                # входит форма справочника, а не модели (она и была поводом вопроса).
                final_de = f"{ref} {noun}"
            elif dec == "keep" and noun and not ref and not stored:
                raise ValueError("у существительного нет артикля — нужна кнопка der/die/das")
            if dec == "drop":
                cur.execute(
                    "UPDATE bt_3_sprint_accepted_review SET status = 'removed', decision = %s, "
                    "decided_at = NOW() WHERE id = %s", (decision_label, int(row_id)),
                )
                # Пример к снятому слову больше не нужен — иначе тренажёр показал бы
                # карточку «верного выбора» для слова, которого в списке нет.
                cur.execute("SELECT accepted, trainer_json FROM bt_3_sprint_bank WHERE sprint_id = %s FOR UPDATE",
                            (sprint_id,))
                bank = cur.fetchone()
                if bank:
                    ex = list((bank[1] or {}).get("correct_examples") or [])
                    new_ex = [e for e in ex if str((e or {}).get("word") or "").strip().lower() != de.lower()]
                    if len(new_ex) != len(ex):
                        tj = dict(bank[1] or {}); tj["correct_examples"] = new_ex
                        cur.execute("UPDATE bt_3_sprint_bank SET trainer_json = %s::jsonb WHERE sprint_id = %s",
                                    (json.dumps(tj, ensure_ascii=False), sprint_id))
                conn.commit()
                return {"sprint_id": sprint_id, "wort": wort, "de": de, "ru": ru, "kept": False}
            cur.execute("SELECT accepted, retired, retired_reason FROM bt_3_sprint_bank "
                        "WHERE sprint_id = %s FOR UPDATE", (sprint_id,))
            bank = cur.fetchone()
            if not bank:
                raise RuntimeError(f"записи банка {sprint_id} больше нет")
            accepted = list(bank[0] or [])
            if not any(str((a or {}).get("de") or "").strip().lower() == final_de.lower() for a in accepted):
                accepted.append({"de": final_de, "ru": ru})
            unretired = False
            if bank[1] and str(bank[2] or "") == "thin_accepted" and \
                    len(accepted) >= MIN_ACCEPTED.get(relation, 3):
                unretired = True
            cur.execute(
                "UPDATE bt_3_sprint_bank SET accepted = %s::jsonb"
                + (", retired = FALSE, retired_reason = ''" if unretired else "")
                + " WHERE sprint_id = %s",
                (json.dumps(accepted, ensure_ascii=False), sprint_id),
            )
            cur.execute(
                "UPDATE bt_3_sprint_accepted_review SET status = 'kept', decision = %s, "
                "decided_at = NOW() WHERE id = %s", (decision_label, int(row_id)),
            )
        conn.commit()
    return {"sprint_id": sprint_id, "wort": wort, "de": final_de, "ru": ru, "kept": True,
            "unretired": unretired, "article_changed": final_de != de}


apply_owner_decision = apply_decision   # прежнее имя (до 08.09.2026)


# ── применение двери к записи банка (накопленное) ─────────────────────────────

def pending_example_keys(res: "GateResult") -> set[str]:
    """Кандидаты, у которых решение ещё впереди (судья) — их примеры живут."""
    return {r.de.lower() for r in res.rejected if r.reason in FOR_JUDGE}


def _filter_examples(trainer_json: dict, kept_de: set[str]) -> tuple[dict, int]:
    """Примеры «верного выбора» строились по старому accepted; снятое слово не должно
    остаться раундом. Ничего не перегенерируем (0 запросов к модели): фильтр."""
    tj = dict(trainer_json or {})
    ex = list(tj.get("correct_examples") or [])
    # Примеры строились по СТАРОМУ accepted, где «die Option» лежала шесть раз, — значит
    # и пример к ней лежит несколько раз. Первое вхождение остаётся, остальные — дубли.
    # Поймано 06.09.2026 на «экране после»: список очистился, а примеры — нет.
    seen: set[str] = set()
    new_ex = []
    for e in ex:
        key = str((e or {}).get("word") or "").strip().lower()
        if key in kept_de and key not in seen:
            seen.add(key)
            new_ex.append(e)
    tj["correct_examples"] = new_ex
    return tj, len(ex) - len(new_ex)


def dedup_examples_pass(*, apply: bool, log: Callable[[str], None] = print) -> int:
    """Разовый дочист: убрать дубли и снятые слова из trainer_json.correct_examples у ВСЕХ
    записей (в том числе уже проверенных). Возвращает число изменённых записей."""
    from backend.database import get_db_connection_context
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sprint_id, wort, accepted, trainer_json FROM bt_3_sprint_bank ORDER BY wort")
            rows = cur.fetchall() or []
    changed = 0
    for sprint_id, wort, accepted, tj in rows:
        kept = {str((a or {}).get("de") or "").strip().lower() for a in (accepted or [])}
        new_tj, dropped = _filter_examples(tj or {}, kept)
        if not dropped:
            continue
        changed += 1
        log(f"{wort}: примеров снято {dropped}")
        if apply:
            with get_db_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE bt_3_sprint_bank SET trainer_json = %s::jsonb WHERE sprint_id = %s",
                                (json.dumps(new_tj, ensure_ascii=False), sprint_id))
                conn.commit()
    return changed


def hygiene_pass(*, limit: int | None = None, apply: bool = True, relation: str | None = None,
                 force: bool = False,
                 confirm=None, article=None, log: Callable[[str], None] = print) -> dict:
    """Прогнать накопленное через ту же дверь. Записи с accepted_checked_at IS NULL;
    force=True — и уже проверенные (нужно, когда правило двери стало строже: 06.09.2026
    антонимы получили подтверждение источником после первого прохода).

    apply=False — сухой прогон: печатает «было → стало» по каждой записи, базу не трогает.
    Возвращает сводку для отчёта."""
    from backend.database import get_db_connection_context
    ensure_sprint_intake_schema()
    where = ["TRUE" if force else "accepted_checked_at IS NULL"]
    params: list = []
    if relation:
        where.append("relation = %s"); params.append(str(relation))
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sprint_id, relation, wort, hint_ru, accepted, trainer_json, retired, retired_reason "
                "FROM bt_3_sprint_bank WHERE " + " AND ".join(where) + " ORDER BY created_at, sprint_id"
                + (" LIMIT %s" if limit else ""), tuple(params + ([int(limit)] if limit else [])),
            )
            rows = cur.fetchall() or []
    summary = {"checked": 0, "changed": 0, "retired_thin": 0, "queued": 0,
               **{k: 0 for k in ALL_REASONS}, "examples_dropped": 0}
    for sprint_id, relation, wort, hint_ru, accepted, trainer_json, retired, retired_reason in rows:
        # Принятое решением (судья «да», владелец «оставить») дверь не пересматривает.
        res = clean_accepted(wort, relation, list(accepted or []), confirm=confirm, article=article,
                             decided_keep=load_decided_keep(sprint_id))
        for k in ALL_REASONS:
            summary[k] += res.stats[k]
        # Пример стираем только у окончательно снятого (дубль, самослово). У того, что
        # ушло судье или владельцу, пример остаётся до решения: скажут «да» — карточка
        # «верного выбора» уже готова, без второго похода к модели.
        kept_de = {k["de"].lower() for k in res.kept} | pending_example_keys(res)
        new_tj, dropped = _filter_examples(trainer_json or {}, kept_de)
        changed = [dict(a) for a in (accepted or [])] != res.kept or dropped > 0
        thin = not res.enough
        # Снятое за нехватку слово, у которого подтверждённых снова хватает (судья
        # добавил, косвенная антонимия подтвердила), возвращается в показ.
        revive = (not thin) and bool(retired) and str(retired_reason or "") == "thin_accepted"
        summary["checked"] += 1
        summary["changed"] += int(changed or revive)
        summary["retired_thin"] += int(thin and not retired)
        summary["revived"] = summary.get("revived", 0) + int(revive)
        summary["examples_dropped"] += dropped
        removed = [f"{r.de} [{r.reason}]" for r in res.rejected]
        log(f"{'ИЗМЕНИТСЯ' if changed else 'без изменений'} {relation} «{wort}»: "
            f"{len(accepted or [])} → {len(res.kept)}"
            + (f", примеров снято {dropped}" if dropped else "")
            + (f", СНИМАЕТСЯ С ПОКАЗА (меньше {res.min_needed})" if thin else "")
            + (", ВОЗВРАЩАЕТСЯ В ПОКАЗ" if revive else "")
            + (": " + ", ".join(removed) if removed else ""))
        if not apply:
            summary["queued"] += sum(1 for r in res.rejected if r.reason in FOR_JUDGE)
            continue
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE bt_3_sprint_bank SET accepted = %s::jsonb, trainer_json = %s::jsonb, "
                    "accepted_checked_at = NOW()"
                    + (", retired = TRUE, retired_reason = 'thin_accepted'" if thin and not retired else "")
                    + (", retired = FALSE, retired_reason = ''" if revive else "")
                    + " WHERE sprint_id = %s",
                    (json.dumps(res.kept, ensure_ascii=False), json.dumps(new_tj, ensure_ascii=False),
                     sprint_id),
                )
            conn.commit()
        summary["queued"] += queue_for_judge(sprint_id=sprint_id, relation=relation, wort=wort,
                                             hint_ru=hint_ru or "", rejected=res.rejected)
    return summary


async def backfill_missing_examples(*, limit_words: int | None = None, log: Callable[[str], None] = print) -> dict:
    """Каждому слову списка — пример для карточки «верного выбора». Раньше примеров было
    не больше 10 на слово (CORRECT_EXAMPLE_CAP) и они строились по грязному списку;
    теперь список короткий и честный, а у части слов (в т.ч. принятых судьёй) примера нет.
    Один запрос к модели на слово (run_substitute_correct_examples), только там, где есть
    базовое предложение (trainer_json.target_example.de)."""
    from backend.database import get_db_connection_context
    from backend.openai_manager import run_substitute_correct_examples
    with get_db_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sprint_id, relation, wort, accepted, trainer_json FROM bt_3_sprint_bank "
                        "WHERE NOT retired AND COALESCE(trainer_json->'target_example'->>'de', '') <> '' "
                        "ORDER BY wort")
            rows = cur.fetchall() or []
    summary = {"words": 0, "asked": 0, "added": 0, "failed": 0}
    for sprint_id, relation, wort, accepted, tj in rows:
        tj = dict(tj or {})
        have = {str((e or {}).get("word") or "").strip().lower() for e in (tj.get("correct_examples") or [])}
        missing = [str(a.get("de") or "").strip() for a in (accepted or [])
                   if str(a.get("de") or "").strip() and str(a.get("de") or "").strip().lower() not in have]
        if not missing:
            continue
        summary["words"] += 1
        if limit_words and summary["asked"] >= limit_words:
            continue
        summary["asked"] += 1
        got = await run_substitute_correct_examples(target_word=wort, relation=relation,
                                                    base_de=str(tj["target_example"]["de"]), answers=missing)
        got = [g for g in (got or []) if str((g or {}).get("word") or "").strip().lower() in {m.lower() for m in missing}
               and str((g or {}).get("sentence_de") or "").strip()]
        if not got:
            summary["failed"] += 1
            log(f"{wort}: примеры не собрались для {len(missing)}: {', '.join(missing)}")
            continue
        tj["correct_examples"] = list(tj.get("correct_examples") or []) + got
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE bt_3_sprint_bank SET trainer_json = %s::jsonb WHERE sprint_id = %s",
                            (json.dumps(tj, ensure_ascii=False), sprint_id))
            conn.commit()
        summary["added"] += len(got)
        log(f"{wort}: добавлено примеров {len(got)} из {len(missing)}")
    return summary


def remember_last_stats(kind: str, stats: dict) -> None:
    """Сводка последнего прогона — для строки «🧩 Синонимы» в утреннем отчёте."""
    from backend.database import admin_kv_set
    payload = dict(stats)
    payload["at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        admin_kv_set(f"sprint_intake_last_{kind}", json.dumps(payload, ensure_ascii=False))
    except Exception:
        logging.warning("sprint_intake: не записал сводку %s", kind, exc_info=True)
        raise
