# -*- coding: utf-8 -*-
"""Слово должно подходить СВОЕЙ ТЕМЕ. Проверка большинством голосов.

ЗАЧЕМ. Открытым пунктом с 16.08.2026 висело: «проверки „подходит ли слово ТЕМЕ“
не существует вообще — страж спрашивает только „нужно ли в быту“». 19–20.08.2026
это подтвердилось живыми примерами:
  • `der Linkshänder` («левша») лежал в подтеме «интернет и связь» темы про компьютеры;
  • ночной добор принёс в «Праздники и застолья» слова `das Deo` (дезодорант) и
    `die Schwangerschaft` (беременность), а в «Магазин и услуги» — `der Hausflur`.

Почему это дефект, а не мелочь. Набор дня подписан именем темы. Человек открывает
«Праздники» и получает беременность — он перестаёт верить подписи, а вместе с ней
и всему остальному. Заголовок обязан соответствовать содержимому: на этом же
основании из набора дня когда-то убрали долив словами чужих тем.

ИСТОЧНИК. Здесь справочника быть не может: «подходит ли слово теме» — вопрос
смысла, а не факта о языке. Поэтому спрашиваем модель — но так же, как это уже
делает страж бытовых слов: ТРЕМЯ голосами и по большинству. Один голос неустойчив,
это измерено 16.08.2026 (12% разнобоя на повторе того же вопроса).

Тема описывается модели не своим ключом, а ЧЕЛОВЕЧЕСКИМ ИМЕНЕМ И СПИСКОМ ПОДТЕМ —
тем же самым, по которому слова для неё и генерировались. Иначе судья и генератор
понимали бы тему по-разному, и страж резал бы то, что сам же и попросил.

Ничья и «не дошёл ни один голос» — разные вещи. Ничья = «не подходит» (та же
осторожность, что и у соседнего стража). Ни одного голоса — исключение
`ThemeFitJudgeUnavailable`: обрыв сети не должен объявлять пачку слов чужими.
"""
from __future__ import annotations

import json
import logging

VOTES = 3
BATCH = 40


class ThemeFitJudgeUnavailable(RuntimeError):
    """Ни один голос не дошёл. Это НЕ «слово не подходит»."""


def _ask_model(prompt: str) -> dict:
    """Один запрос к модели, строгий JSON. Зовём так же, как соседний страж
    (`article_word_gate._judge_everyday_once`): та же модель, температура 0,
    `response_format=json_object`. Расхождение в способе вызова между двумя
    стражами одного банка кончилось бы расхождением в поведении.

    Сбой — это НЕ «нет». Наружу идёт ThemeFitJudgeUnavailable, и решает вызывающий."""
    import os
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), timeout=90)
        resp = client.chat.completions.create(
            model="gpt-4.1", temperature=0, max_tokens=2000,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
        )
        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception as exc:
        logging.warning("страж темы: голос не получен", exc_info=True)
        raise ThemeFitJudgeUnavailable(str(exc)) from exc
    if not isinstance(data, dict):
        raise ThemeFitJudgeUnavailable(f"ответ не словарь: {type(data).__name__}")
    return data


def _theme_description(theme: dict) -> str:
    label = str(theme.get("label_ru") or theme.get("label_de") or theme.get("key") or "").strip()
    subs = [str(s).strip() for s in (theme.get("subtopics") or []) if str(s).strip()]
    text = f"ТЕМА: «{label}»"
    if subs:
        text += "\nЧто в неё входит:\n" + "\n".join(f"  — {s}" for s in subs)
    return text


def _judge_once(theme: dict, items: list[dict]) -> dict[str, bool]:
    """ОДИН голос. items = [{"word": ..., "meaning_ru": ...}]."""
    listing = "\n".join(
        f"{i + 1}. {it['word']}" + (f" — {it['meaning_ru']}" if it.get("meaning_ru") else "")
        for i, it in enumerate(items))
    prompt = (
        "Ниже тема тренажёра немецких артиклей и список слов, которые сейчас лежат "
        "в этой теме.\n\n"
        f"{_theme_description(theme)}\n\n"
        "Для каждого слова ответь, на своём ли оно месте.\n"
        "ДА — слово относится к этой теме так, как её понял бы обычный человек, "
        "открывший её по названию.\n"
        "НЕТ — слово про другое, даже если связь притянуть можно: дезодорант не про "
        "праздники, беременность не про застолье, левша не про интернет, подъезд "
        "не про услуги.\n"
        "Сомневаешься — отвечай НЕТ.\n\n"
        "Слова:\n" + listing + "\n\n"
        'Ответ — ТОЛЬКО JSON вида {"слово": true/false, ...}, без пояснений.'
    )
    raw = _ask_model(prompt)
    by_lower = {str(k).strip().lower(): bool(v) for k, v in raw.items()}
    return {it["word"]: by_lower.get(it["word"].lower(), False) for it in items}


def judge_theme_fit(theme: dict, items: list[dict], *, votes: int = VOTES) -> dict[str, bool]:
    """{слово: подходит ли теме} — большинством из `votes` голосов.

    Большинство считается от ДОШЕДШИХ голосов; ничья = «не подходит». Ни одного
    голоса — ThemeFitJudgeUnavailable, а не молчаливое «все чужие»."""
    clean = [{"word": str(i.get("word") or "").strip(),
              "meaning_ru": str(i.get("meaning_ru") or "").strip()}
             for i in (items or []) if str((i or {}).get("word") or "").strip()]
    if not clean:
        return {}
    yes: dict[str, int] = {i["word"]: 0 for i in clean}
    heard = 0
    last_exc: Exception | None = None
    for _ in range(max(1, int(votes))):
        try:
            verdict = _judge_once(theme, clean)
        except ThemeFitJudgeUnavailable as exc:
            last_exc = exc
            continue
        heard += 1
        for word, ok in verdict.items():
            if ok and word in yes:
                yes[word] += 1
    if not heard:
        raise ThemeFitJudgeUnavailable(str(last_exc or "нет ни одного голоса"))
    if heard < max(1, int(votes)):
        logging.warning("страж темы: дошло %s голосов из %s", heard, votes)
    return {i["word"]: yes[i["word"]] * 2 > heard for i in clean}


def suggest_theme(items: list[dict], themes: list[dict]) -> dict[str, str]:
    """{слово: ключ темы, которой оно подходит} — для слов, оказавшихся не в своей.

    Слово в чужой теме — НЕ мусор: оно хорошее, просто лежит не там. Поэтому
    спрашиваем не «выбросить ли», а «куда его». Модель выбирает ТОЛЬКО из
    существующих тем; ответ, которого нет в списке, отбрасывается — придумывать
    новые темы она права не имеет."""
    clean = [i for i in (items or []) if str((i or {}).get("word") or "").strip()]
    if not clean or not themes:
        return {}
    allowed = {str(t.get("key") or "").strip(): str(t.get("label_ru") or "") for t in themes}
    catalogue = "\n".join(f"  {k} — {v}" for k, v in allowed.items() if k)
    listing = "\n".join(
        f"{i + 1}. {it['word']}" + (f" — {it.get('meaning_ru') or ''}")
        for i, it in enumerate(clean))
    prompt = (
        "Ниже список тем тренажёра немецких артиклей и слова, которые сейчас лежат "
        "не в своей теме.\n\nТЕМЫ:\n" + catalogue + "\n\nСЛОВА:\n" + listing + "\n\n"
        "Для каждого слова выбери ОДНУ тему из списка выше, куда оно подходит лучше "
        "всего. Если ни одна тема не подходит — верни пустую строку.\n"
        'Ответ — ТОЛЬКО JSON вида {"слово": "ключ_темы", ...}, без пояснений.'
    )
    raw = _ask_model(prompt)
    by_lower = {str(k).strip().lower(): str(v).strip() for k, v in raw.items()}
    out: dict[str, str] = {}
    for it in clean:
        pick = by_lower.get(it["word"].lower(), "")
        if pick in allowed and pick:
            out[it["word"]] = pick
    return out
