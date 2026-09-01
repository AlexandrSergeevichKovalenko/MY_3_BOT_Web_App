"""Текст ролика для читалки: то, что никогда не должно вернуться.

Дефект, от которого эти проверки стоят: модель, не справившись с куском субтитров,
молча возвращает его начало. Наружу это выглядит как нормальный абзац — человек не
узнает, что у ролика было продолжение, и будет учить обрезанный текст, считая его
целым. Второй такой же случай — ответ не по-немецки: модель объясняет по-русски,
почему не может, а мы кладём это в читалку как «текст видео».

Здесь же закреплён порог режимов. Он в СИМВОЛАХ, а не в минутах, и это не мелочь:
по замеру живой базы 01.09.2026 ролик на 29 минут дал 90 278 символов, а ролик на
66 минут — 48 566. По минутам мы обработали бы скороговорку как короткий ролик.
"""
import pytest

from backend import video_reader_text as vrt


def _items(text: str, *, pieces: int = 10):
    """Разложить строку по репликам субтитров, как их отдаёт YouTube."""
    step = max(1, len(text) // pieces)
    return [{"text": text[i:i + step], "start": float(i)} for i in range(0, len(text), step)]


def test_порог_режимов_считается_в_символах_а_не_в_минутах():
    assert vrt.pick_mode(9_778) == "verbatim"
    assert vrt.pick_mode(48_566) == "verbatim"      # 66 минут медленной речи
    assert vrt.pick_mode(vrt.VERBATIM_CHAR_LIMIT) == "verbatim"
    assert vrt.pick_mode(90_278) == "condensed"     # 29 минут скороговорки
    assert vrt.pick_mode(151_368) == "condensed"


def test_куски_режутся_по_границам_реплик_и_ничего_не_теряют():
    items = _items("Guten Tag. " * 400, pieces=200)
    chunks = vrt.split_into_chunks(items, chunk_chars=500)
    assert len(chunks) > 1
    # Ни одна реплика не пропала и ни одна не разрезана посередине.
    joined = " ".join(chunks)
    for item in items:
        assert item["text"].strip() in joined


def test_усохший_кусок_не_проходит_стража():
    problem = vrt.check_chunk_answer(
        "Kurz.", source_chars=4000, target_chars=4000, mode="verbatim",
    )
    assert problem, "проглоченный конец куска обязан быть замечен"
    assert "усох" in problem


def test_ответ_не_по_немецки_не_проходит_стража():
    problem = vrt.check_chunk_answer(
        "Извините, я не могу обработать этот фрагмент субтитров целиком.",
        source_chars=70, target_chars=70, mode="verbatim",
    )
    assert problem == "ответ пришёл не по-немецки"


def test_честный_ответ_стража_проходит():
    source = "guten tag hier ist ein beispiel " * 20
    answer = "Guten Tag. Hier ist ein Beispiel. " * 20
    assert vrt.check_chunk_answer(
        answer, source_chars=len(source), target_chars=len(source), mode="verbatim",
    ) == ""


def test_сборка_переспрашивает_один_раз_и_потом_честно_падает():
    """Плохой ответ не подменяется сырыми субтитрами и не выдаётся за готовый текст."""
    calls = []

    def always_short(system, user, *, model, what, user_id=None):
        calls.append(what)
        return "Zu kurz."

    with pytest.raises(RuntimeError) as failure:
        vrt.build_reader_text(items=_items("Guten Tag. " * 300), ask=always_short)
    assert "усох" in str(failure.value)
    # Ровно два обращения по первому куску: попытка и один переспрос. Не бесконечно.
    assert len(calls) == 2
    assert calls[1].endswith("(повтор)")


def test_сборка_склеивает_куски_по_порядку():
    def echo(system, user, *, model, what, user_id=None):
        return user  # честный ответ той же длины

    result = vrt.build_reader_text(items=_items("Guten Tag. " * 300), ask=echo)
    assert result["mode"] == "verbatim"
    assert result["chunks"] >= 1
    assert result["result_chars"] > 0
    assert "Guten Tag." in result["text"]


def test_прогресс_доходит_до_последнего_куска():
    seen = []

    def echo(system, user, *, model, what, user_id=None):
        return user

    result = vrt.build_reader_text(
        items=_items("Guten Tag. " * 600),
        ask=echo,
        on_progress=lambda done, total: seen.append((done, total)),
    )
    assert seen, "прогресс обязан приходить, иначе кнопка молчит"
    assert seen[-1] == (result["chunks"], result["chunks"])


def test_пустые_субтитры_падают_а_не_дают_пустую_книгу():
    with pytest.raises(ValueError):
        vrt.build_reader_text(items=[{"text": "Hallo"}], ask=lambda *a, **k: "Hallo.")
