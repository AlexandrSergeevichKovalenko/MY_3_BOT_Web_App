"""Пересказ ролика: то, что никогда не должно вернуться.

Три дефекта, от которых стоят эти проверки.

1. РАЗДАЧА СУБТИТРОВ. Первая версия на коротких роликах чистила субтитры и отдавала их
   почти дословно: замер 01.09.2026 — 72% двенадцатисловных окон совпадали слово в
   слово, длиннейший дословный кусок 119 слов. Владелец остановил это 02.09.2026:
   текст обязан быть ПЕРЕСКАЗОМ с лексикой оригинала, а не копией субтитров.
   На словах в задании модели это не держится — держится порогом.

2. ПРОГЛОЧЕННЫЙ КОНЕЦ. Модель, не справившись с куском, молча возвращает его начало.
   Наружу это выглядит как нормальный абзац: человек не узнает, что у ролика было
   продолжение, и выучит обрезанное, считая целым.

3. ОТВЕТ НЕ ПО-НЕМЕЦКИ. Модель объясняет по-русски, почему не может, — а мы кладём
   это в читалку как «текст видео».

Здесь же закреплён порог режимов. Он в СИМВОЛАХ, а не в минутах: по замеру живой базы
01.09.2026 ролик на 29 минут дал 90 278 символов, а ролик на 66 минут — 48 566.
По минутам мы обработали бы скороговорку как короткий ролик.
"""
import pytest

from backend import video_reader_text as vrt


def _items(text: str, *, pieces: int = 10):
    """Разложить строку по репликам субтитров, как их отдаёт YouTube."""
    step = max(1, len(text) // pieces)
    return [{"text": text[i:i + step], "start": float(i)} for i in range(0, len(text), step)]


# ── Порог режимов ─────────────────────────────────────────────────────────────

def test_порог_режимов_считается_в_символах_а_не_в_минутах():
    assert vrt.pick_mode(9_778) == "close"
    assert vrt.pick_mode(48_566) == "close"       # 66 минут медленной речи
    assert vrt.pick_mode(vrt.VERBATIM_CHAR_LIMIT) == "close"
    assert vrt.pick_mode(90_278) == "condensed"   # 29 минут скороговорки
    assert vrt.pick_mode(151_368) == "condensed"


def test_длинный_ролик_жмётся_до_читаемого_объёма_а_короткий_нет():
    assert vrt.target_ratio(10_000) == vrt.CLOSE_TARGET_RATIO
    # 151 368 символов → примерно 45 000: иначе читателю достанется трёхчасовая стена.
    assert 0.25 < vrt.target_ratio(151_368) < 0.35


# ── Дословность: главный запрет владельца 02.09.2026 ──────────────────────────

def test_копия_субтитров_не_проходит():
    """Текст, совпадающий с субтитрами, — это раздача чужого, а не наш пересказ."""
    source = ("guten tag heute sprechen wir über verben mit sagen und ihre bedeutung "
              "im alltag denn diese verben begegnen euch überall ") * 6
    problem = vrt.check_chunk_answer(source, source=source, target_chars=len(source))
    assert problem, "дословная копия обязана быть замечена"
    assert "копия" in problem


def test_пересказ_с_той_же_лексикой_проходит():
    """Слова и обороты оригинала — это лернматериал, за них наказывать нельзя."""
    source = (
        "also ähm heute geht es um das verb absagen absagen bedeutet dass etwas das "
        "man geplant hat doch nicht stattfindet zum beispiel erik hat seine "
        "geburtstagsparty abgesagt er hatte eine party geplant und leute eingeladen "
    ) * 4
    answer = (
        "Das Verb absagen bedeutet, dass ein geplantes Ereignis doch nicht stattfindet. "
        "Erik hatte eine Geburtstagsparty geplant und Leute eingeladen, dann aber "
        "abgesagt. "
    ) * 4
    assert vrt.check_chunk_answer(answer, source=source, target_chars=len(answer)) == ""


def test_доля_совпадений_меряется_окнами_а_не_словами():
    source = "eins zwei drei vier fünf sechs sieben acht neun zehn elf zwölf dreizehn"
    assert vrt.verbatim_overlap(source, source) == 1.0
    assert vrt.verbatim_overlap(source, "völlig andere wörter stehen hier und sonst nichts") == 0.0


def test_длиннейший_дословный_кусок_находится():
    source = "a b c d e f g h i j k l m n o p"
    assert vrt.longest_verbatim_run(source, "x y c d e f g z") == 5
    assert vrt.longest_verbatim_run(source, "völlig andere") == 0


def test_пороги_стоят_между_копией_и_честным_пересказом():
    """Замер 02.09.2026: копия — 72% и 119 слов, пересказ — до 12,4% и до 30 слов.
    Пороги обязаны лежать МЕЖДУ этими мирами, иначе один из них перестанет работать."""
    assert 0.124 < vrt.MAX_VERBATIM_OVERLAP < 0.72
    assert 30 < vrt.MAX_VERBATIM_RUN < 119


# ── Объём и язык ──────────────────────────────────────────────────────────────

def test_усохший_кусок_не_проходит_стража():
    problem = vrt.check_chunk_answer("Kurz.", source="ganz anderer text " * 200,
                                     target_chars=4000)
    assert problem, "проглоченный конец куска обязан быть замечен"
    assert "усох" in problem


def test_ответ_не_по_немецки_не_проходит_стража():
    problem = vrt.check_chunk_answer(
        "Извините, я не могу обработать этот фрагмент субтитров целиком и полностью.",
        source="ganz anderer deutscher text hier", target_chars=70,
    )
    assert problem == "ответ пришёл не по-немецки"


# ── Сборка ────────────────────────────────────────────────────────────────────

def test_куски_режутся_по_границам_реплик_и_ничего_не_теряют():
    items = _items("Guten Tag. " * 400, pieces=200)
    chunks = vrt.split_into_chunks(items, chunk_chars=500)
    assert len(chunks) > 1
    joined = " ".join(chunks)
    for item in items:
        assert item["text"].strip() in joined


def test_сборка_переспрашивает_один_раз_и_потом_честно_падает():
    """Плохой ответ не подменяется сырыми субтитрами и не выдаётся за готовый текст."""
    calls = []

    def always_short(system, user, *, model, what, user_id=None):
        calls.append(what)
        return "Zu kurz."

    with pytest.raises(RuntimeError) as failure:
        vrt.build_reader_text(items=_items("Guten Tag heute lernen wir. " * 300),
                              ask=always_short)
    assert "усох" in str(failure.value)
    # Ровно два обращения по первому куску: попытка и один переспрос. Не бесконечно.
    assert len(calls) == 2
    assert calls[1].endswith("(повтор)")


def test_сборка_не_отдаёт_копию_даже_если_модель_вернула_субтитры():
    """Самый опасный случай: модель «справилась», вернув исходник. Это не текст."""
    def echo(system, user, *, model, what, user_id=None):
        return user

    with pytest.raises(RuntimeError) as failure:
        vrt.build_reader_text(items=_items("Heute sprechen wir über deutsche Verben. " * 200),
                              ask=echo)
    assert "копия" in str(failure.value)


def test_прогресс_доходит_до_последнего_куска():
    seen = []

    def retell(system, user, *, model, what, user_id=None):
        # Пересказ той же длины, но своими словами: страж копии его пропускает.
        return "Ein voellig neuer Satz ueber dasselbe Thema. " * (len(user) // 44 + 1)

    result = vrt.build_reader_text(
        items=_items("Guten Tag heute lernen wir etwas Neues. " * 600),
        ask=retell,
        on_progress=lambda done, total: seen.append((done, total)),
    )
    assert seen, "прогресс обязан приходить, иначе кнопка молчит"
    assert seen[-1] == (result["chunks"], result["chunks"])
    assert result["mode"] == "close"


def test_пустые_субтитры_падают_а_не_дают_пустую_книгу():
    with pytest.raises(ValueError):
        vrt.build_reader_text(items=[{"text": "Hallo"}], ask=lambda *a, **k: "Hallo.")
