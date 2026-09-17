"""Сторож двери сохранения: немецкие правила не трогают чужой язык.

Это ЕДИНСТВЕННОЕ место, через которое проходит запись любой карточки — и бот, и веб,
и импорт. До 17.09.2026 три немецких правила (словарная форма, артикль множественного,
немецкое правило заглавной буквы) срабатывали ВЫШЕ строк, где вычисляется язык пары,
и условием запуска было «колонка не пуста».

Портится при этом не показ, а САМА ЗАПИСЬ. Экран переделать можно, запись — нет.
"""
import inspect
import os
import re

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")


def _исходник():
    from backend import database
    return inspect.getsource(database._save_webapp_dictionary_query_returning_id_with_conn)


def test_язык_вычисляется_ДО_немецких_правил():
    """Порядок строк тут — не стиль, а суть: правило, сработавшее до того, как узнали
    язык, уже испортило слово."""
    src = _исходник()
    язык = src.index("normalized_source_lang = _normalize_lang_code")
    правило = src.index("german_dictionary_headword(word_de)")
    assert язык < правило, "немецкое правило снова срабатывает раньше, чем узнан язык"


def test_немецкие_правила_под_условием_языка():
    src = _исходник()
    assert "немецкое_наверняка" in src, "дверь сохранения снова не спрашивает язык"
    # все три правила должны быть внутри условия — то есть с большим отступом
    for правило in ("german_dictionary_headword(word_de)",
                    "_fix_plural_article_on_headword(word_de)",
                    "_lowercase_when_russian_says_not_a_noun(word_de"):
        строка = next(l for l in src.split("\n") if правило in l)
        assert строка.startswith("        "), f"правило вне условия языка: {правило.strip()}"


def test_за_профилем_в_базу_не_ходим():
    """Скорость ответа человеку — условие владельца. Лишнего запроса тут быть не должно."""
    src = _исходник()
    кусок = src[:src.index("source_text, target_text =")]
    assert "bt_3_user_language_profile" not in кусок
    assert "_get_user_language_pair" not in кусок


def test_вопрос_узкий_а_не_есть_ли_de_в_паре():
    """У пары en→de немецкий в паре ЕСТЬ, а слово английское. Проверяем, что дверь
    задаёт узкий вопрос через общий ответчик, а не пишет своё условие."""
    from backend.lang_of_word import немецкое_наверняка
    assert немецкое_наверняка(source_lang="en", target_lang="de") is False
    assert немецкое_наверняка(source_lang="ru", target_lang="de") is True
