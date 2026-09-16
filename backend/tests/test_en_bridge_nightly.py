"""Сторож ночного моста немецкий→английский.

Проверяет не работу модели, а СБОРКУ: что задача прописана всюду, где её забывают
прописать, и что условие отбора не разъехалось надвое. Всё это уже ломалось:
  · задача не была внесена в список «новый путь» и уехала на мёртвый Assistants → 404;
  · условие отбора я сначала вырезал из запроса строковой операцией, и счётчик
    «сколько осталось» сломался о вложенный WHERE.
"""
import os

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")


def test_инструкция_моста_есть_в_реестре():
    from backend.openai_manager import system_message
    assert system_message.get("en_bridge"), "системная инструкция en_bridge потерялась"


def test_расход_моста_идёт_на_дом_а_не_на_человека():
    """Мост просит НЕ человек, а мы; результат достаётся всем. Без этой строки счёт
    лёг бы на того, чьё слово попалось в пачку первым."""
    from backend.openai_manager import _SYSTEM_ATTRIBUTION_TASKS
    assert "en_bridge" in _SYSTEM_ATTRIBUTION_TASKS


def test_путь_к_модели_задан_явно_а_не_переменной_окружения():
    """В проде шлюз в режиме responses, локально — нет. Задача обязана называть путь
    сама, иначе уедет на Assistants и получит 404 (поймано 16.09.2026)."""
    import inspect
    from backend.openai_manager import run_en_bridge
    исходник = inspect.getsource(run_en_bridge)
    assert "responses_only=True" in исходник
    assert "allow_assistants_fallback=False" in исходник


def test_условие_отбора_одно_на_оба_запроса():
    """Выборка и счётчик «сколько осталось» обязаны спрашивать ОДНО И ТО ЖЕ. Разъедутся —
    и утренний отчёт начнёт врать про объём работы, а поймать это будет нечем."""
    from backend.en_bridge_nightly import _УСЛОВИЕ, _КАНДИДАТЫ, _СКОЛЬКО
    assert _УСЛОВИЕ.strip() in _КАНДИДАТЫ
    assert _УСЛОВИЕ.strip() in _СКОЛЬКО


def test_ждущие_решения_человека_не_переводятся():
    """Решение владельца 16.09.2026: слово, по которому человеку отправлено предложение
    поправить и он не ответил, на английский не переводим — текст ещё изменится."""
    from backend.en_bridge_nightly import _УСЛОВИЕ
    assert "bt_3_phrase_review" in _УСЛОВИЕ and "'open'" in _УСЛОВИЕ
    assert "bt_3_user_word_review" in _УСЛОВИЕ and "'pending'" in _УСЛОВИЕ


def test_мост_только_добавляет_и_ничего_не_удаляет():
    """Операция обязана быть добавляющей. Ни DELETE, ни понижения ранга у чужих связей."""
    import inspect
    from backend import en_bridge_nightly
    исходник = inspect.getsource(en_bridge_nightly).upper()
    assert "DELETE" not in исходник, "мост не имеет права удалять"
    assert "GREATEST(RANK" not in исходник, "мост не имеет права понижать чужие связи"


def test_актёр_и_расписание_на_месте():
    import inspect
    from backend import background_jobs, scheduler_service
    assert hasattr(background_jobs, "run_en_bridge_nightly_actor")
    расписание = inspect.getsource(scheduler_service)
    assert "_dispatch_en_bridge_nightly" in расписание
    assert "EN_BRIDGE_NIGHTLY_ENABLED" in расписание
