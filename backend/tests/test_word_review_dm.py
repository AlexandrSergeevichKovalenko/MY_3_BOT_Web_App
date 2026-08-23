# -*- coding: utf-8 -*-
"""Слова без подтверждения уходят владельцу кнопками, а не строкой в отчёте.

Владелец 23.08.2026: «я иду по улице с телефоном, приходит такое сообщение — мои
действия какие? Я что-то могу откорректировать сейчас или как?»

Повод серьёзнее вопроса. Функция, собирающая такие слова, жила в коде с 19.08.2026, и
её НЕ ВЫЗЫВАЛ НИКТО: очередь копилась, до владельца не доходила, и три чужих слова в
немецком словаре («slay», «bore», «aspettiamo») он получил только потому, что я наткнулся
на них руками. Мёртвый список выглядит как сделанная работа — это тот же класс, что и
заглушка.

Поэтому здесь проверяется не только текст, но и ПОДКЛЮЧЕНО ЛИ ЭТО ВООБЩЕ.
"""
import pytest

from backend import word_review as R


class TestКнопкиЗависятОтВопроса:
    def test_у_чужого_слова_кнопки_убрать_и_оставить(self):
        keys = R._keyboard("slay", "модель: слово есть, язык en")["inline_keyboard"]
        actions = [b["callback_data"] for row in keys for b in row]
        assert "wrev:drop:slay" in actions
        assert "wrev:keep:slay" in actions
        assert "wrev:skip:slay" in actions

    def test_у_формы_нескольких_слов_кнопкой_идёт_каждое(self):
        """«rast» — форма и «rasten» (отдыхать), и «rasen» (мчаться). Выбирает человек."""
        keys = R._keyboard("rast", "справочник: это форма слов rasten, rasen")["inline_keyboard"]
        actions = [b["callback_data"] for row in keys for b in row]
        assert "wrev:form|rasten:rast" in actions
        assert "wrev:form|rasen:rast" in actions
        labels = [b["text"] for row in keys for b in row]
        assert any("rasten" in t for t in labels)

    def test_кнопка_влезает_в_телеграм(self):
        """callback_data ограничен 64 байтами. Длинное слово обязано умещаться."""
        long_word = "Unternehmensberatungsgesellschaft"
        keys = R._keyboard(long_word,
                           "справочник: это форма слов Unternehmensberatung")["inline_keyboard"]
        for row in keys:
            for button in row:
                assert len(button["callback_data"].encode("utf-8")) <= 64, button


class TestТекстОбъясняетПочемуСловоЗдесь:
    @pytest.mark.parametrize("status, source, expect", [
        ("не подтверждено", "модель: слово есть, язык en", "НЕ немецкое"),
        ("не слово", "модель: такого слова нет", "не существует"),
        ("не подтверждено", "справочник: это форма слов rasten, rasen", "форма"),
        ("не подтверждено", "модель предложила другое написание, справочник не подтвердил",
         "догадка"),
        ("не подтверждено", "модель: слово есть, справочник не знает", "редкое"),
    ])
    def test_каждый_случай_объяснён_человеческими_словами(self, status, source, expect):
        text = R._word_text("слово", status, source, "", index=1, total=3, left=14)
        assert expect in text
        assert "_" not in text and "bt_3" not in text, "в текст утекло имя из кода"

    def test_видно_сколько_ещё_ждёт(self):
        text = R._word_text("slay", "не слово", "", "", index=2, total=8, left=14)
        assert "2 из 8" in text and "14" in text


class TestРазборСловКудаТоПодключён:
    """Мёртвый список — запрещён. Проверяем цепочку целиком, а не отдельные куски."""

    def test_есть_ночная_задача(self):
        import backend.background_jobs as jobs
        assert hasattr(jobs, "run_word_review_dm_actor")

    def test_задача_стоит_в_расписании(self):
        import inspect

        import backend.scheduler_service as sched
        source = inspect.getsource(sched)
        assert "_dispatch_word_review_dm" in source
        assert "WORD_REVIEW_ENABLED" in source, "рассылку нельзя включить/выключить"

    def test_нажатие_кнопки_ловится_ботом(self):
        import pathlib
        bot = pathlib.Path(__file__).resolve().parents[2] / "bot_3.py"
        source = bot.read_text(encoding="utf-8")
        assert 'pattern=r"^wrev:"' in source, "кнопки некому обработать"
        assert "handle_word_review_callback" in source

    def test_очередь_читается_дверью_слова(self):
        from backend.german_word_gate import (
            count_words_awaiting_owner,
            mark_word_reviewed,
            words_awaiting_owner,
        )
        assert callable(words_awaiting_owner)
        assert callable(count_words_awaiting_owner)
        assert callable(mark_word_reviewed)


class TestНажатиеДелаетТоЧтоНаписаноНаКнопке:
    def test_оставить_значит_слово_настоящее_а_не_молчи(self, monkeypatch):
        """Владелец 23.08.2026: «а что произойдёт, когда я нажму оставить? где программа
        возьмёт перевод, артикль, род, склонение?»

        Кнопка не должна просто затыкать канал: она записывает ПОДТВЕРЖДЕНИЕ, и слово
        идёт обычным путём дообогащения. Иначе слово навсегда осталось бы сомнительным,
        а единственный способ о нём узнать мы бы сами и выключили."""
        confirmed: list[str] = []
        monkeypatch.setattr("backend.german_word_gate.confirm_word_by_owner",
                            confirmed.append)
        monkeypatch.setattr(R, "_drop_word", lambda w: pytest.fail("слово удалено!"))
        text = R.apply_word_review("keep", "Arbeitsumfeld")
        assert confirmed == ["Arbeitsumfeld"]
        assert "настоящее слово" in text
        assert "Род и формы доберёт ночь" in text, "не сказано, откуда возьмутся данные"

    def test_убрать_зовёт_удаление_со_снимком(self, monkeypatch):
        monkeypatch.setattr("backend.german_word_gate.mark_word_reviewed", lambda w: None)
        monkeypatch.setattr(R, "_drop_word", lambda w: (True, "слово снято"))
        text = R.apply_word_review("drop", "slay")
        assert "убрано" in text and "Вернуть можно" in text

    def test_неудача_удаления_не_выдаётся_за_успех(self, monkeypatch):
        """Ошибка записи обязана быть видна: иначе владелец думает, что слово убрано."""
        def _fail(word):
            pytest.fail("отмечать разобранным нельзя — ничего не удалось")

        monkeypatch.setattr("backend.german_word_gate.mark_word_reviewed", _fail)
        monkeypatch.setattr(R, "_drop_word", lambda w: (False, "ошибка записи"))
        text = R.apply_word_review("drop", "slay")
        assert "не смог убрать" in text and "осталось" in text

    def test_это_форма_переименовывает(self, monkeypatch):
        monkeypatch.setattr("backend.german_word_gate.mark_word_reviewed", lambda w: None)
        seen: list[tuple[str, str]] = []
        monkeypatch.setattr(R, "_retitle_to_base",
                            lambda w, b: (seen.append((w, b)), (True, "готово"))[1])
        text = R.apply_word_review("form|rasten", "rast")
        assert seen == [("rast", "rasten")]
        assert "rasten" in text

    def test_незнакомая_кнопка_не_молчит(self):
        assert "Не понял" in R.apply_word_review("чтототакое", "slay")


class TestУбратьЧиститМусорИУЛюдей:
    """Решение владельца 23.08.2026: обрубок стирается и из личных списков, чужое
    настоящее слово — нет.

        «Да, но только для обрубков и опечаток: это мусор, и человек его учит. Для чужих
        слов вроде Sweatpants — нет: человек сохранил его осознанно, это его право.»
    """

    def test_обрубок_считается_мусором(self, monkeypatch):
        monkeypatch.setattr(R, "_verdict_of", lambda w: "не слово")
        assert R.word_is_garbage("Abschiebu") is True

    def test_чужое_настоящее_слово_не_мусор(self, monkeypatch):
        monkeypatch.setattr(R, "_verdict_of", lambda w: "не подтверждено")
        assert R.word_is_garbage("Sweatpants") is False

    def test_когда_вердикт_не_прочитался_карточки_не_трогаем(self, monkeypatch):
        """Ошибаться нужно в сторону сохранности чужих данных: лишняя карточка
        безобиднее стёртой."""
        def _boom(word):
            raise RuntimeError("база недоступна")

        monkeypatch.setattr(R, "_verdict_of", _boom)
        assert R.word_is_garbage("Abschiebu") is False

    def test_в_ответе_видно_тронуты_ли_карточки(self, monkeypatch):
        monkeypatch.setattr("backend.german_word_gate.mark_word_reviewed", lambda w: None)
        monkeypatch.setattr(R, "_drop_word",
                            lambda w: (True, "обрубок: слово, поиск и 2 личных карточек сняты"))
        assert "личных карточек" in R.apply_word_review("drop", "Abschiebu")
        monkeypatch.setattr(R, "_drop_word",
                            lambda w: (True, "слово и общий поиск сняты, личные карточки людей не тронуты"))
        assert "не тронуты" in R.apply_word_review("drop", "Sweatpants")
