# -*- coding: utf-8 -*-
"""Жалоба на разбор: человек жалуется, ночь судит, решает владелец, человек узнаёт ответ.

ПОВОД. Кнопка «Перевод не тот» писала строку, которую никто не читал, и закрывала слово
навсегда: нажал → пообещали → ничего → слово ушло из очереди с плохим переводом.
Проверено grep'ом 26.08.2026, нажатий за всё время — ноль.

Схема, которую задал владелец 26.08.2026: жалоба ничего не меняет → ночью модель судит и
ДАЁТ ПРЕДЛОЖЕНИЕ → пачка владельцу → решает он → человеку уходит ответ.

⚠ ЧТО ЗДЕСЬ ГЛАВНОЕ:
  · модель НИЧЕГО не применяет сама — разбор общий, правка меняет карточку всем;
  · «модель не ответила» не превращается в «человек неправ»;
  · отметка «человеку сказали» ставится только ПОСЛЕ успешной отправки.
"""
import os
import unittest
from unittest import mock

os.environ.setdefault("SKIP_STARTUP_SCHEMA_BOOTSTRAP", "1")
os.environ.setdefault("SECOND_VOICE_CHECK_DISABLED", "1")

from backend import card_complaints as жалобы  # noqa: E402
from backend import word_confirm_digest as сводка  # noqa: E402

ВЕРДИКТ = {"card_is_wrong": True, "chto_ne_tak": "Перевод не соответствует немецкому слову.",
           "pole": "translation", "predlozhenie": {"translation_ru": "депортировать"},
           "predlozhenie_slovami": "поставить «депортировать» вместо «убраться»",
           "uverennost": "высокая"}


class ПоддельныйКурсор:
    def __init__(self, ответы=None):
        self._ответы = list(ответы or [])
        self.запросы = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.запросы.append((sql, params))

    def fetchall(self):
        return self._ответы.pop(0) if self._ответы else []

    def fetchone(self):
        строки = self._ответы.pop(0) if self._ответы else []
        return строки[0] if строки else None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class ПоддельноеСоединение:
    def __init__(self, курсор):
        self._курсор = курсор

    def cursor(self, *a, **k):
        return self._курсор

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _с_базой(курсор):
    return mock.patch("backend.database.get_db_connection_context",
                      return_value=ПоддельноеСоединение(курсор))


class НочнойСудья(unittest.TestCase):
    def _судить(self, ответ_модели):
        курсор = ПоддельныйКурсор([[(7, "abschieben", "перевод неверный", {"x": 1})]])
        with _с_базой(курсор), \
             mock.patch.object(жалобы, "ensure_card_complaint_schema"), \
             mock.patch("backend.openai_manager.run_card_complaint_verdict",
                        return_value=ответ_модели) as судья:
            итог = жалобы.judge_new_complaints(10)
        return итог, курсор, судья

    def test_verdict_is_stored_and_nothing_is_applied(self):
        итог, курсор, _ = self._судить(dict(ВЕРДИКТ))
        self.assertEqual(итог["разобрано"], 1)
        self.assertEqual(итог["правы"], 1)
        запросы = " ".join(str(q[0]) for q in курсор.запросы)
        self.assertIn("UPDATE bt_3_card_complaints", запросы)
        # Ни одного обращения к слою слов: ночь готовит материал, а не правит карточки.
        self.assertNotIn("bt_3_lex_units SET", запросы)

    def test_silent_model_is_not_a_verdict(self):
        """«Не ответила» — не «человек неправ». Жалоба остаётся новой."""
        итог, курсор, _ = self._судить(None)
        self.assertEqual(итог["не ответила"], 1)
        self.assertEqual(итог["разобрано"], 0)
        self.assertNotIn("UPDATE", " ".join(str(q[0]) for q in курсор.запросы))

    def test_the_card_is_shown_to_the_judge(self):
        _, _, судья = self._судить(dict(ВЕРДИКТ))
        self.assertEqual(судья.call_args.kwargs["word"], "abschieben")
        self.assertEqual(судья.call_args.kwargs["card"], {"x": 1})


class ВердиктСверяетсяСамСобой(unittest.TestCase):
    """Живой прогон 26.08.2026 на «Alle meine Kollegen»: модель ответила «карточку надо
    менять» и тут же написала «перевод полностью соответствует, ошибки нет». Владельцу
    это пришло бы противоречием прямо в заголовке. Жалоба, поддержанная БЕЗ единой
    правки, — пустая жалоба."""

    def _ответ(self, тело):
        ответ = mock.Mock()
        ответ.choices = [mock.Mock(message=mock.Mock(content=тело))]
        ответ.usage = None
        клиент = mock.Mock()
        клиент.chat.completions.create.return_value = ответ
        return клиент

    def _спросить(self, тело):
        from backend import openai_manager
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "тест"}), \
             mock.patch("backend.synthetic_load.build_sync_openai_client",
                        return_value=self._ответ(тело)):
            # Карточка настоящая по составу: страж имён полей сверяет предложение
            # именно с ней, и подсунуть сюда {"a": 1} значит проверять не то.
            return openai_manager.run_card_complaint_verdict(
                word="Alle meine Kollegen", note="перевод не тот",
                card={"translation_ru": "Все мои коллеги", "usage_examples": []})

    def test_supported_without_a_fix_is_not_supported(self):
        итог = self._спросить(
            '{"card_is_wrong": true, "chto_ne_tak": "ошибки нет", "predlozhenie": {}}')
        self.assertFalse(итог["card_is_wrong"])

    def test_a_real_fix_survives(self):
        итог = self._спросить(
            '{"card_is_wrong": true, "chto_ne_tak": "перевод неверен",'
            ' "predlozhenie": {"translation_ru": "депортировать"}}')
        self.assertTrue(итог["card_is_wrong"])

    def test_garbage_is_not_a_verdict(self):
        """Не разобрали ответ — значит не судили. Пустого «всё хорошо» здесь нет."""
        self.assertIsNone(self._спросить("не json вовсе"))
        self.assertIsNone(self._спросить('{"что-то": "другое"}'))


class ПредложениеДолжноБытьПРИМЕНИМЫМ(unittest.TestCase):
    """Владелец 26.08.2026: «как я могу нажать "принять вариант", если я не вижу, какой
    именно вариант мне предлагают?» Отсюда три правила, и все три — про одно: кнопка
    обязана делать ровно то, что написано.

    Живой прогон на «der Wortschwall» дал все три случая сразу: модель предложила поле
    `meanings`, которого в карточке НЕТ (значения там в `dictionary_senses`); поле
    `word_ru` со значением, равным нынешнему; и переименование самого слова.
    """

    def _судить(self, предложение, card):
        from backend import openai_manager
        тело = ('{"card_is_wrong": true, "chto_ne_tak": "перепутаны слова",'
                ' "predlozhenie": ' + __import__("json").dumps(предложение) + '}')
        ответ = mock.Mock()
        ответ.choices = [mock.Mock(message=mock.Mock(content=тело))]
        ответ.usage = None
        клиент = mock.Mock()
        клиент.chat.completions.create.return_value = ответ
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "тест"}), \
             mock.patch("backend.synthetic_load.build_sync_openai_client",
                        return_value=клиент):
            return openai_manager.run_card_complaint_verdict(
                word="der Wortschwall", note="не то слово", card=card)

    def test_an_empty_card_can_still_be_renamed(self):
        """Живой случай 27.08.2026: «das Nässchen putzen», разбора нет вовсе. Список
        полей выходил пустым, модель честно не предлагала ничего — и владельцу
        доставался экран без единой кнопки правки, хотя починка очевидна: выражение не
        немецкое, надо «die Nase putzen». Имя слова живёт отдельно от разбора."""
        итог = self._судить({"word_de": "die Nase putzen"}, {})
        self.assertEqual(итог["predlozhenie"], {"word_de": "die Nase putzen"})
        _можно, заголовок = жалобы._разделить_предложение(итог["predlozhenie"])
        self.assertEqual(заголовок, {"word_de": "die Nase putzen"})

    def test_the_headword_is_offered_to_the_model_even_for_an_empty_card(self):
        from backend import openai_manager
        клиент = mock.Mock()
        ответ = mock.Mock()
        ответ.choices = [mock.Mock(message=mock.Mock(
            content='{"card_is_wrong": false, "predlozhenie": {}}'))]
        ответ.usage = None
        клиент.chat.completions.create.return_value = ответ
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "тест"}), \
             mock.patch("backend.synthetic_load.build_sync_openai_client",
                        return_value=клиент):
            openai_manager.run_card_complaint_verdict(word="х", note="", card={})
        задание = клиент.chat.completions.create.call_args.kwargs["messages"][0]["content"]
        self.assertIn("word_de", задание)
        self.assertIn("article", задание)

    def test_a_field_the_card_does_not_have_is_dropped(self):
        итог = self._судить({"meanings": "поток слов"},
                            {"dictionary_senses": [], "translation_ru": "Словоблудие"})
        self.assertEqual(итог["predlozhenie"], {})
        self.assertFalse(итог["card_is_wrong"], "нечего применять — значит и кнопки нет")

    def test_a_change_that_changes_nothing_is_dropped(self):
        итог = self._судить({"translation_ru": "Словоблудие"},
                            {"translation_ru": "Словоблудие"})
        self.assertEqual(итог["predlozhenie"], {})

    def test_a_real_change_survives_both_guards(self):
        итог = self._судить({"translation_ru": "поток слов"},
                            {"translation_ru": "Словоблудие"})
        self.assertEqual(итог["predlozhenie"], {"translation_ru": "поток слов"})
        self.assertTrue(итог["card_is_wrong"])

    def test_renaming_the_word_is_not_offered_as_a_field_edit(self):
        """Заголовок живёт не в разборе, а в самой единице: save_unit_card его не тронет,
        и «принять» дало бы полукарточку — ровно то, на что и пожаловались."""
        можно, заголовок = жалобы._разделить_предложение(
            {"word_de": "das Geschwafel", "article": "das", "usage_examples": ["…"]})
        self.assertEqual(sorted(заголовок), ["article", "word_de"])
        self.assertEqual(list(можно), ["usage_examples"])

    def test_renaming_the_word_is_its_own_decision(self):
        """Владелец 27.08.2026: «я хочу поменять шапку, но карточку не пересобирать —
        где кнопка?» Её не было: экран говорил «карточка про другое слово» и не давал
        это сделать. Переименование идёт тем же местом, что и решение по спорной фразе."""
        курсор = ПоддельныйКурсор([
            [(4242, 99, 117649764, "der Wortschwall",
              {"card_is_wrong": True,
               "predlozhenie": {"word_de": "Geschwafel", "article": "das"}})],
            [("der Wortschwall", {"word_de": "der Wortschwall"})],
        ])
        with _с_базой(курсор), \
             mock.patch("backend.lex_units.retitle_unit") as переименовать, \
             mock.patch("backend.database.spread_correction_everywhere") as разнести:
            итог = жалобы.apply_owner_decision(1, "переименовать")
        self.assertTrue(итог["ok"])
        self.assertEqual(переименовать.call_args[0][2], "das Geschwafel",
                         "артикль обязан приклеиться к новому имени")
        разнести.assert_called_once()
        self.assertIn("переименовано", итог["result"])

    def test_renaming_without_a_new_name_is_refused(self):
        курсор = ПоддельныйКурсор([[(4242, 99, 117649764, "der Wortschwall",
                                     {"card_is_wrong": True, "predlozhenie": {}})]])
        with _с_базой(курсор):
            итог = жалобы.apply_owner_decision(1, "переименовать")
        self.assertEqual(итог["reason"], "no_new_title")

    def test_only_a_rename_means_no_accept_button(self):
        курсор = ПоддельныйКурсор([[(4242, 99, 117649764, "der Wortschwall",
                                     {"card_is_wrong": True,
                                      "predlozhenie": {"word_de": "das Geschwafel"}})]])
        with _с_базой(курсор):
            итог = жалобы.apply_owner_decision(1, "принять")
        self.assertEqual(итог["reason"], "no_proposal")

    def test_the_screen_shows_what_will_be_replaced(self):
        """Без «было» рядом со «станет» решение принять нельзя."""
        было = {"translation_ru": "Словоблудие", "usage_examples": ["старый"]}
        self.assertEqual(
            жалобы._текущее_по_полям(было, {"translation_ru": "поток слов"}),
            {"translation_ru": "Словоблудие"})

    def test_a_field_missing_from_the_card_is_shown_as_empty_not_blank(self):
        self.assertEqual(жалобы._текущее_по_полям({}, {"memory_tip": "х"}),
                         {"memory_tip": None})


class ПереименованиеДоводитсяДоКонца(unittest.TestCase):
    """Живая проверка 27.08.2026 после переименования «der Wortschwall» → «das
    Geschwafel»: само слово, лемма и обе личные карточки поменялись, а ПЯТЬ мест
    остались со старым — род 'der' при артикле 'das', `source_text` внутри разбора,
    шесть ключей поиска (включая формы старого слова), запись в пуле и два ответа в
    кеше. Поиск «Wortschwall» вёл на «Geschwafel», хотя это настоящее немецкое слово.
    """

    def _подчистить(self, разбор, вид="word"):
        # Курсор отвечает по порядку: сперва вид записи (род ставим только слову),
        # потом сам разбор.
        курсор = ПоддельныйКурсор([[(вид,)], [(разбор,)]])
        итог = жалобы.подчистить_после_переименования(
            курсор, unit_id=27287, old_text="der Wortschwall", new_text="das Geschwafel")
        return итог, " ".join(str(q[0]) for q in курсор.запросы), курсор

    def test_gender_follows_the_new_article(self):
        _, запросы, курсор = self._подчистить({})
        self.assertIn("SET gender=%s", запросы)
        родовые = [q for q in курсор.запросы if "SET gender=%s" in str(q[0])]
        self.assertEqual(родовые[0][1][0], "das")

    def test_a_collocation_gets_no_gender_at_all(self):
        """Найдено 27.08.2026 на «die Nase putzen»: это оборот, и «die» здесь артикль
        существительного ВНУТРИ него, а не род записи. Я поставил роду 'die' и получил
        бессмыслицу — у оборота рода нет."""
        итог, запросы, _ = self._подчистить({}, вид="collocation")
        self.assertNotIn("SET gender=%s,", запросы)
        self.assertIn("SET gender=NULL", запросы)
        self.assertLessEqual(итог["род"], 0)

    def test_the_old_name_leaves_the_card_itself(self):
        итог, _, курсор = self._подчистить({"source_text": "das Wortschwall",
                                            "word_de": "der Wortschwall"})
        записано = [q for q in курсор.запросы if "SET card=%s" in str(q[0])]
        self.assertTrue(записано, "разбор со старым именем внутри не переписан")
        self.assertNotIn("Wortschwall", записано[0][1][0])
        # Два имени плюс артикль, которого в разборе не было вовсе.
        self.assertEqual(итог["поля разбора"], 3)

    def test_the_old_word_stops_pointing_at_the_new_one(self):
        """«Wortschwall» существует сам по себе — вести по нему на другое слово нельзя."""
        _, запросы, _ = self._подчистить({})
        self.assertIn("DELETE FROM bt_3_lex_surfaces", запросы)
        self.assertIn("INSERT INTO bt_3_lex_surfaces", запросы)

    def test_copies_are_matched_by_the_word_not_by_the_headline(self):
        """Первый заход искал «der Wortschwall» целиком и прошёл мимо «das Wortschwall»:
        артикль в копиях гуляет, а слово — нет."""
        _, _, курсор = self._подчистить({})
        удаления = [q for q in курсор.запросы
                    if "bt_3_dictionary_entries" in str(q[0])
                    or "bt_3_dictionary_lookup_cache" in str(q[0])]
        self.assertEqual(len(удаления), 2)
        for _sql, параметры in удаления:
            self.assertIn("wortschwall", параметры[0])
            self.assertNotIn("der ", параметры[0])
            # Границы слова: «Wortschwallartig» — другое слово, его трогать нельзя.
            self.assertTrue(параметры[0].startswith("\\m"))


class ПачкаВладельцу(unittest.TestCase):
    def test_ten_is_enough(self):
        курсор = ПоддельныйКурсор([[(10, False)]])
        with _с_базой(курсор):
            пора, сколько = жалобы.due_for_owner()
        self.assertTrue(пора)
        self.assertEqual(сколько, 10)

    def test_a_lone_complaint_does_not_wait_forever(self):
        """Одна жалоба, но залежалась неделю — шлём: порог ИЛИ срок, что раньше."""
        курсор = ПоддельныйКурсор([[(1, True)]])
        with _с_базой(курсор):
            пора, _ = жалобы.due_for_owner()
        self.assertTrue(пора)

    def test_three_fresh_complaints_wait(self):
        курсор = ПоддельныйКурсор([[(3, False)]])
        with _с_базой(курсор):
            пора, сколько = жалобы.due_for_owner()
        self.assertFalse(пора)
        self.assertEqual(сколько, 3)

    def test_unknown_count_is_not_zero(self):
        """База не ответила — это «не знаю», а не «жалоб нет»."""
        with mock.patch("backend.database.get_db_connection_context",
                        side_effect=RuntimeError("база молчит")):
            пора, сколько = жалобы.due_for_owner()
        self.assertFalse(пора)
        self.assertEqual(сколько, -1)


class РешениеВладельца(unittest.TestCase):
    def test_unknown_decision_changes_nothing(self):
        итог = жалобы.apply_owner_decision(1, "что-нибудь")
        self.assertFalse(итог["ok"])
        self.assertEqual(итог["reason"], "unknown_decision")

    def test_accept_goes_through_the_same_door_as_the_night(self):
        курсор = ПоддельныйКурсор([
            [(4242, 99, 117649764, "abschieben", dict(ВЕРДИКТ))],   # сама жалоба
            [({"translation_ru": "убраться"},)],                     # нынешний разбор
        ])
        with _с_базой(курсор), \
             mock.patch("backend.lex_units.save_unit_card", return_value=True) as дверь:
            итог = жалобы.apply_owner_decision(1, "принять")
        self.assertTrue(итог["ok"])
        self.assertEqual(итог["result"], "исправлено")
        разбор = дверь.call_args[0][1]
        self.assertEqual(разбор["translation_ru"], "депортировать")

    def test_a_refused_write_is_not_reported_as_done(self):
        """Второй голос забраковал — жалоба НЕ закрывается."""
        курсор = ПоддельныйКурсор([
            [(4242, 99, 117649764, "abschieben", dict(ВЕРДИКТ))],
            [({"translation_ru": "убраться"},)],
        ])
        with _с_базой(курсор), \
             mock.patch("backend.lex_units.save_unit_card", return_value=False):
            итог = жалобы.apply_owner_decision(1, "принять")
        self.assertFalse(итог["ok"])
        self.assertEqual(итог["reason"], "not_saved")

    def test_rebuild_uses_the_owners_existing_mechanism(self):
        курсор = ПоддельныйКурсор([[(4242, 99, 117649764, "abschieben", {})]])
        with _с_базой(курсор), \
             mock.patch("backend.database.reset_dictionary_card_for_rebuild",
                        return_value={"ok": True}) as сброс:
            итог = жалобы.apply_owner_decision(1, "пересобрать")
        сброс.assert_called_once()
        self.assertEqual(итог["result"], "поставлено на пересборку")

    def test_accepting_without_a_proposal_is_refused(self):
        пустой = dict(ВЕРДИКТ, predlozhenie={})
        курсор = ПоддельныйКурсор([[(4242, 99, 117649764, "abschieben", пустой)]])
        with _с_базой(курсор):
            итог = жалобы.apply_owner_decision(1, "принять")
        self.assertEqual(итог["reason"], "no_proposal")


class ОтветЧеловеку(unittest.TestCase):
    def test_the_answer_leads_the_letter(self):
        текст = сводка._reminder_text(0, 0, [{"id": 1, "word": "abschieben",
                                              "result": "исправлено"}])
        self.assertIn("Разобрали, на что ты жаловался", текст)
        self.assertIn("abschieben", текст)
        self.assertIn("исправлено", текст)

    def test_the_answer_does_not_replace_the_check_list(self):
        текст = сводка._reminder_text(3, 0, [{"id": 1, "word": "abschieben",
                                              "result": "исправлено"}])
        self.assertIn("Разобрали, на что ты жаловался", текст)
        self.assertIn("3 слова", текст)

    def test_nothing_to_say_means_the_usual_letter(self):
        текст = сводка._reminder_text(3, 0, [])
        self.assertNotIn("жаловался", текст)

    def test_told_is_marked_only_after_the_letter_left(self):
        """Пометить до отправки значит потерять ответ навсегда, если Telegram откажет."""
        курсор = ПоддельныйКурсор([[(1, 117649764, "abschieben", "исправлено")]])
        with _с_базой(курсор):
            карта = жалобы.answers_by_user()
        self.assertEqual(карта[117649764][0]["word"], "abschieben")
        self.assertNotIn("told_at=NOW()", " ".join(str(q[0]) for q in курсор.запросы))


class СтараяКнопкаБольшеНеЗаглушка(unittest.TestCase):
    def test_retrans_now_files_a_complaint(self):
        курсор = ПоддельныйКурсор()
        with _с_базой(курсор), \
             mock.patch.object(жалобы, "add_complaint",
                               return_value={"ok": True, "id": 1}) as жалоба:
            счёт = сводка.apply_decisions(117649764, [
                {"word": "abschieben", "action": "retrans"}])
        жалоба.assert_called_once()
        self.assertEqual(счёт["на пересборку"], 1)

    def test_retrans_no_longer_closes_the_word_forever(self):
        """Слово закроет решение владельца по жалобе, а не сам факт нажатия."""
        курсор = ПоддельныйКурсор()
        with _с_базой(курсор), \
             mock.patch.object(жалобы, "add_complaint", return_value={"ok": True, "id": 1}):
            сводка.apply_decisions(117649764, [{"word": "abschieben", "action": "retrans"}])
        запросы = " ".join(str(q[0]) for q in курсор.запросы)
        self.assertNotIn("bt_3_word_confirm_digest", запросы)


if __name__ == "__main__":
    unittest.main()


# ── Пустое предложение модели = «поле убрать» ────────────────────────────────
class TestПустоеПредложениеЭтоУборкаПоля:
    """Модель по «Finster» предложила {"forms": {}} — убрать таблицу форм, потому что в
    карточке лежало склонение ФАМИЛИИ. Владелец увидел на экране слово «станет» и
    пустоту после него: предложение было законным, а понять его было нельзя."""

    def test_пустое_значение_очищает_поле_сохраняя_тип(self):
        from backend.card_complaints import _применить_правку
        карточка = {"forms": {"plural": "die Finster", "genitive": "des Finsters"},
                    "synonyms": ["dunkel"], "memory_tip": "подсказка"}
        итог = _применить_правку(карточка, {"forms": {}})
        assert итог == {"forms": {}}, "словарь очищается словарём, а не строкой"
        assert _применить_правку(карточка, {"synonyms": []}) == {"synonyms": []}
        assert _применить_правку(карточка, {"memory_tip": ""}) == {"memory_tip": ""}

    def test_модель_ответила_строкой_а_поле_словарь(self):
        """Тип поля читают экраны. Правка не имеет права его менять."""
        from backend.card_complaints import _применить_правку
        карточка = {"forms": {"plural": "die Finster"}}
        assert _применить_правку(карточка, {"forms": ""}) == {"forms": {}}

    def test_непустое_значение_ложится_как_есть(self):
        from backend.card_complaints import _применить_правку
        assert _применить_правку({"word_ru": "темнота"},
                                 {"word_ru": "мрак"}) == {"word_ru": "мрак"}
