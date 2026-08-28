# -*- coding: utf-8 -*-
"""Озвучка банка аудирования обязана оставлять след в ведомости расходов.

ПОВОД, 28.08.2026. Владелец: «счётчик слепой — почини». Сверка счёта Google с нашей
ведомостью по дням за август:

    день      Google WaveNet   ведомость   банк аудирования
    01.08         15 564          5 637          8 240
    08.08          8 348            420          8 076
    16.08          9 514            983          9 319
    21.08         42 484            780         42 357

Всплески совпали в символ: 67 992 символа за месяц уходили в Google, не оставляя в
`bt_3_billing_events` НИ ОДНОЙ строки.

Корень был тонкий и потому живучий. `get_or_create_tts_clip` честно зовёт внутри
`_note_tts_synthesis(text)` — но тот пишет в ПОТОКОВЫЙ (thread-local) ledger, который
существует только внутри `tts_synthesis_accounting()`. Ночная работа
`_backfill_listening_audio` этот контекст не открывала, и заметка о синтезе падала в
пустоту. Снаружи всё выглядело исправным: функция вызывалась, лог писался, звук
получался — не было только денег.

Чем это было опасно: банк наполняется пачками (в ночь на 21.08 — 37 записей). Один
такой прогон съедает 4–5% месячного бесплатного лимита WaveNet, и ни в деньгах, ни в
остатке лимита это не видно НИКАК. До перерасхода можно было дойти молча.

Тест сторожит две вещи: что учёт вообще открывается вокруг синтеза, и что расход
пишется НА ДОМ (user_id=None) — банк переиспользуется всеми, а по схеме владельца
на человека идёт только то, что никому больше не пригодится.
"""
import re
import unittest
from pathlib import Path

_BOT = Path(__file__).resolve().parent.parent.parent / "bot_3.py"
_SERVER = Path(__file__).resolve().parent.parent / "backend_server.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


class ListeningBackfillOpensAccountingTest(unittest.TestCase):
    """Разбор исходника, а не запуск ночной работы: поднять её в тесте значит позвать
    Google и записать расход в боевую ведомость. Смотрим на то место, где дефект жил."""

    def setUp(self):
        text = _read(_BOT)
        match = re.search(
            r"async def _backfill_listening_audio\(.*?(?=\nasync def |\ndef )", text, re.S)
        self.assertIsNotNone(match, "функция _backfill_listening_audio исчезла из bot_3.py")
        self.body = match.group(0)

    def test_synthesis_runs_inside_an_accounting_context(self):
        self.assertIn("tts_synthesis_accounting()", self.body)
        self.assertIn("bill_listening_bank_tts", self.body)

    def test_the_clip_call_is_not_left_bare(self):
        """Голый `get_or_create_tts_clip` в этой работе = вернувшееся слепое место.
        Единственный законный вызов — внутри обёртки с учётом."""
        wrapper = re.search(
            r"def _synthesize_with_accounting\(.*?\n(?=\n    for e in entries)", self.body, re.S)
        self.assertIsNotNone(wrapper, "обёртка с учётом пропала")
        outside = self.body.replace(wrapper.group(0), "")
        # Комментарии не исполняются: в блоке «ПОЧИНЕНО» имя функции упомянуто нарочно,
        # чтобы следующий читатель понял, где был дефект.
        code_only = "\n".join(
            line for line in outside.splitlines() if not line.lstrip().startswith("#"))
        self.assertNotIn("get_or_create_tts_clip", code_only)

    def test_billing_survives_a_failure_further_down_the_line(self):
        """Google получил символы даже если упадёт упаковка или выгрузка — расход
        пишется в finally, а не после успешного возврата."""
        wrapper = re.search(
            r"def _synthesize_with_accounting\(.*?\n(?=\n    for e in entries)", self.body, re.S).group(0)
        self.assertIn("finally:", wrapper)
        self.assertLess(wrapper.index("finally:"), wrapper.index("bill_listening_bank_tts"))


class ListeningBillingGoesToTheHouseTest(unittest.TestCase):
    def setUp(self):
        text = _read(_SERVER)
        match = re.search(r"def bill_listening_bank_tts\(.*?(?=\ndef )", text, re.S)
        self.assertIsNotNone(match, "bill_listening_bank_tts исчезла из backend_server.py")
        self.body = match.group(0)

    def test_charged_to_the_house_not_to_a_person(self):
        """Банк переиспользуется всеми — расход идёт на дом. Если тут появится
        конкретный user_id, один человек начнёт платить за общий банк."""
        self.assertIn("user_id=None", self.body)

    def test_it_bills_the_premium_bucket_in_chars(self):
        """Голос банка — Neural2, Google выставляет его по тарифу WaveNet, то есть
        премиум-бакет google_tts, а не дешёвый google_tts_standard."""
        self.assertIn('provider="google_tts"', self.body)
        self.assertIn('units_type="chars"', self.body)
        self.assertNotIn('provider="google_tts_standard"', self.body)

    def test_zero_characters_write_nothing(self):
        """Пустой синтез — это не расход нуля, это отсутствие расхода. Строка с нулём
        засорила бы ведомость и сделала бы «сколько раз звали Google» неверным."""
        self.assertIn("if chars <= 0:", self.body)

    def test_a_failure_to_bill_is_shouted_not_swallowed(self):
        """Если запись расхода не удалась — это дырка в учёте, и она обязана быть в
        логах предупреждением, а не debug-строкой, которую никто не включает."""
        self.assertIn("logging.warning", self.body)
        self.assertNotIn("logging.debug", self.body)


if __name__ == "__main__":
    unittest.main()
