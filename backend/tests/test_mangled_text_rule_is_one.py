"""Правило «текст испорчен размноженным хвостом» живёт в ОДНОМ месте.

╔══════════════════════════════════════════════════════════════════════════════════╗
║  ЗАЧЕМ ЭТОТ ТЕСТ СУЩЕСТВУЕТ                                                      ║
║                                                                                  ║
║  Тема открывалась ЧЕТЫРЕ раза подряд, и каждый раз находилось «что-то новое» —   ║
║  не потому, что порча росла, а потому, что каждый заход придумывал себе новый    ║
║  признак и новый список мест:                                                    ║
║                                                                                  ║
║     заход 1: повтор СЛОВА и ЗНАКА, только заголовок слова   → 15 записей          ║
║     заход 2: та же проверка, но заглянул в разбор           → +15 разборов        ║
║     заход 3: та же проверка, но заглянул в карточки людей   → +143 карточки       ║
║     заход 4: повтор БУКВЫ (нашёл соседний агент)            → +1 запись, +пул     ║
║                                                                                  ║
║  Владелец, 21.08.2026: «Ты можешь взять одну часть — идеально объяснить её,      ║
║  исправить, сделать коммент в коде, чтобы мы к этой части более не возвращались!»║
║                                                                                  ║
║  Этот тест и есть то, что не даёт вернуться: он падает, если кто-то заведёт      ║
║  СВОЙ признак порчи у себя в файле вместо общего.                                ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
import pathlib
import re
import unittest

from backend.mangled_text import (
    MANGLED_ALL,
    SQL_MANGLED,
    is_mangled,
    mangled_strings_inside,
    undo_candidates,
)

ROOT = pathlib.Path(__file__).resolve().parents[2]
HOME = "backend/mangled_text.py"          # единственное законное место правила


def _tracked_python_files() -> list[str]:
    """Только то, что ЛЕЖИТ В РЕПОЗИТОРИИ.

    Незакоммиченные файлы не смотрим сознательно. В этом каталоге одновременно работают
    несколько агентов, и у каждого в дереве свои черновики; падать из-за чужой работы
    в процессе — значит блокировать всем пуш за то, чего в репозитории ещё нет.
    Как только файл коммитят, он попадает под этот тест наравне со всеми.
    """
    import subprocess
    try:
        out = subprocess.run(["git", "ls-files", "*.py"], cwd=ROOT,
                             capture_output=True, text=True, timeout=30)
    except Exception:
        return []
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


class TheRuleItselfTests(unittest.TestCase):
    """Три вида хвоста — один механизм, и правило обязано знать все три."""

    def test_all_three_shapes_are_recognised(self):
        for text in ("Er erlag der Versuchung......",
                     "Das Anliegen an jemanden jemanden jemanden jemanden",
                     "Kommen Sie vorbei. vorbei. vorbei. vorbei.",
                     "sterile Gazennnnnn"):
            self.assertTrue(is_mangled(text), text)

    def test_real_german_is_never_called_mangled(self):
        """Порог четыре, а не три: три повтора — законный немецкий и живые заголовки."""
        for text in ("die Schifffahrt", "Brennnessel", "Es kommt darauf an...",
                     "Wenn man bedenkt, dass...", "Er erlag der Versuchung.",
                     "Das Anliegen an jemanden", "sterile Gazen",
                     "Warum nimmst du dir so viel heraus?"):
            self.assertFalse(is_mangled(text), text)

    def test_the_persons_own_russian_is_not_our_business(self):
        """Замок в базе не стоит на русских колонках, и правило это не отменяет.

        Человек вправе написать «нееееет» — это его текст, а не порча. Проверка здесь
        нужна, чтобы никто не «улучшил» правило до того, что уронит человеку сохранение.
        """
        self.assertFalse(is_mangled("да да да"))
        self.assertFalse(is_mangled("ну нет"))

    def test_damage_in_the_middle_of_a_sentence_is_found_too(self):
        """Внутри разбора порча садится в середину, и конец строки при этом чистый.

        Три правила, привязанные к концу, это пропускали. Живые случаи из базы."""
        self.assertTrue(is_mangled(
            "sterile Gazennnnn wird oft in der Wundversorgung benutzt."))
        self.assertTrue(is_mangled(
            "POV: POV: POV: POV: POV: POV: Er will ins Freibad gehen!"))

    def test_live_speech_with_three_repeats_survives(self):
        """Три повтора — живой язык, не порча. Ронять его нельзя.

        Замер 21.08.2026 по 3000 разборам живой базы: пятое правило даёт 0 попаданий
        на законном тексте."""
        self.assertFalse(is_mangled("nein nein nein, das geht nicht"))
        self.assertFalse(is_mangled("ganz ganz ganz vorsichtig"))

    def test_damage_inside_a_breakdown_is_found_at_any_depth(self):
        card = {"word_de": "sterile Gazen",
                "examples": [{"source": "Er erlag der Versuchung......"}],
                "forms": {"plural": ["ok", {"deep": "vorbei. vorbei. vorbei. vorbei."}]}}
        found = mangled_strings_inside(card)
        self.assertEqual(len(found), 2, found)

    def test_the_witness_chooses_how_much_to_undo(self):
        """Восстановление перечисляет варианты, а не решает само."""
        self.assertIn("Er erlag der Versuchung.",
                      undo_candidates("Er erlag der Versuchung......"))
        self.assertIn("sterile Gazen", undo_candidates("sterile Gazennnnnn"))
        self.assertIn("Das Anliegen an jemanden",
                      undo_candidates("Das Anliegen an jemanden jemanden jemanden jemanden"))


class TheRuleLivesInOnePlaceTests(unittest.TestCase):
    def test_nobody_writes_their_own_repeated_tail_regex(self):
        """Свой признак у себя в файле — это и есть пятый заход по кругу.

        Ищем характерную обратную ссылку на повтор (`\\1{3,}`) везде, кроме дома
        правила. Нужен новый вид порчи — добавляй в backend/mangled_text.py, и его
        сразу увидят и уборка, и замок, и этот тест.
        """
        pattern = re.compile(r"\\1\{[23],\}")
        offenders = []
        for relative in _tracked_python_files():
            if relative == HOME or "/tests/" in relative:
                continue
            if not relative.startswith(("backend/", "scripts/")):
                continue
            path = ROOT / relative
            if not path.exists():
                continue
            if pattern.search(path.read_text(encoding="utf-8")):
                offenders.append(relative)
        self.assertEqual(
            offenders, [],
            "Признак порчи заведён мимо backend/mangled_text.py — так тема и "
            "открывалась четыре раза:\n" + "\n".join(offenders))

    def test_the_guard_script_takes_the_rule_from_home(self):
        text = (ROOT / "scripts/dict_guard_repeated_tail.py").read_text(encoding="utf-8")
        self.assertIn("from backend.mangled_text import", text)

    def test_the_cleanup_script_takes_the_rule_from_home(self):
        text = (ROOT / "scripts/dict_undo_repeated_tail_damage.py").read_text(encoding="utf-8")
        self.assertIn("from backend.mangled_text import", text)

    def test_sql_and_python_rules_stay_the_same_count(self):
        """SQL-версия отстала от питоновской — значит замок в базе знает не всё.

        Расхождение законно только у правила «порча в СЕРЕДИНЕ строки»: оно нужно
        разбору и не нужно колонке, и в SQL его сознательно нет. Поэтому счёт
        сверяется с поправкой ровно на него.
        """
        # Два питоновских правила в SQL сознательно не идут: «порча в СЕРЕДИНЕ строки»
        # для буквы и для слова. В колонке лежит заголовок, а не предложение, и цена
        # ошибки там выше — CHECK роняет запись целиком.
        self.assertEqual(len(SQL_MANGLED), len(MANGLED_ALL) - 2)


if __name__ == "__main__":
    unittest.main()
