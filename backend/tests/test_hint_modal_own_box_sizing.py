"""Окно «Подсказка по слову» в интерактиве: кнопка «Понятно» под краем экрана (06.09.2026).

Что было. Окно (frontend/src/components/WordHintModal.jsx) общее для двух хозяев:
Space Rep внутри приложения и интерактив «Слова со вчерашних тренировок», который
приходит в личку. Приложение грузит App.css с глобальным `* { box-sizing: border-box }`;
оверлей интерактивов App.css не грузит, а свой сброс держит под `.ans-root *` и
`.wp-root *`. Окно порталом уходит в <body> — мимо обоих — и в интерактиве считалось по
content-box: оверлей на 20px выше экрана, карточка ещё на 39px, кнопка «Понятно» под
нижним краем на всех десяти телефонах матрицы (стенд hintlab_interactive, 06.09.2026).

Починка — сброс у самого окна в WordHintModal.css. Этот файл держит КЛАСС, а не слово:
1) каждый общий компонент, который интерактивы порталят в <body>, обязан нести свой
   сброс box-sizing в своём CSS;
2) измеритель обещания `hint_modal_own_box_sizing` узнаёт правило в минифицированном CSS
   живого сайта и честно говорит «не измерено», если это не собранный фронт.
"""
import pathlib
import re
import unittest
from unittest import mock

FRONT = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "src"
ANSWER = FRONT / "answer"

IMPORT_RE = re.compile(r"""from\s+['"]\.\./(components|dictionary)/([A-Za-z0-9_]+)(?:\.jsx?)?['"]""")
CSS_IMPORT_RE = re.compile(r"""import\s+['"]\./([A-Za-z0-9_.-]+\.css)['"]""")
# Правило, отдающее border-box всему поддереву: селектор «.корень *».
OWN_RESET_RE = re.compile(r"(\.[A-Za-z0-9_-]+)\s+\*[^{]*\{[^}]*box-sizing\s*:\s*border-box", re.S)


def _shared_components_portaled_to_body():
    """Общие компоненты (components/, dictionary/), которые интерактивы порталят в <body>."""
    found = {}
    for jsx in sorted(ANSWER.glob("*.jsx")):
        for folder, name in IMPORT_RE.findall(jsx.read_text(encoding="utf-8")):
            for cand in (FRONT / folder / f"{name}.jsx", FRONT / folder / f"{name}.js"):
                if cand.exists():
                    src = cand.read_text(encoding="utf-8")
                    if "createPortal(" in src and "document.body" in src:
                        found[cand] = src
    return found


class ОбщееОкноНесётСвойСброс(unittest.TestCase):
    def test_окно_подсказки_в_списке(self):
        """Сам страж должен видеть виновника: иначе зелёный тест ничего не доказывает."""
        names = {p.name for p in _shared_components_portaled_to_body()}
        self.assertIn("WordHintModal.jsx", names)

    def test_каждый_портал_в_body_несёт_border_box_в_своём_css(self):
        for path, src in _shared_components_portaled_to_body().items():
            css_names = CSS_IMPORT_RE.findall(src)
            self.assertTrue(css_names, f"{path.name}: порталится в <body>, а своего CSS не подключает")
            css = "".join((path.parent / n).read_text(encoding="utf-8") for n in css_names)
            roots = OWN_RESET_RE.findall(css)
            self.assertTrue(
                roots,
                f"{path.name}: порталится в <body> мимо сбросов интерактива (.ans-root *, "
                f".wp-root *), но в {css_names} нет правила «.корень * {{ box-sizing: border-box }}» "
                f"— в интерактиве окно посчитается по content-box и вылезет за экран",
            )


MINIFIED_WITH = (".x{color:red}.word-hint-overlay,.word-hint-overlay *,.word-hint-overlay *:before,"
                 ".word-hint-overlay *:after{box-sizing:border-box}.word-hint-overlay{position:fixed}")
MINIFIED_WITHOUT = ".x{color:red}.word-hint-overlay{position:fixed;left:0}.word-hint-card{width:100%}"
NOT_OUR_FRONT = ".foo{color:red}.bar *{box-sizing:border-box}"


class ИзмерительУзнаётПравило(unittest.TestCase):
    def test_есть_правило_один(self):
        from backend.fix_promises import _count_hint_modal_own_box_sizing
        self.assertEqual(_count_hint_modal_own_box_sizing(MINIFIED_WITH), 1)

    def test_нет_правила_ноль_а_не_ошибка(self):
        from backend.fix_promises import _count_hint_modal_own_box_sizing
        self.assertEqual(_count_hint_modal_own_box_sizing(MINIFIED_WITHOUT), 0)

    def test_чужой_css_это_не_измерено(self):
        from backend.fix_promises import _count_hint_modal_own_box_sizing
        with self.assertRaises(LookupError):
            _count_hint_modal_own_box_sizing(NOT_OUR_FRONT)

    def test_собранный_нами_css_проходит(self):
        """Правило из настоящего WordHintModal.css после минификации Vite узнаётся."""
        from backend.fix_promises import _count_hint_modal_own_box_sizing
        src = (FRONT / "components" / "WordHintModal.css").read_text(encoding="utf-8")
        src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
        mini = re.sub(r"\s*([{}:;,])\s*", r"\1", src)
        mini = re.sub(r"\s+", " ", mini).replace("::", ":")
        self.assertEqual(_count_hint_modal_own_box_sizing(mini), 1)

    def test_обещание_держится_и_нарушается(self):
        from backend import fix_promises as fp
        with mock.patch.object(fp, "_served_webapp_css", return_value=MINIFIED_WITH):
            self.assertEqual(fp._hint_modal_box_sizing_missing(), 0)
            self.assertIn("ЕСТЬ", fp._hint_modal_screen())
        with mock.patch.object(fp, "_served_webapp_css", return_value=MINIFIED_WITHOUT):
            self.assertEqual(fp._hint_modal_box_sizing_missing(), 1)
            self.assertIn("НЕТ", fp._hint_modal_screen())

    def test_обещание_в_реестре(self):
        from backend.fix_promises import by_key
        p = by_key("hint_modal_own_box_sizing")
        self.assertEqual(p.expected, 0)
        self.assertIsNotNone(p.screen)


if __name__ == "__main__":
    unittest.main()
