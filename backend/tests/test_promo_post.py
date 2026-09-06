"""Пост для пересылки обязан влезать в подпись под фото.

Telegram отказывает в sendPhoto целиком, если caption длиннее 1024 символов, —
обрезки не бывает. Поэтому промах мимо лимита ломает не вид, а саму отправку:
/post просто ничего не пришлёт. Тест ловит это на правке текста, а не в проде.
"""
from backend.promo_post import CAPTION_LIMIT, POST_VARIANTS


def test_каждый_вариант_поста_влезает_в_подпись_под_фото():
    assert POST_VARIANTS, "вариантов поста нет — /post нечего слать"
    for label, text in POST_VARIANTS:
        assert text.strip(), f"вариант {label} пустой"
        assert len(text) <= CAPTION_LIMIT, (
            f"вариант {label}: {len(text)} символов при лимите {CAPTION_LIMIT} — "
            "Telegram откажет в отправке фото целиком"
        )


def test_метки_вариантов_не_повторяются():
    labels = [label for label, _text in POST_VARIANTS]
    assert len(labels) == len(set(labels)), f"повторяющиеся метки: {labels}"
