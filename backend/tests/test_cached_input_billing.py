"""Кешированный вход считается по своей цене, а не по цене свежего.

Почему это важно настолько, чтобы держать тест: любой промпт с длинной постоянной
инструкцией OpenAI кеширует автоматически, и кешированная часть стоит вчетверо дешевле.
У ночного разбора словаря из кеша идёт 92% входа (замер 02.08.2026) — пока мы писали
весь вход по полной цене, отчёт показывал эту работу в полтора раза дороже, чем она есть,
и решения принимались по завышенной цифре.
"""
import re

from backend.openai_usage_logging import cached_input_tokens, split_input_tokens


class _SdkUsage:
    """Ответ SDK: usage — объект, детали лежат во вложенном объекте."""

    class _Details:
        cached_tokens = 2688

    prompt_tokens = 2921
    completion_tokens = 1074
    prompt_tokens_details = _Details()


def test_cached_tokens_read_from_every_shape():
    # плоское поле — так его кладёт наш собственный сборщик usage
    assert cached_input_tokens({"cached_prompt_tokens": 2688}) == 2688
    # chat completions
    assert cached_input_tokens({"prompt_tokens_details": {"cached_tokens": 2688}}) == 2688
    # responses API
    assert cached_input_tokens({"input_tokens_details": {"cached_tokens": 512}}) == 512
    # объект SDK, а не словарь
    assert cached_input_tokens(_SdkUsage()) == 2688


def test_no_cache_reported_means_zero_not_a_guess():
    assert cached_input_tokens({"prompt_tokens": 2921}) == 0
    assert cached_input_tokens(None) == 0
    assert split_input_tokens({"prompt_tokens": 900}, 900) == (0, 900)


def test_split_matches_the_measured_nightly_call():
    cached, fresh = split_input_tokens({"cached_prompt_tokens": 2688}, 2921)
    assert (cached, fresh) == (2688, 233)
    assert cached + fresh == 2921  # ни один токен не потерялся и не задвоился


def test_cache_larger_than_input_never_goes_negative():
    # провайдер иногда округляет; отрицательный «свежий» вход обнулил бы строку расхода
    assert split_input_tokens({"cached_prompt_tokens": 5000}, 2921) == (2921, 0)


def test_price_env_pattern_reads_cached_side():
    """Ленивая группа модели: жадная съела бы «..._CACHED» как часть названия модели и
    прочитала бы сторону как обычный INPUT — цена кеша молча встала бы полной."""
    pattern = re.compile(r"^OPENAI_PRICE_(.+?)_(CACHED|INPUT|OUTPUT)_PER_1M$")
    assert pattern.match("OPENAI_PRICE_GPT_4_1_MINI_CACHED_PER_1M").groups() == ("GPT_4_1_MINI", "CACHED")
    assert pattern.match("OPENAI_PRICE_GPT_4_1_MINI_INPUT_PER_1M").groups() == ("GPT_4_1_MINI", "INPUT")
    assert pattern.match("OPENAI_PRICE_GPT_4_1_2025_04_14_OUTPUT_PER_1M").groups() == (
        "GPT_4_1_2025_04_14", "OUTPUT",
    )


def test_cost_of_the_nightly_word_after_the_fix():
    """Цена одного ночного слова по прайсу OpenAI (вход $0.40, кеш $0.10, выход $1.60
    за 1M). До правки вход считался целиком по $0.40 — отсюда и завышение."""
    cached, fresh = split_input_tokens({"cached_prompt_tokens": 2688}, 2921)
    correct = (cached * 0.10 + fresh * 0.40 + 1074 * 1.60) / 1_000_000
    overstated = (2921 * 0.40 + 1074 * 1.60) / 1_000_000
    assert round(correct, 6) == 0.002080
    assert round(overstated, 6) == 0.002887
    assert overstated > correct * 1.35
