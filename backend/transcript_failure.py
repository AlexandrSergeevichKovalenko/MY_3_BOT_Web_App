"""Почему субтитры не достались — и имеем ли мы право судить ролик навсегда.

Владелец, 29.08.2026:

> «Ну а мы возьмём просто за 25 секунд навсегда решим что этот ролик плохой и его в
> чёрный список добавим?? Но это неправильно. Нужно дать время, чтобы мы точно знали,
> что мы хороший ролик не выбросили.»

Он прав, и правило отсюда простое: **приговор выносится по ОТВЕТУ, а не по секундомеру.**
Медленно — не значит «нету». Замер 29.08.2026 по ролику sPJLkkwyLYs: 91 секунда ожидания
оказалась не «висит сеть», а девять последовательных внятных ответов «таких субтитров
нет». А те же 91 секунда у другого ролика могли бы означать заблокированный адрес — и
выбросить его навсегда было бы ошибкой, которую никто потом не заметит.

Различить их можно только по типу ошибки, который отдаёт youtube_transcript_api. Поэтому
лестница в backend_server.py больше не глушит типы (`except Exception: continue`), а
складывает их в текст ошибки, а разбирает этот текст функция ниже.

Направление сомнения задано осознанно: **сомневаешься — НЕ приговор.** Если среди причин
есть хоть один признак «нас не пустили», весь ответ считается временным, даже если другие
ступени успели сказать «субтитров нет»: заблокированная ступень означает, что проверка
была неполной, а приговор по неполной проверке — это и есть выброшенный хороший ролик.
"""
from __future__ import annotations

# Ролик проверен и субтитров у него вправду нет — это ответ YouTube, а не наша догадка.
VERDICT_NO_CAPTIONS = "no_captions"
# Ролик недоступен нам в принципе: удалён, приватный, возрастной, неиграбельный.
VERDICT_UNUSABLE = "unusable"
# Нас не пустили: блокировка адреса, лимит запросов, сеть, мёртвый прокси.
VERDICT_BLOCKED = "blocked"
# Мы сами перестали ждать. Про ролик это не говорит НИЧЕГО.
VERDICT_TIMEOUT = "timeout"
# Причину распознать не удалось. Тоже не приговор.
VERDICT_UNKNOWN = "unknown"
# Одна из ступеней лестницы вообще не запускалась (мёртвый прокси, не прошла проверка
# страны). Проверка была НЕПОЛНОЙ, поэтому судить ролик нельзя — даже если то, что
# успело ответить, говорило «субтитров нет».
VERDICT_INCOMPLETE = "incomplete"

# Приговор навсегда имеют право выносить только эти два.
PERMANENT_VERDICTS = frozenset({VERDICT_NO_CAPTIONS, VERDICT_UNUSABLE})

_BLOCKED_MARKERS = (
    "RequestBlocked", "IpBlocked", "TooManyRequests", "YouTubeRequestFailed",
    "PoTokenRequired", "CookieError", "CookieInvalid", "CookiePathInvalid",
    "FailedToCreateConsentCookie", "YouTubeDataUnparsable",
    "ProxyError", "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout",
    "SSLError", "HTTPError", "MaxRetryError",
)
# Ни одна ступень не запустилась — сказать о ролике нечего.
#
# ПРОВЕРЕНО 29.08.2026: строку «generic rejected country» сюда НЕ возвращать. Четвёртая
# ступень (свой прокси DE/AU) не запускается, потому что владелец за эти адреса больше не
# платит и они мертвы. Отсутствие ступени — вопрос покрытия ко всей системе, он виден
# числом в bt_3_transcript_source_stats (строка skipped:generic), а не сомнение в
# конкретном ответе YouTube: этот ответ уже подтверждён напрямую и трижды через немецкие
# адреса webshare. Если считать её пропуск сомнением, ни один ролик никогда не получит
# приговора, реестр не наполнится и полка снова встанет.
_INCOMPLETE_MARKERS = ("ни одна ступень не запускалась",)
_UNUSABLE_MARKERS = (
    "VideoUnavailable", "InvalidVideoId", "AgeRestricted", "VideoUnplayable",
)
_NO_CAPTIONS_MARKERS = (
    "TranscriptsDisabled", "NoTranscriptFound", "TranslationLanguageNotAvailable",
    "NotTranslatable", "no transcripts for language order",
)


def classify_transcript_failure(error_text: str | None) -> str:
    """Разобрать текст ошибки лестницы в один из вердиктов выше.

    Порядок проверок — это и есть направление сомнения: сперва ищем признаки «нас не
    пустили», и только если их НЕТ, разрешаем себе приговор.
    """
    text = str(error_text or "")
    if not text.strip():
        return VERDICT_UNKNOWN
    if any(marker in text for marker in _BLOCKED_MARKERS):
        return VERDICT_BLOCKED
    if any(marker in text for marker in _INCOMPLETE_MARKERS):
        return VERDICT_INCOMPLETE
    if any(marker in text for marker in _UNUSABLE_MARKERS):
        return VERDICT_UNUSABLE
    if any(marker in text for marker in _NO_CAPTIONS_MARKERS):
        return VERDICT_NO_CAPTIONS
    return VERDICT_UNKNOWN


def is_permanent(verdict: str | None) -> bool:
    """Можно ли на основании этого вердикта больше никогда не трогать ролик."""
    return str(verdict or "") in PERMANENT_VERDICTS


def verdict_ru(verdict: str | None) -> str:
    """Человеческое название вердикта — оно уходит владельцу в ночном письме."""
    return {
        VERDICT_NO_CAPTIONS: "субтитров нет",
        VERDICT_UNUSABLE: "ролик недоступен",
        VERDICT_BLOCKED: "нас не пустили",
        VERDICT_TIMEOUT: "не дождались",
        VERDICT_UNKNOWN: "причина неясна",
        VERDICT_INCOMPLETE: "проверка неполная",
    }.get(str(verdict or ""), str(verdict or "—"))
