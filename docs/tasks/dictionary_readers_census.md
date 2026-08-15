# Перепись читателей разбора

15.08.2026. Кто в коде достаёт разбор слова и откуда: из ЛИЧНОЙ копии в карточке
человека или из ОБЩЕГО слова (`bt_3_lex_units`).

Зачем перепись. Копии разбора в личных карточках плодятся не по недосмотру: ночная
раздача `fill_thin_cards_from_units` (backend_server.py:11015) их создаёт намеренно,
и в её же комментарии написано зачем — «тренажёр и подсказка читают ЛИЧНУЮ карточку,
а туда разбор сам не переезжает». Проверено на живой базе: 2 315 снятых копий за одну
ночь вернулись обратно в количестве 1 994. Пока читатели читают личную карточку, снимать
копии бессмысленно. Значит сначала — перепись, потом перевод читателей, и только потом
снятие копий.

## Как считалось (и три ловушки, каждая давала ложный ответ)

1. **Считать по всему коду, а не по `backend/`.** Телеграм-бот `bot_3.py` (44 654 строки)
   лежит в корне. Первый прогон его не увидел и объявил четыре живые функции мёртвыми.
2. **Искать вызовы по дереву кода, а не текстом.** В боте аргументы вызова разнесены по
   строкам, поиск `имя(` в одной строке их не находит.
3. **Считать не только вызовы, но и упоминания имени.** Бот вызывает функции через
   `asyncio.to_thread(имя, ...)` — узла «вызов» в коде нет, имя стоит аргументом.
   Прогон по вызовам объявил `apply_flashcard_feel_feedback` и
   `list_low_accuracy_telegram_quiz_entries` мёртвыми — обе живые.

Итоговый способ: разобрать код в дерево, взять функции, у которых `response_json` стоит
в СПИСКЕ ВЫБОРКИ запроса к `bt_3_webapp_dictionary_queries` (а не в условии), и найти
все упоминания их имён во всём репозитории. Скрипты прогона — в рабочей папке сессии.

## A. Читают личную копию, общее слово не спрашивают

| Что видит человек | Вход | Функция чтения |
|---|---|---|
| Быстрый словарь и «Мои слова» | `POST /api/webapp/dictionary/cards` | `get_webapp_dictionary_entries` |
| Ответ на повторении (после переворота) | `POST /api/cards/review` | `get_dictionary_entry_for_user` |
| План на день, новое слово к вводу | `_build_today_plan_for_user` | `get_next_new_srs_candidate` |
| Тренажёр предложений | `_build_sentence_training_set` | `get_webapp_dictionary_entries` |
| «Почувствовать слово» в чате | `_dispatch_flashcard_feel_messages` | `_fetch_flashcard_feel_entries_for_user` |
| Квизы в чате: новый квиз и пул | `bot_3.py` `_select_new_scheduled_quiz`, `prepare_scheduled_quiz_pool` | `get_random_dictionary_entry`, `get_random_dictionary_entry_for_quiz_type` |
| Квизы в чате: неправильные варианты | `bot_3.py` `_pick_anagram_distractors`, `_build_prefix_distractors` | `get_random_dictionary_entry` |
| Повтор слов с низкой точностью | `bot_3.py` (через `asyncio.to_thread`) | `list_low_accuracy_telegram_quiz_entries` |
| Озвучка вперёд (фон) | `_dispatch_tts_prewarm` | `_list_predicted_tts_candidates_for_user` |
| Прогрев предложений (фон) | `_dispatch_sentence_prewarm` | `get_webapp_dictionary_entries` |

`GET /api/mobile/dashboard` тоже читает личную копию, но фронт его не вызывает ни разу —
проверено поиском по `frontend/src`. Перед переводом проверить, не мёртвый ли это экран.

## B. Читают общее слово — так и должно быть

| Что видит человек | Вход | Функция |
|---|---|---|
| Очередь повторения | `_list_srs_queue_cards` | + `attach_unit_content_to_cards` |
| Список словаря | `POST /api/webapp/vocabulary/list` | `list_user_vocabulary` |
| Одна карточка по номеру | — | `get_dictionary_entry_by_id` |

Обрати внимание: словарь читается ДВУМЯ разными путями. `/vocabulary/list` спрашивает
общее слово, `/dictionary/cards` — нет. Оба вызываются фронтом.

## C. Служебные — разбор человеку не показывают

Ночная раздача копий `fill_thin_cards_from_units` · копировальня подписки
`materialize_subscription_card` · дозаполнение `_run_dictionary_card_metainfo_backfill`,
`backfill_quick_dictionary_translations`, `repair_dictionary_cards_from_raw_text` ·
пересборка заданий `regenerate_all_sentence_gap_tasks` · отчёт
`get_dictionary_pool_report_stats`.

## D. Мёртвый код — не используется нигде

`get_dictionary_entries_for_tts_prewarm` · `get_latest_dictionary_language_pair_for_user`

## Что из этого следует

Читателей личной копии — десять живых поверхностей, из них семь показывают разбор
человеку прямо сейчас. Перевести их на общее слово можно по одному: у каждого на входе
уже есть номер слова (`lex_unit_id`), а готовая функция слияния —
`attach_unit_content_to_cards`. Порядок: сначала самые заметные (быстрый словарь, ответ
на повторении, квизы в чате), после каждого — проверка на живой базе, что карточка не
опустела. Когда переведены все — выключается ночная раздача, и только тогда снимаются
копии.
