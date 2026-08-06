-- Слой ЕДИНИЦ словаря. Создаётся РЯДОМ со старым банком bt_3_dictionary_entries:
-- ни одна существующая таблица не меняется и не удаляется, старый банк остаётся
-- рабочим до переключения и служит журналом происхождения.
--
-- Смысл слоя: банк опознаёт СТРОКИ ТЕКСТА («что спросили → что ответили»), поэтому
-- одно слово живёт в нескольких строках, обратное направление не находится, а склейка
-- разбора одного слова с заголовком другого возможна в принципе. Здесь опознаётся
-- СЛОВО, а все способы его напечатать — отдельные указатели на него.

-- ── 1. Единицы: то, что учат ────────────────────────────────────────────────────
-- Язык — свойство единицы: в банке уже лежат английские и итальянские карточки,
-- поэтому немецкое нигде не зашивается.
CREATE TABLE IF NOT EXISTS bt_3_lex_units (
    id            BIGSERIAL PRIMARY KEY,
    lang          TEXT NOT NULL,                 -- 'de' | 'ru' | 'en' | …
    kind          TEXT NOT NULL,                 -- 'word' | 'collocation' | 'sentence'
    lemma         TEXT NOT NULL,                 -- каноническое написание без артикля
    lemma_key     TEXT NOT NULL,                 -- нормализованный ключ опознания
    pos           TEXT,                          -- часть речи; NULL = ещё не установлена
    pos_source    TEXT,                          -- 'wiktionary' | 'pool' | 'gloss' | 'gpt'
    gender        TEXT,                          -- 'der'|'die'|'das'; только сущ. в de
    gender_source TEXT,                          -- 'wiktionary' | 'pool' | 'gpt' | 'user'
    display       TEXT NOT NULL,                 -- как показывать: «der Rüpel», «sich freuen»
    card          JSONB,                         -- разбор: формы, транскрипция, примеры
    card_source   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Заслон: слово обязано быть написано СВОИМ алфавитом ────────────────────────
-- Немецкая единица с русским текстом («Ты ужасно воняешь.» как немецкое слово) —
-- не опечатка, а перепутанные стороны: карточка после этого указывает на чужое слово,
-- разбор к ней не приезжает, а поиск такую единицу не находит вовсе.
--
-- Замер 05.08.2026: 124 немецких единицы с русским текстом и 350 русских с немецким.
-- Почти все — от массовой сборки 27 июля и от разовых прогонов, то есть от кода,
-- который писал В ОБХОД проверок на пути сохранения. Проверки в коде такое не ловят по
-- определению: каждый новый скрипт заводит свои или не заводит никаких.
--
-- Поэтому правило стоит в САМОЙ БАЗЕ: мимо него не пройдёт ни скрипт, ни ветка, ни
-- будущий агент. NOT VALID — чтобы запрет начал действовать на новые записи немедленно,
-- не спотыкаясь о накопленное; старое чистится отдельно и потом проверяется командой
-- VALIDATE CONSTRAINT.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_script_matches_lang'
    ) THEN
        ALTER TABLE bt_3_lex_units
            ADD CONSTRAINT chk_lex_units_script_matches_lang
            CHECK (
                (lang <> 'de' OR display ~ '[A-Za-zÄÖÜäöüß]')
                AND (lang <> 'ru' OR display ~ '[А-Яа-яЁё]')
            ) NOT VALID;
    END IF;
END $$;

-- ── Правила, которые стережёт САМА БАЗА ────────────────────────────────────────
-- Всё, что здесь перечислено, замерено на живой базе 06.08.2026 и уже соблюдается.
-- Смысл ограничений не в том, чтобы что-то починить, а в том, чтобы это НЕ СЛОМАЛОСЬ
-- впредь: пишут в эти таблицы пять разных путей (сохранение человеком, ночной добор,
-- импорт подписки, массовые сборки, разовые скрипты), и проверка в коде одного из них
-- остальных не касается.
--
-- Язык проверяется как «две строчные буквы», а не списком: пара немецкий-английский
-- заработает без правки этого файла.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_kind') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_kind
            CHECK (kind IN ('word', 'collocation', 'sentence'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_lang_code') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_lang_code
            CHECK (lang ~ '^[a-z]{2}$');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_names_filled') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_names_filled
            CHECK (BTRIM(display) <> '' AND BTRIM(lemma) <> '' AND BTRIM(lemma_key) <> '');
    END IF;
    -- Ключ опознания с заглавной буквой или пробелом по краям не найдётся никогда:
    -- поиск нормализует запрос, а в базе лежит ненормализованное.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_key_normalized') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_key_normalized
            CHECK (lemma_key = LOWER(BTRIM(lemma_key)));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_gender_values') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_gender_values
            CHECK (gender IS NULL OR gender IN ('der', 'die', 'das'));
    END IF;
    -- Род у нерусского слова — признак перепутанных сторон, а не опечатка.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_gender_german_only') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_gender_german_only
            CHECK (gender IS NULL OR lang = 'de');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_card_is_object') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT chk_lex_units_card_is_object
            CHECK (card IS NULL OR jsonb_typeof(card) = 'object');
    END IF;
END $$;

-- Опознание слова = лемма + часть речи + род. Именно род разводит омографы:
-- «der Kiefer» (челюсть) и «die Kiefer» (сосна) — разные единицы, а «Rüpel» и
-- «der Rüpel» — одна. COALESCE, потому что в уникальном индексе NULL не совпадает
-- сам с собой и записи без части речи размножились бы.
CREATE UNIQUE INDEX IF NOT EXISTS uq_lex_units_identity
    ON bt_3_lex_units (lang, kind, lemma_key, COALESCE(pos, ''), COALESCE(gender, ''));

-- ── 2. Указатели: все способы напечатать ────────────────────────────────────────
-- «rüpel», «der rüpel», «Rüpeln», «des Rüpels» → одна и та же единица.
CREATE TABLE IF NOT EXISTS bt_3_lex_surfaces (
    id          BIGSERIAL PRIMARY KEY,
    lang        TEXT NOT NULL,
    surface_key TEXT NOT NULL,                   -- нормализованное написание
    unit_id     BIGINT NOT NULL REFERENCES bt_3_lex_units(id) ON DELETE CASCADE,
    match_kind  TEXT NOT NULL,                   -- 'exact'|'no_article'|'inflected'|'typo'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lang, surface_key, unit_id)
);
CREATE INDEX IF NOT EXISTS idx_lex_surfaces_lookup
    ON bt_3_lex_surfaces (lang, surface_key);

-- Написание с заглавной буквой или пробелом по краям не найдётся: поиск нормализует
-- запрос, а в базе лежало бы ненормализованное.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_surfaces_key_normalized') THEN
        ALTER TABLE bt_3_lex_surfaces ADD CONSTRAINT chk_lex_surfaces_key_normalized
            CHECK (surface_key = LOWER(BTRIM(surface_key)) AND BTRIM(surface_key) <> '');
    END IF;
    -- Написание обязано быть на языке своего слова. Проверкой одной строки это не
    -- выразить — язык лежит в соседней таблице, поэтому ссылка идёт ПАРОЙ (слово, язык).
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_lex_units_id_lang') THEN
        ALTER TABLE bt_3_lex_units ADD CONSTRAINT uq_lex_units_id_lang UNIQUE (id, lang);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_lex_surfaces_unit_lang') THEN
        ALTER TABLE bt_3_lex_surfaces ADD CONSTRAINT fk_lex_surfaces_unit_lang
            FOREIGN KEY (unit_id, lang) REFERENCES bt_3_lex_units (id, lang) ON DELETE CASCADE;
    END IF;
END $$;

-- ── 3. Переводы: связь ЕДИНИЦЫ с ЕДИНИЦЕЙ, а не текста с текстом ───────────────
-- Отсюда обратное направление работает само: «враг» — единица, у неё связь с
-- «der Feind». И синоним не может подменить слово: связь ведёт к своей единице,
-- а не переписывает заголовок чужой.
CREATE TABLE IF NOT EXISTS bt_3_lex_links (
    id          BIGSERIAL PRIMARY KEY,
    from_unit   BIGINT NOT NULL REFERENCES bt_3_lex_units(id) ON DELETE CASCADE,
    to_unit     BIGINT NOT NULL REFERENCES bt_3_lex_units(id) ON DELETE CASCADE,
    rank        INTEGER NOT NULL DEFAULT 100,    -- меньше = главнее
    source      TEXT,                            -- 'pool'|'base_dict'|'wiktionary'|'gpt'|'user'
    saves_count INTEGER NOT NULL DEFAULT 0,      -- сколько РАЗНЫХ людей сохранили эту пару
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_unit, to_unit)
);
CREATE INDEX IF NOT EXISTS idx_lex_links_from ON bt_3_lex_links (from_unit, rank);
CREATE INDEX IF NOT EXISTS idx_lex_links_to   ON bt_3_lex_links (to_unit, rank);

-- ── 4. Происхождение: из каких строк старого банка собрана единица ──────────────
-- Старые строки не трогаются; здесь только ссылка на них, чтобы всегда было видно,
-- откуда что взялось, и чтобы сборку можно было прогнать заново.
CREATE TABLE IF NOT EXISTS bt_3_lex_unit_sources (
    unit_id  BIGINT NOT NULL REFERENCES bt_3_lex_units(id) ON DELETE CASCADE,
    entry_id BIGINT NOT NULL,                    -- bt_3_dictionary_entries.id
    side     TEXT NOT NULL,                      -- 'source' | 'target'
    PRIMARY KEY (unit_id, entry_id, side)
);
CREATE INDEX IF NOT EXISTS idx_lex_unit_sources_entry
    ON bt_3_lex_unit_sources (entry_id);

-- ── 5. Значения: перевод крепится к ЗНАЧЕНИЮ, а не к слову ─────────────────────
-- Так устроены все нормальные словари (Wiktionary/Wikidata, PONS, Duden): у значения
-- свой номер, переводы висят на нём. У нас перевод был просто текстом, поэтому в базу
-- попадали свалки вида «1прикладывать; накладывать 2 надевать 3 строить» — одно
-- «значение», внутри которого их шесть. Разрезаем на входе и складываем сюда.
--
-- Пометки («перен.», «разг.», «Genitiv/Dativ») тоже живут здесь отдельными полями,
-- а не внутри текста перевода: тогда их можно показать значком, скрыть или отфильтровать.
CREATE TABLE IF NOT EXISTS bt_3_lex_senses (
    id         BIGSERIAL PRIMARY KEY,
    unit_id    BIGINT NOT NULL REFERENCES bt_3_lex_units(id) ON DELETE CASCADE,
    sense_no   INTEGER NOT NULL,          -- порядковый номер значения у слова
    label      TEXT,                      -- пометка: «перен.», «разг.», область
    note       TEXT,                      -- пояснение из скобок, если оно осмысленное
    source     TEXT,                      -- откуда значение: разрез | обогащение | вручную
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (unit_id, sense_no)
);
CREATE INDEX IF NOT EXISTS idx_lex_senses_unit ON bt_3_lex_senses (unit_id, sense_no);

-- Связь-перевод принадлежит значению, если оно известно. Колонка необязательная:
-- старые связи продолжают работать как раньше, пока значение не проставлено.
ALTER TABLE bt_3_lex_links ADD COLUMN IF NOT EXISTS sense_id BIGINT
    REFERENCES bt_3_lex_senses(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_lex_links_sense ON bt_3_lex_links (sense_id);


-- Связь-перевод: слово не переводит само себя, а ранг не бывает отрицательным —
-- отрицательный обошёл бы любой приоритет и встал первым.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_links_not_self') THEN
        ALTER TABLE bt_3_lex_links ADD CONSTRAINT chk_lex_links_not_self
            CHECK (from_unit <> to_unit);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_links_rank_sane') THEN
        ALTER TABLE bt_3_lex_links ADD CONSTRAINT chk_lex_links_rank_sane
            CHECK (rank >= 0);
    END IF;
END $$;

-- Английская сторона. Правило то же, что для немецкой, но поставить его сразу было
-- нельзя: 239 из 494 английских единиц содержали русский текст — это была русская
-- сторона пары, помеченная чужим языком. 06.08.2026 они развёрнуты и слиты с
-- правильными русскими единицами (осталось 255 английских, все на латинице), и правило
-- можно ставить полноценным — не NOT VALID.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_lex_units_english_is_latin'
    ) THEN
        ALTER TABLE bt_3_lex_units
            ADD CONSTRAINT chk_lex_units_english_is_latin
            CHECK (lang <> 'en' OR display ~ '[A-Za-z]');
    END IF;
END $$;
