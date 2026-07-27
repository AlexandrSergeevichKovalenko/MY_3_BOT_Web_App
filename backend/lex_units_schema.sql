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
