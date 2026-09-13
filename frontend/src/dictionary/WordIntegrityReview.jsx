import React, { useCallback, useEffect, useState } from 'react';
// ⛔ БЕЗ ЭТОЙ СТРОКИ ЭКРАН БЕЛЫЙ, А ТЕКСТ БЛЕДНО-СЕРЫЙ. Разметка ниже построена на
// классах `ans-root` / `ans-card`, а описаны они в answer.css — и этот экран был
// ЕДИНСТВЕННЫМ, кто их не подключал (у словаря по соседству строка есть:
// DictionaryOverlay.jsx). Без файла нет ни карточки, ни фона, ни светлой темы: цвета
// текста брались из theme.css, где они сделаны для ТЁМНОГО фона (#E2E8F0, #94A3B8), и
// ложились на белое. Владелец 27.08.2026: «Ты видишь цвет текста?»
import '../answer/answer.css';
import './dict.css';
import { api, haptic } from './WordBreakdown';
import { humanizeDictError } from './errors.js';

/**
 * Разбор противоречивых записей словаря — экран владельца.
 *
 * Приходит ссылкой из личных сообщений (понедельник и воскресенье, 09:00) и открывается
 * здесь, а не в чате: только на экране можно показать «было → станет» и дать выбрать
 * решение, не набирая команд.
 *
 * ⛔ КАРТОЧКА ПОКАЗЫВАЕТ ТО, ЧТО РЕШАЕТСЯ. До 27.08.2026 крупно стояла ВИТРИНА записи
 * («der Degenerierte»), а решение принималось про ОСНОВУ («degeneriert») — и диагноз под
 * витриной врал: «существительное, а написано со строчной буквы» стояло под словом, где
 * существительное как раз с большой. Теперь видны обе строки, и подпись описывает ровно
 * то, что проверено.
 *
 * Решения: выбрать одно из прочтений (кнопка со СЛОВОМ, а не «Исправить» — прочтений
 * бывает два, и выбирать за человека нельзя), вписать своё, оставить, удалить.
 * «Применить» выполняет ВСЕ отмеченные решения за один заход — по одному действию на
 * запись, а не два над одной (владелец 26.08.2026 просил сказать это прямо).
 *
 * Ничего не пропадает: неотмеченное остаётся в очереди и придёт снова.
 */

const POS_RU = {
  noun: 'существительное', verb: 'глагол', adjective: 'прилагательное',
  adverb: 'наречие', preposition: 'предлог', conjunction: 'союз',
  participle: 'причастие', particle: 'частица', pronoun: 'местоимение',
};

// scope: 'shared' — общий словарь (только владелец), 'mine' — свои слова (каждому).
// Экран один: и там, и там человек решает судьбу записи кнопками.
const ENDPOINTS = {
  shared: { list: '/api/webapp/dictionary/integrity/list', apply: '/api/webapp/dictionary/integrity/apply' },
  mine: { list: '/api/webapp/dictionary/mywords/review', apply: '/api/webapp/dictionary/mywords/apply' },
};

const TITLES = {
  shared: {
    badge: '🧹 Разбор словаря',
    title: 'Записи, которые противоречат себе',
    empty: 'Противоречивых записей нет — словарь чист.',
  },
  mine: {
    badge: '📚 Мои слова',
    title: 'Слова, которые стоит поправить',
    empty: 'Всё в порядке — ваши слова записаны верно.',
  },
};

// Личный разбор своих слов присылает ОДНУ правку старым полем `suggestion`. Приводим её
// к тому же виду, что и прочтения общего словаря, чтобы экран был один, а не два.
//
// ⛔ ПРАВКУ «ТОЛЬКО ЧАСТЬ РЕЧИ» ЗДЕСЬ ТЕРЯЛИ. НЕ ВОЗВРАЩАТЬ УСЛОВИЕ БЕЗ `to_pos`.
// Отбор стоял `if (!word && !fix.to_translation) return []`, то есть вариант признавался
// существующим только когда в нём есть новое СЛОВО или новый ПЕРЕВОД. Правка вида
// «слово написано верно, а помечено не тем» ({to_pos: 'verb'}) не несёт ни того ни
// другого — и молча выбрасывалась ВСЯ.
//
// Наружу это выглядело так, будто система не знает ответа: под записью стояло
// «НЕ ВОССТАНОВИЛИ. Ни справочник, ни модель это написание не подтвердили», а кнопок
// было две — «Оставить» и «Удалить». На самом деле ответ был: справочник отвечает про
// `begreifen` → verb за доли секунды, и сервер эту правку исправно присылал.
//
// Замер живой базы 13.09.2026: из 10 записей в очереди «помечено существительным» у 6
// (begreifen, jahrelang, vordringen, hart, jucken, erschießen) справочник дал
// однозначный ответ — и ни одна из шести не показывала кнопки.
//
// Это РЕГРЕССИЯ коммита 08576136 (27.08.2026): до него экран рисовал такую правку
// отдельной строкой «существительное → глагол» и давал кнопку. Переписывание на общий
// `optionsOf` эту ветку не перенесло.
function optionsOf(row) {
  if (Array.isArray(row.options) && row.options.length) return row.options;
  const fix = row.suggestion;
  if (!fix) return [];
  const word = fix.to_display || fix.to_lemma || fix.to_word;
  if (!word && !fix.to_translation && !fix.to_pos) return [];
  return [{
    word: word || '',
    pos: fix.to_pos || '',
    translation: fix.to_translation || '',
    source: 'справочник',
    why: fix.why || '',
    legacy: fix,
  }];
}

// Что написать НА КНОПКЕ. Кнопка называется тем, что случится по нажатию, а не словом
// «Исправить»: прочтений бывает несколько, и человек выбирает между ними глазами.
// У правки про часть речи на кнопке стоит сама часть речи — «глагол».
function optionLabel(opt) {
  if (opt.word) return opt.word;
  if (opt.translation) return opt.translation;
  if (opt.pos) return POS_RU[opt.pos] || opt.pos;
  return 'Исправить';
}

// Беды, где «вписать своё» означает ПЕРЕВОД. У остальных — написание самого слова.
// Список общий с сервером (database.py, apply_user_word_decisions): там он решает, в
// какую колонку лечь вписанному, здесь — о чём спросить человека. Решение принимает
// сервер; здесь только текст, чтобы вопрос и ответ были про одно и то же.
const OWN_ASKS_TRANSLATION = new Set(['no_translation', 'translation_equals_word']);

export default function WordIntegrityReview({ scope = 'shared' }) {
  const urls = ENDPOINTS[scope] || ENDPOINTS.shared;
  const copy = TITLES[scope] || TITLES.shared;
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [decisions, setDecisions] = useState({});   // id → { action, option }
  const [ownText, setOwnText] = useState({});       // id → что владелец вписал сам
  const [ownOpen, setOwnOpen] = useState({});       // у каких записей открыто поле
  const [phase, setPhase] = useState('loading');
  const [error, setError] = useState('');
  const [done, setDone] = useState(null);
  const [translations, setTranslations] = useState([]); // не смогли перевести — ждут владельца
  const [typed, setTyped] = useState({});               // вписанные переводы

  const load = useCallback(async (rescan = false) => {
    setPhase('loading');
    setError('');
    try {
      const data = await api(urls.list, { rescan });
      setItems(Array.isArray(data?.items) ? data.items : []);
      setTotal(Number(data?.total) || 0);
      setTranslations(Array.isArray(data?.translations) ? data.translations : []);
      setPhase('ready');
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('error');
    }
  }, [urls]);

  useEffect(() => { void load(true); }, [load]);

  const choose = useCallback((id, action, option = 0) => {
    haptic('light');
    setDecisions((prev) => {
      const next = { ...prev };
      const now = next[id];
      if (now && now.action === action && now.option === option) delete next[id];
      else next[id] = { action, option };
      return next;
    });
  }, []);

  const typeOwn = useCallback((id, text) => {
    setOwnText((prev) => ({ ...prev, [id]: text }));
    setDecisions((prev) => {
      const next = { ...prev };
      if (String(text || '').trim()) next[id] = { action: 'own', option: 0 };
      else if (next[id]?.action === 'own') delete next[id];
      return next;
    });
  }, []);

  const apply = useCallback(async () => {
    const list = Object.entries(decisions).map(([id, pick]) => {
      const row = items.find((x) => String(x.id) === String(id));
      const chosen = optionsOf(row || {})[pick.option] || null;
      if (pick.action === 'own') {
        return { id: Number(id), action: 'own', word: String(ownText[id] || '').trim() };
      }
      if (pick.action !== 'fix') return { id: Number(id), action: pick.action };
      // Общий словарь: с экрана уходит НОМЕР прочтения, само написание сервер берёт из
      // очереди — иначе в словарь можно было бы записать что угодно мимо источников.
      if (scope === 'shared') return { id: Number(id), action: 'fix', option: pick.option };
      // Личный разбор своих слов: прежний договор с сервером, поля из старой правки.
      const legacy = chosen?.legacy || {};
      return {
        id: Number(id),
        action: 'fix',
        ...(legacy.to_word ? { to_word: legacy.to_word } : {}),
        ...(legacy.to_pos ? { to_pos: legacy.to_pos } : {}),
        ...(legacy.to_translation ? { to_translation: legacy.to_translation } : {}),
      };
    }).filter((d) => d.action !== 'own' || d.word);

    if (!list.length && !Object.keys(typed).length) return;
    setPhase('applying');
    haptic('ok');
    try {
      const filled = Object.entries(typed)
        .filter(([, text]) => String(text || '').trim())
        .map(([id, text]) => ({ id: Number(id), translation: text }));
      const data = await api(urls.apply, { decisions: list, translations: filled });
      setDone({
        fixed: Number(data?.fixed) || 0,
        kept: Number(data?.kept) || 0,
        deleted: Number(data?.deleted) || 0,
        left: Number(data?.left) || 0,
      });
      setDecisions({});
      setOwnText({});
      setOwnOpen({});
      setTyped({});
      await load(false);
    } catch (e) {
      setError(humanizeDictError(e));
      setPhase('ready');
    }
  }, [decisions, items, ownText, typed, scope, urls, load]);

  const marked = Object.keys(decisions).length;

  return (
    <div className="ans-root">
      <div className="ans-card wi-root">
        <div className="wi-head">
          <span className="badge">{copy.badge}</span>
          <span className="badge">{total}</span>
        </div>

        <div className="wi-title">{copy.title}</div>
        <div className="wi-sub">
          Отметьте решение у каждой. Применится всё разом — по одному действию на запись.
          Неотмеченное останется и придёт снова.
        </div>

        {done && (
          <div className="wi-done">
            Готово: исправлено <b>{done.fixed}</b>, оставлено <b>{done.kept}</b>,
            удалено <b>{done.deleted}</b>.
            {done.left > 0 ? ` Осталось разобрать: ${done.left}.` : ' Очередь пуста.'}
          </div>
        )}

        {phase === 'loading' && <div className="wi-empty">Смотрю словарь…</div>}
        {phase === 'error' && error && <div className="wi-error">{error}</div>}

        {phase !== 'loading' && !items.length && (
          <div className="wi-empty">{copy.empty}</div>
        )}

        {items.map((row) => {
          const pick = decisions[row.id] || null;
          const options = optionsOf(row);
          const stored = row.stored || row.lemma || row.word || '';
          const shown = row.shown || '';
          const twoWords = Boolean(shown) && shown.toLowerCase() !== stored.toLowerCase();
          return (
            <div className={`wi-row${pick ? ` is-${pick.action === 'own' ? 'fix' : pick.action}` : ''}`} key={row.id}>
              {twoWords ? (
                <>
                  <div className="wi-word">{row.issue_text}</div>
                  <div className="wi-two">
                    <div className="wi-two-line">
                      <span className="wi-two-w">{shown}</span>
                      <span className="wi-two-m">показывается в словаре</span>
                    </div>
                    <div className="wi-two-line">
                      <span className="wi-two-w">{stored}</span>
                      <span className="wi-two-m">лежит в базе — решается именно оно</span>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  <div className="wi-word">{stored}</div>
                  {row.translation && <div className="wi-what">{row.translation}</div>}
                  <div className="wi-what">{row.issue_text}</div>
                </>
              )}

              {options.map((opt, index) => (
                <div className="wi-src" key={`why-${index}`}>
                  <span className={`wi-tag ${opt.source === 'модель' ? 'model' : 'book'}`}>
                    {opt.source === 'модель' ? 'Модель' : 'Справочник'}
                  </span>
                  <span>
                    <b>{optionLabel(opt)}</b>
                    {opt.word && opt.pos ? ` — ${POS_RU[opt.pos] || opt.pos}` : ''}
                    {opt.why ? `. ${opt.why}` : ''}
                  </span>
                </div>
              ))}
              {options.length > 1 && (
                <div className="wi-what">Справочник знает оба. Какое имелось в виду?</div>
              )}
              {/* Текст «не восстановили» обязан говорить про ТУ САМУЮ беду, что в записи.
                  До 13.09.2026 у записи «перевод повторяет само слово» стояло «ни
                  справочник, ни модель это НАПИСАНИЕ не подтвердили» — про написание,
                  которое никто не оспаривал. И про модель: её здесь не спрашивают вовсе
                  (check_word зовётся с allow_model=False), так что ссылаться на неё
                  значило приписывать себе проверку, которой не было. */}
              {!options.length && (
                <div className="wi-src">
                  <span className="wi-tag none">Не восстановили</span>
                  <span>
                    {OWN_ASKS_TRANSLATION.has(row.issue)
                      ? 'Перевод придумывать не станем. Впишите свой или удалите карточку.'
                      : 'Справочник это слово не подтвердил. Если знаете сами — впишите.'}
                  </span>
                </div>
              )}
              {row.expression_hint && (
                <div className="wi-src">
                  <span className="wi-tag book">Справочник</span>
                  <span>
                    Устойчивое выражение. По-немецки объясняют так: <b>{row.expression_hint}</b>
                  </span>
                </div>
              )}
              {(row.issue === 'no_translation' || row.issue === 'translation_equals_word')
                && !options.length && (
                <div className="wi-src">Перевод подберёт ночная работа — обычно к утру.</div>
              )}

              <div className="wi-acts">
                {options.map((opt, index) => (
                  <button
                    type="button"
                    key={`opt-${index}`}
                    className={`wi-act fix${pick?.action === 'fix' && pick.option === index ? ' is-on' : ''}`}
                    onClick={() => choose(row.id, 'fix', index)}
                  >
                    {optionLabel(opt)}
                  </button>
                ))}
                <button
                  type="button"
                  className={`wi-act keep${pick?.action === 'keep' ? ' is-on' : ''}`}
                  onClick={() => choose(row.id, 'keep')}
                >
                  Оставить
                </button>
                <button
                  type="button"
                  className={`wi-act delete${pick?.action === 'delete' ? ' is-on' : ''}`}
                  onClick={() => choose(row.id, 'delete')}
                >
                  Удалить
                </button>
                {/* ⛔ КНОПКА И ПОЛЕ СТОЯЛИ ПОД `scope === 'shared'`, А ПОДПИСЬ «ЕСЛИ
                    ЗНАЕТЕ САМИ — ВПИШИТЕ» ПОКАЗЫВАЛАСЬ ВСЕМ. На экране «Мои слова»
                    вписать было некуда: две кнопки, «Оставить» и «Удалить», и текст,
                    зовущий сделать третье. Владелец 13.09.2026: «как я тут могу вписать
                    это слово?! на хуя эта статистика тупая?»
                    Замок снят с обоих экранов, и сервер научен действию «своё»
                    (database.py, apply_user_word_decisions) — без него кнопка была бы
                    такой же пустой подписью, только нажимаемой. */}
                <button
                  type="button"
                  className="wi-act own"
                  onClick={() => setOwnOpen((prev) => ({ ...prev, [row.id]: !prev[row.id] }))}
                >
                  ✎ Своё
                </button>
              </div>

              {ownOpen[row.id] && (
                <div className="wi-own">
                  <input
                    className="wi-input"
                    type="text"
                    placeholder={OWN_ASKS_TRANSLATION.has(row.issue)
                      ? 'Напишите перевод'
                      : 'Напишите верную форму'}
                    value={ownText[row.id] || ''}
                    onChange={(e) => typeOwn(row.id, e.target.value)}
                  />
                  <div className="wi-src">
                    {OWN_ASKS_TRANSLATION.has(row.issue)
                      ? 'Запишем ровно то, что вы написали: ваш перевод встанет в карточку.'
                      : 'Запишем ровно то, что вы написали: ваше слово главнее нашего справочника.'}
                  </div>
                </div>
              )}
            </div>
          );
        })}

        {scope === 'shared' && translations.length > 0 && (
          <div className="wi-block">
            <div className="wi-title">Не смогли перевести</div>
            <div className="wi-sub">
              Ночь перевода не нашла. Впишите свой — он встанет в карточку человека сразу.
            </div>
            {translations.map((row) => (
              <div className="wi-row" key={`tr-${row.id}`}>
                <div className="wi-word">{row.word}</div>
                <input
                  className="wi-input"
                  type="text"
                  placeholder="Перевод…"
                  value={typed[row.id] || ''}
                  onChange={(e) => setTyped((prev) => ({ ...prev, [row.id]: e.target.value }))}
                />
              </div>
            ))}
          </div>
        )}

        {(items.length > 0 || translations.length > 0) && (
          <>
            <button
              type="button"
              className={`wi-apply${marked ? '' : ' off'}`}
              onClick={apply}
              disabled={!marked || phase === 'applying'}
            >
              {phase === 'applying' ? 'Применяю…' : marked ? `Применить: ${marked} решени${marked === 1 ? 'е' : 'й'}` : 'Применить'}
            </button>
            <div className="wi-hint">
              {marked
                ? `Останется ${Math.max(0, total - marked)} — вернутся в следующий разбор`
                : 'Ничего не отмечено'}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
