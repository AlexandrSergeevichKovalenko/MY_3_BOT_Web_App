import React, { useEffect, useRef, useState } from 'react';
import { saveGermanWordViaLookup, savePhraseWithTranslation } from '../dictionary/saveUtils.js';
import { describeSaveError } from './saveNotice.js';
import { playWordTts } from './wordTts.js';

/**
 * Плашка перевода для интерактивов: что человек выделил в тексте, как это по-русски,
 * и что с этим можно сделать.
 *
 * Три кнопки, как в читалке, минус «Разбор»: за ним тянется отдельное окно поверх игры
 * и ~600 строк логики, а игра идёт на время — решение владельца 10.09.2026.
 *
 * ⛔ ЧТО ИМЕННО ЛОЖИТСЯ В СЛОВАРЬ. В тексте слова стоят в изменённой форме («möchte»),
 * а в словаре по закону проекта стоит СЛОВАРНАЯ форма. Порядок поиска ответа —
 * от бесплатного источника к дорогому, и ни на одном шаге мы ничего не выдумываем:
 *   1. ответ быстрого перевода уже несёт `form_of`, если НАШ словарь знает написание
 *      как форму («möchte → mögen») — берём его, это ноль запросов;
 *   2. иначе спрашиваем сервер (`/api/webapp/normalize/de`) — тот же путь, которым
 *      сохраняет кнопка «Сохранить» в читалке: за 30 дней 25 слов, форм среди них 0;
 *   3. если форма изменилась, машинный перевод формы НЕ приписываем словарному слову
 *      («beruhen — основывался» было бы враньём): перевод для карточки запрашивает
 *      сама дверь сохранения по словарному слову.
 * Путь «Разбор → сохранить» из читалки мы намеренно НЕ повторяем: именно он положил в
 * словарь 4 формы заголовками (wirbt, gesponnen, beruhte, angetrieben).
 */

const KIND_LABEL = { word: 'слово', phrase: 'фраза', sentence: 'предложение' };

export default function SelectionSheet({ api, selection, onClose, origin = 'trainer_text_save' }) {
  const [state, setState] = useState({ phase: 'loading' });   // loading | ready | error
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState('');                     // '' | 'new' | 'known'
  const [speaking, setSpeaking] = useState(false);
  const reqRef = useRef(0);

  const text = String(selection?.text || '').trim();
  const kind = String(selection?.kind || 'word');

  useEffect(() => {
    if (!text) return undefined;
    const my = ++reqRef.current;
    let alive = true;
    setState({ phase: 'loading' });
    setSaved('');
    api('/api/translate/quick', { text, source_lang: 'de', target_lang: 'ru' })
      .then((data) => {
        if (!alive || my !== reqRef.current) return;
        const translation = String(data?.translation || '').trim();
        if (!translation) {
          // Пустой ответ — это НЕ перевод. Молчать нельзя, но и выдумывать нечего:
          // говорим человеку прямо.
          setState({ phase: 'error', message: 'Перевод не пришёл. Попробуй ещё раз.' });
          return;
        }
        setState({
          phase: 'ready',
          translation,
          formOf: String(data?.form_of || '').trim(),
          machine: Boolean(data?.machine),
        });
      })
      .catch((err) => {
        if (!alive || my !== reqRef.current) return;
        setState({
          phase: 'error',
          message: err?.status === 429
            ? 'Сегодня переводов больше нет. Завтра снова будет.'
            : 'Не получилось перевести. Проверь связь и нажми ещё раз.',
        });
      });
    return () => { alive = false; };
  }, [text, api]);

  // Лист закрывается тапом мимо него и клавишей Escape: он стоит над кнопкой «Дальше»,
  // и без этого человек, прочитав перевод, оказывался с закрытой кнопкой (стенд
  // 10.09.2026). Тап по ДРУГОМУ слову закроет этот лист и откроет новый — это и нужно.
  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const outside = (event) => {
      const t = event.target;
      if (t instanceof Element && t.closest('.sel-sheet')) return;
      onClose?.();
    };
    const esc = (event) => { if (event.key === 'Escape') onClose?.(); };
    document.addEventListener('pointerdown', outside);
    document.addEventListener('keydown', esc);
    return () => {
      document.removeEventListener('pointerdown', outside);
      document.removeEventListener('keydown', esc);
    };
  }, [onClose]);

  const speak = async () => {
    if (speaking) return;
    setSpeaking(true);
    try { await playWordTts(text); } catch (_e) { /* молча: озвучка — не главное действие */ }
    setSpeaking(false);
  };

  // Словарное слово для сохранения: сперва наш словарь, потом сервер. Возвращает
  // { word, changed } — changed говорит, можно ли приписать этому слову перевод формы.
  const resolveHeadword = async (formOf) => {
    if (kind !== 'word') return { word: text, changed: false };
    const known = String(formOf || '').trim();
    if (known && known.toLowerCase() !== text.toLowerCase()) return { word: known, changed: true };
    try {
      const res = await api('/api/webapp/normalize/de', { text });
      const normalized = String(res?.normalized || '').trim();
      if (normalized && normalized.toLowerCase() !== text.toLowerCase()) {
        return { word: normalized, changed: true };
      }
    } catch (_e) {
      // Сервер не ответил — сохраняем то, что человек видел на экране. Это честнее,
      // чем потерять слово; ночная проверка заголовков разберётся.
    }
    return { word: text, changed: false };
  };

  const save = async () => {
    if (saving || saved || state.phase !== 'ready') return;
    setSaving(true);
    try {
      if (kind === 'word') {
        const { word, changed } = await resolveHeadword(state.formOf);
        const res = await saveGermanWordViaLookup({
          api,
          word,
          // Перевод формы словарному слову не приписываем — пусть карточку соберёт разбор.
          fallbackTranslation: changed ? '' : state.translation,
          origin,
        });
        setSaved(res?.inserted === false ? 'known' : 'new');
      } else {
        await savePhraseWithTranslation({ api, phrase: text, translation: state.translation, origin });
        setSaved('new');
      }
    } catch (err) {
      const notice = describeSaveError(err);
      setState((prev) => ({ ...prev, saveError: notice?.text || 'Не удалось сохранить.' }));
    }
    setSaving(false);
  };

  if (!text) return null;

  // Живёт ВНУТРИ .ans-root, а не в портале body: цвета светлой темы объявлены на корне
  // интерактива, и в портале плашка оставалась тёмной на светлом экране (поймано на
  // стенде 10.09.2026). Внутри корня она забирает тему и сброс отступов даром.
  return (
    <div className="sel-sheet" role="dialog" aria-label="Перевод выделенного">
      <div className="sel-sheet-head">
        <span className="sel-sheet-kind">{KIND_LABEL[kind] || 'слово'}</span>
        <button type="button" className="sel-sheet-x" onClick={onClose} aria-label="Закрыть">✕</button>
      </div>
      <div className="sel-sheet-src" lang="de">{text}</div>

      {state.phase === 'loading' ? <div className="sel-sheet-wait">Перевожу…</div> : null}
      {state.phase === 'error' ? <div className="sel-sheet-err">{state.message}</div> : null}
      {state.phase === 'ready' ? (
        <>
          <div className="sel-sheet-tra">{state.translation}</div>
          {state.formOf && state.formOf.toLowerCase() !== text.toLowerCase() ? (
            <div className="sel-sheet-form">форма слова <b lang="de">{state.formOf}</b></div>
          ) : null}
          {state.saveError ? <div className="sel-sheet-err">{state.saveError}</div> : null}
        </>
      ) : null}

      <div className="sel-sheet-acts">
        <button type="button" className="sel-sheet-act" onClick={speak} disabled={speaking}>
          {speaking ? '🔊 …' : '🔊 Слушать'}
        </button>
        <button
          type="button"
          className={`sel-sheet-act${saved ? ' is-done' : ''}`}
          onClick={save}
          disabled={saving || !!saved || state.phase !== 'ready'}
        >
          {saved === 'new' ? '✓ В словаре' : saved === 'known' ? '✓ Уже было' : saving ? '💾 …' : '💾 В словарь'}
        </button>
      </div>
    </div>
  );
}
