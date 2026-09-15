import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { saveGermanWordViaLookup } from '../dictionary/saveUtils.js';
import { PICK_CAPTION, PICK_FAILED_TOAST, chipLabel } from './pickCopy.js';
import useFitText from './useFitText.js';
import Toast, { useToast } from './Toast.jsx';
import { saveErrorToast } from './saveNotice.js';
import SelectableText from './SelectableText.jsx';
import SelectionSheet from './SelectionSheet.jsx';

// «ПОДСТАВЬ СИНОНИМ» — средняя ступень между узнаванием и припоминанием.
//
// Стратегия: docs/tasks/synonym_gap_wednesday_strategy.md (владелец, 13.09.2026).
// Понедельник и вторник человек ВЫБИРАЛ слово из пяти карточек, в четверг ему в спринте
// придётся достать его из головы без всякой опоры. Здесь опора есть, но готового ответа
// нет: предложение с пропуском плюс первая буква и длина слова.
//
// Одно предложение — несколько слов. Дверь приёма синонимов строит подстановочный ряд:
// в ОДНО и то же предложение подходят все синонимы слова, меняя оттенок. Поэтому
// предложение висит наверху неподвижно, а меняется то, какое слово в него сейчас ищут,
// и русский перевод именно этого варианта.
//
// Считать ответы и строить пропуски — дело сервера (backend/relation_gap.py): немецкую
// форму нельзя вывести арифметикой, её берут из готового предложения. Клиент только
// показывает и отправляет.

const REL = {
  synonym: { title: 'Подставь синоним', ask: 'синоним', emoji: '🟢' },
  antonym: { title: 'Подставь антоним', ask: 'антоним', emoji: '🔴' },
};

const GAP = '___';

// Подсказка: первая буква и длина (решение владельца 13.09.2026). Без неё среда
// сливается со спринтом, с вариантами на выбор — с тренировкой.
//
// Выбор владельца 13.09.2026 из четырёх макетов: КЛЕТКИ. Сколько букв в слове —
// столько клеток, и они заполняются по мере ввода. Считать точки глазом больше не надо,
// длина видна сразу. Первая буква дана и стоит в первой клетке.
export default function GapGame({ id, api, haptic, onClose, task = null }) {
  const [phase, setPhase] = useState(task ? 'intro' : 'loading'); // loading|intro|playing|done|error
  const [meta, setMeta] = useState(task);
  const [error, setError] = useState('');
  const [gi, setGi] = useState(0);              // какой пропуск сейчас
  const [value, setValue] = useState('');
  const [attempt, setAttempt] = useState(1);    // 1 или 2; вторая попытка — последняя
  const [verdict, setVerdict] = useState(null); // {outcome, filler, synonym, ...}
  const [score, setScore] = useState(0);
  const [solved, setSolved] = useState(() => []);   // что человек уже вписал верно
  // Лампочка, как в кроссворде (решение владельца 13.09.2026): до ДВУХ букв сверх той,
  // что дана изначально. Считаем их отдельно от набранного — и чтобы подсветить другим
  // цветом (подсказка, а не твой ответ), и чтобы записать в ответ: помогла ли лампочка,
  // видно будет только если знать, сколько раз её нажали.
  const [hints, setHints] = useState(0);
  // ГДЕ СЕЙЧАС КАРЕТКА — номер клетки, в которую встанет следующая буква.
  // Владелец 14.09.2026: «не подсвечивается буква, не понимаю какую сейчас писать» и
  // «не могу тапнуть на букву в середине и исправить — приходится стирать всё».
  // Обе жалобы про одно: экран не показывал позицию ввода и не давал её менять.
  const [caret, setCaret] = useState(0);
  // Выделена ли буква В КЛЕТКЕ (тап по уже написанной): печать её заменит.
  const [picked, setPicked] = useState(false);
  // Куда поставить каретку ПОСЛЕ того, как значение доедет до поля (подсказка вписывает
  // сразу несколько букв, и в момент клика значение ещё старое).
  const pendingCaret = useRef(null);
  const [saved, setSaved] = useState(() => new Set());
  const [selection, setSelection] = useState(null);
  const inputRef = useRef(null);
  const toast = useToast();

  // Каретка из самого поля: браузер двигает её при вводе, стрелках и тапе.
  const syncCaret = useCallback(() => {
    const el = inputRef.current;
    if (!el) return;
    const a = Math.max(0, Number(el.selectionStart || 0));
    const b = Math.max(0, Number(el.selectionEnd || 0));
    setCaret(a);
    setPicked(b === a + 1);
  }, []);

  // Отложенная каретка — только там, где значение поставили мы сами (подсказка).
  // В обработчике клика делать это нельзя: значение ещё старое.
  useEffect(() => {
    const el = inputRef.current;
    if (!el || verdict) return;
    if (pendingCaret.current == null) return;
    const pos = pendingCaret.current;
    pendingCaret.current = null;
    try { el.setSelectionRange(pos, pos); } catch (_e) { /* noop */ }
    setCaret(pos);
  }, [value, verdict]);

  // Тап по клетке ставит каретку ИМЕННО В НЕЁ — можно поправить букву в середине, а не
  // стирать слово целиком. preventDefault держит фокус на поле: без него нажатие уводит
  // его на сам span, и на айфоне клавиатура прячется.
  // Ставим каретку по КЛИКУ, а не по нажатию: <label> сам наводит фокус на поле после
  // нажатия, и каретка, выставленная раньше, тут же уезжала бы в конец. Клик — тоже
  // касание пользователя, так что на айфоне клавиатура поднимается.
  const tapCell = useCallback((i) => () => {
    if (verdict) return;
    const el = inputRef.current;
    if (!el) return;
    el.focus();
    const len = String(value || '').length;
    const pos = Math.min(i, len);
    // ТАП ПО БУКВЕ ВЫДЕЛЯЕТ ЕЁ, а не ставит курсор перед ней: следующая напечатанная
    // буква встаёт НА ЕЁ МЕСТО. Так и ждёт человек от клеток — каждая клетка это одна
    // буква. Первая попытка ставила курсор перед буквой, и печать сдвигала слово
    // вправо вместо замены; владелец 15.09.2026: «приходится всё удалять».
    // Пустая клетка в конце — просто курсор, выделять там нечего.
    try {
      if (pos < len) el.setSelectionRange(pos, pos + 1);
      else el.setSelectionRange(pos, pos);
    } catch (_e) { /* noop */ }
    setCaret(pos);
    setPicked(pos < len);
  }, [verdict, value]);

  const heroRef = useFitText(`${phase}|${meta?.wort || ''}`, { max: 'css', min: 15, padding: 10, fitBy: 'word' });

  useEffect(() => {
    if (task) return;                            // предпросмотр: задание пришло пропсом
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/gap/task', { kind: 'lk', id });
        if (cancelled) return;
        if (!(data.items || []).length) { setError('Для этого слова пока нет предложений.'); setPhase('error'); return; }
        setMeta(data); setPhase('intro');
      } catch (e) {
        try { console.warn('[gap] load failed', e); } catch (_err) { /* noop */ }
        if (!cancelled) { setError('Не удалось загрузить. Попробуйте позже.'); setPhase('error'); }
      }
    })();
    return () => { cancelled = true; };
  }, [id, api, task]);

  const rel = REL[meta?.relation] || REL.synonym;
  const items = meta?.items || [];
  const total = items.length;
  const item = items[gi];

  // Предложение с пропуском, разрезанное для разметки: до / подсказка / после.
  const parts = useMemo(() => {
    const s = String(item?.sentence_gapped || '');
    const i = s.indexOf(GAP);
    if (i < 0) return { before: s, after: '' };
    return { before: s.slice(0, i), after: s.slice(i + GAP.length) };
  }, [item]);

  // Из чего собран ряд клеток. Первая клетка показывает ДАННУЮ букву, пока человек не
  // напечатал свою: подсказка не исчезает от первого касания клавиатуры.
  const cells = useMemo(() => {
    const len = Math.max(0, Number(item?.hint_len || 0));
    const typed = String(value || '');
    const n = Math.max(len, typed.length);
    const out2 = [];
    for (let i = 0; i < n; i += 1) {
      const ch = typed[i] || (i === 0 ? (item?.hint_letter || '') : '');
      const given = !typed[i] && i === 0 && ch;
      // Подсказано = первая буква (она дана всегда) плюс открытые лампочкой.
      const revealed = i < 1 + hints && !!typed[i];
      let cls = '';
      if (i >= len) cls = ' over';                       // перебрал длину — видно сразу
      else if (revealed) cls = ' given';                 // подсказка, а не твой ответ
      else if (typed[i]) cls = ' typed';
      else if (given) cls = ' given';
      out2.push({ ch, cls });
    }
    return out2;
  }, [item, value, hints]);

  // Сколько клеток в ряду. Больше одиннадцати в строку телефона не влезает: замер
  // 13.09.2026 на 430 px — семнадцать клеток переносились сами и вставали «15 + 2»,
  // что читается как поломка. Считаем ряды заранее и делаем их РОВНЫМИ: длинное слово
  // ложится двумя одинаковыми строками, как перенос слова, а не как обрыв.
  const cellCols = useMemo(() => {
    const n = cells.length;
    if (n <= 11) return Math.max(1, n);
    return Math.ceil(n / Math.ceil(n / 11));
  }, [cells.length]);

  const start = useCallback(() => {
    setPhase('playing'); setGi(0); setValue(''); setAttempt(1);
    setVerdict(null); setScore(0); setSolved([]); setHints(0); setCaret(0); setPicked(false);
  }, []);

  // Лампочка открывает СЛЕДУЮЩУЮ букву слова и ставит ввод на этот префикс. Не
  // «показать где-то сбоку», а именно вписать: человек продолжает с той точки, до
  // которой ему помогли, и не переписывает начало заново.
  const HINTS_MAX = 2;
  const hintsLeft = HINTS_MAX - hints;
  const useHint = useCallback(() => {
    if (hints >= HINTS_MAX || verdict) return;
    const full = String(item?.filler || '');
    const nextLen = Math.min(full.length, 2 + hints);   // 1-я буква дана, открываем 2-ю, затем 3-ю
    // ФОКУС СИНХРОННО, до любых setState: айфон поднимает клавиатуру ТОЛЬКО внутри
    // самого касания. Прежняя версия звала focus() из setTimeout(30) — жест к тому
    // моменту уже кончался, клавиатура не выезжала, и человек оставался с одними
    // подсказанными буквами и кнопкой «Проверить» (поймано на живом проходе 14.09.2026).
    try { inputRef.current?.focus(); } catch (_e) { /* noop */ }
    pendingCaret.current = nextLen;
    setHints((h) => h + 1);
    setValue(full.slice(0, nextLen));
    try { haptic?.('ok'); } catch (_e) { /* noop */ }
  }, [hints, verdict, item, haptic]);

  const check = useCallback(async () => {
    const text = value.trim();
    if (!text || verdict) return;
    let res;
    try {
      res = await api('/api/gap/answer', {
        kind: 'lk', id, index: item?.index ?? gi, answer: text, attempt, hints,
      });
    } catch (e) {
      try { console.warn('[gap] answer failed', e); } catch (_err) { /* noop */ }
      toast.show('Не получилось отправить ответ. Попробуйте ещё раз.');
      return;
    }
    setVerdict(res);
    const ok = res.outcome === 'correct';
    if (ok) {
      if (attempt === 1) setScore((s) => s + 1);
      setSolved((list) => [...list, { de: res.filler, ru: res.synonym_ru, synonym: res.synonym }]);
    }
    try { haptic?.(ok ? 'ok' : 'bad'); } catch (_e) { /* noop */ }
  }, [api, id, item, gi, value, attempt, hints, verdict, haptic, toast]);

  // Вторая попытка — последняя (решение владельца 13.09.2026). Слово человек вспомнил,
  // не хватает окончания: показать ЧТО не так и дать дописать, а не хлопнуть дверью.
  const retry = useCallback(() => {
    // Тот же закон, что у лампочки: фокус синхронно, внутри касания. Поле теперь не
    // размонтируется на время разбора, поэтому фокусировать есть что.
    try { inputRef.current?.focus(); } catch (_e) { /* noop */ }
    pendingCaret.current = String(value || '').length;   // дописывать — с конца
    setVerdict(null); setAttempt(2);
  }, [value]);

  const next = useCallback(() => {
    setSelection(null); setVerdict(null); setValue(''); setAttempt(1); setHints(0); setCaret(0); setPicked(false);
    if (gi + 1 >= total) { setPhase('done'); return; }
    setGi((i) => i + 1);
  }, [gi, total]);

  const saveChip = useCallback((de, ru) => {
    if (!de || saved.has(de)) return;
    setSaved((s) => new Set(s).add(de));
    saveGermanWordViaLookup({ api, word: de, translation: ru, origin: 'gap_chip' })
      .then((ok) => { if (ok) toast.show(chipLabel(de)); else toast.show(PICK_FAILED_TOAST); })
      .catch((err) => {
        setSaved((s) => { const n = new Set(s); n.delete(de); return n; });
        toast.show(saveErrorToast(err));
        try { haptic?.('bad'); } catch (_e2) { /* noop */ }
      });
  }, [api, saved, haptic, toast]);

  const shell = (body, cls = '', wide = null) => (
    // Здесь человек ПЕЧАТАЕТ, поэтому `--keepkbd` не ставим: карточка должна
    // перестроиться под клавиатуру, иначе поле ввода уезжает под неё.
    <div className="ans-root">
      <div className={`ans-card ${cls}`} data-wide={wide || undefined}>{body}</div>
      {selection ? (
        <SelectionSheet api={api} selection={selection} onClose={() => setSelection(null)} origin="gap_text_save" />
      ) : null}
      <Toast state={toast.state} onClose={toast.hide} />
    </div>
  );

  if (phase === 'loading') return shell(<><div className="ans-skel" /><div className="ans-skel sm" /></>);

  if (phase === 'error') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">⚠️ Hoppla</span></div>
      <p className="ans-sub">{error}</p>
      <button className="ans-btn" onClick={onClose}>Schließen</button>
    </>
  );

  if (phase === 'intro') return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">{rel.emoji} {rel.title}</span></div>
      <div className="gp-hero">
        <div className="gp-hero-word"><span className="fit-word" lang="de" ref={heroRef}>{meta?.wort}</span></div>
        {meta?.hint_ru ? <div className="gp-hero-hint">{meta.hint_ru}</div> : null}
      </div>
      <div className="gp-intro">
        <p>В это предложение подходит <b>{total}</b> {total === 1 ? rel.ask : total < 5 ? `${rel.ask}а` : `${rel.ask}ов`} —
          впиши их по одному.</p>
        <p className="gp-intro-dim">
          Первая буква и длина слова подсказаны. Завтра это слово вернётся в спринте —
          там подсказок уже не будет 🔥
        </p>
      </div>
      <button className="ans-btn gp-go" onClick={start}>▶️ Начать</button>
    </>
  );

  if (phase === 'playing' && item) {
    const out = verdict?.outcome;
    // Ответ открыт: либо угадал, либо вторая попытка израсходована.
    const revealDone = !!out && out !== 'correct' && attempt === 2;
    return shell(
      <>
        <div className="gp-top ans-r-head">
          <span className="gp-top-rel">{rel.emoji} {rel.ask}</span>
          <span className="gp-top-prog">{gi + 1} / {total}</span>
        </div>
        <div className="gp-bar ans-r-bar"><div className="gp-bar-fill" style={{ width: `${((gi + (out === 'correct' ? 1 : 0)) / total) * 100}%` }} /></div>

        <div className="gp-anchor ans-r-prompt">
          <div className="gp-anchor-label">Впиши {rel.ask} к слову</div>
          <div className="gp-anchor-word" lang="de">{meta?.wort}</div>
          {meta?.hint_ru ? <div className="gp-anchor-hint">{meta.hint_ru}</div> : null}
        </div>

        {/* Предложение — предмет работы, поэтому у него своя подложка. Выключка по
            ЛЕВОМУ краю: центрированный немецкий в три строки с дыркой посередине
            владелец 13.09.2026 назвал хаосом, и он прав — так набирают заголовки,
            а не предложения, которые читают. */}
        <div className="gp-panel ans-r-work">
          <div className="gp-sentence" lang="de">
            <SelectableText text={parts.before} onSelect={setSelection} haptic={haptic} />
            {/* В предложении — только МЕТКА МЕСТА, а не сами клетки. Замер 13.09.2026:
                половина слов от 9 букв, каждое пятое от 12, самое длинное 17
                («unverhältnismäßig») — семнадцать клеток внутри строки разорвали бы
                предложение. Клетки живут отдельной строкой ниже и там читаются. */}
            {/* Разбор окончен — слово ВСТАЁТ В ПРЕДЛОЖЕНИЕ, даже если человек его не
                угадал (владелец 13.09.2026: «почему не отражается тут слово?»). Ради
                этого задание и существует: увидеть готовую немецкую фразу целиком, а
                не только правильный ответ отдельной строчкой внизу. */}
            <span className={`gp-slot ${out === 'correct' ? 'ok' : (revealDone ? 'shown' : (out ? 'bad' : ''))}`}>
              {(out === 'correct' || revealDone) ? item.filler : <span className="gp-slot-mark" />}
            </span>
            <SelectableText text={parts.after} onSelect={setSelection} haptic={haptic} />
          </div>
          {item.sentence_ru ? <div className="gp-sentence-ru">{item.sentence_ru}</div> : null}
        </div>

        {/* Клетки и поле НЕ ИСЧЕЗАЮТ на время разбора. Раньше здесь стояла развилка
            «либо ввод, либо вердикт», и поле размонтировалось вместе с фокусом: после
            «Попробовать ещё раз» айфон клавиатуру уже не поднимал. Владелец поймал это
            первым же живым проходом 14.09.2026 — десять пропусков подряд ушли на
            проверку с одними подсказанными буквами, потому что дописать было нечем. */}
        <div className="gp-form">
          <label className="gp-cells" style={{ '--gp-cols': cellCols }}>
            <input
              ref={inputRef}
              className="gp-cells-input"
              lang="de"
              value={value}
              readOnly={!!verdict}
              onChange={(e) => { setValue(e.target.value); syncCaret(); }}
              onSelect={syncCaret}
              onKeyUp={syncCaret}
              onClick={syncCaret}
              onKeyDown={(e) => { if (e.key === 'Enter' && !verdict) check(); }}
              aria-label={attempt === 2 ? 'Поправь форму' : 'Впиши слово по буквам'}
              autoComplete="off" autoCorrect="off" autoCapitalize="off" spellCheck={false}
            />
            {cells.map((c, i) => (
              <span key={i}
                className={`gp-cell${c.cls}${!verdict && i === caret ? (picked ? ' picked' : ' next') : ''}`}
                onClick={tapCell(i)}
                aria-hidden="true">{c.ch}</span>
            ))}
          </label>
          {!verdict ? (
            <>
              <div className="gp-cells-row">
                <span className="gp-cells-hint">
                  {value.trim()
                    ? `${value.trim().length} из ${item.hint_len} букв`
                    : (attempt === 2 ? 'поправь сюда форму' : 'нажми на клетки и впиши слово')}
                </span>
                {/* Лампочка как в кроссворде. Две буквы — потолок: дальше это уже не
                    припоминание, а списывание.
                    onPointerDown + preventDefault — чтобы нажатие НЕ уводило фокус с
                    поля: на айфоне ушедший фокус убирает клавиатуру, и вернуть её
                    отложенным вызовом нельзя, только внутри самого касания. */}
                {hintsLeft > 0 ? (
                  <button type="button" className="gp-hint-btn"
                    onPointerDown={(e) => e.preventDefault()}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={useHint}>
                    💡 подсказка<span className="gp-hint-left">{hintsLeft}</span>
                  </button>
                ) : (
                  <span className="gp-hint-btn is-spent">💡 подсказок больше нет</span>
                )}
              </div>
              <button className="ans-btn gp-check" disabled={!value.trim()} onClick={check}>Проверить</button>
            </>
          ) : null}
        </div>

        {/* Вердикт — ОТДЕЛЬНЫЙ блок под клетками, а не ветка «вместо них». Когда клетки
            перестали исчезать на время разбора (14.09.2026), от прежней развилки
            остались хвосты «) : (» и «)}», и React вывел их НА ЭКРАН КАК ТЕКСТ: владелец
            увидел их 15.09.2026 в живом задании. Условие теперь честно открыто здесь. */}
        {verdict ? (
          <div className={`gp-feedback ans-body ans-r-note ${out === 'correct' ? 'ok' : out === 'wrong' ? 'bad' : 'warn'}`}>
            {out === 'correct' ? (
              <>
                <div className="gp-fb-head">Верно</div>
                <div className="gp-fb-word" lang="de">{verdict.filler}
                  {verdict.synonym_ru ? <span className="gp-fb-ru">{verdict.synonym_ru}</span> : null}
                </div>
                {verdict.nuance ? <div className="gp-fb-why">{verdict.nuance}</div> : null}
              </>
            ) : null}

            {out === 'wrong_form' ? (
              <>
                <div className="gp-fb-head">Слово верное, форма нет</div>
                <div className="gp-fb-why">
                  В этом предложении <b lang="de">{verdict.synonym}</b> стоит с другим
                  окончанием.{attempt === 1 ? ' Допиши его.' : ''}
                </div>
              </>
            ) : null}

            {out === 'other_synonym' ? (
              <>
                <div className="gp-fb-head">Это тоже {rel.ask}</div>
                <div className="gp-fb-why">
                  Но здесь ждём слово на «<b>{item.hint_letter}</b>», из {item.hint_len} букв.
                </div>
              </>
            ) : null}

            {out === 'wrong' ? <div className="gp-fb-head">Не то слово</div> : null}

            {out !== 'correct' && attempt === 2 ? (
              <div className="gp-fb-word" lang="de">{item.filler}
                {item.synonym_ru ? <span className="gp-fb-ru">{item.synonym_ru}</span> : null}
              </div>
            ) : null}

            {out !== 'correct' && attempt === 1 ? (
              <button className="ans-btn gp-retry" onClick={retry}>Попробовать ещё раз</button>
            ) : (
              <button className="ans-btn gp-next" onClick={next}>{gi + 1 >= total ? 'Итог' : 'Дальше'}</button>
            )}
          </div>
        ) : null}

        {solved.length ? (
          <div className="gp-solved">
            {solved.map((s, i) => <span key={i} className="gp-solved-chip" lang="de">{s.de}</span>)}
          </div>
        ) : null}
      </>,
      '',
      'split',
    );
  }

  // done
  const all = items.map((it) => ({ de: it.filler, ru: it.synonym_ru, base: it.synonym }));
  return shell(
    <>
      <div className="ans-head"><span className="ans-eyebrow">{rel.emoji} {rel.title}</span></div>
      <div className="gp-score">
        <div className="gp-score-num">{score}<span className="gp-score-of"> / {total}</span></div>
        <div className="gp-score-sub">с первой попытки</div>
      </div>
      <div className="gp-done-note">
        Завтра <b lang="de">{meta?.wort}</b> вернётся в спринте — там {rel.ask}ы придётся вспомнить
        совсем без подсказок 🔥
      </div>
      {all.length ? (
        <div className="gp-all ans-body">
          <div className="gp-all-head">Все {rel.ask}ы <span className="gp-all-dim">· 👆 {PICK_CAPTION}</span>:</div>
          <div className="gp-chips">
            {all.map((c, i) => (
              <button key={i} type="button"
                className={`gp-chip${saved.has(c.base) ? ' saved' : ''}`}
                onClick={() => saveChip(c.base, c.ru)}>
                <span lang="de">{c.base}</span>{c.ru ? <span className="gp-chip-ru">{c.ru}</span> : null}
              </button>
            ))}
          </div>
        </div>
      ) : null}
      <button className="ans-btn" onClick={onClose}>Закрыть</button>
    </>
  );
}
