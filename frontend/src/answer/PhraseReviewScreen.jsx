import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/**
 * Спорные фразы общего словаря (админ). Одна фраза на экран.
 *
 * Почему экран, а не чат. Ночная проверка откладывает фразы, по которым двое судей не
 * сошлись, и раньше разбор шёл сообщениями бота. В чате у владельца сотни непрочитанных
 * учебных сообщений, и каждый ответ бота приходил новым сообщением в самый конец
 * переписки — то есть уносил его от того места, где он читал. Здесь весь разбор в одном
 * месте: список, варианты судей, решение.
 *
 * Почему у каждого варианта своя кнопка. Судьи расходятся постоянно — ровно поэтому
 * фраза сюда и попала. Одна кнопка «Принять» брала бы вариант первого судьи молча, и по
 * ней нельзя понять, что принимаешь. Номер на кнопке — тот же, что у варианта в разборе.
 */
/* Компонент FixCheck и словарь KIND_LABEL убраны 26.08.2026 вместе с перечёркнутым
   текстом: забракованные проверкой правки больше не рисуются зачёркнутой строкой рядом
   с мнением судьи, а собраны в раскрывашку «мы проверили и не советуем», где у каждой
   написано, что именно с ней не так (см. `rejected` в теле экрана). */

export default function PhraseReviewScreen({ api, haptic, onClose, only = '' }) {
  const [items, setItems] = useState(null);
  const [idx, setIdx] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  // Подтверждение прошлого решения. Оно ВСЕГДА про предыдущую фразу, поэтому несёт её
  // текст и само гаснет: пока оно висело просто «✅ вопрос закрыт» над уже следующей
  // фразой, его нельзя было не прочитать как решение по ней.
  const [done, setDone] = useState(null);   // { text, what }
  const doneTimer = useRef(null);
  const [note, setNote] = useState('');
  // Сколько в очереди пустых придирок: заявлена ошибка, а исправить нечего.
  const [noise, setNoise] = useState(0);
  const [total, setTotal] = useState(0);      // вся очередь, а не загруженное окно
  // Сколько всего вопросов каждого вида. На отдельной двери шапка обязана показывать
  // число СВОЕГО вида, иначе «осталось 232» на экране с 79 карточками — это враньё.
  const [byKind, setByKind] = useState(null);
  // Сколько вердиктов вынесено ДО того, как судья стал видеть перевод. Такие слепые:
  // предлог и падеж выбираются по смыслу, а смысла судья не видел.
  const [blind, setBlind] = useState(0);
  // Вопрос своими словами про эту фразу. Владельцу пришлось уходить в другое приложение,
  // чтобы выяснить, что «Wappnen mit» и «Wappnen gegen» — разные значения, а не ошибка;
  // спрашивать надо там же, где принимаешь решение.
  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState('');
  const [asking, setAsking] = useState(false);
  const [own, setOwn] = useState('');
  // Правки ВНУТРИ карточки: перевод и примеры. Ровно то, что уже есть у обычного
  // человека на его экране проверки (`dictionary/WordAudit.jsx`); у владельца этого
  // не было, и поправить один перевод было нечем — только «переписать всё заново».
  const [draft, setDraft] = useState(null);   // { ru, ex: [{de, ru, deleted}], topup }
  // Исправление САМОЙ немецкой фразы. Спрятано за кнопкой: на панельной карточке это
  // редкий случай, а поле, стоящее открытым, читалось как «впиши перевод» — и тогда
  // русский текст уезжал в немецкий заголовок.
  const [fixingText, setFixingText] = useState(false);
  // Русский к своему тексту. Без этого поля перевод после правки сочиняла модель, и
  // выбор владельца пропадал: замер 20.08.2026 — 11 решений из 119 ушли в базу с
  // машинным русским, «Die Zuschlagsstoffe» стали «заполнителями» вместо «добавок».
  const [ownRu, setOwnRu] = useState('');
  const [typing, setTyping] = useState(false);
  // Отложенные на этом сеансе: в базе ничего не меняется, они просто уходят в конец
  // очереди, чтобы не крутиться перед глазами, пока разбираешь остальные.
  const [skipped, setSkipped] = useState(() => new Set());

  const load = useCallback(async (loud = false) => {
    if (loud) { setBusy(true); setError(''); }
    try {
      const r = await api('/api/answer/phrasereview/list', {});
      setItems(r.items || []);
      setTotal(Number(r.total) || (r.items || []).length);
      setNoise(Number(r.noise) || 0);
      setBlind(Number(r.blind) || 0);
      setByKind(r.by_kind || null);
      if (loud) setNote(`🔄 Проверил: спорных фраз — ${Number(r.total) || (r.items || []).length}.`);
    } catch (e) {
      console.warn('[phrasereview] load', e);
      setError(e?.status === 403
        ? 'Этот экран только для администратора.'
        : 'Не удалось загрузить. Попробуйте позже.');
      setItems([]);
    } finally {
      if (loud) setBusy(false);
    }
  }, [api]);

  useEffect(() => { load(false); }, [load]);

  // Отложенные — в конец, порядок остальных не трогаем.
  // `only` приходит с отдельной двери (startapp=ans_frvp_0): по вторникам и пятницам
  // владельцу шлётся сообщение про карточки словаря, и кнопка в нём обязана открывать
  // ТОЛЬКО их. Смешать два вопроса в одном списке — значит вернуть тот экран, на
  // котором нельзя было понять, что решаешь.
  const queue = useMemo(() => {
    const all = items || [];
    // «cards» — это ОБА вопроса про карточку: спор трёх голосов о примерах и
    // неподтверждённый перевод. Владельцу они приходят одним сообщением и разбираются
    // подряд, поэтому и на экране идут одним списком.
    const list = only === 'cards'
      ? all.filter((it) => (it.kind || 'grammar') !== 'grammar')
      : (only ? all.filter((it) => (it.kind || 'grammar') === only) : all);
    return [...list.filter((it) => !skipped.has(it.id)), ...list.filter((it) => skipped.has(it.id))];
  }, [items, skipped, only]);

  const card = queue[Math.min(idx, Math.max(0, queue.length - 1))] || null;

  const flashDone = useCallback((phrase, what) => {
    if (doneTimer.current) clearTimeout(doneTimer.current);
    setDone({ text: phrase, what });
    doneTimer.current = setTimeout(() => setDone(null), 5000);
  }, []);

  useEffect(() => () => { if (doneTimer.current) clearTimeout(doneTimer.current); }, []);

  // Черновик правок привязан к КАРТОЧКЕ, а не к экрану. Сменилась карточка — черновик
  // сброшен: иначе перевод, набранный для одной, уедет в следующую («Отложить»
  // листает список, ничего не решая).
  const cardId = card?.id;
  useEffect(() => {
    setDraft(null); setFixingText(false); setOwn(''); setOwnRu('');
  }, [cardId]);

  // `total` — сколько ВСЕГО ждёт решения, а не сколько влезло в загруженное окно.
  // Владелец 24.08.2026: разбираю фразу за фразой, а «из 200» не двигается. Так и было:
  // окно 200, в очереди 202 — решил одну, сервер дослал следующую, число прежнее.
  const applyResponse = (r) => {
    setItems(r.items || []);
    setTotal(Number(r.total) || (r.items || []).length);
    setNoise(Number(r.noise) || 0);
    setBlind(Number(r.blind) || 0);
    setByKind(r.by_kind || null);
    setNote('');
    setOwn('');
    setOwnRu('');
    // Ответ был про ПРЕДЫДУЩУЮ фразу — на новой ему не место.
    setQuestion(''); setAnswer(''); setAsking(false);
    // То же и с черновиком правок: оставить его на следующей карточке значит однажды
    // записать чужой перевод в чужую карточку.
    setDraft(null);
    setFixingText(false);
    // Список сдвинулся на одну — остаёмся на той же позиции, то есть на следующей
    // фразе. Прыжок в начало заставлял бы каждый раз искать, где ты был.
    setIdx((prev) => Math.max(0, Math.min(prev, (r.items || []).length - 1)));
  };

  const decide = async (decision, extra = {}) => {
    if (!card || busy) return;
    setBusy(true); setError('');
    try {
      const was = card.text;
      const r = await api('/api/answer/phrasereview/decide',
        { review_id: card.id, decision, ...extra });
      haptic?.('ok');
      applyResponse(r);
      flashDone(was, r.note || 'Готово.');
    } catch (e) {
      console.warn('[phrasereview] decide', e);
      haptic?.('bad');
      setError(e?.message || 'Не получилось. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const rejudge = async () => {
    if (!card || busy) return;
    setBusy(true); setError(''); setNote('🔁 Спрашиваю судей заново…');
    try {
      const r = await api('/api/answer/phrasereview/rejudge', { review_id: card.id });
      applyResponse(r);
    } catch (e) {
      console.warn('[phrasereview] rejudge', e);
      setNote('');
      setError(e?.message || 'Судьи не ответили. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const settle = async () => {
    if (!card || busy) return;
    setBusy(true); setError(''); setNote('⚖️ Спрашиваю третейского судью…');
    try {
      const r = await api('/api/answer/phrasereview/settle', { review_id: card.id });
      applyResponse(r);
    } catch (e) {
      console.warn('[phrasereview] settle', e);
      setNote('');
      setError(e?.message || 'Третейский судья не ответил. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const rejudgeAll = async () => {
    if (busy) return;
    setBusy(true); setError('');
    setNote('⚖️ Пересуживаю со смыслом — это займёт с полминуты…');
    try {
      const r = await api('/api/answer/phrasereview/rejudgeall', {});
      applyResponse(r);
      flashDone(`${r.rejudged} шт.`, 'пересужено со смыслом');
    } catch (e) {
      console.warn('[phrasereview] rejudgeall', e);
      setNote('');
      setError(e?.message || 'Судьи не ответили. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const dropNoise = async () => {
    if (busy) return;
    setBusy(true); setError(''); setNote('🧹 Убираю пустые придирки…');
    try {
      const r = await api('/api/answer/phrasereview/dropnoise', {});
      applyResponse(r);
      flashDone(`${r.closed} шт.`, 'пустых придирок убрано');
    } catch (e) {
      console.warn('[phrasereview] dropnoise', e);
      setNote('');
      setError(e?.message || 'Не получилось. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  const ask = async () => {
    const q = question.trim();
    if (!card || busy || !q) return;
    setBusy(true); setError(''); setAnswer(''); setNote('💬 Спрашиваю…');
    try {
      const r = await api('/api/answer/phrasereview/ask', { review_id: card.id, question: q });
      setNote('');
      setAnswer(r.answer || '');
    } catch (e) {
      console.warn('[phrasereview] ask', e);
      setNote('');
      setError(e?.message || 'Не получилось ответить. Попробуйте ещё раз.');
    } finally { setBusy(false); }
  };

  // Свой текст владельца — вместе с его переводом, если он его написал. Перевод едет
  // тем же полем, что и у «Всё равно принять»: сервер ставит его главным ДО похода к
  // модели, поэтому он не зависит от того, ответила ли она.
  const saveOwn = () => {
    if (!own.trim()) return;
    // У вопроса о переводе своё поле — это РУССКАЯ половина, и уходит она другим
    // решением: заменять немецкий текст фразы тут нечем и незачем.
    if (card?.kind === 'translation') {
      decide('link_own', { text: own.trim() });
      return;
    }
    decide('replace', { text: own.trim(), translation: ownRu.trim() });
  };

  // «Всё равно принять» на забракованной правке: тот же путь, что у своего текста,
  // только текст и перевод берутся с экрана, а не печатаются руками.
  const acceptAnyway = (text, ru) => {
    if (!text) return;
    decide('replace', { text, translation: ru || '' });
  };

  // Карточка в том виде, в каком владелец ею распоряжается. Пока он ничего не тронул,
  // это ровно то, что лежит в базе: правкой считается только отличие от неё.
  const содержимое = () => draft || {
    ru: card?.translation || '',
    ex: (card?.examples || []).map((e) => ({
      de: e.de || '', ru: e.ru || '', deleted: false,
    })),
    topup: false,
  };

  const правитьКарточку = (change) => {
    const next = {
      ...содержимое(),
      ex: содержимое().ex.map((e) => ({ ...e })),
    };
    change(next);
    setDraft(next);
  };

  // Что уедет на сервер: примеры, которые остались живыми и заполнены с обеих сторон.
  // Недописанная строка — не пример, и молча дописывать за человека мы её не будем.
  const живыеПримеры = (d) => d.ex
    .filter((e) => !e.deleted && e.de.trim() && e.ru.trim())
    .map((e) => ({ de: e.de.trim(), ru: e.ru.trim() }));

  const saveCardEdit = (d = содержимое()) => {
    decide('edit', {
      translation: (d.ru || '').trim(),
      examples: живыеПримеры(d),
      top_up: !!d.topup,
    });
  };

  // Готовый вариант судьи — в одно касание. Каждое поле правится СВОЕЙ дверью:
  // перевод карточки — правкой карточки, сама фраза — переименованием статьи.
  // Одной дверью на всё это было бы ровно то, что чинится: русский текст, уехавший
  // в немецкий заголовок.
  const applyFix = (claim) => {
    const text = String(claim.fix || '').trim();
    if (!text || busy) return;
    if (claim.field === 'headword') { decide('replace', { text }); return; }
    if (claim.field !== 'translation') return;
    if (card?.kind === 'translation') { decide('link_own', { text }); return; }
    saveCardEdit({ ...содержимое(), ru: text });
  };

  const skip = () => {
    if (!card) return;
    setSkipped((prev) => new Set(prev).add(card.id));
    setNote(''); setError('');
    setQuestion(''); setAnswer(''); setAsking(false);
  };

  if (items === null) {
    return <div className="pinw"><div className="ans-loading">Загружаю спорные фразы…</div></div>;
  }

  if (!card) {
    return (
      <div className="pinw">
        <div className="pinw-top">
          <div className="pinw-title">{only === 'cards' ? '📗 Карточки словаря' : '📝 Спорные фразы'}</div>
          <div className="pinw-sub">Всё разобрано</div>
        </div>
        <div className="pinw-body">
          {note ? <div className="pinrev-note">{note}</div> : null}
          <div className="pinrev-empty-sub">
            Спорных фраз нет. Ночная проверка складывает сюда только то, в чём два судьи
            не сошлись, — если здесь пусто, значит за прошлую ночь спорить было не о чем.
          </div>
          {error ? <div className="pinrev-err">{error}</div> : null}
        </div>
        <div className="pinw-bar">
          <button className="ans-btn-ghost" disabled={busy} onClick={() => load(true)}>
            🔄 Проверить ещё раз
          </button>
          <button className="pinw-close" onClick={onClose}>Закрыть</button>
        </div>
      </div>
    );
  }

  const variants = card.variants || [];
  const arbiter = card.arbiter || null;
  // Два РАЗНЫХ вопроса в одной очереди. Грамматический: судьи разошлись о самой фразе,
  // решение = выбрать текст. Панельный: три голоса разошлись о КАРТОЧКЕ — примеры и
  // перевод, — и выбирать там нечего, там своя кнопка и на экран нужны сами примеры.
  const isPanel = card.kind === 'panel';
  // Вопрос о переводе: проверка не подтвердила, что русский из карточки означает эту
  // фразу. Выбирать немецкий текст тут нечего — решается судьба РУССКОЙ половины.
  const isTranslation = card.kind === 'translation';
  const isCard = isPanel || isTranslation;
  const examples = card.examples || [];
  const history = card.history || [];
  const judges = card.judges || [];

  // Рекомендация третьего судьи: либо он назвал победителя, либо дал свой текст.
  // Ни того ни другого — главного варианта нет, и выделять наугад мы не будем.
  const bestIndex = Number.isInteger(arbiter?.winner_index)
    ? arbiter.winner_index
    : variants.findIndex((v) => v.kind === 'arbiter');

  const whyOfJudge = (no) => String(judges[no - 1]?.why || '').trim();

  // ЗАБРАКОВАННЫЕ ПРАВКИ НЕ ПЕРЕЧЁРКИВАЮТСЯ.
  //
  // Владелец 26.08.2026: «мне приходит перечёркнутый текст — и что это значит?» Черта
  // лежала на немецкой строке и читалась как «фразу удаляют», хотя означала «эту правку
  // судьи забраковала наша проверка». Ни одного слова об этом на экране не было, а
  // рядом стояла кнопка «Всё равно принять» — экран сам себе противоречил. Теперь такие
  // варианты собраны в одну раскрывашку, названную словами, и у каждого написано, ЧТО
  // с ним не так.
  const rejected = [];
  judges.forEach((j) => {
    ['corrected', 'proposal'].forEach((field) => {
      const text = String(j[field] || '').trim();
      const check = j[`${field}_check`];
      if (text && check?.state === 'bad') {
        rejected.push({
          key: `${j.no}-${field}`, who: `Судья ${j.no}`, text,
          ru: String(j[`${field}_ru`] || ''), what: check.what || '', why: check.why || '',
        });
      }
    });
  });
  if (arbiter?.better && arbiter?.better_check?.state === 'bad') {
    rejected.push({
      key: 'arbiter', who: 'Третий судья', text: arbiter.better,
      ru: arbiter.better_ru || '', what: arbiter.better_check.what || '',
      why: arbiter.better_check.why || '',
    });
  }

  // ⛔ ЗАГОЛОВОК ПИШЕТСЯ ПО ТОМУ, О ЧЁМ СПОР, А НЕ ПО ВИДУ ВОПРОСА.
  //
  // ПОЧИНЕНО 31.08.2026. Здесь стояло «Спор о карточке, а не о фразе» — на КАЖДОЙ
  // панельной карточке, включая те, где голоса спорили как раз о самой фразе
  // («в немецком не говорят das Projekt auslassen»). Владелец: «тут ты пишешь, что
  // так не говорят, а сверху написано, что спор не о фразе — так в чём вопрос?»
  // Панель судит четыре поля, и какое из них под вопросом, теперь приезжает с ним.
  const dispute = card.dispute || null;
  const disputeFields = dispute?.fields || [];
  const claims = dispute?.claims || [];
  // Правка — это ОТЛИЧИЕ от того, что лежит в карточке сейчас. Пока отличия нет,
  // записывать нечего: «сохранить» без единого изменения закрыло бы вопрос ничем.
  const черновик = содержимое();
  const естьПравки = isPanel && (
    (черновик.ru || '').trim() !== (card.translation || '').trim()
    || !!черновик.topup
    || JSON.stringify(живыеПримеры(черновик))
      !== JSON.stringify((card.examples || []).map((e) => ({
        de: String(e.de || '').trim(), ru: String(e.ru || '').trim(),
      })).filter((e) => e.de && e.ru))
  );
  const FIELD_TITLE = {
    headword: 'Спор о самой фразе', translation: 'Спор о переводе',
    examples: 'Спор о примерах', meaning: 'Спор о значении',
  };
  const FIELD_TEXT = {
    headword: 'Голоса разошлись о том, говорят ли так по-немецки.',
    translation: 'Голоса разошлись о том, означает ли этот русский эту немецкую фразу.',
    examples: 'Голоса разошлись о примерах в карточке. Сама фраза тут ни при чём.',
    meaning: 'Голоса разошлись о значении, записанном в карточке.',
  };
  const FIELD_RU = {
    headword: 'сама фраза', translation: 'перевод',
    examples: 'примеры', meaning: 'значение',
  };
  const kindTitle = isTranslation
    ? 'Перевод не прошёл проверку'
    : isPanel
    ? (disputeFields.length === 1
      ? FIELD_TITLE[disputeFields[0]] || 'Спор о карточке'
      : disputeFields.length > 1
        ? 'Спор сразу о нескольких частях карточки'
        : 'Голоса разошлись — о чём именно, не записано')
    : 'Судьи разошлись о грамматике';
  const kindText = isTranslation
    ? 'Этот перевод сохранён вместе с карточкой. Прежде чем показывать его другим, мы спросили модель — она не подтвердила. Решаешь ты.'
    : isPanel
    ? (disputeFields.length === 1
      ? FIELD_TEXT[disputeFields[0]] || 'Ниже — что именно не понравилось голосам.'
      : disputeFields.length > 1
        ? `Под вопросом: ${disputeFields.map((f) => FIELD_RU[f] || f).join(', ')}. Ниже — по пунктам.`
        // Так лежат вопросы, заведённые до 31.08.2026: тогда имя поля терялось по
        // дороге. Придумать его задним числом нельзя — говорим как есть.
        : 'Этот вопрос старый: тогда мы не записывали, о какой части карточки спор. Ниже — слова голосов, как они есть.')
    : (variants.length
      ? 'Ниже — тексты, которые прошли проверку. Нажми тот, который сохраняем.'
      : 'Готового варианта, прошедшего проверку, пока нет. Спроси судей заново или впиши свой.');

  return (
    <div className={`pinw frrev-w${typing ? ' typing' : ''}`}>
      <div className="pinw-top pinw-top-row">
        <span className="pinw-title">{only === 'cards' ? '📗 Карточки словаря' : '📝 Спорные фразы'}</span>
        <span className="pinw-count">
          осталось {Math.max(
            (only && byKind ? Number(byKind[only]) : total) || queue.length, 0)}
        </span>
      </div>

      {done ? (
        <div className="frrev-done">
          ✅ «{done.text}» — {done.what.replace(/^Фраза /, '').toLowerCase()}
        </div>
      ) : null}

      {blind > 0 ? (
        <button className="frrev-sweep frrev-sweep-blind" disabled={busy} onClick={rejudgeAll}>
          ⚖️ Судили без перевода: {blind} — пересудить со смыслом
        </button>
      ) : null}

      {noise > 0 ? (
        <button className="frrev-sweep" disabled={busy} onClick={dropNoise}>
          🧹 Убрать пустые придирки ({noise}) — там заявлена ошибка, а исправить нечего
        </button>
      ) : null}

      <div className="frv-scroll">

        <div className={`frv-kind${isPanel ? ' is-panel' : ''}`}>
          <span className="frv-kind-ic">{isTranslation ? '🔤' : isPanel ? '📗' : '⚖️'}</span>
          <span className="frv-kind-tx"><b>{kindTitle}</b>{kindText}</span>
        </div>

        {/* Эту фразу владелец уже правил. Ночь повторный вопрос больше не заводит, но
            ДРУГУЮ ошибку в той же фразе найти может — и тогда он должен видеть, что
            здесь уже было, а не вспоминать. */}
        {history.length ? (
          <div className="frv-hist">
            <div className="frv-hist-h">Эту фразу ты уже правил</div>
            {history.map((h, n) => (
              <div className="frv-hist-row" key={n}>
                <span className="frv-hist-d">
                  {h.decided_at ? h.decided_at.slice(8, 10) + '.' + h.decided_at.slice(5, 7) : 'раньше'}
                </span>
                <span className="frv-hist-t">{h.decided_text || h.text}</span>
              </div>
            ))}
          </div>
        ) : null}

        <div className="frv-subject">
          <div className="frv-de">{card.text}</div>
          {card.translation
            ? <div className="frv-ru">{card.translation}</div>
            : <div className="frv-ru is-missing">Перевода нет</div>}
        </div>

        {/* ⛔ ПРЕТЕНЗИЯ БЕЗ ГОТОВОГО ВАРИАНТА — НЕ ОТВЕТ, А ЗАГАДКА.
            Владелец 31.08.2026: «в немецком не говорят так — ну окей, а как говорят?
            Почему нет предложения, как исправить?» Теперь каждый голос обязан назвать
            исправленный текст, тот проходит вторую проверку, и здесь он стоит кнопкой
            в одно касание. Не назвал — так и написано, и кнопки нет: выдумывать за
            него мы не станем. */}
        {isCard && claims.length ? (
          <>
            <div className="frv-label">Что не так</div>
            <div className="frv-claims">
              {claims.map((c) => {
                const state = c.fix_check?.state || '';
                return (
                  <div className="frv-claim" key={c.no}>
                    <div className="frv-claim-h">
                      {c.field ? (FIELD_RU[c.field] || c.field) : 'о чём именно — не записано'}
                      {c.voice ? ` · голос ${c.voice}` : ''}
                    </div>
                    <div className="frv-claim-w">{c.why}</div>
                    {c.fix ? (
                      <>
                        {state === 'bad' ? (
                          <div className="frv-v-why frv-objection">
                            <b>Наша проверка не согласна:</b> {c.fix_check.why}
                          </div>
                        ) : null}
                        {state === 'unknown' ? (
                          <div className="frv-v-why">
                            <b>Этот вариант проверить не удалось</b> — решаешь ты.
                          </div>
                        ) : null}
                        <button className={state === 'bad' ? 'frv-save is-quiet' : 'frv-save'}
                          disabled={busy} onClick={() => applyFix(c)}>
                          {state === 'bad' ? 'Всё равно записать: ' : 'Записать: '}
                          «{c.fix}»
                        </button>
                      </>
                    ) : (
                      <div className="frv-claim-nofix">
                        {c.field === 'examples' || c.field === 'meaning'
                          ? 'Это чинится пересборкой — кнопка «Переписать примеры и перевод заново» внизу.'
                          : 'Готового варианта голос не назвал. Впиши свой или спроси заново.'}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </>
        ) : null}

        {/* ПРАВКИ ВНУТРИ КАРТОЧКИ. Механизм построен 28.08.2026 и до 31.08 стоял
            только на экране обычного человека: у владельца на ту же карточку было два
            ответа — «переписать всё заново» или «оставить как есть». Поправить один
            перевод было нечем. */}
        {isPanel ? (
          <>
            <div className="frv-label">Перевод карточки</div>
            <input
              className="pinrev-word" value={содержимое().ru} disabled={busy}
              onChange={(e) => правитьКарточку((d) => { d.ru = e.target.value; })}
              onFocus={() => setTyping(true)}
              onBlur={() => setTyping(false)}
              placeholder="перевод по-русски"
              enterKeyHint="done"
              onKeyDown={(e) => { if (e.key === 'Enter') e.target.blur(); }}
            />

            <div className="frv-label">Примеры в карточке</div>
            <div className="frv-ex">
              {содержимое().ex.map((e, n) => (
                <div className={e.deleted ? 'frv-ex-row is-dropped' : 'frv-ex-row'} key={n}>
                  <input
                    className="pinrev-word frv-ex-in" value={e.de} disabled={busy || e.deleted}
                    onChange={(ev) => правитьКарточку((d) => { d.ex[n].de = ev.target.value; })}
                    onFocus={() => setTyping(true)} onBlur={() => setTyping(false)}
                    placeholder="пример по-немецки" />
                  <input
                    className="pinrev-word frv-ex-in" value={e.ru} disabled={busy || e.deleted}
                    onChange={(ev) => правитьКарточку((d) => { d.ex[n].ru = ev.target.value; })}
                    onFocus={() => setTyping(true)} onBlur={() => setTyping(false)}
                    placeholder="перевод примера" />
                  <button className="ans-btn-ghost frv-ex-drop" disabled={busy}
                    onClick={() => правитьКарточку((d) => { d.ex[n].deleted = !d.ex[n].deleted; })}>
                    {e.deleted ? '↺ вернуть' : '🗑 убрать'}
                  </button>
                </div>
              ))}
              <button className="ans-btn-ghost" disabled={busy}
                onClick={() => правитьКарточку((d) => {
                  d.ex.push({ de: '', ru: '', deleted: false });
                })}>+ Добавить свой пример</button>
            </div>
            {живыеПримеры(содержимое()).length < 2 ? (
              <label className="frv-opt">
                <input type="checkbox" checked={!!содержимое().topup} disabled={busy}
                  onChange={() => правитьКарточку((d) => { d.topup = !d.topup; })} />
                <span>Примеров меньше двух — пусть ночь допишет недостающие.
                  Твои останутся как есть.</span>
              </label>
            ) : null}
          </>
        ) : null}

        {variants.length ? <div className="frv-label">Что можно сохранить</div> : null}

        <div className="frv-variants">
          {variants.map((v) => {
            const best = v.index === bestIndex;
            const why = v.kind === 'arbiter' ? String(arbiter?.why || '') : whyOfJudge(v.judge);
            return (
              <div className={`frv-v${best ? ' is-best' : ''}`} key={v.index}>
                <div className="frv-v-top">
                  <span className="frv-who">
                    {v.kind === 'arbiter' ? 'Третий судья · решающий голос' : `Судья ${v.judge}`}
                  </span>
                  {best ? <span className="frv-chip ok">✓ рекомендует третий</span> : null}
                </div>
                <div className="frv-v-de">{v.text}</div>
                {v.ru ? <div className="frv-v-ru">{v.ru}</div> : null}
                {why ? <div className="frv-v-why"><b>Почему так:</b> {why}</div> : null}
                {/* Наша проверка была против, а третий судья — за. Прятать её возражение
                    нельзя: владелец решает сам и должен видеть обе стороны. */}
                {v.objection ? (
                  <div className="frv-v-why frv-objection">
                    <b>Наша проверка не согласна:</b> {v.objection}
                  </div>
                ) : null}
                <button className="frv-save" disabled={busy}
                  onClick={() => decide('accept', { variant: v.index })}>
                  Сохранить этот вариант
                </button>
              </div>
            );
          })}
        </div>

        {rejected.length ? (
          <details className="frv-fold">
            <summary>
              {rejected.length === 1
                ? 'Один вариант мы проверили и не советуем'
                : `Эти варианты мы проверили и не советуем: ${rejected.length}`}
            </summary>
            <div className="frv-fold-body">
              <div className="frv-note">
                Это не значит, что фраза плохая. Это значит: <b>саму правку мы прогнали
                через проверку, и она не прошла</b>. Сохранить всё равно можно — кнопкой ниже.
              </div>
              {rejected.map((r) => (
                <div className="frv-v is-rejected" key={r.key}>
                  <div className="frv-v-top">
                    <span className="frv-who">{r.who}</span>
                    <span className="frv-chip no">не советуем</span>
                  </div>
                  <div className="frv-v-de">{r.text}</div>
                  {r.ru ? <div className="frv-v-ru">{r.ru}</div> : null}
                  <div className="frv-v-why">
                    <b>Что не так:</b> {r.what}{r.what && r.why ? '. ' : ''}{r.why}
                  </div>
                  <button className="frv-save is-quiet" disabled={busy}
                    onClick={() => acceptAnyway(r.text, r.ru)}>
                    Всё равно сохранить этот вариант
                  </button>
                </div>
              ))}
            </div>
          </details>
        ) : null}

        {/* У карточек их слова стоят выше, по пунктам и с готовым вариантом
            (см. `claims`). Прятать их в раскрывашку значит снова заставить владельца
            искать, в чём спор. Раскрывашка осталась грамматическим вопросам: там
            рассуждение объясняет ВЫБОР между вариантами, а не заменяет его. */}
        {!isCard && judges.some((j) => j.why) ? (
          <details className="frv-fold">
            <summary>Как рассуждали судьи</summary>
            <div className="frv-fold-body">
              {judges.map((j) => (
                j.why ? (
                  <div className="frv-judge" key={j.no}>
                    <div className="frv-judge-h">
                      {isCard ? `Голос ${j.no}` : `Судья ${j.no}`}
                      {j.verdict === 'error' && j.category ? ` · ${j.category}` : ''}
                      {j.verdict === 'context' ? ' · зависит от контекста' : ''}
                      {j.verdict === 'style' ? ' · вопрос вкуса' : ''}
                      {j.verdict === 'ok' ? ' · ошибки нет' : ''}
                    </div>
                    <div className="frv-judge-w">{j.why}</div>
                  </div>
                ) : null
              ))}
              {arbiter?.why ? (
                <div className="frv-judge">
                  <div className="frv-judge-h">Третий судья · решение</div>
                  <div className="frv-judge-w">{arbiter.why}</div>
                </div>
              ) : null}
            </div>
          </details>
        ) : null}

        {!variants.length && !isCard && card.all_ok ? (
          <div className="frv-note">
            Оба судьи говорят: ошибки нет. Править нечего — оставь фразу как есть,
            и она больше не вернётся в этот разбор.
          </div>
        ) : null}

        {answer ? <div className="frrev-answer">{answer}</div> : null}

        {asking ? (
          <div className="frrev-ask">
            <input
              className="pinrev-word" value={question} disabled={busy} autoFocus
              onChange={(e) => setQuestion(e.target.value)}
              onFocus={() => setTyping(true)}
              onBlur={() => setTyping(false)}
              placeholder="например: mit и gegen тут разный смысл?"
              enterKeyHint="send"
              onKeyDown={(e) => { if (e.key === 'Enter' && question.trim()) { e.target.blur(); ask(); } }}
            />
            <div className="frrev-row">
              <button className="ans-btn-ghost" disabled={busy || !question.trim()}
                onClick={ask}>💬 Спросить</button>
              <button className="ans-btn-ghost"
                onClick={() => { setAsking(false); setQuestion(''); }}>← передумал</button>
            </div>
          </div>
        ) : null}

        {note ? <div className="pinrev-note">{note}</div> : null}
        {error ? <div className="pinrev-err">{error}</div> : null}
      </div>

      <div className="pinw-bar">
        {/* Панельная карточка: выбирать нечего, главное действие одно — отправить
            примеры и перевод на переписывание ночному переписчику. */}
        {isPanel ? (
          <>
            {/* Главное действие панельной карточки — ЕГО правки, а не пересборка.
                Кнопка гаснет, пока он ничего не тронул: нажать «записать», ничего не
                изменив, значит закрыть вопрос молча и ничем. */}
            <button className="frv-save frv-main" disabled={busy || !естьПравки}
              onClick={() => saveCardEdit()}>
              ✏️ Записать мои правки
            </button>
            <button className="ans-btn-ghost frv-wide" disabled={busy}
              onClick={() => decide('rewrite')}>
              📗 Переписать примеры и перевод заново
            </button>
          </>
        ) : null}
        {isTranslation ? (
          <button className="frv-save frv-main" disabled={busy}
            onClick={() => decide('link_accept')}>
            🔤 Сохранить этот перевод как общий
          </button>
        ) : null}

        {/* ⛔ ЭТО ПОЛЕ ПЕРЕИМЕНОВЫВАЕТ САМУ НЕМЕЦКУЮ ФРАЗУ, И НАЗЫВАТЬСЯ ОНО ДОЛЖНО
            ТАК ЖЕ. До 31.08.2026 на панельной карточке оно называлось «или впиши свой
            перевод» — а уезжало переименованием статьи: русский текст встал бы вместо
            немецкого заголовка. Перевод правится своим полем выше; здесь — только
            сама фраза, и на панельной карточке это спрятано за кнопкой, потому что
            случай редкий. */}
        {isPanel && !fixingText ? (
          <button className="ans-btn-ghost frv-wide" disabled={busy}
            onClick={() => setFixingText(true)}>
            ✍️ Исправить саму фразу по-немецки
          </button>
        ) : null}
        {/* Прячем НЕ атрибутом `hidden`: у `.frv-own` стоит display:flex, и он бы
            перебил его — поле осталось бы на экране. */}
        <div className="frv-own" style={isPanel && !fixingText ? { display: 'none' } : undefined}>
          {isPanel ? <div className="frv-label">Сама фраза по-немецки</div> : null}
          <input
            className="pinrev-word" value={own} disabled={busy}
            onChange={(e) => setOwn(e.target.value)}
            onFocus={() => setTyping(true)}
            onBlur={() => setTyping(false)}
            placeholder={isTranslation ? 'или впиши свой перевод'
              : isPanel ? 'фраза по-немецки, как правильно' : 'или впиши свой вариант'}
            enterKeyHint="next"
            onKeyDown={(e) => {
              if (e.key === 'Enter' && own.trim()) { e.target.blur(); saveOwn(); }
            }}
          />
          {own.trim() && !isTranslation ? (
            <input
              className="pinrev-word frrev-own-ru" value={ownRu} disabled={busy}
              onChange={(e) => setOwnRu(e.target.value)}
              onFocus={() => setTyping(true)}
              onBlur={() => setTyping(false)}
              placeholder="перевод по-русски (можно не писать)"
              enterKeyHint="send"
              onKeyDown={(e) => { if (e.key === 'Enter') { e.target.blur(); saveOwn(); } }}
            />
          ) : null}
          {own.trim() ? (
            <button className="frv-save" disabled={busy} onClick={saveOwn}>
              {isTranslation ? 'Сохранить свой перевод'
                : isPanel ? 'Записать эту фразу вместо прежней' : 'Сохранить свой вариант'}
            </button>
          ) : null}
        </div>

        <div className="frrev-row">
          {/* ⛔ КНОПКА НАЗЫВАЕТСЯ ТЕМ, ЧТО ДЕЛАЕТ. Владелец 31.08.2026: «а что такое
              „отложить“? а что такое „оставить“? в чём между ними разница?» Разница
              была настоящая — «оставить» закрывает вопрос навсегда, «отложить» не
              пишет в базу ничего, — но прочесть её можно было только в серой строчке
              из трёх пунктов под кнопками. Теперь она в самих названиях и в подписи
              под каждой кнопкой. */}
          <button className={variants.length || isCard ? 'ans-btn-ghost' : 'ans-btn frrev-keep'}
            disabled={busy} onClick={() => decide('keep')}>
            👍 {isTranslation ? 'Оставить личным' : 'Всё верно — закрыть вопрос'}
          </button>
          {!asking ? (
            <button className="ans-btn-ghost" disabled={busy}
              onClick={() => { setAsking(true); setAnswer(''); }}>❓ Спросить</button>
          ) : null}
          {/* «Пересудить» только у грамматики: панельную карточку судят три голоса о
              примерах, и пересуживать её этим судьёй бессмысленно. */}
          {!isCard ? (
            <button className="ans-btn-ghost" disabled={busy} onClick={rejudge}>🔁 Пересудить</button>
          ) : null}
          {!isCard && variants.length > 0 && !arbiter ? (
            <button className="ans-btn-ghost" disabled={busy} onClick={settle}>⚖️ Кто прав?</button>
          ) : null}
        </div>

        <div className="frrev-row">
          <button className="ans-btn-ghost" disabled={busy} onClick={skip}>
            ↷ Вернуться позже
          </button>
          <button className="ans-btn-ghost pinrev-skip" disabled={busy}
            onClick={() => decide('delete')}>🗑 Удалить фразу</button>
          <button className="ans-btn-ghost frrev-closebtn" onClick={onClose}>✕ Закрыть</button>
        </div>
        {/* Подпись у каждой кнопки, а не общей строкой: разница между «закрыть вопрос»
            и «вернуться позже» — это разница между «больше не спросим» и «спросим
            снова», и читать её в слитном тексте владелец не должен. */}
        <div className="frrev-hint frrev-hint-rows">
          <div><b>Всё верно</b> — вопрос закрыт навсегда, эта карточка больше не придёт.</div>
          <div><b>Вернуться позже</b> — в базе ничего не меняется, карточка уезжает в
            конец списка и придёт снова.</div>
          <div><b>Удалить фразу</b> — уносит её из общего словаря вместе с подписными
            карточками.</div>
        </div>
      </div>
    </div>
  );
}
