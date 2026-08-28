import React, { useCallback, useEffect, useState } from 'react';
import './wordAudit.css';
import { requestTabletFullscreen } from '../utils/tabletFullscreen.js';

/**
 * Проверка слов: человек решает, что делать со словами, которые мы не смогли подтвердить.
 *
 * Зачем экран. Дверь слова сверяет каждое сохранённое слово со справочниками. Часть слов
 * не подтверждается: редкие, из другого языка, или потерявшие букву при сохранении.
 * Молча удалять их нельзя — человек сохранял их осознанно. Молча оставлять тоже нельзя:
 * он будет учить слово в кривом виде.
 *
 * Почему не сообщение с кнопками в личке. Владелец 20.08.2026: «сделай красивый интерфейс
 * в мини-аппе, а не сообщение». В чате не помещается объяснение, не помещается подсказка
 * правильного написания и негде править текст руками.
 *
 * Правило экрана: НИКАКИХ РЕБУСОВ. У каждого слова написано, почему оно здесь, простыми
 * словами. Последствие решения видно до нажатия «Готово». До «Готово» не меняется ничего.
 *
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  МОЛЧАНИЕ НЕ УДАЛЯЕТ. УДАЛЯЕТ ТОЛЬКО КНОПКА «УДАЛИТЬ».                        ║
 * ║  Решение владельца 25.08.2026, отменяет его же правило от 19.08 «отмеченные    ║
 * ║  остаются, остальные удаляются». Дословно: «нельзя удалять просто потому что   ║
 * ║  кто-то не увидел, может просмотрел случайно».                                 ║
 * ║                                                                              ║
 * ║  Список бывает на сто слов, экран длинный, палец скользит: пропустить          ║
 * ║  карточку — это норма поведения, а не решение. Цена ошибки несимметрична —    ║
 * ║  лишнее сомнительное слово придёт на проверку снова, стёртое нужное не         ║
 * ║  вернуть. Поэтому не отмеченное ничем слово остаётся жить и приходит опять.    ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 */

async function api(path, body = {}) {
  const initData = window.Telegram?.WebApp?.initData || '';
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Telegram-Init-Data': initData },
    body: JSON.stringify({ initData, ...body }),
  });
  const data = await response.json().catch(() => null);
  if (!response.ok || !data) {
    const error = new Error(data?.message || 'Не удалось связаться с сервером');
    error.status = response.status;
    throw error;
  }
  return data;
}

const KEEP = 'keep';
const FIXED = 'fixed';
const MANUAL = 'manual';
const RETRANS = 'retrans';
const DROP = 'drop';
const EDIT = 'edit';        // правки ВНУТРИ карточки: примеры и перевод
const REBUILD = 'rebuild';  // собрать карточку заново ночью

/** Содержимое карточки в том виде, в каком человек им распоряжается. */
function свежийРазбор(it) {
  return {
    ru: it.translation || '',
    topup: false,
    ex: (it.examples || []).map((e) => ({
      de: e.de || '', ru: e.ru || '', flag: '',
      deleted: false, editing: false, edited: false, added: false,
    })),
  };
}

export default function WordAudit() {
  const [items, setItems] = useState(null);
  const [state, setState] = useState({});       // слово → решение
  // Фраза → ТЕКСТ принятого варианта, а не его номер.
  // ┌─ ПОЧИНЕНО 28.08.2026. НОМЕР УКАЗЫВАЛ В ДРУГОЙ СПИСОК. ────────────────────────┐
  // │ Здесь лежал номер кнопки, а сервер применял его к ПОЛНОМУ списку вариантов —  │
  // │ тогда как этот экран показывает урезанный (без забракованных, не больше двух).│
  // │ Замер по живой базе 28.08.2026: из 40 решений владельца за сутки два записали │
  // │ не то, что он нажал (#317, #319). Текст в чужой список указать не может.      │
  // └──────────────────────────────────────────────────────────────────────────────┘
  const [variant, setVariant] = useState({});
  const [typed, setTyped] = useState({});       // слово → написание, вписанное руками
  const [typedTrans, setTypedTrans] = useState({}); // слово → перевод, вписанный руками
  const [editing, setEditing] = useState({});   // слово → открыто ли поле правки
  // Содержимое карточки, когда спор идёт НЕ о фразе, а о её наполнении.
  // Ключ — та же фраза; внутри {ru, ex:[{de,ru,deleted,editing,edited,added,flag}], topup}.
  // Заводится при первом касании и до «Готово» живёт только здесь.
  const [inner, setInner] = useState({});
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(null);

  useEffect(() => { requestTabletFullscreen(); }, []);

  const load = useCallback(async () => {
    setError('');
    try {
      const data = await api('/api/webapp/word-audit/list');
      if (!data.ok) { setError(data.message || 'Не удалось загрузить список.'); return; }
      setItems(data.items || []);
    } catch (e) {
      // Пустой список и «не загрузилось» — разные вещи, и человек должен их различать.
      setError('Не удалось загрузить список. Проверь связь и попробуй ещё раз.');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const pick = (word, action) => {
    setState((prev) => ({ ...prev, [word]: prev[word] === action ? '' : action }));
  };

  /* ── Управление содержимым карточки ────────────────────────────────────────
     Правило: любое касание внутренностей — это РЕШЕНИЕ, а не черновик. Оно само
     встаёт действием «Записать мои правки», и то, что человек оставил, уезжает
     на сервер целиком. Пересборкой карточка при этом не занимается.
     Владелец 28.08.2026: «я должен иметь возможность каждый пример внутри карточки
     либо откорректировать, либо удалить, либо оставить как есть». */
  const карточка = (it) => inner[it.word] || свежийРазбор(it);

  const тронута = (it, d) => (
    (d.ru || '').trim() !== (it.translation || '').trim()
    || d.ex.some((e) => e.deleted || e.edited || e.added)
  );

  const правитьКарточку = (it, изменить) => {
    setInner((prev) => {
      const d = JSON.parse(JSON.stringify(prev[it.word] || свежийРазбор(it)));
      изменить(d);
      // Отметка «добери ночью» живёт ровно пока условие верно: вернул примеры —
      // она снимается сама, иначе на сервер уехало бы «добери» при полной карточке.
      if (!(тронута(it, d) && d.ex.filter((e) => !e.deleted).length < 2)) d.topup = false;
      setState((s) => ({ ...s, [it.word]: тронута(it, d) ? EDIT : '' }));
      return { ...prev, [it.word]: d };
    });
  };

  const saveManual = (word) => {
    const text = (typed[word] || '').trim();
    if (!text) return;
    setState((prev) => ({ ...prev, [word]: MANUAL }));
    setEditing((prev) => ({ ...prev, [word]: false }));
  };

  const submit = async () => {
    if (!items) return;
    setBusy(true);
    setError('');
    const decisions = items.map((it) => ({
      word: it.word,
      action: state[it.word] || '',
      text: state[it.word] === MANUAL ? (typed[it.word] || '').trim() : it.suggestion,
      // Свой перевод уходит только вместе со своим вариантом — в остальных
      // решениях перевод человек не трогал, и переписывать его нечем.
      translation: state[it.word] === MANUAL ? (typedTrans[it.word] || '').trim() : '',
      // Фраза правится тем же механизмом, что и на экране владельца, и ему нужен
      // номер строки проверки. Сервер сверяет его заново по базе — присланному
      // номеру он не верит. Принятый вариант уходит ТЕКСТОМ (см. useState выше):
      // сервер ищет его среди тех кнопок, которые этот экран имел право показать.
      kind: it.kind || 'word',
      review_id: it.review_id || 0,
      variant_text: variant[it.word] || '',
      // Правки внутри карточки уезжают ЦЕЛИКОМ: что человек оставил, то и ляжет.
      // Не команда «пересобери», а готовое содержимое — это его работа, не машины.
      ...(state[it.word] === EDIT ? {
        translation: (карточка(it).ru || '').trim(),
        examples: карточка(it).ex.filter((e) => !e.deleted)
          .map((e) => ({ de: (e.de || '').trim(), ru: (e.ru || '').trim() })),
        top_up: !!карточка(it).topup,
      } : {}),
    }));
    try {
      // Сервер ТОЛЬКО ПРИНИМАЕТ решения и сразу отвечает — сама работа идёт под
      // капотом, а её итог приходит человеку сообщением в чат.
      // ┌─ ПОЧИНЕНО 28.08.2026. ЧЕЛОВЕК ЖДАЛ «СОХРАНЯЮ…» ПЯТЬ МИНУТ И НЕ ДОЖДАЛСЯ. ┐
      // │ Раньше здесь ждали, пока сервер применит ВСЕ решения. На каждую правку   │
      // │ фразы он ходит к модели: замер 28.08.2026 — 30 фраз за 295 секунд при    │
      // │ лимите воркера 300, то есть ответа не было в принципе.                    │
      // │ Владелец: «мы пользователя отпускаем, работу делаем под капотом».         │
      // └─────────────────────────────────────────────────────────────────────────┘
      const data = await api('/api/webapp/word-audit/apply', { decisions });
      if (!data.ok) { setError(data.message || 'Не удалось сохранить.'); setBusy(false); return; }
      setDone({ accepted: Number(data.accepted || 0) });
    } catch (e) {
      setError('Не удалось отправить решения. Ничего не изменилось — попробуй ещё раз.');
    }
    setBusy(false);
  };

  if (items === null && !error) {
    return <div className="wa"><div className="wa-loading">Загружаю твои слова…</div></div>;
  }

  if (done) {
    const n = Number(done.accepted || 0);
    return (
      <div className="wa">
        <div className="wa-final">
          <div className="wa-final-emoji">🦊</div>
          <h2>{n ? 'Принято' : 'Ничего не отмечено'}</h2>
          <p>
            {n
              ? `Твоих решений: ${n}. Дальше можно не ждать — закрывай экран и занимайся своим.`
              : 'Ты не нажал ни одной кнопки, поэтому ничего не изменилось. Всё осталось на месте.'}
          </p>
          {n ? (
            <p className="wa-final-note">
              Правки применяем под капотом — это занимает несколько минут.
              Когда закончим, пришлём сообщение в чат: что исправили, что оставили.
            </p>
          ) : null}
        </div>
      </div>
    );
  }

  if (error && !items?.length) {
    return (
      <div className="wa">
        <div className="wa-empty">
          <p>{error}</p>
          <button type="button" className="wa-retry" onClick={load}>Попробовать ещё раз</button>
        </div>
      </div>
    );
  }

  if (!items.length) {
    return (
      <div className="wa">
        <div className="wa-empty">
          <div className="wa-final-emoji">✅</div>
          <h2>В словаре всё в порядке</h2>
          <p>Мы сверили твои слова со справочниками, а фразы прочитали ночью — проверять нечего.</p>
        </div>
      </div>
    );
  }

  // Фразы и слова живут на одном экране, но спрашивают о разном, поэтому и
  // заголовок честно называет то, что внизу лежит.
  const phrases = items.filter((it) => it.kind === 'phrase').length;
  const words = items.length - phrases;
  const heading = phrases && words
    ? `${items.length} ${plural(items.length, 'запись', 'записи', 'записей')} ждут твоего решения`
    : phrases
      ? `${phrases} ${plural(phrases, 'фраза', 'фразы', 'фраз')} ${plural(phrases, 'ждёт', 'ждут', 'ждут')} твоего решения`
      : `${words} ${plural(words, 'слово', 'слова', 'слов')} ${plural(words, 'ждёт', 'ждут', 'ждут')} твоего решения`;

  // Удаляется РОВНО то, у чего нажата кнопка «Удалить». Всё остальное — включая
  // слова, которых человек вовсе не касался, — остаётся жить (см. рамку в шапке).
  const drop = items.filter((it) => state[it.word] === DROP).length;
  const retrans = items.filter((it) => state[it.word] === RETRANS).length;
  const keep = items.length - drop - retrans;

  return (
    <div className="wa">
      <header className="wa-head">
        <div className="wa-kicker">🦊 {phrases ? 'Проверка слов и фраз' : 'Проверка слов'}</div>
        <h1>{heading}</h1>
        <p className="wa-lede">
          Ты их сохранял, а мы не смогли подтвердить, что они написаны правильно.
          Посмотри и реши, что с ними делать. Ничего не удалится само — только то,
          что ты сам отметишь кнопкой «Удалить».
        </p>
      </header>

      <details className="wa-why">
        <summary>Откуда это взялось и что будет дальше</summary>
        <p><b>Откуда.</b> Мы сверяем каждое сохранённое слово с немецкими справочниками.
          Эти не нашлись: возможно, слово редкое, возможно — из другого языка, а возможно,
          при сохранении потерялась буква.</p>
        {phrases ? (
          <p><b>Про фразы.</b> Сохранённые фразы ночью читают два проверяющих. Если они
            нашли ошибку или разошлись во мнении, фраза приходит сюда — с готовым
            вариантом, если он есть. Мы ничего не исправляем за тебя молча.</p>
        ) : null}
        <p><b>Зачем проверять.</b> Если слово с ошибкой останется, ты будешь учить его
          в таком виде — и запомнишь неправильно.</p>
        <p><b>Что делать.</b> Если мы догадались, как слово пишется правильно, вверху будет
          готовая кнопка <b>«Да, это …»</b> — одно касание. <b>«Оставить как есть»</b> —
          слово верное, ничего не меняем. <b>«Перевод не тот»</b> — слово верное, а перевод
          плохой: соберём карточку заново этой ночью. <b>«Свой вариант»</b> — впиши слово
          так, как надо, и, если нужно, свой перевод: мы сохраним именно их.
          <b>«Удалить»</b> — слово тебе не нужно, убираем его из словаря.</p>
        <p><b>Ничего не удалится само.</b> Слово уходит из словаря, только если ты сам
          нажал у него «Удалить». Всё, чего ты не тронул, остаётся на месте и просто
          придёт на проверку в следующий раз. И даже отмеченное на удаление ничего не
          теряет, пока ты не нажмёшь «Готово» внизу.</p>
        <p><b>Сколько делать.</b> Сколько хочешь. Непроверенные придут снова через несколько дней.</p>
      </details>

      <div className="wa-tally">
        <span className="wa-t-keep"><b>{keep}</b>оставим</span>
        <span className="wa-t-fix"><b>{retrans}</b>переделаем</span>
        <span className="wa-t-drop"><b>{drop}</b>удалим</span>
      </div>

      {items.map((it) => {
        const chosen = state[it.word] || '';
        const isPhrase = it.kind === 'phrase';
        const variants = Array.isArray(it.variants) ? it.variants : [];
        // Спор о НАПОЛНЕНИИ карточки, а не о самой фразе: другой вопрос — другие кнопки.
        const проКарточку = it.question === 'card';
        const разбор = проКарточку ? карточка(it) : null;
        const живые = проКарточку ? разбор.ex.filter((e) => !e.deleted) : [];
        const убрано = проКарточку ? разбор.ex.length - живые.length : 0;
        const поправлено = проКарточку ? разбор.ex.filter((e) => e.edited && !e.deleted).length : 0;
        const дописано = проКарточку ? разбор.ex.filter((e) => e.added && !e.deleted).length : 0;
        const переводДругой = проКарточку
          && (разбор.ru || '').trim() !== (it.translation || '').trim();
        const естьПравки = проКарточку && (убрано || поправлено || дописано || переводДругой);
        const судьи = Array.isArray(it.judges) ? it.judges : [];
        return (
          <div className={isPhrase ? 'wa-card wa-card-phrase' : 'wa-card'}
               data-state={chosen} key={it.word}>
            <div className="wa-word-row">
              <span className="wa-word">{state[it.word] === MANUAL ? typed[it.word] : it.word}</span>
            </div>
            {it.translation ? <p className="wa-trans">{it.translation}</p> : null}
            {/* Претензии панели идут пунктами: у неё их обычно две и о разном —
                про примеры и про перевод. Сплошным абзацем это не читается. */}
            {проКарточку && (it.doubts || []).length ? (
              <div className="wa-doubt">
                <p className="wa-doubt-who">Сомнение не во фразе, а в карточке</p>
                <ul>{it.doubts.map((d) => <li key={d}>{d}</li>)}</ul>
              </div>
            ) : <div className="wa-reason">{it.why}</div>}
            {it.safe && !chosen ? (
              <div className="wa-safe">Слово настоящее — трогать ничего не нужно.</div>
            ) : null}

            {/* У фразы готовых вариантов бывает несколько, а бывает ни одного:
                проверяющие не всегда сходятся, и придумывать за них мы не станем.
                Каждый вариант — своя кнопка, чтобы по нажатию было видно, что
                именно принимаешь: ровно то же правило, что на экране владельца. */}
            {/* Подсвечена РОВНО нажатая кнопка. До 28.08.2026 подсветку давал
                css-селектор на всю карточку («.wa-card[data-state="fixed"] .wa-suggest»),
                и при двух вариантах загорались оба: выбранный отличался только
                галочкой. Владелец 28.08.2026: «я нажимаю один и подсвечивается сразу
                оба, как будто они выбраны». Признак выбора теперь ровно один и на
                самой кнопке. */}
            {/* ⚠ ВАРИАНТ СТОИТ РЯДОМ СО СЛОВАМИ СВОЕГО ПРОВЕРЯЮЩЕГО.
                ┌─ ПОЧИНЕНО 28.08.2026. ОБОСНОВАНИЯ НЕ ДОЕЗЖАЛИ ВОВСЕ. ───────────────┐
                │ Проверяющие пишут, ПОЧЕМУ так: таких мнений 319 из 322 (замер той    │
                │ же даты). На экран не выводилось ни одно — вместо них стояла одна    │
                │ обобщённая строчка «Похоже, слова стоят не в том порядке», а кнопки  │
                │ висели без подписи, кто и на каком основании это предложил.          │
                │ Владелец 28.08.2026: «почему не показать размышления каждого судьи,  │
                │ чтобы я мог выбрать вариант того, который мне ближе».                │
                └─────────────────────────────────────────────────────────────────────┘ */}
            {isPhrase && !проКарточку ? судьи.map((с) => {
              const свои = variants.filter((v) => v.judge === с.n);
              return (
                <div className={`wa-judge wa-judge-${с.n}`} key={`j${с.n}`}>
                  <p className="wa-judge-who">
                    <span className="wa-dot" />
                    Проверяющий {с.n === 1 ? 'первый' : 'второй'}
                    {it.arbiter && it.arbiter.winner === с.n ? ' · его вариант признан верным' : ''}
                  </p>
                  <p className="wa-judge-why">{с.why}</p>
                  {!свои.length ? (
                    <p className="wa-judge-none">Своего варианта не предложил.</p>
                  ) : null}
                  {свои.map((v) => {
                    const picked = chosen === FIXED && variant[it.word] === v.text;
                    return (
                      <button type="button" key={v.text} aria-pressed={picked}
                              className={picked ? 'wa-suggest wa-suggest-phrase is-picked'
                                                : 'wa-suggest wa-suggest-phrase'}
                              onClick={() => {
                                // Нажали УЖЕ выбранный — снимаем. Нажали соседний —
                                // переключаемся. Общий pick() умеет только
                                // «включить/выключить», и переход с одного варианта
                                // на другой гасил выбор целиком.
                                if (picked) { pick(it.word, FIXED); return; }
                                setVariant((p) => ({ ...p, [it.word]: v.text }));
                                setState((p) => ({ ...p, [it.word]: FIXED }));
                              }}>
                        <span className="wa-suggest-de">{picked ? '✓ ' : ''}{v.text}</span>
                        {v.ru ? <span className="wa-suggest-ru">{v.ru}</span> : null}
                        {v.kind ? <span className="wa-suggest-meta">{v.kind}</span> : null}
                      </button>
                    );
                  })}
                </div>
              );
            }) : null}

            {/* Вариант третьего судьи своего блока «проверяющего» не имеет —
                он рождается уже в разборе спора и стоит после обоих. */}
            {isPhrase && !проКарточку
              ? variants.filter((v) => !судьи.some((с) => с.n === v.judge)).map((v) => {
                const picked = chosen === FIXED && variant[it.word] === v.text;
                return (
                  <button type="button" key={v.text} aria-pressed={picked}
                          className={picked ? 'wa-suggest wa-suggest-phrase is-picked'
                                            : 'wa-suggest wa-suggest-phrase'}
                          onClick={() => {
                            if (picked) { pick(it.word, FIXED); return; }
                            setVariant((p) => ({ ...p, [it.word]: v.text }));
                            setState((p) => ({ ...p, [it.word]: FIXED }));
                          }}>
                    <span className="wa-suggest-de">{picked ? '✓ ' : ''}{v.text}</span>
                    {v.ru ? <span className="wa-suggest-ru">{v.ru}</span> : null}
                    <span className="wa-suggest-meta">{v.kind || 'текст третьего судьи'}</span>
                  </button>
                );
              }) : null}

            {isPhrase && !проКарточку && it.arbiter ? (
              <div className="wa-arb">
                <p className="wa-arb-who">⚖ Третий проверяющий рассудил спор</p>
                <p>{it.arbiter.why}</p>
              </div>
            ) : null}

            {/* ── Содержимое карточки: каждым примером распоряжаются отдельно ──── */}
            {проКарточку ? (
              <>
                <div className="wa-sub">
                  <span>Перевод фразы</span>
                  {переводДругой ? (
                    <button type="button" className="wa-tool wa-tool-back"
                            onClick={() => правитьКарточку(it, (d) => { d.ru = it.translation || ''; })}>
                      Вернуть исходный
                    </button>
                  ) : null}
                </div>
                <div className={переводДругой ? 'wa-trans-box edited' : 'wa-trans-box'}>
                  <input value={разбор.ru} aria-label="перевод фразы"
                         onChange={(e) => правитьКарточку(it, (d) => { d.ru = e.target.value; })} />
                </div>

                <div className="wa-sub">
                  <span>Примеры в карточке · {живые.length} из {разбор.ex.length}</span>
                  {убрано < разбор.ex.length ? (
                    <button type="button" className="wa-tool wa-tool-del"
                            onClick={() => правитьКарточку(it, (d) => d.ex.forEach((e) => {
                              e.deleted = true; e.editing = false;
                            }))}>Удалить все</button>
                  ) : (
                    <button type="button" className="wa-tool wa-tool-back"
                            onClick={() => правитьКарточку(it, (d) => d.ex.forEach((e) => {
                              e.deleted = false;
                            }))}>Вернуть все</button>
                  )}
                </div>

                <ul className="wa-ex">
                  {разбор.ex.map((e, i) => (
                    <li key={i} className={[e.deleted ? 'gone' : '',
                                            (e.edited || e.added) && !e.deleted ? 'edited' : '']
                                            .filter(Boolean).join(' ')}>
                      <div>
                        {e.editing ? (
                          <div className="wa-ex-edit">
                            <input value={e.de} aria-label="пример по-немецки"
                                   placeholder="пример по-немецки"
                                   onChange={(ev) => правитьКарточку(it, (d) => {
                                     d.ex[i].de = ev.target.value;
                                   })} />
                            <input value={e.ru} aria-label="перевод примера"
                                   placeholder="перевод примера"
                                   onChange={(ev) => правитьКарточку(it, (d) => {
                                     d.ex[i].ru = ev.target.value;
                                   })} />
                            <div className="row">
                              <button type="button"
                                      onClick={() => правитьКарточку(it, (d) => {
                                        d.ex[i].editing = false;
                                        // Свой пример считается добавленным, а не
                                        // исправленным: иначе он попал бы в оба
                                        // счётчика сразу и итог соврал бы человеку.
                                        if (!d.ex[i].added) d.ex[i].edited = true;
                                      })}>Записать</button>
                              <button type="button" className="ghost"
                                      onClick={() => правитьКарточку(it, (d) => {
                                        d.ex[i].editing = false;
                                        if (d.ex[i].added && !d.ex[i].de && !d.ex[i].ru) {
                                          d.ex.splice(i, 1);   // пустую заготовку не держим
                                        }
                                      })}>Отмена</button>
                            </div>
                          </div>
                        ) : (
                          <>
                            <span className="wa-ex-de">{e.de}</span>
                            <span className="wa-ex-ru">{e.ru}</span>
                            {e.deleted ? <span className="wa-ex-mark m-gone">удалишь по «Готово»</span>
                              : e.added ? <span className="wa-ex-mark m-new">твой пример</span>
                              : e.edited ? <span className="wa-ex-mark m-edit">исправил ты</span> : null}
                            <div className="wa-ex-tools">
                              {e.deleted ? (
                                <button type="button" className="wa-tool wa-tool-back"
                                        onClick={() => правитьКарточку(it, (d) => {
                                          d.ex[i].deleted = false;
                                        })}>Вернуть</button>
                              ) : (
                                <>
                                  <button type="button" className="wa-tool"
                                          onClick={() => правитьКарточку(it, (d) => {
                                            d.ex[i].editing = true;
                                          })}>Править</button>
                                  <button type="button" className="wa-tool wa-tool-del"
                                          onClick={() => правитьКарточку(it, (d) => {
                                            d.ex[i].deleted = true; d.ex[i].editing = false;
                                          })}>Удалить</button>
                                </>
                              )}
                            </div>
                          </>
                        )}
                      </div>
                    </li>
                  ))}
                </ul>
                <button type="button" className="wa-tool wa-tool-wide"
                        onClick={() => правитьКарточку(it, (d) => d.ex.push({
                          de: '', ru: '', deleted: false, editing: true,
                          edited: false, added: true,
                        }))}>+ Добавить свой пример</button>

                <div className="wa-card-tally">
                  {естьПравки ? (
                    <>Останется примеров: <b>{живые.length}</b>.{' '}
                      {[переводДругой ? 'перевод исправлен' : '',
                        поправлено ? `исправлено примеров: ${поправлено}` : '',
                        дописано ? `добавлено: ${дописано}` : '',
                        убрано ? `удалим примеров: ${убрано}` : ''
                      ].filter(Boolean).join(' · ')}</>
                  ) : (
                    <span className="none">Пока ничего не тронул — карточка останется как есть.</span>
                  )}
                </div>

                {естьПравки && живые.length < 2 ? (
                  <label className="wa-opt">
                    <input type="checkbox" checked={!!разбор.topup}
                           onChange={() => правитьКарточку(it, (d) => { d.topup = !d.topup; })} />
                    <span>Остался один пример — пусть ночь допишет второй.
                      Твой останется как есть.</span>
                  </label>
                ) : null}
              </>
            ) : null}

            {!isPhrase && it.suggestion ? (
              <button type="button" aria-pressed={chosen === FIXED}
                      className={chosen === FIXED ? 'wa-suggest is-picked' : 'wa-suggest'}
                      onClick={() => pick(it.word, FIXED)}>
                {chosen === FIXED ? '✓ ' : ''}Да, это «{it.suggestion}»
              </button>
            ) : null}

            {/* «Записать мои правки» — главное действие панельной карточки. Оно
                означает: содержимое, которое ты оставил, и есть решение. Пересборка
                при этом НЕ запускается — незачем собирать заново то, что уже верно. */}
            {проКарточку ? (
              <div className="wa-actions">
                <button type="button" className="wa-act wa-act-apply wa-act-wide"
                        disabled={!естьПравки}
                        onClick={() => setState((p) => ({
                          ...p, [it.word]: p[it.word] === EDIT ? '' : EDIT,
                        }))}>
                  {chosen === EDIT ? '✓ ' : ''}Записать мои правки
                </button>
                <button type="button" className="wa-act wa-act-rebuild wa-act-wide"
                        onClick={() => pick(it.word, REBUILD)}>
                  {chosen === REBUILD ? '✓ ' : ''}Пересобрать всю карточку ночью
                </button>
              </div>
            ) : null}

            <div className="wa-actions">
              <button type="button" className="wa-act wa-act-keep"
                      onClick={() => pick(it.word, KEEP)}>
                {проКарточку ? 'Карточка нормальная' : 'Оставить как есть'}
              </button>
              {/* «Перевод не тот» у фразы нет намеренно: за словом стоит ночная
                  пересборка карточки, а за фразой — нет, и кнопка, которая ничего
                  не делает, хуже отсутствующей. */}
              {isPhrase ? null : (
              <button type="button" className="wa-act wa-act-retrans"
                      onClick={() => pick(it.word, RETRANS)}>Перевод не тот</button>
              )}
              {/* Отдельного «точно удалить?» нет намеренно: кнопка работает как все
                  остальные — нажал, отметилось, нажал ещё раз, снялось. Ничего не
                  происходит до «Готово», а внизу экрана всё это время видно число
                  «удалим N». Переспрашивать на каждом слове в списке из ста — хуже. */}
              <button type="button" className="wa-act wa-act-drop"
                      onClick={() => pick(it.word, DROP)}>
                {chosen === DROP ? '✓ ' : ''}{проКарточку ? 'Удалить карточку' : 'Удалить'}
              </button>
            </div>

            {editing[it.word] ? (
              <div className="wa-edit">
                <label>{isPhrase ? 'Фраза по-немецки' : 'Слово по-немецки'}
                  <input value={typed[it.word] ?? (it.suggestion || it.word)}
                         aria-label="правильное написание"
                         onChange={(e) => setTyped((p) => ({ ...p, [it.word]: e.target.value }))} />
                </label>
                <label>Перевод <span className="wa-opt">— если и он не тот</span>
                  <input value={typedTrans[it.word] ?? ''}
                         aria-label="перевод"
                         placeholder={it.translation || 'как переводится'}
                         onChange={(e) => setTypedTrans((p) => ({ ...p, [it.word]: e.target.value }))} />
                </label>
                <button type="button" onClick={() => saveManual(it.word)}>Сохранить</button>
              </div>
            ) : (
              <button type="button" className="wa-manual"
                      onClick={() => setEditing((p) => ({ ...p, [it.word]: true }))}>
                свой вариант — вписать {isPhrase ? 'фразу' : 'слово'} и перевод
              </button>
            )}
          </div>
        );
      })}

      {error ? <div className="wa-error">{error}</div> : null}
      <p className="wa-tail">Проверка приходит два раза в неделю. Подтверждённые слова больше не спросим,
        а те, что ты не тронул, придут снова.</p>

      <div className="wa-bar">
        <div className="wa-bar-inner">
          <p className="wa-bar-note">
            {drop === 0
              ? <>Ничего не удалим — оставим <b>{keep}</b>{retrans ? <> · переделаем <b>{retrans}</b></> : null}</>
              : <>Оставим <b>{keep}</b> · переделаем <b>{retrans}</b> · удалим <b>{drop}</b> — по твоей кнопке</>}
          </p>
          <button type="button" className="wa-done" disabled={busy} onClick={submit}>
            {/* Не «Сохраняю…»: сохранение идёт под капотом, а здесь мы только
                отправляем решения — это доли секунды. Обещать длительность,
                которой нет, значит снова посадить человека ждать. */}
            {busy ? 'Отправляю…' : 'Готово'}
          </button>
        </div>
      </div>
    </div>
  );
}

function plural(n, one, few, many) {
  const mod10 = n % 10;
  const mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return few;
  return many;
}
