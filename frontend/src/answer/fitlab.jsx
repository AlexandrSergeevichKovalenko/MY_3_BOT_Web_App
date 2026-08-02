// Стенд для замера подгонки под экран: настоящий компонент игры, заглушённый API.
// Не входит в сборку прода (отдельная точка входа fitlab.html, в проде не собирается).
import React, { useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import './answer.css';
import TrainerGame from './TrainerGame.jsx';
import ArtikelSprintGame from './ArtikelSprintGame.jsx';
import ArtikelLearnGame from './ArtikelLearnGame.jsx';
import WoFrageLearnGame from './WoFrageLearnGame.jsx';
import AdjektivLearnGame from './AdjektivLearnGame.jsx';
import ArtikelReviewGame from './ArtikelReviewGame.jsx';
import WoFrageReviewGame from './WoFrageReviewGame.jsx';
import installCardAutoFit from './fitCard.js';

const LONG = 'Weigerung bedeutet, sich zu weigern oder abzulehnen, also das Gegenteil von Zustimmung, und passt deshalb nicht als Synonym.';
const MID = 'Erwiderung ist eine Reaktion oder Antwort, aber diese kann auch ein Widerspruch oder eine neutrale Stellungnahme sein, keine zwingende Zustimmung.';
const SHORT = 'Beschreibt eine ablehnende Haltung.';

const D = (de, ru, why) => ({ de, ru_gloss: ru, why_not: why, example_de: `„Ein Satz mit ${de} als Beispiel."` });

const TASK = {
  wort: (typeof window !== 'undefined' && window.__WORT) || 'die Zustimmung',
  hint_ru: 'согласие',
  relation: 'synonym',
  target_example: { de: '„Er gab seine Zustimmung zu dem Projekt."' },
  correct: [
    { de: 'die Erlaubnis', ru: 'разрешение' },
    { de: 'das Zugeständnis', ru: 'уступка' },
    { de: 'die Einwilligung', ru: 'согласие' },
    { de: 'die Genehmigung', ru: 'разрешение' },
    { de: 'das Einverständnis', ru: 'согласие' },
    { de: 'die Billigung', ru: 'одобрение' },
    { de: 'die Bestätigung', ru: 'подтверждение' },
    { de: 'die Akzeptanz', ru: 'принятие' },
  ],
  correct_examples: [
    { word: 'die Erlaubnis', nuance: 'Формальное разрешение сверху.', sentence_de: '„Er gab seine Erlaubnis zu dem Projekt."', sentence_ru: '«Он дал разрешение на проект.»' },
    { word: 'das Zugeständnis', nuance: 'Уступка после спора.', sentence_de: '„Er machte ein Zugeständnis zu dem Projekt."', sentence_ru: '«Он пошёл на уступку по проекту.»' },
    { word: 'die Einwilligung', nuance: 'Юридически значимое согласие.', sentence_de: '„Er gab seine Einwilligung zu dem Projekt."', sentence_ru: '«Он дал согласие на проект.»' },
    { word: 'die Genehmigung', nuance: 'Официальное утверждение.', sentence_de: '„Er erteilte die Genehmigung zu dem Projekt."', sentence_ru: '«Он выдал разрешение на проект.»' },
    { word: 'das Einverständnis', nuance: 'Взаимное согласие сторон.', sentence_de: '„Er gab sein Einverständnis zu dem Projekt."', sentence_ru: '«Он дал согласие на проект.»' },
    { word: 'die Billigung', nuance: 'Одобрение, поддержка.', sentence_de: '„Er fand Billigung für das Projekt."', sentence_ru: '«Проект нашёл одобрение.»' },
    { word: 'die Bestätigung', nuance: 'Подтверждение уже решённого.', sentence_de: '„Er gab die Bestätigung zu dem Projekt."', sentence_ru: '«Он подтвердил проект.»' },
    { word: 'die Akzeptanz', nuance: 'Принятие как факта.', sentence_de: '„Das Projekt fand breite Akzeptanz."', sentence_ru: '«Проект был широко принят.»' },
  ],
  // разной длины разборы: короткий / средний / длинный — как в бою
  distractors: [
    D('die Weigerung', 'отказ, сопротивление', LONG),
    D('die Erwiderung', 'ответ, возражение, реакция', MID),
    D('die Ablehnung', 'отклонение', SHORT),
    D('der Widerspruch', 'противоречие', MID),
    D('das Wohlwollen', 'доброжелательность', LONG),
    D('die Verweigerung', 'отказ', SHORT),
    D('der Einwand', 'возражение', MID),
    D('der Protest', 'протест', SHORT),
  ],
};

// Спринт артиклей — вторая семья экранов: короткое слово и три крупные кнопки. Именно она
// сильнее всего страдала от раздувания, поэтому меряем её отдельно от «синонимов».
// Слова разной длины, включая заведомо длинное — на нём и проверяется, не режется ли оно.
const ARTIKEL_WORDS = [
  { w: 'Ladegerät', a: 'das' },
  { w: 'Geschwindigkeitsbegrenzung', a: 'die' },
  { w: 'Sanktion', a: 'die', tg: false },
  { w: 'Auto', a: 'das' },
  { w: 'Führerschein', a: 'der' },
];
const ARTIKEL_SET = {
  ok: true, set_id: 1, theme_label: 'Computer & Geräte', duration_s: 120, words: ARTIKEL_WORDS,
};

// Карточка «выбери артикль» — самый частый экран продукта и тот, что на скриншотах.
// Длинное слово стоит первым: на нём видно, режется оно или нет.
const LEARN_DECK = {
  ok: true, theme_label: 'Computer & Geräte', streak: 3, has_more: false,
  cards: [
    { w: 'Ladegerät', a: 'das', ru: 'зарядное устройство', tip: '-gerät → das.' },
    { w: 'Geschwindigkeitsbegrenzung', a: 'die', ru: 'ограничение скорости', tip: '-ung → die.' },
    { w: 'Sanktion', a: 'die', ru: 'санкция', tip: '-tion → die.' },
  ],
};

const WO_DECK = {
  ok: true,
  items: [
    { s: '___ bemühst du dich?', a: 'Worum', opts: ['Wovor', 'Um wen', 'Worum', 'Wogegen'],
      clue: '— Jonas.', frage_ru: 'О чём ты стараешься?', lemma: 'sich bemühen um', verb_ru: 'стараться',
      erklaerung: 'Предлог um + вещь → wo(r)+um = worum.', tip: 'Про человека было бы «um wen».' },
    { s: '___ freust du dich?', a: 'Worauf', opts: ['Worauf', 'Auf wen', 'Wovon', 'Wobei'],
      clue: '— Auf den Urlaub.', frage_ru: 'Чего ты ждёшь с радостью?', lemma: 'sich freuen auf', verb_ru: 'радоваться',
      erklaerung: 'Предлог auf + вещь → worauf.', tip: 'Про человека — «auf wen».' },
  ],
};

// Окончания прилагательных — экран со скриншотов пользователя. Две карточки НАРОЧНО
// разной длины разбора: с примером (длинный) и без (короткий). Именно на этой паре видно,
// занимает карточка экран целиком или висит полоской посреди пустоты.
const ADJ_DECK = {
  ok: true,
  items: [
    { before: 'stark', after: ' Muskelkater', adj: 'stark', noun: 'Muskelkater',
      a: 'er', full: 'starker Muskelkater', ru: 'Именительный · муж. род · сильное',
      erklaerung: 'Без артикля, муж. род, Именительный → окончание -er.',
      tip: 'Без артикля прилагательное «надевает шляпу артикля» → копирует -er (как der).',
      example: 'neuer Wein, kleiner Tag' },
    { before: 'der verantwortlich', after: ' Erbe', adj: 'verantwortlich', noun: 'Erbe',
      a: 'e', full: 'der verantwortliche Erbe', ru: 'Именительный · муж. род · слабое',
      erklaerung: 'После der/die/das, муж. род, Именительный → окончание -e.',
      tip: 'der/die/das уже показал род и падеж →' },
  ],
};

const ART_REVIEW = {
  ok: true, remaining: 5,
  cards: [
    { id: 1, w: 'Ladegerät', a: 'das', ru: 'зарядное устройство', tip: '-gerät → das.' },
    { id: 2, w: 'Sanktion', a: 'die', ru: 'санкция', tip: '-tion → die.' },
  ],
};

const WO_REVIEW = {
  ok: true, remaining: 5,
  cards: WO_DECK.items.map((it, n) => ({ id: n + 1, ...it })),
};

const stub = (data) => () => new Promise((r) => setTimeout(() => r(data), 30));

function App() {
  useEffect(() => { installCardAutoFit(); }, []);
  const game = new URLSearchParams(location.search).get('game') || 'trainer';
  if (game === 'sprint') return <ArtikelSprintGame api={stub(ARTIKEL_SET)} haptic={() => {}} onClose={() => {}} />;
  if (game === 'artikel') return <ArtikelLearnGame api={stub(LEARN_DECK)} haptic={() => {}} onClose={() => {}} />;
  if (game === 'wofrage') return <WoFrageLearnGame api={stub(WO_DECK)} haptic={() => {}} onClose={() => {}} />;
  if (game === 'adjektiv') return <AdjektivLearnGame api={stub(ADJ_DECK)} haptic={() => {}} onClose={() => {}} />;
  if (game === 'artreview') return <ArtikelReviewGame api={stub(ART_REVIEW)} haptic={() => {}} onClose={() => {}} />;
  if (game === 'woreview') return <WoFrageReviewGame api={stub(WO_REVIEW)} haptic={() => {}} onClose={() => {}} />;
  return <TrainerGame id={1} api={stub(TASK)} haptic={() => {}} onClose={() => {}} />;
}

document.documentElement.setAttribute('data-scheme', new URLSearchParams(location.search).get('scheme') || 'light');
createRoot(document.getElementById('root')).render(<App />);
