// Стенд для замера подгонки под экран: настоящий компонент игры, заглушённый API.
// Не входит в сборку прода (отдельная точка входа fitlab.html, в проде не собирается).
import React, { useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import './answer.css';
import TrainerGame from './TrainerGame.jsx';
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

function App() {
  useEffect(() => { installCardAutoFit(); }, []);
  const api = () => new Promise((r) => setTimeout(() => r(TASK), 30));
  return <TrainerGame id={1} api={api} haptic={() => {}} onClose={() => {}} />;
}

document.documentElement.setAttribute('data-scheme', new URLSearchParams(location.search).get('scheme') || 'light');
createRoot(document.getElementById('root')).render(<App />);
