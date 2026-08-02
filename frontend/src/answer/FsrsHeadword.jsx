import useFitText from './useFitText.js';

// Немецкое слово в карточке интервального повторения.
//
// Раньше длинное составное слово («das Fingerspitzengefühl») переносилось на
// вторую строку и вдобавок разрезалось посреди букв («…gefü|hl»). Теперь оно
// остаётся одной строкой, а под ширину карточки подгоняется КЕГЛЬ.
//
// Потолок размера берём из CSS (`max: 'css'`) — там он свой для телефона и для
// планшета, хук только уменьшает, если слово не влезло. Ниже `min` не опускаемся:
// если даже в 18px строка не помещается, браузер переносит по пробелу (артикль
// отдельной строкой), а слово всё равно остаётся целым.
export default function FsrsHeadword({ text }) {
  const fitRef = useFitText(text, { max: 'css', min: 18, padding: 2 });
  return (
    <div className="fsrs-card-target-fit">
      <div className="fsrs-card-target" ref={fitRef}>{text}</div>
    </div>
  );
}
