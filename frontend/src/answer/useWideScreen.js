import { useEffect, useState } from 'react';

// Планшет/широкий экран — тот же порог, что и у планшетной раскладки в answer.css
// (`@media (min-width:700px) and (min-height:560px)`). Держим порог в ОДНОМ месте: если
// он разъедется с CSS, игра начнёт считать себя телефоном там, где вёрстка уже планшетная.
export const WIDE_QUERY = '(min-width: 700px) and (min-height: 560px)';

// Нужен там, где решение зависит не от вёрстки, а от кода: например, потолок кегля для
// подгонки текста (на телефоне он фиксированный и проверенный, на планшете его задаёт CSS).
export default function useWideScreen() {
  const [wide, setWide] = useState(() => {
    try { return window.matchMedia(WIDE_QUERY).matches; } catch (_e) { return false; }
  });
  useEffect(() => {
    let mq;
    try { mq = window.matchMedia(WIDE_QUERY); } catch (_e) { return undefined; }
    const on = () => setWide(mq.matches);
    on();
    mq.addEventListener?.('change', on);
    return () => mq.removeEventListener?.('change', on);
  }, []);
  return wide;
}
