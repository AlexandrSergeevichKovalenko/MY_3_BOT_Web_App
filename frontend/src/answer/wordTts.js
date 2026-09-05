// Озвучка немецкого слова через существующий конвейер TTS (generate → poll → play).
// Вынесено из AnswerOverlay.jsx (05.09.2026), чтобы экран «Слова со вчерашних тренировок»
// (WordPickGame.jsx) звучал той же дорожкой, а не своей.

const tg = typeof window !== 'undefined' ? window.Telegram?.WebApp : null;

// Та же функция, что в AnswerOverlay.jsx / DeepDiveOverlay.jsx: каждый файл оверлея
// держит свою копию, чтобы не тянуть сюда тяжёлый словарный модуль и не замыкать
// импорт на AnswerOverlay.
function getInitData() {
  if (tg?.initData) return tg.initData;
  if (typeof window !== 'undefined') {
    return new URLSearchParams(window.location.search).get('initData') || '';
  }
  return '';
}

// Play a German word/phrase via the existing TTS pipeline (generate → poll → play).
export async function playWordTts(text) {
  const t = String(text || '').trim();
  if (!t) return;
  const initData = getInitData();
  await fetch('/api/webapp/tts/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ initData, text: t, language: 'de-DE' }),
  });
  const params = new URLSearchParams({ text: t, language: 'de-DE' });
  for (let i = 0; i < 30; i += 1) {
    const res = await fetch(`/api/webapp/tts/url?${params.toString()}`, {
      method: 'GET', headers: { 'X-Telegram-InitData': initData },
    });
    const data = await res.json().catch(() => ({}));
    if (data.status === 'ready' && data.audio_url) { await new Audio(data.audio_url).play(); return; }
    if (data.status === 'failed') throw new Error(data.message || 'TTS');
    await new Promise((r) => setTimeout(r, data.retry_after_ms || 700));
  }
  throw new Error('Zeitüberschreitung');
}
