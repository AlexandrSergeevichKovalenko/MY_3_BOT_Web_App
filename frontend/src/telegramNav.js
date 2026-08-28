// Переход в ЧАТ С БОТОМ из мини-аппа. Одна реализация на весь проект.
//
// ⛔ ЗАЧЕМ ЭТОТ ФАЙЛ СУЩЕСТВУЕТ.
// Замер на живом телефоне владельца 28.08.2026 (видео): кнопка «Открыть чат с ботом»
// не делала ВИДИМО ничего. Причина не в ссылке — вызов openTelegramLink отрабатывал
// честно, но мини-апп оставался НА ЭКРАНЕ, поверх открытого чата. Со стороны это
// неотличимо от мёртвой кнопки.
//
// В проекте таких кнопок нашлось ЧЕТЫРЕ, и все были написаны одинаково — открыть
// ссылку и ничего больше:
//   · экран очереди и экран «доступ закрыт» (main.jsx);
//   · «Запросить доступ к боту» для гостя, пришедшего по ссылке «Поделиться»
//     (dictionary/DeepAnalysis.jsx);
//   · «Открыть бота» в игре, открытой из группы (answer/AnswerOverlay.jsx);
//   · «Открыть бота» в словаре, когда бот заблокирован (dictionary/DictionaryOverlay.jsx).
//
// ЗАКРЫТИЕ МИНИ-АППА И ЕСТЬ ПЕРЕХОД В ЧАТ для того, кто пришёл из чата: чат лежит
// прямо под приложением. А вызов openTelegramLink нужен второму случаю — человеку,
// который попал в приложение по прямой ссылке и чата с ботом не имеет: у него ссылка
// этот чат создаёт. Поэтому делаются ОБА действия, в этом порядке.
//
// ⚠ ЭТО НЕ ГОДИТСЯ ДЛЯ ССЫЛОК `t.me/share/url` (окно «поделиться») И ДЛЯ `?startapp=`
// (повторное открытие мини-аппа на другом экране): оттуда человек должен ВЕРНУТЬСЯ в
// приложение, и закрывать его нельзя. Такие вызовы остаются как были.

const BOT_USERNAME_FALLBACK = 'Ich_Deutsch_bot';

function telegramApp() {
  try {
    return (typeof window !== 'undefined' && window.Telegram && window.Telegram.WebApp) || null;
  } catch (_e) {
    return null;
  }
}

/**
 * Открыть чат с ботом и уйти туда из мини-аппа.
 * @param {string} botUsername имя бота (можно с «@»)
 * @param {string} [startPayload] значение для ?start=… — что бот увидит при открытии
 */
export function openBotChat(botUsername, startPayload) {
  const uname = String(botUsername || '').replace(/^@/, '').trim() || BOT_USERNAME_FALLBACK;
  const payload = String(startPayload || '').trim();
  const httpsUrl = `https://t.me/${uname}${payload ? `?start=${encodeURIComponent(payload)}` : ''}`;
  const tg = telegramApp();

  if (tg && typeof tg.openTelegramLink === 'function') {
    try { tg.openTelegramLink(httpsUrl); } catch (_e) { /* закрытие ниже всё равно уводит в чат */ }
    // Небольшая пауза — чтобы Telegram успел принять ссылку до того, как мы закроемся.
    setTimeout(() => {
      try { if (typeof tg.close === 'function') tg.close(); } catch (_e) { /* ignore */ }
    }, 150);
    return;
  }

  // Вне Telegram (браузер или установленное на экран приложение) — обычный переход,
  // там он работает и никакого «закрыть» не требуется.
  try { window.open(httpsUrl, '_blank'); return; } catch (_e) { /* последний способ ниже */ }
  try { window.location.href = httpsUrl; } catch (_e) { /* ignore */ }
}
