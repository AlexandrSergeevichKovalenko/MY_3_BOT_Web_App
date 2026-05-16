# Интеграция Reader Redesign — Фаза 1 (CSS-скин)

## Что это

Drop-in CSS-файл, который перерисовывает секцию «Читалка» в стиле, согласованном с
домашним экраном (Inter + изумрудный акцент, прозрачные стеклянные панели, серифный
шрифт Newsreader для книжного текста).

**Важно:** этот файл ничего не меняет в JSX, в state, в обработчиках, в API.
Все функции читалки сохраняются как есть:

- Библиотека: поиск, архив, refresh, добавление по URL/тексту/файлу
- Карточки книг: обложка, прогресс-стрипа, ссылки «открыть/переименовать/архив/удалить»
- Чтение: top-bar (immersive + collapsed peek), таймер чтения, переключение
  vertical/horizontal, кнопка настроек, кнопка свернуть/развернуть
- Article: рендер по страницам, structured-text (sentence/word selection),
  drag-selection для фраз, обработчики swipe + touch + wheel, idle-timeout
- Закладка на странице (уголок + индикатор), переход к закладке из scrubber
- Scrubber: page-jump dialog, прогресс-бар, переход к закладке
- Оглавление (TOC drawer)
- Настройки чтения: размер шрифта, жирность, чувствительность свайпа, кнопка «Оригинал»
- Аудио-документ: оффлайн-аудио, плеер, скачивание

## Установка

### Шаг 1 — Скопировать файл

```
integration/reader-redesign.css  →  frontend/src/components/reader-redesign.css
```

### Шаг 2 — Импортировать ПОСЛЕ App.css

В `frontend/src/App.jsx` найдите место, где импортируется `App.css`,
и добавьте сразу после него:

```js
import './App.css';
import './components/reader-redesign.css';   // ← новая строка
```

> Альтернатива: добавить `@import url('./components/reader-redesign.css');` в самый
> конец `App.css`. Так не нужно править `App.jsx`.

### Шаг 3 — Проверить визуально

Перейдите в раздел **Читалка**:

- Хедер «Моя библиотека» с акцентной кнопкой «+ Добавить» (изумрудный градиент)
- Поиск + чек-бокс «Архив» — компактная строка
- Сетка обложек: карточки без рамки, обложка с фактурой и легендной тенью,
  под обложкой — название серифом, мета (прогресс + источник), снизу — мини-ряд
  действий (Открыть выделен изумрудом, Удалить — приглушённый красный)
- Откройте книгу: top-bar тонкий, frosted, серифный заголовок, бэйдж таймера
- Текст книги: Newsreader 18px, line-height 1.7, центрированная номерация
  страниц, тонкая жёлтая закладка-уголок при `is-current-page-bookmarked`
- Внизу: scrubber-bar с прозрачным фоном, изумрудный thumb
- TOC и Settings sheet — выдвигаются снизу с drag-handle, скруглённые углы,
  тёмно-стеклянный фон

### Шаг 4 (опционально) — Удалить старые reader-* правила

После того как новый дизайн зайдёт, можно подчистить `App.css`. В файле есть
ДВА исторических блока reader-стилей:

- старый, ~строки 4322–4980
- v2, ~строки 15010–15568 (комментарий `Reader v2: Dark Minimal Premium Study Mode`)

Их можно удалить — `reader-redesign.css` полностью покрывает все селекторы.
Но это **необязательно** для работы: новый файл переопределяет старые правила
за счёт более специфичных селекторов (`.webapp-reader .selector`).

## Что НЕ покрывается этой фазой

| Аспект | Причина |
|---|---|
| Hero-карточка «Продолжаешь читать» в библиотеке | Её нет в текущем DOM. Нужен JSX-патч (Фаза 2). |
| Двойной разворот для tablet/desktop | Требует другой DOM-структуры страницы. |
| Аудио-чтение с подсветкой текущего предложения | Новая фича, нужен бэкенд (TTS-таймкоды) + JSX. |
| Заметки к выделенным фразам | Существует только концепция; в текущем коде нет state. |
| Цветные закладки + список закладок | Сейчас закладка одна на книгу; нужен расширенный data-model. |

Все эти возможности заложены в макет (Reader.html, экраны 05–07). Они требуют
**Фазы 2 — JSX-рефактор** (см. ниже).

## Фаза 2 — JSX-патчи

Эти изменения требуют правки `App.jsx`. Каждый патч независим — можно
применять по одному и проверять.

### Патч 2.1 — Hero-карточка «Продолжаешь читать»

**Что это:** на входе в Читалку — большой акцентный блок с обложкой текущей
книги, прогрессом и кнопкой «Продолжить». CSS для него уже включён в
`reader-redesign.css` (раздел *HERO CARD*).

**Куда вставить:** в `App.jsx`, внутри блока `showLibraryMode`, после
закрытия `</div>` существующей формы добавления (`reader-add-form-wrap`) и
ПЕРЕД `<section className="reader-library">`.

Ориентир — около строки **29460** (после блока `{readerAddOpen && (...)}`):

```jsx
{readerAddOpen && (
  <div className="reader-add-form-wrap">
    {/* ... existing form ... */}
  </div>
)}

{/* ↓↓↓ ВСТАВИТЬ ЭТОТ БЛОК ↓↓↓ */}
{!readerArchiveOpen && (() => {
  // Книга для продолжения: последняя открытая (если есть в списке),
  // иначе книга с наибольшим прогрессом среди недочитанных.
  let candidate = null;
  if (readerDocumentId) {
    candidate = readerDocuments.find(
      (d) => Number(d?.id) === Number(readerDocumentId) && !d?.is_archived
    );
  }
  if (!candidate) {
    const inProgress = readerDocuments
      .filter((d) =>
        !d?.is_archived &&
        Number(d?.progress_percent || 0) > 0 &&
        Number(d?.progress_percent || 0) < 100
      )
      .sort((a, b) => Number(b?.progress_percent || 0) - Number(a?.progress_percent || 0));
    candidate = inProgress[0] || null;
  }
  if (!candidate) return null;

  const progress = Math.max(0, Math.min(100, Number(candidate?.progress_percent || 0)));
  const coverUrl = getReaderCoverUrl(candidate);
  const initials = getReaderCoverInitials(candidate?.title);
  const gradient = getReaderCoverGradient(candidate);
  const meta = buildReaderArchiveMeta(candidate);
  const isOpening = Number(readerOpeningDocumentId) === Number(candidate.id);

  return (
    <div
      className="reader-hero-card"
      onClick={() => openReaderDocument(candidate.id)}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && openReaderDocument(candidate.id)}
    >
      <div
        className="reader-hero-cover"
        style={{ background: `linear-gradient(150deg, ${gradient[0]} 0%, ${gradient[1]} 100%)` }}
      >
        {coverUrl
          ? <img src={coverUrl} alt="" loading="lazy" />
          : <span className="reader-hero-cover-initials">{initials}</span>}
      </div>
      <div className="reader-hero-body">
        <div className="reader-hero-kicker">
          <span className="reader-hero-dot" aria-hidden="true" />
          {tr('Продолжаешь читать', 'Du liest gerade')}
        </div>
        <div className="reader-hero-title">{candidate.title || tr('Без названия', 'Ohne Titel')}</div>
        <div className="reader-hero-meta">
          <span>{meta || ''}</span>
          <span className="reader-hero-pct">{Math.round(progress)}%</span>
        </div>
        <div className="reader-hero-progress">
          <div className="reader-hero-progress-fill" style={{ width: `${progress}%` }} />
        </div>
        <button
          type="button"
          className="reader-hero-continue"
          onClick={(e) => { e.stopPropagation(); openReaderDocument(candidate.id); }}
          disabled={isOpening}
        >
          {isOpening
            ? tr('Открываем…', 'Oeffnen…')
            : `▶  ${tr('Продолжить', 'Weiterlesen')}`}
        </button>
      </div>
    </div>
  );
})()}
{/* ↑↑↑ КОНЕЦ ВСТАВКИ ↑↑↑ */}

<section className="reader-library">
  {/* ... existing search/grid ... */}
</section>
```

**Поведение:**
- В архиве (`readerArchiveOpen === true`) карточка не показывается.
- Если ни одна книга не открывалась или нет книг с прогрессом 1–99% — карточка
  не рисуется (`return null`).
- Клик по карточке (или по кнопке «Продолжить») — вызывает
  `openReaderDocument(candidate.id)`, тот же handler, что у обычной карточки.
- На время `readerOpeningDocumentId === candidate.id` кнопка показывает
  «Открываем…» и блокируется.

**Что использует:** `readerDocuments`, `readerDocumentId`, `readerArchiveOpen`,
`readerOpeningDocumentId`, `openReaderDocument`, `getReaderCoverUrl`,
`getReaderCoverInitials`, `getReaderCoverGradient`, `buildReaderArchiveMeta`, `tr`
— всё уже существует в `App.jsx`.

**Откат:** удалить добавленный блок. Никаких других изменений не требуется.

---

### Патч 2.2 — Темы чтения (dark / sepia / cream)

**Что это:** в Settings sheet — сегментированный контрол на 3 темы. На корневом
`<section>` живёт атрибут `data-reader-theme`, и CSS меняет фон страницы, цвет
текста, кнопок верхней панели, scrubber'а. Библиотека и overlay'и остаются
тёмными для контраста.

Требует двух правок в `App.jsx`:

#### A) Добавить state + persist в localStorage

Около строки **4585** (рядом с другими reader-state'ами), добавьте:

```jsx
{/* ↓↓↓ ВСТАВИТЬ ↓↓↓ */}
const [readerColorTheme, setReaderColorTheme] = useState(() => {
  try {
    const v = localStorage.getItem('reader_color_theme');
    return (v === 'sepia' || v === 'cream') ? v : 'dark';
  } catch { return 'dark'; }
});
const applyReaderColorTheme = (next) => {
  setReaderColorTheme(next);
  try { localStorage.setItem('reader_color_theme', next); } catch {}
};
{/* ↑↑↑ КОНЕЦ ВСТАВКИ ↑↑↑ */}
```

#### B) Прокинуть атрибут на корневой `<section>` читалки

Около строки **29364** — там сейчас:

```jsx
<section className={`webapp-section webapp-reader ${readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : ''} ${readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : ''}`} ref={readerRef}>
```

Заменить на:

```jsx
<section
  className={`webapp-section webapp-reader ${readerHasContent && readerImmersive && !readerArchiveOpen ? 'is-immersive' : ''} ${readerHasContent && readerImmersive && !readerArchiveOpen && readerTopbarCollapsed ? 'is-topbar-collapsed' : ''}`}
  data-reader-theme={readerColorTheme}
  ref={readerRef}
>
```

#### C) Добавить контрол в Settings sheet

В `App.jsx` найдите `reader-settings-sheet` (около строки **30150**) и
перед существующей `<label className="webapp-field">` для «Размер шрифта»
вставьте новое поле:

```jsx
{/* ↓↓↓ ВСТАВИТЬ ↓↓↓ */}
<label className="webapp-field">
  <span>{tr('Тема страницы', 'Hintergrund')}</span>
  <div className="reader-theme-seg">
    {[
      { k: 'dark',  l: tr('Тёмная', 'Dunkel') },
      { k: 'sepia', l: tr('Сепия',  'Sepia')  },
      { k: 'cream', l: tr('Бумага', 'Papier') },
    ].map((opt) => (
      <button
        key={opt.k}
        type="button"
        className={`reader-theme-seg-btn ${readerColorTheme === opt.k ? 'is-active' : ''}`}
        onClick={() => applyReaderColorTheme(opt.k)}
      >
        <span className={`reader-theme-swatch reader-theme-swatch-${opt.k}`} aria-hidden="true">Aa</span>
        {opt.l}
      </button>
    ))}
  </div>
</label>
{/* ↑↑↑ КОНЕЦ ВСТАВКИ ↑↑↑ */}

<label className="webapp-field">
  <span>{tr('Размер шрифта', 'Schriftgroesse')}</span>
  {/* ... existing slider ... */}
</label>
```

**Поведение:**
- Тема применяется к открытой книге сразу (CSS-токены меняются с transition .3s)
- Не влияет на библиотеку (она всегда тёмная — там нет смысла менять)
- Не влияет на TOC drawer / Settings sheet / page-jump dialog (overlay'и тёмные)
- Сохраняется в `localStorage['reader_color_theme']` — переживает перезагрузку

**Откат:** удалить три вставленных блока. Также убрать атрибут `data-reader-theme`
с `<section>`.

---

### Патч 2.3 (зависит от бэкенда) — Список закладок и заметок

Текущая модель в БД: одна `bookmark_percent` на книгу, заметок нет вовсе.
Расширенный drawer «Главы / Закладки / Заметки» из макета (Reader.html,
экран 05) требует доработки:

- Таблица `reader_bookmarks` (book_id, page, color, snippet, created_at)
- Таблица `reader_notes` (book_id, page, highlight, note, created_at)
- API endpoints + миграции
- Расширение UI (контекстное меню при выделении → «Сохранить как заметку»)

Это — отдельная инженерная задача, не CSS/JSX-only. Когда будете готовы
к бэкенду, скажите — выдам спецификацию схемы и API.

---

### Патч 2.4 (зависит от бэкенда) — Аудио-чтение с подсветкой

Текущее API возвращает целый WAV без word-таймкодов. Для синхронной подсветки
нужны таймкоды слов/предложений от TTS. Когда переключитесь на TTS-движок
с word-level timing (Google TTS SSML marks, Azure Speech `WordBoundary`),
можно делать UI.

---

## Откат всего редизайна

Удалить одну строку импорта `reader-redesign.css` в `App.jsx`. JSX-патчи Фазы 2
тоже можно откатить вручную — каждый коммит очерчен комментариями `↓↓↓ ВСТАВИТЬ`
и `↑↑↑ КОНЕЦ ВСТАВКИ`.
