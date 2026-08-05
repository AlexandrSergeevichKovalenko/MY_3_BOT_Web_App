import { useEffect, useRef, useState } from 'react';

/**
 * «Моё» — личные заметки человека к слову.
 *
 * Живут отдельно от общего разбора: разбор один на всех и постоянно улучшается, а
 * заметка принадлежит только этому человеку, и никакое обновление слова её не тронет.
 *
 * Название необязательное: не написал — показываем просто текст. На повторении блок
 * работает в режиме чтения и стоит на раскрытой стороне: заметку пишут ровно затем,
 * чтобы увидеть её в этот момент, а не искать специально.
 */
export default function CardOwnNotes({
  notes = [],
  limit = 5,
  readOnly = false,
  tr = (ru) => ru,
  onSave,
}) {
  const [draft, setDraft] = useState(notes);
  const [addedAt, setAddedAt] = useState(-1);
  const textRefs = useRef([]);

  // Список мог измениться снаружи (сохранение прошло, карточка перечиталась).
  // Пока человек печатает, ничего не подменяем — иначе текст прыгает под пальцами.
  useEffect(() => {
    if (addedAt < 0) setDraft(notes);
  }, [notes, addedAt]);

  useEffect(() => {
    if (addedAt < 0) return;
    const field = textRefs.current[addedAt];
    if (field) {
      try { field.focus(); } catch (_error) { /* ignore */ }
    }
  }, [addedAt]);

  if (readOnly && !draft.length) return null;

  const commit = (next) => {
    setDraft(next);
    onSave?.(next.filter((note) => String(note.text || '').trim()));
  };

  const patch = (index, field, value) => {
    setDraft((prev) => prev.map((note, i) => (i === index ? { ...note, [field]: value } : note)));
  };

  return (
    <div className={`own-notes ${readOnly ? 'is-readonly' : ''}`}>
      <div className="own-notes-head">
        <span>{tr('Моё', 'Meins')}</span>
        {!readOnly && <span className="own-notes-count">{draft.length} {tr('из', 'von')} {limit}</span>}
      </div>

      {draft.length === 0 && !readOnly && (
        <p className="own-notes-empty">
          {tr('Ассоциация, своя ошибка, случай с работы — что поможет вспомнить.',
              'Eine Eselsbrücke, dein Fehler, ein Fall aus der Arbeit — was beim Erinnern hilft.')}
        </p>
      )}

      {draft.map((note, index) => (
        <div className="own-note" key={`own-note-${index}`}>
          <div className="own-note-body">
            {readOnly ? (
              <>
                {note.label && <div className="own-note-label">{note.label}</div>}
                <div className="own-note-text">{note.text}</div>
              </>
            ) : (
              <>
                <input
                  className="own-note-label-input"
                  type="text"
                  value={note.label || ''}
                  maxLength={24}
                  placeholder={tr('Название — необязательно', 'Titel — optional')}
                  onChange={(e) => patch(index, 'label', e.target.value)}
                  onBlur={() => commit(draft)}
                />
                <textarea
                  ref={(el) => { textRefs.current[index] = el; }}
                  className="own-note-text-input"
                  rows={2}
                  value={note.text || ''}
                  maxLength={300}
                  placeholder={tr('Что записать?', 'Was notieren?')}
                  onChange={(e) => patch(index, 'text', e.target.value)}
                  onBlur={() => { setAddedAt(-1); commit(draft); }}
                />
              </>
            )}
          </div>
          {!readOnly && (
            <button
              type="button"
              className="own-note-del"
              onClick={() => { setAddedAt(-1); commit(draft.filter((_, i) => i !== index)); }}
              aria-label={tr('Убрать заметку', 'Notiz entfernen')}
            >
              ×
            </button>
          )}
        </div>
      ))}

      {!readOnly && (
        <button
          type="button"
          className="own-note-add"
          disabled={draft.length >= limit}
          onClick={() => {
            setDraft((prev) => [...prev, { label: '', text: '' }]);
            setAddedAt(draft.length);
          }}
        >
          {draft.length >= limit
            ? tr('Больше пяти не нужно', 'Mehr als fünf braucht es nicht')
            : `＋ ${tr('Добавить своё', 'Eigenes hinzufügen')}`}
        </button>
      )}
    </div>
  );
}
