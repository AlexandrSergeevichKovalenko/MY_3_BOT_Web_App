import React, { useEffect, useRef, useState } from 'react';

/**
 * Всплывающая плашка на 5 секунд — общая для всех интерактивов.
 *
 * Раньше сообщение о неудачном сохранении вставлялось В КАРТОЧКУ обычным блоком: оно
 * раздвигало содержимое, отъедало и без того дефицитную высоту экрана и висело до конца
 * сессии. Плашка живёт над содержимым (position: fixed), места в раскладке не занимает и
 * гаснет сама.
 *
 * Использование: const toast = useToast(); toast.show({ text, kind }); …
 * и один <Toast state={toast.state} /> где угодно в дереве игры.
 */

const LIFETIME_MS = 5000;

export function useToast() {
  const [state, setState] = useState(null);   // null | { text, hint, kind, id }
  const timer = useRef(null);
  const idRef = useRef(0);

  useEffect(() => () => { if (timer.current) clearTimeout(timer.current); }, []);

  const hide = () => {
    if (timer.current) { clearTimeout(timer.current); timer.current = null; }
    setState(null);
  };
  const show = (payload) => {
    const next = typeof payload === 'string' ? { text: payload } : (payload || {});
    if (!next.text) return;
    idRef.current += 1;
    if (timer.current) clearTimeout(timer.current);
    setState({ kind: 'bad', hint: '', ...next, id: idRef.current });
    timer.current = setTimeout(() => { timer.current = null; setState(null); }, LIFETIME_MS);
  };

  return { state, show, hide };
}

export default function Toast({ state, onClose }) {
  if (!state) return null;
  return (
    <div className={`ans-toast ${state.kind || 'bad'}`} role="status" key={state.id}
         onClick={onClose}>
      <div className="ans-toast-text">{state.text}</div>
      {state.hint ? <div className="ans-toast-hint">{state.hint}</div> : null}
    </div>
  );
}
