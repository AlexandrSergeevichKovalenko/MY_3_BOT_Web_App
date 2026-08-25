import React, { useEffect, useState } from 'react';

/**
 * Теория по теме тренажёра — экран «🎬 Посмотреть видео».
 *
 * Владелец, 25.08.2026: «под этой кнопкой нужно открываться страничка, на которой будет
 * три видео… если хотите освежить память либо просто начать — рекомендуем пройти вот эти
 * видео. И эти видео должны открываться так, как у нас они открываются в Wo-Fragen».
 *
 * «Как в Wo-Fragen» — это карточка «тебе было сложно» (AufgabeGame.jsx → AufgabeVideo):
 * список превью, тап по превью → ролик играет прямо в мини-аппе, из приложения не
 * выходим. Поэтому здесь те же классы `au-video-*` из answer.css — один вид у обоих
 * экранов, а не вторая похожая вёрстка, которая завтра разъедется с первой.
 *
 * Ролики приходят с сервера (/api/answer/topic/videos) и берутся ТОЛЬКО из отобранных
 * человеком (/addvideo). Экран ничего не досыпает «чтобы было три»: сколько отобрано —
 * столько и показано.
 */
export default function TopicVideoScreen({ topic, api, haptic, onClose }) {
  const [state, setState] = useState({ loading: true, error: '', data: null });
  const [active, setActive] = useState(null);   // video_id, который сейчас играет

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await api('/api/answer/topic/videos', { topic });
        if (!cancelled) setState({ loading: false, error: '', data });
      } catch (e) {
        try { console.warn('[topic-videos] load failed', e); } catch (_err) { /* noop */ }
        // Человеку — человеческий текст; техчасть ушла в консоль.
        if (!cancelled) setState({ loading: false, error: 'Не удалось загрузить видео. Попробуйте позже.', data: null });
      }
    })();
    return () => { cancelled = true; };
  }, [topic, api]);

  const videos = state.data?.videos || [];
  const title = state.data?.topic_ru || '';

  const open = (id) => { try { haptic?.('tap'); } catch (_e) { /* noop */ } setActive(id); };

  return (
    <div className="ans-root ans-root--keepkbd">
      <div className="ans-card">
        <div className="ans-head">
          <span className="ans-eyebrow">🎬 Theorie</span>
          {title ? <h2 className="ans-title">{title}</h2> : null}
        </div>

        {state.loading ? <p className="au-hint" style={{ textAlign: 'center' }}>Загружаем…</p> : null}
        {state.error ? <p className="ans-sub">{state.error}</p> : null}

        {!state.loading && !state.error ? (
          <>
            <div className="au-video-intro">
              <p className="au-hint" style={{ textAlign: 'center' }}>
                Хотите освежить память — или только начинаете тему?
              </p>
              <p className="au-hint" style={{ textAlign: 'center', marginTop: 6 }}>
                {videos.length
                  ? 'Рекомендуем сначала посмотреть эти видео 👇'
                  : 'Видео по этой теме ещё не подобраны — мы уже этим занимаемся.'}
              </p>
            </div>

            <div className="au-video-list">
              {videos.map((v) => {
                const id = String(v.video_id || '');
                if (!id) return null;
                if (active === id) {
                  return (
                    <div className="au-video-frame" key={id}>
                      <iframe
                        title={v.video_title || 'YouTube'}
                        src={`https://www.youtube.com/embed/${id}?rel=0&modestbranding=1&playsinline=1&autoplay=1`}
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                        allowFullScreen
                      />
                      {v.video_title ? <div className="au-video-caption">{v.video_title}</div> : null}
                    </div>
                  );
                }
                return (
                  <button type="button" className="au-video-pick" key={id} onClick={() => open(id)}>
                    <span className="au-video-thumb-wrap">
                      <img
                        className="au-video-thumb" loading="lazy" alt=""
                        src={`https://i.ytimg.com/vi/${id}/hqdefault.jpg`}
                      />
                      <span className="au-video-play">▶</span>
                    </span>
                    {v.video_title ? <span className="au-video-pick-title">{v.video_title}</span> : null}
                  </button>
                );
              })}
            </div>
          </>
        ) : null}

        <button className="ans-btn-ghost" onClick={onClose}>Закрыть</button>
      </div>
    </div>
  );
}
