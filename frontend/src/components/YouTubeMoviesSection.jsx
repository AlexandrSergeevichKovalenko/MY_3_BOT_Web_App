import React from 'react';
import { useYouTubeMoviesContext } from '../providers/YouTubeMoviesProvider.jsx';

export function YouTubeMoviesSection({
  moviesRef,
  heroStickerSrc,
  tr,
}) {
  if (!moviesRef || typeof moviesRef !== 'object') {
    throw new Error('YouTubeMoviesSection requires moviesRef');
  }
  if (typeof tr !== 'function') {
    throw new Error('YouTubeMoviesSection requires tr');
  }

  const {
    movies,
    moviesLoading,
    moviesError,
    moviesCollapsed,
    moviesLanguageFilter,
    movieLanguageOptions,
    moviesFiltered,
    setMoviesLanguageFilter,
    selectMovie,
  } = useYouTubeMoviesContext();

  if (moviesCollapsed) return null;

  return (
    <section className="webapp-movies" ref={moviesRef}>
      <div className="webapp-section-title webapp-section-title-with-logo">
        <h2>{tr('Фильмы', 'Filme')}</h2>
        <p>{tr('Видео с доступными субтитрами, сохранённые в каталоге.', 'Videos mit verfuegbaren Untertiteln im Katalog.')}</p>
        <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
      </div>
      {moviesLoading && <div className="webapp-muted">{tr('Загружаем каталог...', 'Katalog wird geladen...')}</div>}
      {moviesError && <div className="webapp-error">{moviesError}</div>}
      {!moviesLoading && !moviesError && movies.length === 0 && (
        <div className="webapp-muted">{tr('Пока нет сохранённых видео.', 'Noch keine gespeicherten Videos.')}</div>
      )}
      {!moviesLoading && movieLanguageOptions.length > 0 && (
        <div className="movies-language-filter">
          <button
            type="button"
            className={`movies-filter-chip ${moviesLanguageFilter === 'all' ? 'is-active' : ''}`}
            onClick={() => setMoviesLanguageFilter('all')}
          >
            {tr('Все', 'Alle')}
          </button>
          {movieLanguageOptions.map((code) => (
            <button
              type="button"
              key={code}
              className={`movies-filter-chip ${moviesLanguageFilter === code ? 'is-active' : ''}`}
              onClick={() => setMoviesLanguageFilter(code)}
            >
              {code.toUpperCase()}
            </button>
          ))}
        </div>
      )}
      {!moviesLoading && moviesFiltered.length > 0 && (
        <div className="movies-grid">
          {moviesFiltered.map((item) => (
            <button
              type="button"
              key={item.video_id}
              className="movie-card"
              onClick={() => selectMovie(item)}
            >
              <div className="movie-thumb">
                <img src={item.thumbnail} alt={item.title} loading="lazy" />
              </div>
              <div className="movie-meta">
                <div className="movie-title">{item.title}</div>
                <div className="movie-subtitle">
                  {item.author ? `${item.author} • ` : ''}
                  {(item.language ? `${String(item.language).toUpperCase()} • ` : '')}
                  {item.items_count ? `${item.items_count} ${tr('строк', 'Zeilen')}` : tr('Субтитры', 'Untertitel')}
                </div>
              </div>
            </button>
          ))}
        </div>
      )}
      {!moviesLoading && movies.length > 0 && moviesFiltered.length === 0 && (
        <div className="webapp-muted">{tr('Для выбранного языка пока нет фильмов.', 'Fuer die gewaehlt Sprache gibt es noch keine Videos.')}</div>
      )}
    </section>
  );
}

export default React.memo(YouTubeMoviesSection);
