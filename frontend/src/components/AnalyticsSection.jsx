import React, { useContext } from 'react';
import { AnalyticsContext, formatAnalyticsCalendarDisplayDate } from '../providers/AnalyticsProvider';

export function AnalyticsSection({
  analyticsRef,
  tr,
  uiLang,
  heroStickerSrc,
  getActiveLanguagePairLabel,
  weeklyPlan,
  weeklyMetricRows,
  formatWeeklyValue,
}) {
  const {
    analyticsPeriod, setAnalyticsPeriod,
    analyticsPeriodSelectVersion,
    analyticsCalendarOpen, setAnalyticsCalendarOpen,
    analyticsCalendarDraftStartDate, setAnalyticsCalendarDraftStartDate,
    analyticsCalendarDraftEndDate, setAnalyticsCalendarDraftEndDate,
    analyticsLoading,
    analyticsError,
    analyticsSummary,
    analyticsRank,
    analyticsScopeKey,
    analyticsScopeLoading,
    analyticsScopeSaving,
    analyticsScopeError,
    progressResetError,
    progressResetModalOpen, setProgressResetModalOpen,
    progressResetLoading,
    progressResetSaving,
    progressResetDraftDate, setProgressResetDraftDate,
    analyticsCalendarRangeValid,
    analyticsCalendarDraftValid,
    analyticsScopeSelectorRequired,
    progressResetMaxDate,
    analyticsCalendarLabel,
    progressResetDateLabel,
    analyticsScopeOptions,
    analyticsScopeStatusText,
    analyticsCompareInsight,
    analyticsFinalScoreFormulaText,
    analyticsCardGroups,
    analyticsCalendarRef,
    analyticsPeriodSelectRef,
    analyticsTrendRef,
    analyticsCompareRef,
    analyticsCompareWordsRef,
    analyticsCompareErrorsRef,
    openAnalyticsCalendar,
    applyAnalyticsCalendarRange,
    handleAnalyticsScopeSelect,
    reloadVisibleAnalytics,
    openProgressResetModal,
    applyProgressReset,
    setProgressResetError,
  } = useContext(AnalyticsContext);

  return (
    <section className="webapp-section webapp-analytics" ref={analyticsRef}>
      <div className="webapp-section-title webapp-section-title-with-logo">
        <h2>{tr('Аналитика', 'Analytik')}</h2>
        <p className="webapp-muted">{tr('Языковая пара', 'Sprachpaar')}: {getActiveLanguagePairLabel()}</p>
        <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
      </div>
      <div className="analytics-controls">
        <label className="webapp-field analytics-period-field">
          <span>{tr('Период', 'Zeitraum')}</span>
          <select
            key={`analytics-period-${analyticsPeriodSelectVersion}`}
            ref={analyticsPeriodSelectRef}
            value={analyticsPeriod}
            onChange={(event) => {
              const nextValue = String(event.target.value || '').trim();
              setAnalyticsPeriod(nextValue);
              try {
                event.target.blur();
              } catch (_error) {
                // ignore blur issues on native selects
              }
              if (nextValue === 'calendar') {
                openAnalyticsCalendar();
                return;
              }
              setAnalyticsCalendarOpen(false);
            }}
          >
            <option value="day">{tr('День', 'Tag')}</option>
            <option value="week">{tr('Неделя', 'Woche')}</option>
            <option value="month">{tr('Месяц', 'Monat')}</option>
            <option value="quarter">{tr('Квартал', 'Quartal')}</option>
            <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
            <option value="year">{tr('Год', 'Jahr')}</option>
            <option value="all">{tr('Все время', 'Gesamt')}</option>
            <option value="calendar">{tr('Календарь', 'Kalender')}</option>
          </select>
          {analyticsPeriod === 'calendar' && (
            <div className="analytics-calendar-wrap" ref={analyticsCalendarRef}>
              <button
                type="button"
                className="secondary-button analytics-calendar-trigger"
                onClick={openAnalyticsCalendar}
              >
                {analyticsCalendarLabel}
              </button>
              {analyticsCalendarOpen && (
                <>
                  <button
                    type="button"
                    className="analytics-calendar-backdrop"
                    aria-label={tr('Закрыть выбор диапазона дат', 'Datumsauswahl schliessen')}
                    onClick={() => setAnalyticsCalendarOpen(false)}
                  />
                  <div className="analytics-calendar-popover" role="dialog" aria-modal="true">
                    <div className="analytics-calendar-title">
                      {tr('Диапазон дат', 'Datumsbereich')}
                    </div>
                    <label className="webapp-field analytics-calendar-field">
                      <span>{tr('С даты', 'Von')}</span>
                      <div className="analytics-calendar-date-shell">
                        <span className={`analytics-calendar-date-value ${analyticsCalendarDraftStartDate ? '' : 'is-placeholder'}`}>
                          {formatAnalyticsCalendarDisplayDate(analyticsCalendarDraftStartDate, uiLang === 'de' ? 'de-AT' : 'ru-RU') || tr('Выберите дату', 'Datum waehlen')}
                        </span>
                        <input
                          className="analytics-calendar-native-input"
                          type="date"
                          value={analyticsCalendarDraftStartDate}
                          max={analyticsCalendarDraftEndDate || undefined}
                          onChange={(event) => setAnalyticsCalendarDraftStartDate(event.target.value)}
                        />
                      </div>
                    </label>
                    <label className="webapp-field analytics-calendar-field">
                      <span>{tr('По дату', 'Bis')}</span>
                      <div className="analytics-calendar-date-shell">
                        <span className={`analytics-calendar-date-value ${analyticsCalendarDraftEndDate ? '' : 'is-placeholder'}`}>
                          {formatAnalyticsCalendarDisplayDate(analyticsCalendarDraftEndDate, uiLang === 'de' ? 'de-AT' : 'ru-RU') || tr('Выберите дату', 'Datum waehlen')}
                        </span>
                        <input
                          className="analytics-calendar-native-input"
                          type="date"
                          value={analyticsCalendarDraftEndDate}
                          min={analyticsCalendarDraftStartDate || undefined}
                          onChange={(event) => setAnalyticsCalendarDraftEndDate(event.target.value)}
                        />
                      </div>
                    </label>
                    {!analyticsCalendarDraftValid && (
                      <div className="webapp-muted analytics-calendar-error">
                        {tr('Выберите корректный диапазон дат.', 'Waehle einen gueltigen Datumsbereich.')}
                      </div>
                    )}
                    <div className="analytics-calendar-actions">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          setAnalyticsCalendarOpen(false);
                          try {
                            analyticsPeriodSelectRef.current?.blur?.();
                            if (typeof document !== 'undefined' && document.activeElement instanceof HTMLElement) {
                              document.activeElement.blur();
                            }
                          } catch (_error) {
                            // ignore focus cleanup issues on mobile clients
                          }
                        }}
                      >
                        {tr('Закрыть', 'Schliessen')}
                      </button>
                      <button
                        type="button"
                        className="primary-button"
                        onClick={applyAnalyticsCalendarRange}
                        disabled={!analyticsCalendarDraftValid}
                      >
                        {tr('Применить', 'Anwenden')}
                      </button>
                    </div>
                  </div>
                </>
              )}
            </div>
          )}
        </label>
        <label className="webapp-field analytics-scope-field">
          <span>{tr('Режим участия', 'Teilnahme-Modus')}</span>
          <select
            value={analyticsScopeKey}
            onChange={(event) => {
              void handleAnalyticsScopeSelect(event.target.value);
            }}
            disabled={analyticsScopeLoading || analyticsScopeSaving}
          >
            {analyticsScopeOptions.map((item) => (
              <option key={item.key} value={item.key}>{item.label}</option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="secondary-button"
          onClick={() => reloadVisibleAnalytics(undefined, analyticsScopeKey)}
          disabled={analyticsLoading || analyticsScopeLoading || analyticsScopeSaving || (analyticsPeriod === 'calendar' && !analyticsCalendarRangeValid)}
        >
          {analyticsLoading ? tr('Считаем...', 'Berechnen...') : tr('Обновить', 'Aktualisieren')}
        </button>
        {analyticsRank && (
          <div className="analytics-rank">{tr('Ваше место', 'Dein Rang')}: #{analyticsRank}</div>
        )}
      </div>
      <div className={`webapp-muted analytics-scope-hint ${analyticsScopeSelectorRequired ? 'is-warning' : ''}`}>
        {analyticsScopeStatusText}
        {analyticsScopeSelectorRequired ? ` ${tr('У вас несколько групп: выберите нужный режим участия.', 'Du hast mehrere Gruppen: waehle den passenden Teilnahme-Modus.')}` : ''}
        {analyticsScopeLoading ? ` ${tr('Определяем контекст...', 'Kontext wird ermittelt...')}` : ''}
        {analyticsScopeSaving ? ` ${tr('Сохраняем режим...', 'Modus wird gespeichert...')}` : ''}
      </div>
      <div className="analytics-reset-card">
        <div className="analytics-reset-copy">
          <strong>{tr('Начать все сначала', 'Neu anfangen')}</strong>
          <span>
            {tr('Новая точка отсчета для KPI и skills', 'Neuer Startpunkt fuer KPI und Skills')}: {progressResetDateLabel}
          </span>
          <small>
            {tr('Словарь, карточки, квизы, книги и остальной контент не удаляются.', 'Woerterbuch, Karteikarten, Quizze, Buecher und Inhalte bleiben erhalten.')}
          </small>
        </div>
        <button
          type="button"
          className="secondary-button analytics-reset-button"
          onClick={openProgressResetModal}
          disabled={progressResetLoading || progressResetSaving}
        >
          {progressResetLoading
            ? tr('Загружаем...', 'Laden...')
            : progressResetSaving
              ? tr('Сохраняем...', 'Speichern...')
              : tr('Начать все сначала', 'Neu anfangen')}
        </button>
      </div>

      {analyticsScopeError && <div className="webapp-error">{analyticsScopeError}</div>}
      {progressResetError && !progressResetModalOpen && <div className="webapp-error">{progressResetError}</div>}
      {analyticsError && <div className="webapp-error">{analyticsError}</div>}

      {analyticsSummary && (
        <div className="analytics-card-groups">
          {analyticsCardGroups.map((group) => (
            <div className="analytics-card-group" key={`analytics-group-${group.key}`}>
              <div className="analytics-card-group-title">{group.title}</div>
              <div className="analytics-cards">
                {group.items.map((item) => (
                  <div className="analytics-card" key={`analytics-card-${group.key}-${item.key}`}>
                    <span>{item.label}</span>
                    <strong>{item.value}</strong>
                    <small>{item.hint}</small>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
      {analyticsSummary && (
        <div className="webapp-muted analytics-compare-hint">{analyticsFinalScoreFormulaText}</div>
      )}

      {weeklyPlan && (
        <div className="analytics-goals-grid">
          {weeklyMetricRows.map((item) => (
            <div className="analytics-goal-card" key={`analytics-goal-${item.key}`}>
              <span>{item.title}</span>
              <strong>
                {formatWeeklyValue(item.data?.actual)} / {formatWeeklyValue(item.data?.goal)} {item.unit}
              </strong>
              <small>
                {tr('Прогноз', 'Prognose')}: {formatWeeklyValue(item.data?.forecast, 1)} {item.unit}
                {' • '}
                {tr('% выполнения', '% Erfuellung')}: {formatWeeklyValue(item.data?.completion_percent, 1)}%
              </small>
            </div>
          ))}
        </div>
      )}

      <div className="analytics-chart" ref={analyticsTrendRef} />
      <div className="webapp-muted analytics-compare-hint">{analyticsCompareInsight}</div>
      <div className="analytics-compare-sections">
        <div className="analytics-compare-section">
          <div className="analytics-card-group-title">{tr('Общий результат', 'Gesamtergebnis')}</div>
          <div className="analytics-chart analytics-compare" ref={analyticsCompareRef} />
        </div>
        <div className="analytics-compare-section">
          <div className="analytics-card-group-title">{tr('Слова', 'Woerter')}</div>
          <div className="analytics-chart analytics-compare" ref={analyticsCompareWordsRef} />
        </div>
        <div className="analytics-compare-section">
          <div className="analytics-card-group-title">{tr('Ошибок на 1 предложение', 'Fehler pro Satz')}</div>
          <div className="analytics-chart analytics-compare" ref={analyticsCompareErrorsRef} />
        </div>
      </div>
      {progressResetModalOpen && (
        <div
          className="language-profile-gate analytics-reset-gate"
          role="dialog"
          aria-modal="true"
          onClick={() => {
            if (progressResetSaving) return;
            setProgressResetModalOpen(false);
            setProgressResetError('');
          }}
        >
          <div className="language-profile-card analytics-reset-modal" onClick={(event) => event.stopPropagation()}>
            <h3>{tr('Начать все сначала', 'Neu anfangen')}</h3>
            <p className="webapp-muted">
              {tr('Выберите дату, которая станет новой точкой отсчета для KPI и skills. Словарь, карточки, квизы, книги и остальной контент останутся как есть.', 'Waehle das Datum, das als neuer Startpunkt fuer KPI und Skills gilt. Woerterbuch, Karteikarten, Quizze, Buecher und Inhalte bleiben unveraendert.')}
            </p>
            <label className="webapp-field analytics-reset-field">
              <span>{tr('Новая дата отсчета', 'Neues Startdatum')}</span>
              <div className="analytics-calendar-date-shell analytics-reset-date-shell">
                <span className={`analytics-calendar-date-value ${progressResetDraftDate ? '' : 'is-placeholder'}`}>
                  {formatAnalyticsCalendarDisplayDate(progressResetDraftDate, uiLang === 'de' ? 'de-AT' : 'ru-RU') || tr('Выберите дату', 'Datum waehlen')}
                </span>
                <input
                  className="analytics-calendar-native-input"
                  type="date"
                  value={progressResetDraftDate}
                  max={progressResetMaxDate || undefined}
                  onChange={(event) => setProgressResetDraftDate(event.target.value)}
                />
              </div>
            </label>
            <div className="webapp-muted analytics-reset-caption">
              {tr('С этой даты начнут считаться personal KPI и skill analytics.', 'Ab diesem Datum werden persoenliche KPI und Skill-Analytik neu berechnet.')}
            </div>
            {progressResetError && <div className="webapp-error">{progressResetError}</div>}
            <div className="language-profile-actions analytics-reset-actions">
              <button
                type="button"
                className="language-profile-close-btn"
                onClick={() => {
                  setProgressResetModalOpen(false);
                  setProgressResetError('');
                }}
                disabled={progressResetSaving}
              >
                {tr('Отмена', 'Abbrechen')}
              </button>
              <button
                type="button"
                className="language-profile-save-btn"
                onClick={applyProgressReset}
                disabled={progressResetSaving || !progressResetDraftDate}
              >
                {progressResetSaving ? tr('Сохраняем...', 'Speichern...') : tr('Применить', 'Anwenden')}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
