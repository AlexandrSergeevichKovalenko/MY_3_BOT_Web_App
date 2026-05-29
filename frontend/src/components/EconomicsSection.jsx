import { useContext } from 'react';
import {
  EconomicsContext,
  formatEconomicsProviderLabel,
  formatEconomicsUnitsLabel,
  formatEconomicsCompactNumber,
} from '../providers/EconomicsProvider';

export function EconomicsSection({ economicsRef, tr, uiLang, heroStickerSrc }) {
  const {
    economicsPeriod, setEconomicsPeriod,
    economicsProvider, setEconomicsProvider,
    economicsForecastUsersDraft, setEconomicsForecastUsersDraft,
    economicsRailwayAppRamDraft, setEconomicsRailwayAppRamDraft,
    economicsRailwayAppCpuDraft, setEconomicsRailwayAppCpuDraft,
    economicsRailwayPostgresRamDraft, setEconomicsRailwayPostgresRamDraft,
    economicsRailwayPostgresVolumeDraft, setEconomicsRailwayPostgresVolumeDraft,
    economicsRailwayRedisRamDraft, setEconomicsRailwayRedisRamDraft,
    economicsRailwayEgressDraft, setEconomicsRailwayEgressDraft,
    economicsLoading,
    economicsError,
    economicsSummary,
    loadEconomics,
    economicsProviderOptions,
    economicsBudgetRows,
    economicsPerUserResourceRows,
    economicsFreeTierCapacityRows,
    economicsFreeTierBottleneck,
    economicsForecastUsers,
    economicsForecastResourceRows,
    economicsForecastTotals,
    economicsRailwayInfra,
    economicsRailwayTrackedBaselineUsd,
    economicsRailwayLivePostgres,
    economicsRailwayLiveRedis,
    economicsRailwayFixedComponents,
    economicsRailwayForecast,
    economicsLedgerIsEmpty,
    selectedEconomicsProviderLabel,
  } = useContext(EconomicsContext);

  return (
    <section className="webapp-section webapp-economics" ref={economicsRef}>
      <div className="webapp-section-title webapp-section-title-with-logo">
        <h2>{tr('Экономика', 'Kosten')}</h2>
        <p className="webapp-muted">{tr('Глобальная экономика приложения: расходы, провайдеры, usage и модели по всему продукту.', 'Globale App-Oekonomie: Kosten, Provider, Usage und Modelle fuer das gesamte Produkt.')}</p>
        <img src={heroStickerSrc} alt="" aria-hidden="true" className="section-corner-logo" />
      </div>
      <div className="analytics-controls economics-controls">
        <label className="webapp-field">
          <span>{tr('Период', 'Zeitraum')}</span>
          <select value={economicsPeriod} onChange={(event) => setEconomicsPeriod(event.target.value)}>
            <option value="day">{tr('День', 'Tag')}</option>
            <option value="week">{tr('Неделя', 'Woche')}</option>
            <option value="month">{tr('Месяц', 'Monat')}</option>
            <option value="quarter">{tr('Квартал', 'Quartal')}</option>
            <option value="half-year">{tr('Полугодие', 'Halbjahr')}</option>
            <option value="year">{tr('Год', 'Jahr')}</option>
            <option value="all">{tr('Все время', 'Gesamt')}</option>
          </select>
        </label>
        <label className="webapp-field">
          <span>{tr('Провайдер', 'Provider')}</span>
          <select value={economicsProvider} onChange={(event) => setEconomicsProvider(event.target.value)}>
            {economicsProviderOptions.map((providerKey) => (
              <option key={`economics-provider-${providerKey}`} value={providerKey}>
                {providerKey === 'all'
                  ? tr('Все провайдеры', 'Alle Provider')
                  : formatEconomicsProviderLabel(providerKey)}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="secondary-button"
          onClick={() => loadEconomics(economicsPeriod, economicsProvider)}
          disabled={economicsLoading}
        >
          {economicsLoading ? tr('Считаем...', 'Berechnen...') : tr('Обновить', 'Aktualisieren')}
        </button>
      </div>

      {economicsError && <div className="webapp-error">{economicsError}</div>}
      {!economicsError && economicsLoading && <div className="webapp-muted">{tr('Считаем расходы...', 'Kosten werden berechnet...')}</div>}

      {economicsSummary && (
        <>
          <div className="analytics-cards economics-cards">
            <div className="analytics-card">
              <span>{tr('Переменные', 'Variabel')}</span>
              <strong>{Number(economicsSummary?.totals?.variable_cost_total || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Fixed', 'Fixed')}</span>
              <strong>{Number(economicsSummary?.totals?.fixed_cost_total || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Итого', 'Gesamt')}</span>
              <strong>{Number(economicsSummary?.totals?.total_cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('События', 'Ereignisse')}</span>
              <strong>{Number(economicsSummary?.totals?.events_count || 0)}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Без цены (events)', 'Ohne Preis (Events)')}</span>
              <strong>{Number(economicsSummary?.totals?.unpriced_events || 0)}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Ср. цена billing-event', 'Ø Kosten pro Billing-Event')}</span>
              <strong>{Number(economicsSummary?.totals?.avg_cost_per_event || 0).toFixed(4)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Активных пользователей', 'Aktive Nutzer')}</span>
              <strong>{Number(economicsSummary?.totals?.active_users || 0)}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Событий на пользователя', 'Events pro Nutzer')}</span>
              <strong>{Number(economicsSummary?.totals?.avg_events_per_active_user || 0).toFixed(1)}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Ср. переменные затраты на пользователя', 'Ø variable Kosten pro Nutzer')}</span>
              <strong>{Number(economicsSummary?.totals?.avg_variable_cost_per_active_user || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
            <div className="analytics-card">
              <span>{tr('Ср. постоянные затраты на пользователя', 'Ø Fixkosten pro Nutzer')}</span>
              <strong>{Number(economicsSummary?.totals?.avg_fixed_cost_per_active_user || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
            </div>
          </div>

          <div className="economics-total-spotlight">
            <span>{tr('СРЕДНЯЯ ИТОГОВАЯ СТОИМОСТЬ НА 1 ПОЛЬЗОВАТЕЛЯ', 'DURCHSCHNITTLICHE GESAMTKOSTEN PRO 1 NUTZER')}</span>
            <strong>{Number(economicsSummary?.totals?.avg_cost_per_active_user || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
          </div>

          <div className="economics-meta-row">
            <span>{tr('Диапазон', 'Zeitraum')}: {economicsSummary?.range?.start_date} — {economicsSummary?.range?.end_date}</span>
            <span>{tr('Фильтр провайдера', 'Provider-Filter')}: {selectedEconomicsProviderLabel}</span>
            <span>{tr('Охват', 'Abdeckung')}: {tr('все пользователи и системные события', 'alle Nutzer und Systemevents')}</span>
          </div>
          <div className="webapp-muted analytics-scope-hint">
            {tr(
              'Billing ledger = внутренний журнал учтённых cost-записей. Один реальный сценарий может создать несколько billing-events: например модель, TTS, R2 и voice считаются отдельно.',
              'Billing-Ledger = internes Journal aller erfassten Kosten-Eintraege. Ein echter Nutzungsvorgang kann mehrere Billing-Events erzeugen: z. B. Modell, TTS, R2 und Voice getrennt.'
            )}
          </div>
          <div className="webapp-muted analytics-scope-hint">
            {tr(
              'Блоки реальной себестоимости ниже показывают номинальную цену ресурсов без вычета бесплатных лимитов. Это сделано специально, чтобы видеть настоящую стоимость продукта на пользователя.',
              'Die echten Kosten unten zeigen den nominalen Ressourcenpreis ohne Abzug von Freikontingenten. So sieht man bewusst die reale Produktkosten pro Nutzer.',
            )}
          </div>

          {economicsLedgerIsEmpty && (
            <div className="webapp-muted analytics-scope-hint is-warning">
              {tr(
                'В billing ledger пока нет записанных событий. Нули ниже означают отсутствие billing-данных в базе, а не обязательно нулевое реальное использование.',
                'Im Billing-Ledger sind aktuell keine Events gespeichert. Die Nullen unten bedeuten fehlende Billing-Daten in der Datenbank, nicht zwingend echten Null-Verbrauch.',
              )}
            </div>
          )}

          {economicsPerUserResourceRows.length > 0 && (
            <div className="economics-breakdown-card economics-breakdown-card-spotlight">
              <h4>{tr('Реальная себестоимость на 1 активного пользователя по ресурсам', 'Echte Kosten pro 1 aktiven Nutzer nach Ressource')}</h4>
              <div className="webapp-muted" style={{ marginBottom: 10, fontSize: 12 }}>
                {tr(
                  'Это средняя цена одного активного пользователя по каждому провайдеру. Здесь free tier не вычитается.',
                  'Das ist der Durchschnittspreis eines aktiven Nutzers je Provider. Das Freikontingent wird hier nicht abgezogen.',
                )}
              </div>
              {economicsPerUserResourceRows.map((item) => {
                const avgUnitsRows = Array.isArray(item?.avg_units_by_type_per_active_user) ? item.avg_units_by_type_per_active_user : [];
                const avgUnitsLabel = avgUnitsRows
                  .slice(0, 2)
                  .map((unitRow) => `${formatEconomicsCompactNumber(unitRow?.avg_units_per_active_user || 0)} ${formatEconomicsUnitsLabel(unitRow?.units_type, uiLang)}`)
                  .join(' • ');
                return (
                  <div className="economics-breakdown-row economics-breakdown-row-rich" key={`provider-per-user-${item.provider}`}>
                    <div className="economics-breakdown-copy">
                      <span>{formatEconomicsProviderLabel(item.provider || 'n/a')}</span>
                      <small>
                        {tr('активных', 'aktive')}: {Number(item.active_users || 0)}
                        {avgUnitsLabel ? ` • ${tr('ср. usage', 'Ø Usage')}: ${avgUnitsLabel}` : ''}
                        {Number(item.avg_events_per_active_user || 0) > 0
                          ? ` • ${tr('events/user', 'Events/Nutzer')}: ${Number(item.avg_events_per_active_user || 0).toFixed(1)}`
                          : ''}
                      </small>
                    </div>
                    <div className="economics-breakdown-value">
                      <strong>{Number(item.avg_total_cost_per_active_user || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                      <small>
                        {tr('переменные', 'variabel')}: {Number(item.avg_variable_cost_per_active_user || 0).toFixed(3)}
                        {' • '}
                        {tr('fixed', 'fix')}: {Number(item.avg_fixed_cost_per_active_user || 0).toFixed(3)}
                      </small>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          <div className="economics-breakdown-card economics-breakdown-card-spotlight economics-forecast-block">
            <div className="economics-forecast-head">
              <h4>{tr('Прогноз затрат при выбранном количестве пользователей', 'Kostenprognose fuer eine gewaehlte Nutzerzahl')}</h4>
              <label className="webapp-field economics-forecast-field">
                <span>{tr('Сколько пользователей', 'Wie viele Nutzer')}</span>
                <input
                  type="number"
                  min="1"
                  step="1"
                  inputMode="numeric"
                  value={economicsForecastUsersDraft}
                  onChange={(event) => setEconomicsForecastUsersDraft(event.target.value)}
                  placeholder="100"
                />
              </label>
            </div>
            <div className="webapp-muted" style={{ marginBottom: 10, fontSize: 12 }}>
              {tr(
                'Это линейный прогноз по текущему среднему usage на 1 активного пользователя. Railway сюда сознательно пока не включён, потому что для него нужна отдельная ступенчатая infra-модель.',
                'Das ist eine lineare Prognose auf Basis des aktuellen durchschnittlichen Usage pro aktivem Nutzer. Railway ist hier bewusst noch nicht enthalten, weil dafuer ein separates stufenweises Infra-Modell noetig ist.',
              )}
            </div>
            {economicsForecastUsers > 0 ? (
              <>
                <div className="analytics-cards economics-cards">
                  <div className="analytics-card">
                    <span>{tr('Прогноз: переменные', 'Prognose: variabel')}</span>
                    <strong>{Number(economicsForecastTotals.forecastVariableCost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  </div>
                  <div className="analytics-card">
                    <span>{tr('Прогноз: fixed', 'Prognose: fix')}</span>
                    <strong>{Number(economicsForecastTotals.forecastFixedCost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  </div>
                  <div className="analytics-card">
                    <span>{tr('Прогноз: итого', 'Prognose: gesamt')}</span>
                    <strong>{Number(economicsForecastTotals.forecastTotalCost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  </div>
                  <div className="analytics-card">
                    <span>{tr('Free-tier bottleneck', 'Free-Tier-Bottleneck')}</span>
                    <strong>
                      {economicsForecastTotals.freeTierBottleneckProvider
                        ? `${formatEconomicsProviderLabel(economicsForecastTotals.freeTierBottleneckProvider)} · ${Number(economicsForecastTotals.freeTierBottleneckUsers || 0)}`
                        : '—'}
                    </strong>
                  </div>
                </div>
                <div className="economics-breakdown-grid economics-breakdown-grid-tight">
                  {economicsForecastResourceRows.slice(0, 10).map((item) => {
                    const unitsLabel = item.forecastUnitsByType
                      .slice(0, 2)
                      .map((unitRow) => `${formatEconomicsCompactNumber(unitRow.forecastUnits || 0)} ${formatEconomicsUnitsLabel(unitRow.unitsType, uiLang)}`)
                      .join(' • ');
                    return (
                      <div className="economics-breakdown-card economics-breakdown-card-mini" key={`forecast-provider-${item.provider}`}>
                        <h4>{formatEconomicsProviderLabel(item.provider || 'n/a')}</h4>
                        <div className="economics-breakdown-row economics-breakdown-row-rich">
                          <div className="economics-breakdown-copy">
                            <span>{tr('Прогноз стоимости', 'Kostenprognose')}</span>
                            <small>{tr('для', 'fuer')} {economicsForecastUsers} {tr('польз.', 'Nutzer')}</small>
                          </div>
                          <div className="economics-breakdown-value">
                            <strong>{Number(item.forecastTotalCost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                            <small>
                              {tr('переменные', 'variabel')}: {Number(item.forecastVariableCost || 0).toFixed(3)}
                              {' • '}
                              {tr('fixed', 'fix')}: {Number(item.forecastFixedCost || 0).toFixed(3)}
                            </small>
                          </div>
                        </div>
                        <div className="economics-breakdown-row economics-breakdown-row-rich">
                          <div className="economics-breakdown-copy">
                            <span>{tr('Прогноз usage', 'Usage-Prognose')}</span>
                            <small>
                              {unitsLabel || '—'}
                              {Number(item.forecastEvents || 0) > 0
                                ? ` • ${tr('events', 'Events')}: ${Number(item.forecastEvents || 0).toFixed(0)}`
                                : ''}
                            </small>
                          </div>
                          <div className="economics-breakdown-value">
                            <strong>
                              {item.freeTierTotalUsersCapacity > 0
                                ? `${Math.max(0, economicsForecastUsers - item.freeTierTotalUsersCapacity)}`
                                : '—'}
                            </strong>
                            <small>
                              {item.freeTierTotalUsersCapacity > 0
                                ? tr('сверх free tier', 'ueber Free Tier')
                                : tr('нет free-tier data', 'keine Free-Tier-Daten')}
                            </small>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </>
            ) : (
              <div className="webapp-muted">{tr('Введите количество пользователей больше нуля.', 'Bitte gib eine Nutzerzahl groesser als null ein.')}</div>
            )}
          </div>

          {economicsRailwayInfra && (
            <div className="economics-breakdown-card economics-breakdown-card-spotlight economics-railway-block">
              <div className="economics-forecast-head">
                <h4>{tr('Railway infra audit + forecast', 'Railway Infra Audit + Prognose')}</h4>
              </div>
              <div className="webapp-muted" style={{ marginBottom: 10, fontSize: 12 }}>
                {tr(
                  'Здесь Railway считается отдельно и честно: live audit показывает только то, что backend реально видит сейчас, а ниже вы задаёте серверную модель слоями App / Postgres / Redis / Network.',
                  'Railway wird hier separat und ehrlich gerechnet: Das Live-Audit zeigt nur, was das Backend jetzt wirklich sieht, und unten gibst du das Servermodell in den Schichten App / Postgres / Redis / Network an.',
                )}
              </div>
              <div className="analytics-cards economics-cards">
                <div className="analytics-card">
                  <span>{tr('Tracked infra baseline', 'Tracked-Infra-Basis')}</span>
                  <strong>{economicsRailwayTrackedBaselineUsd.toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                </div>
                <div className="analytics-card">
                  <span>{tr('Postgres size now', 'Postgres-Groesse jetzt')}</span>
                  <strong>
                    {economicsRailwayLivePostgres?.available
                      ? `${Number(economicsRailwayLivePostgres?.db_size_mb || 0).toFixed(1)} MB`
                      : '—'}
                  </strong>
                </div>
                <div className="analytics-card">
                  <span>{tr('Redis memory now', 'Redis-Speicher jetzt')}</span>
                  <strong>
                    {economicsRailwayLiveRedis?.available
                      ? `${Number(economicsRailwayLiveRedis?.memory_used_mb || 0).toFixed(1)} MB`
                      : '—'}
                  </strong>
                </div>
                <div className="analytics-card">
                  <span>{tr('Redis peak / keys', 'Redis Peak / Keys')}</span>
                  <strong>
                    {economicsRailwayLiveRedis?.available
                      ? `${Number(economicsRailwayLiveRedis?.memory_peak_mb || 0).toFixed(1)} MB`
                      : '—'}
                  </strong>
                  <small className="webapp-muted">
                    {economicsRailwayLiveRedis?.available
                      ? `${tr('keys', 'Keys')}: ${Number(economicsRailwayLiveRedis?.keys || 0)}`
                      : ''}
                  </small>
                </div>
              </div>
              {economicsRailwayFixedComponents.length > 0 && (
                <div className="economics-breakdown-grid economics-breakdown-grid-tight" style={{ marginTop: 10 }}>
                  {economicsRailwayFixedComponents.map((item) => (
                    <div className="economics-breakdown-card economics-breakdown-card-mini" key={`railway-fixed-${item.provider}`}>
                      <h4>{formatEconomicsProviderLabel(item.provider || 'n/a')}</h4>
                      <div className="economics-breakdown-row economics-breakdown-row-rich">
                        <div className="economics-breakdown-copy">
                          <span>{tr('Fixed / month', 'Fix / Monat')}</span>
                          <small>{tr('из tracked infra baseline', 'aus der getrackten Infra-Basis')}</small>
                        </div>
                        <div className="economics-breakdown-value">
                          <strong>{Number(item.fixed_cost_month_usd || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div className="economics-breakdown-grid economics-breakdown-grid-tight">
                <div className="economics-breakdown-card economics-breakdown-card-mini">
                  <h4>{tr('App layer', 'App-Schicht')}</h4>
                  <div className="economics-railway-inputs">
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('App RAM GB / month', 'App-RAM GB / Monat')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayAppRamDraft}
                        onChange={(event) => setEconomicsRailwayAppRamDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('App vCPU / month', 'App-vCPU / Monat')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayAppCpuDraft}
                        onChange={(event) => setEconomicsRailwayAppCpuDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                  </div>
                </div>
                <div className="economics-breakdown-card economics-breakdown-card-mini">
                  <h4>{tr('Postgres layer', 'Postgres-Schicht')}</h4>
                  <div className="economics-railway-inputs">
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('Postgres RAM GB / month', 'Postgres-RAM GB / Monat')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayPostgresRamDraft}
                        onChange={(event) => setEconomicsRailwayPostgresRamDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('Postgres volume GB / month', 'Postgres-Volume GB / Monat')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayPostgresVolumeDraft}
                        onChange={(event) => setEconomicsRailwayPostgresVolumeDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                  </div>
                </div>
                <div className="economics-breakdown-card economics-breakdown-card-mini">
                  <h4>{tr('Redis layer', 'Redis-Schicht')}</h4>
                  <div className="economics-railway-inputs">
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('Redis RAM GB / month', 'Redis-RAM GB / Monat')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayRedisRamDraft}
                        onChange={(event) => setEconomicsRailwayRedisRamDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                  </div>
                </div>
                <div className="economics-breakdown-card economics-breakdown-card-mini">
                  <h4>{tr('Network layer', 'Netzwerk-Schicht')}</h4>
                  <div className="economics-railway-inputs">
                    <label className="webapp-field economics-forecast-field">
                      <span>{tr('Egress GB', 'Egress GB')}</span>
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        inputMode="decimal"
                        value={economicsRailwayEgressDraft}
                        onChange={(event) => setEconomicsRailwayEgressDraft(event.target.value)}
                        placeholder="0"
                      />
                    </label>
                  </div>
                </div>
              </div>
              <div className="webapp-muted" style={{ marginTop: 10, marginBottom: 10, fontSize: 12 }}>
                {tr(
                  'Ставки Railway сейчас считаются так: RAM $10 / GB-month, CPU $20 / vCPU-month, volume $0.15 / GB-month, egress $0.05 / GB. Postgres volume и Redis RAM могут подхватываться из live audit, а app RAM / app vCPU / Postgres RAM вы задаёте вручную.',
                  'Railway wird aktuell so gerechnet: RAM $10 / GB-Monat, CPU $20 / vCPU-Monat, Volume $0.15 / GB-Monat, Egress $0.05 / GB. Postgres-Volume und Redis-RAM koennen aus dem Live-Audit kommen, waehrend App-RAM / App-vCPU / Postgres-RAM manuell gesetzt werden.',
                )}
              </div>
              <div className="analytics-cards economics-cards">
                <div className="analytics-card">
                  <span>{tr('App compute', 'App-Compute')}</span>
                  <strong>{(Number(economicsRailwayForecast.appRamCost || 0) + Number(economicsRailwayForecast.appCpuCost || 0)).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  <small className="webapp-muted">
                    {tr('RAM', 'RAM')}: {Number(economicsRailwayForecast.appRamCost || 0).toFixed(3)}
                    {' • '}
                    {tr('CPU', 'CPU')}: {Number(economicsRailwayForecast.appCpuCost || 0).toFixed(3)}
                  </small>
                </div>
                <div className="analytics-card">
                  <span>{tr('Postgres layer', 'Postgres-Schicht')}</span>
                  <strong>{(Number(economicsRailwayForecast.postgresRamCost || 0) + Number(economicsRailwayForecast.postgresVolumeCost || 0)).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  <small className="webapp-muted">
                    {tr('RAM', 'RAM')}: {Number(economicsRailwayForecast.postgresRamCost || 0).toFixed(3)}
                    {' • '}
                    {tr('volume', 'Volume')}: {Number(economicsRailwayForecast.postgresVolumeCost || 0).toFixed(3)}
                  </small>
                </div>
                <div className="analytics-card">
                  <span>{tr('Redis + network', 'Redis + Netzwerk')}</span>
                  <strong>{(Number(economicsRailwayForecast.redisRamCost || 0) + Number(economicsRailwayForecast.egressCost || 0)).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  <small className="webapp-muted">
                    {tr('Redis RAM', 'Redis-RAM')}: {Number(economicsRailwayForecast.redisRamCost || 0).toFixed(3)}
                    {' • '}
                    {tr('egress', 'Egress')}: {Number(economicsRailwayForecast.egressCost || 0).toFixed(3)}
                  </small>
                </div>
                <div className="analytics-card">
                  <span>{tr('Railway total with baseline', 'Railway gesamt mit Basis')}</span>
                  <strong>{Number(economicsRailwayForecast.totalCost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                  <small className="webapp-muted">
                    {tr('sum RAM', 'RAM gesamt')}: {Number(economicsRailwayForecast.totalRamGb || 0).toFixed(2)} GB
                  </small>
                </div>
              </div>
            </div>
          )}

          {economicsBudgetRows.length > 0 && (
            <div className="economics-breakdown-card economics-breakdown-card-spotlight economics-budget-monitor">
              <h4>{tr('Монитор free tier / quota по провайдерам', 'Free-Tier / Quota Monitor nach Providern')}</h4>
              <div className="webapp-muted" style={{ marginBottom: 10, fontSize: 12 }}>
                {tr(
                  'Это отдельный operational monitor. Он показывает текущее бесплатное окно или quota провайдера и не равен реальной себестоимости продукта.',
                  'Das ist ein separater operationaler Monitor. Er zeigt das aktuelle Freikontingent oder Quota-Fenster des Providers und ist nicht gleich den realen Produktkosten.',
                )}
              </div>
              <div className="analytics-cards economics-voice-cards">
              {economicsBudgetRows.map((row) => {
                const usedUnits = Number(row?.used_units || 0);
                const limitUnits = row?.effective_limit_units == null ? null : Number(row.effective_limit_units || 0);
                const usageRatio = Number(row?.usage_ratio || 0);
                const budgetKind = String(row?.metadata?.budget_kind || '').trim().toLowerCase();
                const budgetLabel = String(row?.period_label || '').trim() || String(row?.period_month || '').slice(0, 7);
                const activeUsers = Number(row?.active_users || 0);
                const avgUnitsPerActiveUser = Number(row?.avg_units_per_active_user || 0);
                const budgetWindowLabel = budgetKind === 'daily_quota'
                  ? tr('окно: день', 'Fenster: Tag')
                  : tr('окно: месяц', 'Fenster: Monat');
                const livekitTone = String(row?.metadata?.color || '').trim().toLowerCase();
                const cardTone =
                  row?.provider === 'livekit'
                    ? `is-${livekitTone || 'green'}`
                    : usageRatio >= 1
                      ? 'is-red'
                      : usageRatio >= 0.8
                        ? 'is-yellow'
                        : 'is-green';
                return (
                  <div className={`analytics-card economics-voice-card ${cardTone}`} key={`provider-budget-${row.provider}`}>
                    <span>{formatEconomicsProviderLabel(row?.provider || row?.label || '')}</span>
                    <strong>
                      {usedUnits.toFixed(2)}
                      {limitUnits != null ? ` / ${limitUnits.toFixed(2)}` : ''}
                    </strong>
                    <small className="webapp-muted">
                      {budgetKind === 'daily_quota' ? tr('daily quota', 'Tagesquote') : tr('free tier', 'Freikontingent')}
                      {' • '}
                      {budgetWindowLabel}
                      {budgetLabel ? ` ${budgetLabel}` : ''}
                      {' • '}
                      {formatEconomicsUnitsLabel(row?.units_type || row?.unit, uiLang)}
                      {Number.isFinite(usageRatio) && usageRatio > 0
                        ? ` • ${Math.round(usageRatio * 100)}%`
                        : ''}
                    </small>
                    {activeUsers > 0 && (
                      <small className="webapp-muted">
                        {tr('avg/user', 'Ø/Nutzer')}: {formatEconomicsCompactNumber(avgUnitsPerActiveUser)} {formatEconomicsUnitsLabel(row?.units_type || row?.unit, uiLang)}
                        {' • '}
                        {activeUsers} {tr('активных', 'aktive')}
                      </small>
                    )}
                  </div>
                );
              })}
              </div>
            </div>
          )}

          {economicsFreeTierCapacityRows.length > 0 && (
            <div className="economics-breakdown-card economics-breakdown-card-spotlight economics-budget-capacity">
              <h4>{tr('Сколько пользователей помещается в free tier', 'Wie viele Nutzer in den Free Tier passen')}</h4>
              <div className="webapp-muted" style={{ marginBottom: 10, fontSize: 12 }}>
                {tr(
                  'Расчёт идёт от среднего usage на 1 активного пользователя. Самый маленький показатель ниже — это bottleneck текущего бесплатного набора провайдеров.',
                  'Die Rechnung basiert auf dem durchschnittlichen Usage pro aktivem Nutzer. Der kleinste Wert unten ist der aktuelle Bottleneck des kostenlosen Provider-Sets.',
                )}
              </div>
              {economicsFreeTierBottleneck && (
                <div className="economics-capacity-bottleneck">
                  <span>{tr('Бутылочное горлышко сейчас', 'Aktueller Bottleneck')}</span>
                  <strong>
                    {formatEconomicsProviderLabel(economicsFreeTierBottleneck.provider || 'n/a')}: {Number(economicsFreeTierBottleneck.totalUsersCapacity || 0)} {tr('польз.', 'Nutzer')}
                  </strong>
                  <small className="webapp-muted">
                    {tr('ещё осталось по текущему окну', 'noch frei im aktuellen Fenster')}: {Number(economicsFreeTierBottleneck.remainingUsersCapacity || 0)} {tr('польз.', 'Nutzer')}
                  </small>
                </div>
              )}
              <div className="economics-breakdown-grid economics-breakdown-grid-tight">
                {economicsFreeTierCapacityRows.map((row) => (
                  <div className="economics-breakdown-card economics-breakdown-card-mini" key={`free-tier-capacity-${row.provider}`}>
                    <h4>{formatEconomicsProviderLabel(row.provider || 'n/a')}</h4>
                    <div className="economics-breakdown-row economics-breakdown-row-rich">
                      <div className="economics-breakdown-copy">
                        <span>{tr('Всего покрывает', 'Deckt insgesamt')}</span>
                        <small>
                          {row.budgetKind === 'daily_quota' ? tr('окно: день', 'Fenster: Tag') : tr('окно: месяц', 'Fenster: Monat')}
                          {row.budgetLabel ? ` ${row.budgetLabel}` : ''}
                        </small>
                      </div>
                      <div className="economics-breakdown-value">
                        <strong>{Number(row.totalUsersCapacity || 0)}</strong>
                        <small>{tr('пользователей', 'Nutzer')}</small>
                      </div>
                    </div>
                    <div className="economics-breakdown-row economics-breakdown-row-rich">
                      <div className="economics-breakdown-copy">
                        <span>{tr('Осталось сейчас', 'Noch frei jetzt')}</span>
                        <small>
                          {tr('ср. usage/user', 'Ø Usage/Nutzer')}: {formatEconomicsCompactNumber(row.avgUnitsPerActiveUser)} {formatEconomicsUnitsLabel(row.unitsType, uiLang)}
                        </small>
                      </div>
                      <div className="economics-breakdown-value">
                        <strong>{Number(row.remainingUsersCapacity || 0)}</strong>
                        <small>{tr('пользователей', 'Nutzer')}</small>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="economics-breakdown-grid">
            <div className="economics-breakdown-card">
              <h4>{tr('По провайдерам', 'Nach Providern')}</h4>
              {(economicsSummary?.breakdown?.by_provider || []).slice(0, 10).map((item) => (
                <div className="economics-breakdown-row economics-breakdown-row-rich" key={`provider-${item.provider}`}>
                  <div className="economics-breakdown-copy">
                    <span>{formatEconomicsProviderLabel(item.provider || 'n/a')}</span>
                    <small>
                      {Number(item.events || 0)} {tr('events', 'Events')}
                      {Array.isArray(item.units_by_type) && item.units_by_type.length > 0
                        ? ` • ${item.units_by_type.slice(0, 2).map((unitRow) => (
                          `${Number(unitRow.units || 0).toFixed(2)} ${formatEconomicsUnitsLabel(unitRow.units_type, uiLang)}`
                        )).join(' • ')}`
                        : ''}
                    </small>
                  </div>
                  <div className="economics-breakdown-value">
                    <strong>{Number(item.total_cost || item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                    {Number(item.fixed_cost || 0) > 0 && (
                      <small>{tr('fixed', 'fixed')}: {Number(item.fixed_cost || 0).toFixed(3)}</small>
                    )}
                  </div>
                </div>
              ))}
              {(!economicsSummary?.breakdown?.by_provider || economicsSummary.breakdown.by_provider.length === 0) && (
                <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
              )}
            </div>
            <div className="economics-breakdown-card">
              <h4>{tr('По действиям', 'Nach Aktionen')}</h4>
              {(economicsSummary?.breakdown?.by_action_type || []).slice(0, 10).map((item) => (
                <div className="economics-breakdown-row economics-breakdown-row-rich" key={`action-${item.action_type}`}>
                  <div className="economics-breakdown-copy">
                    <span>{item.action_type || 'n/a'}</span>
                    <small>{Number(item.events || 0)} {tr('events', 'Events')}</small>
                  </div>
                  <div className="economics-breakdown-value">
                    <strong>{Number(item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                    <small>{Number(item.units || 0).toFixed(2)} {tr('units', 'Units')}</small>
                  </div>
                </div>
              ))}
              {(!economicsSummary?.breakdown?.by_action_type || economicsSummary.breakdown.by_action_type.length === 0) && (
                <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
              )}
            </div>
            <div className="economics-breakdown-card">
              <h4>{tr('По AI-моделям', 'Nach KI-Modellen')}</h4>
              <div className="webapp-muted" style={{ marginBottom: 8, fontSize: 12 }}>
                {tr('Например GPT / Whisper / Claude, если модель записана в billing event.', 'Zum Beispiel GPT / Whisper / Claude, falls das Modell im Billing-Event gespeichert ist.')}
              </div>
              {(economicsSummary?.breakdown?.by_model || []).slice(0, 10).map((item) => (
                <div className="economics-breakdown-row economics-breakdown-row-rich" key={`model-${item.model}`}>
                  <div className="economics-breakdown-copy">
                    <span>{item.model || 'n/a'}</span>
                    <small>
                      {Number(item.tokens_in || 0).toFixed(0)} {formatEconomicsUnitsLabel('tokens_in', uiLang)}
                      {' • '}
                      {Number(item.tokens_out || 0).toFixed(0)} {formatEconomicsUnitsLabel('tokens_out', uiLang)}
                    </small>
                  </div>
                  <div className="economics-breakdown-value">
                    <strong>{Number(item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                    <small>{Number(item.events || 0)} {tr('events', 'Events')}</small>
                  </div>
                </div>
              ))}
              {(!economicsSummary?.breakdown?.by_model || economicsSummary.breakdown.by_model.length === 0) && (
                <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
              )}
            </div>
            <div className="economics-breakdown-card">
              <h4>{tr('По типу units', 'Nach Unit-Typ')}</h4>
              {(economicsSummary?.breakdown?.by_units_type || []).slice(0, 10).map((item) => (
                <div className="economics-breakdown-row economics-breakdown-row-rich" key={`units-${item.units_type}`}>
                  <div className="economics-breakdown-copy">
                    <span>{formatEconomicsUnitsLabel(item.units_type, uiLang)}</span>
                    <small>{Number(item.events || 0)} {tr('events', 'Events')}</small>
                  </div>
                  <div className="economics-breakdown-value">
                    <strong>{Number(item.cost || 0).toFixed(3)} {economicsSummary?.currency || 'USD'}</strong>
                    <small>{Number(item.units || 0).toFixed(2)}</small>
                  </div>
                </div>
              ))}
              {(!economicsSummary?.breakdown?.by_units_type || economicsSummary.breakdown.by_units_type.length === 0) && (
                <div className="webapp-muted">{tr('Пока нет данных.', 'Noch keine Daten.')}</div>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
