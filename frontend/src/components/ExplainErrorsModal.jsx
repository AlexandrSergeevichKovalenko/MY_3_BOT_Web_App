import React from 'react';

/**
 * Teacher-grade error breakdown for a translation, shown as a centered modal
 * (so it doesn't stretch the result list down the page). Renders the structured
 * payload from /api/webapp/explain: summary, per-error cards (your → correct, why,
 * rule, example), alternative phrasings and synonyms. The 🇩🇪 toggle in the header
 * re-fetches the breakdown in German; unchecked = the learner's language.
 */

const TYPE_META = {
  grammar: { ru: 'Грамматика', de: 'Grammatik', emoji: '🧩', cls: 'is-grammar' },
  vocabulary: { ru: 'Лексика', de: 'Wortschatz', emoji: '📚', cls: 'is-vocab' },
  syntax: { ru: 'Синтаксис', de: 'Syntax', emoji: '🔧', cls: 'is-syntax' },
  style: { ru: 'Стиль', de: 'Stil', emoji: '🎨', cls: 'is-style' },
  orthography: { ru: 'Орфография', de: 'Rechtschreibung', emoji: '✍️', cls: 'is-ortho' },
};

function typeLabel(type, langDe) {
  const m = TYPE_META[type] || TYPE_META.grammar;
  return `${m.emoji} ${langDe ? m.de : m.ru}`;
}

export default function ExplainErrorsModal({
  isOpen,
  onClose,
  tr,
  satz,
  score,
  langDe,
  onToggleLang,
  data,
  loading,
  errorMsg,
  grammar: grammarProp,
  grammarLoading,
  children,
}) {
  if (!isOpen) return null;

  const errors = Array.isArray(data?.errors) ? data.errors : [];
  const alternatives = Array.isArray(data?.alternatives) ? data.alternatives : [];
  const synonyms = Array.isArray(data?.synonyms) ? data.synonyms : [];
  // Grammar of the correct sentence loads progressively via a separate call, so it arrives
  // as its own prop (not part of `data`) and shows a skeleton until it lands.
  const grammar = Array.isArray(grammarProp) ? grammarProp : [];
  const summary = String(data?.summary || '').trim();
  const hasData = !!data && !loading && !errorMsg;

  return (
    <div className="explain-modal-overlay" role="dialog" aria-modal="true">
      <button type="button" className="explain-modal-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="explain-modal" onClick={(e) => e.stopPropagation()}>
        <div className="explain-modal-head">
          <div className="explain-modal-head-copy">
            <div className="explain-modal-eyebrow">🧩 {tr('Разбор ошибок', 'Fehleranalyse')}</div>
            <h3 className="explain-modal-title">
              {tr('Предложение', 'Satz')} {satz ?? '—'}
              {Number.isFinite(Number(score)) ? <span className="explain-modal-score"> · {score}/100</span> : null}
            </h3>
          </div>
          <button type="button" className="explain-modal-close" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose}>×</button>
        </div>

        <label className="explain-modal-langtoggle">
          <input type="checkbox" checked={!!langDe} onChange={(e) => onToggleLang?.(e.target.checked)} />
          <span>🇩🇪 {tr('Объяснение на немецком', 'Erklärung auf Deutsch')}</span>
        </label>

        <div className="explain-modal-body">
          {loading && (
            <div className="explain-modal-loading">
              <div className="explain-skel" /><div className="explain-skel sm" /><div className="explain-skel" />
              <p className="explain-modal-muted">{tr('Готовим разбор…', 'Analyse wird erstellt…')}</p>
            </div>
          )}

          {!loading && errorMsg && (
            <div className="explain-modal-errorbox">⚠️ {errorMsg}</div>
          )}

          {hasData && (
            <>
              {summary && (
                <div className="explain-section explain-summary">
                  <div className="explain-section-title">📝 {tr('Кратко', 'Kurz')}</div>
                  <p>{summary}</p>
                </div>
              )}

              {errors.length > 0 ? (
                <div className="explain-section">
                  <div className="explain-section-title">🔍 {tr('Разбор по ошибкам', 'Fehler im Detail')}</div>
                  {errors.map((err, i) => {
                    const meta = TYPE_META[err.type] || TYPE_META.grammar;
                    return (
                      <div className={`explain-error-card ${meta.cls}`} key={i}>
                        <div className="explain-error-head">
                          <span className="explain-error-num">{i + 1}</span>
                          <span className={`explain-error-type ${meta.cls}`}>{typeLabel(err.type, langDe)}</span>
                        </div>
                        {(err.your || err.correct) && (
                          <div className="explain-frag-row">
                            {err.your ? <span className="explain-frag-bad">{err.your}</span> : null}
                            {err.your && err.correct ? <span className="explain-frag-arrow">→</span> : null}
                            {err.correct ? <span className="explain-frag-good">✅ {err.correct}</span> : null}
                          </div>
                        )}
                        {err.why && (
                          <div className="explain-line"><span className="explain-line-ico">💡</span><span>{err.why}</span></div>
                        )}
                        {err.rule && (
                          <div className="explain-line"><span className="explain-line-ico">📖</span><span><b>{tr('Правило', 'Regel')}:</b> {err.rule}</span></div>
                        )}
                        {err.example && (
                          <div className="explain-line explain-example"><span className="explain-line-ico">📌</span><span>{err.example}</span></div>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="explain-section explain-noerrors">✅ {tr('Грубых ошибок нет — отличная работа!', 'Keine groben Fehler — super gemacht!')}</div>
              )}

              {alternatives.length > 0 && (
                <div className="explain-section">
                  <div className="explain-section-title">🔁 {tr('Как ещё можно сказать', 'Alternative Formulierungen')}</div>
                  {alternatives.map((alt, i) => (
                    <div className="explain-alt" key={i}>
                      <span className="explain-alt-variant">{alt.variant}</span>
                      {alt.note ? <span className="explain-alt-note"> — {alt.note}</span> : null}
                    </div>
                  ))}
                </div>
              )}

              {synonyms.length > 0 && (
                <div className="explain-section">
                  <div className="explain-section-title">🔤 {tr('Синонимы', 'Synonyme')}</div>
                  {synonyms.map((syn, i) => (
                    <div className="explain-syn" key={i}>
                      <b className="explain-syn-word">{syn.word}</b>
                      <span className="explain-syn-arrow"> → </span>
                      <span className="explain-syn-opts">{(syn.options || []).join(', ')}</span>
                    </div>
                  ))}
                </div>
              )}

              {(grammar.length > 0 || grammarLoading) && (
                <div className="explain-section">
                  <div className="explain-section-title">🧠 {tr('Грамматика правильного варианта', 'Grammatik der richtigen Version')}</div>
                  {grammar.length === 0 && grammarLoading && (
                    <div className="explain-grammar-loading">
                      <div className="explain-skel" /><div className="explain-skel sm" />
                      <p className="explain-modal-muted">{tr('Разбираем грамматику…', 'Grammatik wird analysiert…')}</p>
                    </div>
                  )}
                  {grammar.map((g, i) => (
                    <div className="explain-grammar-card" key={i}>
                      <div className="explain-grammar-head">
                        <span className="explain-grammar-num">{i + 1}</span>
                        {g.part ? <span className="explain-grammar-part">{g.part}</span> : null}
                      </div>
                      {g.structure && (
                        <div className="explain-grammar-structure">🧩 {g.structure}</div>
                      )}
                      {g.note && (
                        <div className="explain-line"><span className="explain-line-ico">💡</span><span>{g.note}</span></div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {children ? <div className="explain-modal-followup">{children}</div> : null}
        </div>
      </div>
    </div>
  );
}
