import React, { useState, useRef, useEffect } from 'react';

// Lightweight markdown-ish renderer for the follow-up answer (GPT returns *bold*,
// **bold** and _Wort_ emphasis + blank-line paragraphs). Keeps it readable instead
// of dumping raw asterisks/underscores as plain text.
function renderInline(text) {
  const parts = [];
  const re = /(\*\*[^*]+\*\*|\*[^*\n]+\*|_[^_\n]+_)/g;
  let last = 0; let m; let k = 0;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index));
    const tok = m[0];
    if (tok.startsWith('**')) parts.push(<strong key={k++}>{tok.slice(2, -2)}</strong>);
    else if (tok.startsWith('*')) parts.push(<strong key={k++}>{tok.slice(1, -1)}</strong>);
    else parts.push(<em key={k++} className="story-md-term">{tok.slice(1, -1)}</em>);
    last = re.lastIndex;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts;
}

function renderRichAnswer(text) {
  return String(text || '').split(/\n{2,}/).map((para, i) => (
    <p className="story-md-p" key={i}>
      {para.split('\n').map((line, j) => (
        <React.Fragment key={j}>
          {j > 0 && <br />}
          {renderInline(line)}
        </React.Fragment>
      ))}
    </p>
  ));
}

function linkLabel(item) {
  const lang = (item.lang || '').toUpperCase();
  let host = '';
  try { host = new URL(item.url).hostname.replace(/^www\./, ''); } catch (_e) { /* ignore */ }
  return `${lang ? lang + ' · ' : ''}${host || item.url}`;
}

/**
 * "Загадочная история" result, shown as a wide centered modal (so it doesn't
 * stretch the page or get squeezed into vertical rails). Two-call flow:
 *   Part 1 (instant, from /story/submit): score + guess verdict + answer + links.
 *   Part 2/3 (progressive, from /story/explain): per-sentence teacher breakdown,
 *   recurring-mistake pattern, grammar focus and targeted practice sentences.
 * Reuses the proven `explain-modal` design system; adds a docked "Задать вопрос"
 * follow-up (same /api/webapp/explain/question endpoint as the translations modal).
 */

const TYPE_META = {
  grammar: { ru: 'Грамматика', de: 'Grammatik', emoji: '🧩', cls: 'is-grammar' },
  vocabulary: { ru: 'Лексика', de: 'Wortschatz', emoji: '📚', cls: 'is-vocab' },
  syntax: { ru: 'Синтаксис', de: 'Syntax', emoji: '🔧', cls: 'is-syntax' },
  style: { ru: 'Стиль', de: 'Stil', emoji: '🎨', cls: 'is-style' },
  orthography: { ru: 'Орфография', de: 'Rechtschreibung', emoji: '✍️', cls: 'is-ortho' },
};

const VERDICT_META = {
  correct: { cls: 'is-correct', emoji: '✅' },
  minor: { cls: 'is-minor', emoji: '🟡' },
  major: { cls: 'is-major', emoji: '🔴' },
};

function typeLabel(type, langDe) {
  const m = TYPE_META[type] || TYPE_META.grammar;
  return `${m.emoji} ${langDe ? m.de : m.ru}`;
}

/**
 * A tappable German fragment/word that saves straight into the dictionary — same
 * one-tap-save affordance the learner already has all over the app (translation
 * drafts, reader, YouTube selections). Optimistic: flips to ✓ instantly and the
 * canonical lookup→save runs in the background; on a real failure it shows ↻ to retry.
 */
function SaveDe({ text, onSaveWord, tr, prefix = '', className = '' }) {
  const [state, setState] = useState('idle'); // idle | saved | error
  const clean = String(text || '').trim();
  if (!clean) return null;
  const save = (e) => {
    if (e) { e.preventDefault(); e.stopPropagation(); }
    if (!onSaveWord || state === 'saved') return; // already saved → no-op (idle/error → attempt)
    setState('saved');
    Promise.resolve(onSaveWord(clean))
      .then((ok) => { if (ok === false) setState('error'); })
      .catch(() => setState('error'));
  };
  const ico = state === 'saved' ? '✓' : (state === 'error' ? '↻' : '＋');
  const title = state === 'saved'
    ? tr('Сохранено в словарь', 'Im Wörterbuch gespeichert')
    : (state === 'error' ? tr('Не сохранилось — нажмите ещё раз', 'Nicht gespeichert — nochmal tippen')
      : tr('Сохранить в словарь', 'Ins Wörterbuch speichern'));
  return (
    <button
      type="button"
      className={`story-save-de is-${state} ${className}`}
      onClick={save}
      title={title}
      aria-label={title}
    >
      {prefix ? <span className="story-save-de-prefix">{prefix}</span> : null}
      <span className="story-save-de-text">{clean}</span>
      <span className="story-save-de-ico" aria-hidden="true">{ico}</span>
    </button>
  );
}

function ErrorCard({ err, i, langDe, tr, onSaveWord }) {
  const meta = TYPE_META[err.type] || TYPE_META.grammar;
  return (
    <div className={`explain-error-card ${meta.cls}`}>
      <div className="explain-error-head">
        <span className="explain-error-num">{i + 1}</span>
        <span className={`explain-error-type ${meta.cls}`}>{typeLabel(err.type, langDe)}</span>
      </div>
      {(err.your || err.correct) && (
        <div className="explain-frag-row">
          {err.your ? <span className="explain-frag-bad">{err.your}</span> : null}
          {err.your && err.correct ? <span className="explain-frag-arrow">→</span> : null}
          {err.correct ? <SaveDe text={err.correct} onSaveWord={onSaveWord} tr={tr} prefix="✅" className="is-good" /> : null}
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
}

function SentenceBlock({ sent, langDe, tr, onSaveWord }) {
  const errors = Array.isArray(sent.errors) ? sent.errors : [];
  const alternatives = Array.isArray(sent.alternatives) ? sent.alternatives : [];
  const synonyms = Array.isArray(sent.synonyms) ? sent.synonyms : [];
  const vmeta = VERDICT_META[sent.verdict] || VERDICT_META.minor;
  return (
    <div className={`story-sent-card ${vmeta.cls}`}>
      <div className="story-sent-head">
        <span className="story-sent-num">{sent.index}</span>
        <span className="story-sent-verdict">{vmeta.emoji}</span>
      </div>
      {sent.good && (
        <div className="story-sent-good">👍 {sent.good}</div>
      )}
      {sent.correct && (
        <div className="story-sent-correct">
          <span className="story-sent-correct-label">{tr('Верный вариант', 'Korrekt')}:</span>{' '}
          <SaveDe text={sent.correct} onSaveWord={onSaveWord} tr={tr} className="is-sentence" />
        </div>
      )}
      {errors.length > 0 ? (
        errors.map((err, i) => <ErrorCard key={i} err={err} i={i} langDe={langDe} tr={tr} onSaveWord={onSaveWord} />)
      ) : (
        <div className="story-sent-ok">✅ {tr('Ошибок нет — отлично!', 'Keine Fehler — super!')}</div>
      )}
      {alternatives.length > 0 && (
        <div className="explain-section story-sent-sub">
          <div className="explain-section-title">🔁 {tr('Как ещё сказать', 'Alternativen')}</div>
          {alternatives.map((alt, i) => (
            <div className="explain-alt" key={i}>
              <SaveDe text={alt.variant} onSaveWord={onSaveWord} tr={tr} className="is-alt" />
              {alt.note ? <span className="explain-alt-note"> — {alt.note}</span> : null}
            </div>
          ))}
        </div>
      )}
      {synonyms.length > 0 && (
        <div className="explain-section story-sent-sub">
          <div className="explain-section-title">🔤 {tr('Синонимы', 'Synonyme')}</div>
          {synonyms.map((syn, i) => (
            <div className="explain-syn" key={i}>
              <SaveDe text={syn.word} onSaveWord={onSaveWord} tr={tr} className="is-synword" />
              <span className="explain-syn-arrow"> → </span>
              <span className="explain-syn-opts">
                {(syn.options || []).map((opt, j) => (
                  <SaveDe key={j} text={opt} onSaveWord={onSaveWord} tr={tr} className="is-synopt" />
                ))}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function StoryResultModal({
  isOpen,
  onClose,
  tr,
  storyResult,
  explanation,
  explanationLoading,
  explanationErr,
  langDe,
  onToggleLang,
  initData,
  storyContext,
  onSaveWord,
}) {
  const [qOpen, setQOpen] = useState(false);
  const [qDraft, setQDraft] = useState('');
  const [qLoading, setQLoading] = useState(false);
  const [qAnswer, setQAnswer] = useState('');
  const [qNotice, setQNotice] = useState('');
  const formRef = useRef(null);

  // When the question field opens, bring it into view + focus it (it renders below
  // the long breakdown, so users didn't notice it appeared).
  useEffect(() => {
    if (!qOpen) return;
    const t = window.setTimeout(() => {
      const form = formRef.current;
      if (!form) return;
      form.scrollIntoView({ behavior: 'smooth', block: 'center' });
      const ta = form.querySelector('textarea');
      if (ta) ta.focus({ preventScroll: true });
    }, 90);
    return () => window.clearTimeout(t);
  }, [qOpen]);

  if (!isOpen || !storyResult) return null;

  const sc = storyResult.arena_scores || {};
  const score = Number.isFinite(Number(storyResult.score)) ? storyResult.score : (sc.total ?? null);
  const guessOk = !!storyResult.guess_correct;
  const sentences = Array.isArray(explanation?.sentences) ? explanation.sentences : [];
  const pattern = explanation?.pattern || null;
  const grammarFocus = Array.isArray(explanation?.grammar_focus) ? explanation.grammar_focus : [];
  const practice = Array.isArray(explanation?.practice) ? explanation.practice : [];
  const summary = String(explanation?.summary || '').trim();
  const sourceLinks = Array.isArray(storyResult.source_links) ? storyResult.source_links : [];

  const bars = [
    { label: tr('Грамматика', 'Grammatik'), val: sc.grammar, max: 40 },
    { label: tr('Точность', 'Genauigkeit'), val: sc.accuracy, max: 20 },
    { label: tr('Стиль', 'Stil'), val: sc.style, max: 20 },
    { label: tr('Полнота', 'Vollständigkeit'), val: sc.completeness, max: 20 },
  ];

  const buildContextText = () => {
    const parts = [];
    if (summary) parts.push(`Итог: ${summary}`);
    sentences.forEach((s) => {
      const errs = (s.errors || []).map((e) => `${e.your || ''} → ${e.correct || ''} (${e.rule || ''})`).join('; ');
      parts.push(`Предложение ${s.index}: правильно «${s.correct || ''}». ${errs ? 'Ошибки: ' + errs : 'без ошибок'}`);
    });
    if (pattern?.detected) parts.push(`Повторяющийся паттерн: ${pattern.title}. ${pattern.explanation}`);
    return parts.join('\n');
  };

  const askQuestion = async () => {
    const q = qDraft.trim();
    // Здесь мы уже внутри модалки — второе окно поверх неё было бы хуже, поэтому
    // подсказка остаётся на месте, но спокойной плашкой, а не красной «ошибкой».
    if (!q) { setQNotice(tr('Напишите свой вопрос в поле выше — и я отвечу.', 'Schreib deine Frage ins Feld oben — dann antworte ich.')); return; }
    if (!initData) { setQNotice(tr('Откройте приложение из чата с ботом в Telegram — тогда всё заработает.', 'Öffne die App aus dem Bot-Chat in Telegram — dann läuft alles.')); return; }
    setQLoading(true);
    setQNotice('');
    try {
      const response = await fetch('/api/webapp/explain/question', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          original_text: storyContext?.originalText || '',
          user_translation: storyContext?.userText || '',
          explanation: buildContextText(),
          learner_question: q,
          request_id: `story-q:${Date.now()}`,
        }),
      });
      if (!response.ok) {
        let message = await response.text();
        try { message = JSON.parse(message).error || message; } catch (_e) { /* ignore */ }
        throw new Error(message);
      }
      const data = await response.json();
      setQAnswer(String(data.answer || '').trim());
      setQOpen(false);
      setQDraft('');
    } catch (error) {
      try { console.warn('[story] question failed', error); } catch (_e) { /* noop */ }
      setQNotice(tr('Не удалось отправить вопрос. Попробуйте ещё раз.', 'Frage konnte nicht gesendet werden. Bitte erneut versuchen.'));
    } finally {
      setQLoading(false);
    }
  };

  return (
    <div className="explain-modal-overlay" role="dialog" aria-modal="true">
      <button type="button" className="explain-modal-backdrop" aria-label={tr('Закрыть', 'Schließen')} onClick={onClose} />
      <div className="explain-modal story-modal" onClick={(e) => e.stopPropagation()}>
        <div className="explain-modal-head">
          <div className="explain-modal-head-copy">
            <div className="explain-modal-eyebrow">🧩 {tr('Разбор истории', 'Story-Analyse')}</div>
            <h3 className="explain-modal-title">
              {tr('Загадочная история', 'Rätselhafte Geschichte')}
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
          {/* ── Part 1: instant итог ── */}
          <div className="story-modal-scorebars">
            {bars.map(({ label, val, max }) => (
              <div key={label} className="arena-bar-row">
                <span className="arena-bar-label">{label}</span>
                <div className="arena-bar-track">
                  <div className="arena-bar-fill" style={{ width: `${Math.max(0, Math.min(100, Math.round(((val || 0) / max) * 100)))}%` }} />
                </div>
                <span className="arena-bar-val">{val ?? 0}/{max}</span>
              </div>
            ))}
          </div>

          <div className={`story-modal-guess ${guessOk ? 'is-ok' : 'is-no'}`}>
            <div className="story-modal-guess-verdict">
              {guessOk ? '🎯' : '🤏'} {tr('Твоя догадка', 'Deine Vermutung')}: <b>{guessOk ? tr('верно по смыслу', 'inhaltlich richtig') : tr('мимо', 'daneben')}</b>
            </div>
            <div className="story-modal-guess-answer">
              <b>{tr('Ответ', 'Antwort')}:</b> {storyResult.answer || '—'}
            </div>
            {storyResult.guess_reason && (
              <div className="story-modal-guess-reason">{storyResult.guess_reason}</div>
            )}
          </div>

          {/* ── Part 2/3: progressive breakdown ── */}
          {explanationLoading && !explanation && (
            <div className="explain-modal-loading">
              <div className="explain-skel" /><div className="explain-skel sm" /><div className="explain-skel" />
              <p className="explain-modal-muted">{tr('Готовим подробный разбор по предложениям…', 'Detaillierte Satzanalyse wird erstellt…')}</p>
            </div>
          )}

          {!explanationLoading && explanationErr && (
            <div className="explain-modal-errorbox">{explanationErr}</div>
          )}

          {explanation && (
            <>
              {summary && (
                <div className="explain-section explain-summary">
                  <div className="explain-section-title">📝 {tr('Кратко', 'Kurz')}</div>
                  <p>{summary}</p>
                </div>
              )}

              {sentences.length > 0 && (
                <div className="explain-section">
                  <div className="explain-section-title">🧠 {tr('Разбор по предложениям', 'Satz für Satz')}</div>
                  <div className="story-save-hint">💾 {tr('Совет: коснитесь любого немецкого слова или фразы, чтобы сохранить в словарь.', 'Tipp: Tippe ein deutsches Wort oder eine Phrase an, um es im Wörterbuch zu speichern.')}</div>
                  {sentences.map((s) => <SentenceBlock key={s.index} sent={s} langDe={langDe} tr={tr} onSaveWord={onSaveWord} />)}
                </div>
              )}

              {pattern?.detected && (
                <div className="explain-section story-pattern">
                  <div className="explain-section-title">🔁 {tr('Твой повторяющийся паттерн', 'Dein wiederkehrendes Muster')}</div>
                  {pattern.title && <div className="story-pattern-title">{pattern.title}</div>}
                  {pattern.explanation && <p className="story-pattern-text">{pattern.explanation}</p>}
                  {(pattern.examples || []).map((ex, i) => (
                    <div className="explain-frag-row" key={i}>
                      {ex.wrong ? <span className="explain-frag-bad">{ex.wrong}</span> : null}
                      {ex.wrong && ex.right ? <span className="explain-frag-arrow">→</span> : null}
                      {ex.right ? <SaveDe text={ex.right} onSaveWord={onSaveWord} tr={tr} prefix="✅" className="is-good" /> : null}
                      {ex.note ? <span className="explain-alt-note"> — {ex.note}</span> : null}
                    </div>
                  ))}
                </div>
              )}

              {grammarFocus.length > 0 && (
                <div className="explain-section">
                  <div className="explain-section-title">📚 {tr('Грамматика для проработки', 'Grammatik zum Üben')}</div>
                  {grammarFocus.map((gf, i) => (
                    <div className="story-grammar-item" key={i}>
                      <div className="story-grammar-title">{gf.title}</div>
                      {gf.theory && <p className="story-grammar-theory">{gf.theory}</p>}
                      {(gf.examples || []).map((ex, j) => (
                        <div className="story-grammar-ex" key={j}>
                          <SaveDe text={ex.de} onSaveWord={onSaveWord} tr={tr} className="is-grammar" />
                          {ex.ru ? <span className="story-grammar-ru"> — {ex.ru}</span> : null}
                        </div>
                      ))}
                    </div>
                  ))}
                </div>
              )}

              {practice.length > 0 && (
                <div className="explain-section story-practice">
                  <div className="explain-section-title">✍️ {tr('Потренируйся', 'Übe selbst')}</div>
                  {practice.map((pr, i) => (
                    <div className="story-practice-item" key={i}>
                      <div className="story-practice-ru">{pr.ru}</div>
                      {pr.hint ? <div className="story-practice-hint">💡 {pr.hint}</div> : null}
                    </div>
                  ))}
                </div>
              )}
            </>
          )}

          {/* Material / source links — always kept */}
          {storyResult.extra_de && (
            <div className="explain-section">
              <div className="explain-section-title">📌 {tr('Дополнительно', 'Zusätzlich')}</div>
              <p className="explain-summary-p">{storyResult.extra_de}</p>
            </div>
          )}
          {sourceLinks.length > 0 && (
            <div className="explain-section">
              <div className="explain-section-title">🔗 {tr('Официальные источники', 'Offizielle Quellen')}</div>
              <ul className="story-modal-links">
                {sourceLinks.map((item, idx) => (
                  <li key={`${item.url}-${idx}`}>
                    <a href={item.url} target="_blank" rel="noreferrer">{linkLabel(item)}</a>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* ── Follow-up Q&A ── */}
          <div className="explain-modal-followup">
            <button
              type="button"
              className="secondary-button explanation-followup-button"
              onClick={() => setQOpen((v) => !v)}
              disabled={qLoading}
            >
              {qOpen ? tr('Скрыть вопрос', 'Frage ausblenden') : tr('💬 Задать вопрос', '💬 Frage stellen')}
            </button>
            {qOpen && (
              <div className="webapp-explanation-followup-form story-followup-form" ref={formRef}>
                <textarea
                  rows={3}
                  value={qDraft}
                  onChange={(e) => setQDraft(e.target.value)}
                  placeholder={tr('Спросите что-нибудь по этому разбору…', 'Stelle eine Rückfrage zu dieser Analyse…')}
                />
                <div className="webapp-explanation-followup-actions">
                  <button type="button" className="primary-button webapp-explanation-followup-action-button" onClick={askQuestion} disabled={qLoading || !qDraft.trim()}>
                    {qLoading ? tr('Спрашиваем…', 'Fragen…') : tr('Отправить вопрос', 'Frage senden')}
                  </button>
                  <button type="button" className="secondary-button webapp-explanation-followup-action-button" onClick={() => { setQOpen(false); setQDraft(''); }} disabled={qLoading}>
                    {tr('Отмена', 'Abbrechen')}
                  </button>
                </div>
              </div>
            )}
            {qNotice && <div className="explain-modal-errorbox">{qNotice}</div>}
            {qAnswer && (
              <div className="webapp-explanation-followup-answer story-followup-answer">
                <div className="story-followup-answer-label">💬 {tr('Ответ на ваш вопрос', 'Antwort auf deine Frage')}</div>
                <div className="story-followup-answer-text">{renderRichAnswer(qAnswer)}</div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
