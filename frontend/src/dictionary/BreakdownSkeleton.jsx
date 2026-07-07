import React from 'react';
import './dict.css';

/**
 * Shimmer placeholder for the streaming word breakdown. Each structured section of the
 * deep card (meta → meanings → grammar → examples → synonyms) streams in one at a time;
 * this renders a skeleton ONLY for the sections that have not arrived yet, so the card
 * looks like it is filling in rather than blank-then-pop. `arrived` is a Set of section
 * names already received ('head' | 'meanings' | 'grammar' | 'examples' | 'extra').
 */
const SkLine = ({ w }) => <span className="dq-sk-line" style={{ width: w }} />;

export default function BreakdownSkeleton({ arrived }) {
  const has = (name) => arrived && arrived.has && arrived.has(name);

  return (
    <div className="dq-skeleton" aria-hidden="true">
      {!has('head') && (
        <div className="dq-sk-meta">
          <span className="dq-sk-chip" />
          <span className="dq-sk-chip" />
          <span className="dq-sk-chip sm" />
        </div>
      )}

      {!has('meanings') && (
        <div className="dq-block dq-sk-block">
          <SkLine w="34%" />
          <SkLine w="88%" />
          <SkLine w="72%" />
          <SkLine w="80%" />
        </div>
      )}

      {!has('grammar') && (
        <div className="dq-block dq-sk-block">
          <SkLine w="30%" />
          <span className="dq-sk-box" />
        </div>
      )}

      {!has('examples') && (
        <div className="dq-block dq-sk-block">
          <SkLine w="26%" />
          <SkLine w="90%" />
          <SkLine w="66%" />
        </div>
      )}

      {!has('extra') && (
        <div className="dq-block dq-sk-block">
          <SkLine w="30%" />
          <div className="dq-sk-chips">
            <span className="dq-sk-pill" />
            <span className="dq-sk-pill" />
            <span className="dq-sk-pill" />
          </div>
        </div>
      )}

      <div className="dq-sk-hint">Собираю разбор…</div>
    </div>
  );
}
