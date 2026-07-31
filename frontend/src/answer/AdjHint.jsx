import React from 'react';

// The «падеж, род, склонение» line IS the key to the answer in the endings games:
// without reading it there is nothing to derive the ending from. As grey fine
// print it slipped past the eye, so it gets a plaque of its own — split into
// parts, the gender tinted with its article colour (der/die/das) — and a glance
// is enough. Text comes from the backend as "Винительный, муж. род, сильное";
// anything unexpected falls back to a single part, so it can never blank out.
const GENDERS = [[/муж/i, 'm'], [/жен/i, 'f'], [/\bср\b|средн/i, 'n']];

export default function AdjHint({ text }) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  const parts = raw.split(',').map((p) => p.trim()).filter(Boolean);
  // Parts are told apart by colour and spacing, not by "•" separators: on a narrow
  // phone the plaque wraps to two lines, and a separator would dangle at the end
  // of the first one.
  return (
    <div className="adj-hint" key={raw}>
      {parts.map((p, i) => {
        const g = GENDERS.find(([re]) => re.test(p));
        return <span key={i} className={`adj-hint-part${g ? ` g-${g[1]}` : ''}`}>{p}</span>;
      })}
    </div>
  );
}
