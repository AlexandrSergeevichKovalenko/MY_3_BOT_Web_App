import React from 'react';

// Render Telegram-style inline markdown (*bold*, _italic_, `code`) as React nodes,
// so model answers show real bold/italic instead of leaking literal *…* / _…_.
// Newlines are preserved by the caller's CSS (white-space: pre-wrap).
export function renderRich(text) {
  const nodes = [];
  const re = /(\*[^*\n]+\*|_[^_\n]+_|`[^`\n]+`)/g;
  let last = 0;
  let key = 0;
  let m;
  const src = String(text || '');
  while ((m = re.exec(src)) !== null) {
    if (m.index > last) nodes.push(src.slice(last, m.index));
    const tok = m[0];
    const inner = tok.slice(1, -1);
    if (tok[0] === '*') nodes.push(<strong key={key++}>{inner}</strong>);
    else if (tok[0] === '_') nodes.push(<em key={key++}>{inner}</em>);
    else nodes.push(<code key={key++}>{inner}</code>);
    last = m.index + tok.length;
  }
  if (last < src.length) nodes.push(src.slice(last));
  return nodes;
}

export default renderRich;
