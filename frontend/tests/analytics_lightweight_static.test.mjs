import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appPath = path.resolve(__dirname, '../src/App.jsx');
const source = fs.readFileSync(appPath, 'utf8');

function functionBody(name) {
  const marker = `const ${name} =`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${name} not found`);
  const nextFunction = source.indexOf('\n  const ', start + marker.length);
  assert.notEqual(nextFunction, -1, `${name} end not found`);
  return source.slice(start, nextFunction);
}

[
  'loadAnalyticsScope',
  'loadProgressResetStatus',
  'loadAnalyticsSummary',
  'loadAnalyticsTimeseries',
  'loadAnalyticsCompare',
  'loadWeeklySummarySocialSignal',
].forEach((name) => {
  assert.match(functionBody(name), /isKnownFreePaidSurfaceMode/, `${name} must short-circuit known FREE users`);
});

assert.match(
  functionBody('handleAnalyticsScopeSelect'),
  /isKnownFreePaidSurfaceMode/,
  'handleAnalyticsScopeSelect must not save analytics scope for known FREE users',
);
assert.match(
  functionBody('applyProgressReset'),
  /isKnownFreePaidSurfaceMode/,
  'applyProgressReset must not call progress-reset/apply for known FREE users',
);

assert.match(
  source,
  /analyticsSurfaceProRequired \? \(\s*renderPaidFeatureNotice\(analyticsPaidFeatureError/s,
  'Analytics section must render existing paid-feature card when Analytics requires Pro',
);

console.log('analytics lightweight static checks passed');
