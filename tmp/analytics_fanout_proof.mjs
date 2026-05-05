import fs from 'node:fs';
import { chromium } from '/Users/alexandr/.npm/_npx/e41f203b7505f1fb/node_modules/playwright/index.mjs';

const appUrl = 'https://backendwebbackendserverpy-production.up.railway.app/?mode=webapp';
const initData = fs.readFileSync('/tmp/lazy_translation_bootstrap_init_data.txt', 'utf8').trim();
const chromeExecutablePath = '/Users/alexandr/Library/Caches/ms-playwright/chromium-1217/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing';
const trackedPaths = [
  '/api/webapp/analytics/scope',
  '/api/webapp/progress-reset/status',
  '/api/webapp/analytics/summary',
  '/api/webapp/analytics/timeseries',
  '/api/webapp/analytics/compare',
];

const counts = {
  initial: new Map(),
  deferred: new Map(),
};
const errors = [];
let phase = 'initial';

const browser = await chromium.launch({ headless: true, executablePath: chromeExecutablePath });
const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
await context.addInitScript((storedInitData) => {
  window.localStorage.setItem('browser_init_data', storedInitData);
}, initData);

const page = await context.newPage();
page.on('pageerror', (error) => {
  errors.push(String(error?.message || error));
});
page.on('console', (message) => {
  if (message.type() === 'error') {
    errors.push(message.text());
  }
});
page.on('request', (request) => {
  const url = request.url();
  for (const path of trackedPaths) {
    if (!url.includes(path)) continue;
    const target = counts[phase] || counts.initial;
    target.set(path, (target.get(path) || 0) + 1);
  }
});

const pointerClick = async (locator) => {
  await locator.evaluate((node) => {
    const firePointer = (type, buttons) => node.dispatchEvent(new PointerEvent(type, {
      bubbles: true,
      cancelable: true,
      composed: true,
      pointerType: 'mouse',
      isPrimary: true,
      button: 0,
      buttons,
    }));
    firePointer('pointerdown', 1);
    firePointer('pointerup', 0);
    node.dispatchEvent(new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      composed: true,
      button: 0,
      buttons: 0,
    }));
  });
};

await page.goto(appUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

let analyticsOpened = false;
let defaultContentWorks = false;
let deferredLoadsWorked = false;

try {
  await page.waitForSelector('.hdt-root', { timeout: 20000 });
  await pointerClick(page.locator('.hdt-grid .hdt-tile').filter({ hasText: /Analytics|Аналитика/i }).first());
  await page.waitForSelector('.webapp-section.webapp-analytics', { timeout: 20000 });
  analyticsOpened = true;
  await page.waitForFunction(
    () => performance.getEntriesByType('resource').some((entry) => String(entry?.name || '').includes('/api/webapp/analytics/summary')),
    { timeout: 20000 }
  );
  await page.waitForTimeout(1500);
  defaultContentWorks = await page.locator('.analytics-card-groups').isVisible().catch(() => false);

  phase = 'deferred';
  await page.locator('.analytics-chart').first().scrollIntoViewIfNeeded();
  await page.waitForTimeout(1200);
  await page.locator('.analytics-compare-sections').scrollIntoViewIfNeeded();
  await page.waitForFunction(
    () => {
      const entries = performance.getEntriesByType('resource').map((entry) => String(entry?.name || ''));
      return entries.some((name) => name.includes('/api/webapp/analytics/timeseries'))
        || entries.some((name) => name.includes('/api/webapp/analytics/compare'));
    },
    { timeout: 20000 }
  );
  await page.waitForTimeout(1500);
  deferredLoadsWorked = (
    (counts.deferred.get('/api/webapp/analytics/timeseries') || 0) > 0
    || (counts.deferred.get('/api/webapp/analytics/compare') || 0) > 0
  );
} catch (error) {
  errors.push(String(error?.message || error));
}

const visibleErrors = await page.locator('.webapp-error').allInnerTexts().catch(() => []);
const initialTotal = trackedPaths.reduce((sum, path) => sum + (counts.initial.get(path) || 0), 0);

const result = {
  analytics_opened: analyticsOpened,
  initial_request_counts: Object.fromEntries(trackedPaths.map((path) => [path, counts.initial.get(path) || 0])),
  deferred_request_counts: Object.fromEntries(trackedPaths.map((path) => [path, counts.deferred.get(path) || 0])),
  initial_total_requests: initialTotal,
  initial_requests_reduced: initialTotal < 5,
  default_visible_analytics_content_works: defaultContentWorks,
  deferred_data_loads_when_needed: deferredLoadsWorked,
  visible_errors: visibleErrors,
  errors,
};

fs.writeFileSync('/tmp/analytics_fanout_proof_result.json', JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));

await browser.close();
