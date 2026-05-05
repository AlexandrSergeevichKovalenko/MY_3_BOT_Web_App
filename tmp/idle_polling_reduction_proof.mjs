import fs from 'node:fs';
import { chromium } from '/Users/alexandr/.npm/_npx/e41f203b7505f1fb/node_modules/playwright/index.mjs';

const appUrl = 'https://backendwebbackendserverpy-production.up.railway.app/?mode=webapp';
const initData = fs.readFileSync('/tmp/lazy_translation_bootstrap_init_data.txt', 'utf8').trim();
const chromeExecutablePath = '/Users/alexandr/Library/Caches/ms-playwright/chromium-1217/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing';
const trackedPaths = [
  '/api/webapp/instance/claim',
  '/api/webapp/support/unread',
  '/api/webapp/support/messages/list',
];

const phaseCounts = {
  visible: new Map(),
  hidden: new Map(),
  support: new Map(),
};
const errors = [];
let phase = 'visible';

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
    const targetMap = phaseCounts[phase] || phaseCounts.visible;
    targetMap.set(path, (targetMap.get(path) || 0) + 1);
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

let defaultHomeOpened = false;
let supportOpened = false;
let hiddenReduced = 'unknown';

try {
  await page.waitForSelector('.hdt-root', { timeout: 20000 });
  defaultHomeOpened = true;
} catch (error) {
  errors.push(`home_selector_timeout:${String(error?.message || error)}`);
}

if (defaultHomeOpened) {
  await page.waitForTimeout(3500);

  await page.evaluate(() => {
    const applyVisibility = (state) => {
      const hidden = state === 'hidden';
      const define = (target, key, getter) => {
        try {
          Object.defineProperty(target, key, { configurable: true, get: getter });
          return true;
        } catch (_error) {
          return false;
        }
      };
      if (!define(document, 'visibilityState', () => state)) {
        define(Document.prototype, 'visibilityState', () => state);
      }
      if (!define(document, 'hidden', () => hidden)) {
        define(Document.prototype, 'hidden', () => hidden);
      }
      document.dispatchEvent(new Event('visibilitychange'));
      window.dispatchEvent(new Event(hidden ? 'blur' : 'focus'));
      window.dispatchEvent(new Event(hidden ? 'pagehide' : 'pageshow'));
    };
    window.__setVisibilityForProof = applyVisibility;
  });

  phase = 'hidden';
  await page.evaluate(() => window.__setVisibilityForProof?.('hidden'));
  await page.waitForTimeout(32000);
  const hiddenInstanceCount = phaseCounts.hidden.get('/api/webapp/instance/claim') || 0;
  const hiddenUnreadCount = phaseCounts.hidden.get('/api/webapp/support/unread') || 0;
  hiddenReduced = hiddenInstanceCount === 0 && hiddenUnreadCount === 0 ? 'yes' : 'no';

  phase = 'visible';
  await page.evaluate(() => window.__setVisibilityForProof?.('visible'));
  await page.waitForTimeout(2500);

  phase = 'support';
  try {
    await pointerClick(page.locator('.hdt-grid .hdt-tile').filter({ hasText: /Support|Техподдержка/i }).first());
    await page.waitForSelector('.support-section', { timeout: 20000 });
    await page.waitForFunction(
      () => performance.getEntriesByType('resource').some((entry) => String(entry?.name || '').includes('/api/webapp/support/messages/list')),
      { timeout: 20000 }
    );
    supportOpened = true;
  } catch (error) {
    errors.push(`support_open_failed:${String(error?.message || error)}`);
  }
}

const visibleErrors = await page.locator('.webapp-error').allInnerTexts().catch(() => []);

const result = {
  default_home_opened: defaultHomeOpened,
  support_opened: supportOpened,
  counts: {
    visible: Object.fromEntries(trackedPaths.map((path) => [path, phaseCounts.visible.get(path) || 0])),
    hidden: Object.fromEntries(trackedPaths.map((path) => [path, phaseCounts.hidden.get(path) || 0])),
    support: Object.fromEntries(trackedPaths.map((path) => [path, phaseCounts.support.get(path) || 0])),
  },
  hidden_reduced: hiddenReduced,
  visible_errors: visibleErrors,
  errors,
};

fs.writeFileSync('/tmp/idle_polling_reduction_proof_result.json', JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));

await browser.close();
