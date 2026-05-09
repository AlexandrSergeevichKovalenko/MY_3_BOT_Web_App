import fs from 'node:fs';
import { chromium } from '/Users/alexandr/.npm/_npx/e41f203b7505f1fb/node_modules/playwright/index.mjs';

const appUrl = 'https://backendwebbackendserverpy-production.up.railway.app/?mode=webapp';
const initData = fs.readFileSync('/tmp/lazy_translation_bootstrap_init_data.txt', 'utf8').trim();
const chromeExecutablePath = '/Users/alexandr/Library/Caches/ms-playwright/chromium-1217/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing';
const trackedPaths = [
  '/api/webapp/topics',
  '/api/webapp/session',
  '/api/webapp/sentences',
];

const defaultHomeHits = new Set();
const afterTranslationsHits = new Set();
const errors = [];
let phase = 'home';

const browser = await chromium.launch({ headless: true, executablePath: chromeExecutablePath });
const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
await context.addInitScript((storedInitData) => {
  window.localStorage.setItem('browser_init_data', storedInitData);
}, initData);
await context.addInitScript(() => {
  const originalFetch = window.fetch.bind(window);
  window.__fetchCalls = [];
  window.fetch = async (...args) => {
    const input = args[0];
    const url = typeof input === 'string' ? input : input?.url;
    window.__fetchCalls.push(String(url || ''));
    return originalFetch(...args);
  };
});

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
    if (phase === 'home') {
      defaultHomeHits.add(path);
    } else {
      afterTranslationsHits.add(path);
    }
  }
});

const clickIfVisible = async (selector) => {
  const locator = page.locator(selector).first();
  const visible = await locator.isVisible().catch(() => false);
  if (!visible) {
    return false;
  }
  await locator.evaluate((node) => node.click());
  return true;
};

const waitForTrackedFetches = async () => {
  await page.waitForFunction(
    (requiredPaths) => {
      const calls = Array.isArray(window.__fetchCalls) ? window.__fetchCalls : [];
      return requiredPaths.every((path) => calls.some((url) => String(url || '').includes(path)));
    },
    trackedPaths,
    { timeout: 20000 }
  );
};

await page.goto(appUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });
let defaultHomeOpened = false;
let translationsOpened = false;
let pageTextSample = '';

try {
  await page.waitForSelector('.hdt-root', { timeout: 20000 });
  defaultHomeOpened = true;
} catch (error) {
  errors.push(`home_selector_timeout:${String(error?.message || error)}`);
  pageTextSample = (await page.locator('body').innerText().catch(() => '')).slice(0, 1200);
}

if (defaultHomeOpened) {
  await page.waitForTimeout(5000);
  phase = 'translations';
  try {
    await page.locator('.menu-item-translations').first().evaluate((node) => node.click());
    await page.waitForFunction(
      () => document.querySelector('.menu-item-translations')?.classList.contains('is-active') === true,
      { timeout: 20000 }
    );
    translationsOpened = true;
    await waitForTrackedFetches();
  } catch (error) {
    errors.push(`translations_click_timeout:${String(error?.message || error)}`);
  }
}

const result = {
  default_home_opened: defaultHomeOpened,
  translations_opened: translationsOpened,
  default_home: Object.fromEntries(trackedPaths.map((path) => [path, defaultHomeHits.has(path)])),
  translations_after_open: Object.fromEntries(trackedPaths.map((path) => [path, afterTranslationsHits.has(path)])),
  fetch_calls: await page.evaluate(() => Array.isArray(window.__fetchCalls) ? window.__fetchCalls : []).catch(() => []),
  visible_errors: await page.locator('.webapp-error').allInnerTexts().catch(() => []),
  errors,
  page_text_sample: pageTextSample,
};

fs.writeFileSync('/tmp/lazy_translation_bootstrap_proof_result.json', JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));

await browser.close();
