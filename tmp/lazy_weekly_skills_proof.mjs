import fs from 'node:fs';
import { chromium } from '/Users/alexandr/.npm/_npx/e41f203b7505f1fb/node_modules/playwright/index.mjs';

const appUrl = 'https://backendwebbackendserverpy-production.up.railway.app/?mode=webapp';
const initData = fs.readFileSync('/tmp/lazy_translation_bootstrap_init_data.txt', 'utf8').trim();
const chromeExecutablePath = '/Users/alexandr/Library/Caches/ms-playwright/chromium-1217/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing';
const trackedPaths = [
  '/api/progress/weekly-plan',
  '/api/progress/skills',
  '/api/progress/plan-analytics',
];

const defaultHomeHits = new Set();
const weeklyHits = new Set();
const skillsHits = new Set();
const errors = [];
let phase = 'home';

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
    if (phase === 'home') {
      defaultHomeHits.add(path);
    } else if (phase === 'weekly') {
      weeklyHits.add(path);
    } else if (phase === 'skills') {
      skillsHits.add(path);
    }
  }
});

const pointerClick = async (locator) => {
  await locator.evaluate((node) => {
    const fire = (type) => node.dispatchEvent(new PointerEvent(type, {
      bubbles: true,
      cancelable: true,
      composed: true,
      pointerType: 'mouse',
      isPrimary: true,
      button: 0,
      buttons: 1,
    }));
    fire('pointerdown');
    fire('pointerup');
    node.dispatchEvent(new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      composed: true,
      button: 0,
      buttons: 0,
    }));
  });
};

const domClickSelector = async (selector) => (
  await page.evaluate((targetSelector) => {
    const node = document.querySelector(targetSelector);
    if (!node) return false;
    node.click();
    return true;
  }, selector)
);

const waitForTrackedPath = async (path) => {
  const source = path === '/api/progress/weekly-plan' ? weeklyHits : skillsHits;
  if (source.has(path)) {
    return;
  }
  await page.waitForFunction(
    (targetPath) => {
      const entries = performance.getEntriesByType('resource');
      return entries.some((entry) => String(entry?.name || '').includes(targetPath));
    },
    path,
    { timeout: 20000 }
  );
};

const ensureMoreTilesOpen = async () => {
  const moreTitle = page.locator('.hdt-more-title').first();
  if (await moreTitle.isVisible().catch(() => false)) {
    return;
  }
  const quickBar = page.locator('.hdt-quick-bar').first();
  if (await quickBar.isVisible().catch(() => false)) {
    await domClickSelector('.hdt-quick-bar');
    await page.waitForSelector('.hdt-more-title', { timeout: 20000 });
    return;
  }
  const topbarHomeButton = page.locator('.topbar-home-button').first();
  if (await topbarHomeButton.isVisible().catch(() => false)) {
    await pointerClick(topbarHomeButton);
    await page.waitForTimeout(800);
    if (await moreTitle.isVisible().catch(() => false)) {
      return;
    }
  }
  if (await quickBar.isVisible().catch(() => false)) {
    await domClickSelector('.hdt-quick-bar');
    await page.waitForSelector('.hdt-more-title', { timeout: 20000 });
  } else {
    await page.waitForSelector('.hdt-quick-bar', { timeout: 20000 });
    await domClickSelector('.hdt-quick-bar');
    await page.waitForSelector('.hdt-more-title', { timeout: 20000 });
  }
};

await page.goto(appUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });

let defaultHomeOpened = false;
let weeklyOpened = false;
let skillsOpened = false;

try {
  await page.waitForSelector('.hdt-root', { timeout: 20000 });
  defaultHomeOpened = true;
} catch (error) {
  errors.push(`home_selector_timeout:${String(error?.message || error)}`);
}

if (defaultHomeOpened) {
  await page.waitForTimeout(5000);

  phase = 'weekly';
  try {
    await ensureMoreTilesOpen();
    await pointerClick(page.locator('.hdt-root .hdt-grid .hdt-tile').filter({ hasText: /План|Wochen/i }).first());
    await page.waitForSelector('.weekly-plan-panel', { timeout: 20000 });
    await waitForTrackedPath('/api/progress/weekly-plan');
    weeklyOpened = true;
  } catch (error) {
    errors.push(`weekly_open_failed:${String(error?.message || error)}`);
  }

  phase = 'skills';
  try {
    await ensureMoreTilesOpen();
    await pointerClick(page.locator('.hdt-root .hdt-grid .hdt-tile').filter({ hasText: /Карта|Skill/i }).first());
    await page.waitForSelector('.skill-report-panel', { timeout: 20000 });
    await waitForTrackedPath('/api/progress/skills');
    skillsOpened = true;
  } catch (error) {
    errors.push(`skills_open_failed:${String(error?.message || error)}`);
  }
}

const visibleErrors = await page.locator('.webapp-error').allInnerTexts().catch(() => []);

const result = {
  default_home_opened: defaultHomeOpened,
  weekly_opened: weeklyOpened,
  skills_opened: skillsOpened,
  default_home: Object.fromEntries(trackedPaths.map((path) => [path, defaultHomeHits.has(path)])),
  weekly_after_open: Object.fromEntries(trackedPaths.map((path) => [path, weeklyHits.has(path)])),
  skills_after_open: Object.fromEntries(trackedPaths.map((path) => [path, skillsHits.has(path)])),
  visible_errors: visibleErrors,
  errors,
};

fs.writeFileSync('/tmp/lazy_weekly_skills_proof_result.json', JSON.stringify(result, null, 2));
console.log(JSON.stringify(result));

await browser.close();
