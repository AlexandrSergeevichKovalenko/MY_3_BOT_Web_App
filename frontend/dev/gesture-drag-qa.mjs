// Сценарий прогона стенда жеста выделения. Команда — в dev/gesture-check.html.
export default async function run(page) {
  const client = await page.context().newCDPSession(page);
  await client.send('Emulation.setTouchEmulationEnabled', { enabled: true, maxTouchPoints: 1 });
  await page.waitForSelector('.reader-word');

  const box = (idx) => page.evaluate((i) => {
    const el = document.querySelectorAll('.reader-word')[i];
    const r = el.getBoundingClientRect();
    return { x: r.x + r.width / 2, y: r.y + r.height / 2, text: el.textContent };
  }, idx);
  const touch = (type, p) => client.send('Input.dispatchTouchEvent', {
    type,
    touchPoints: type === 'touchEnd' ? [] : [{ x: p.x, y: p.y }],
  });
  const state = () => page.evaluate(() => ({
    selected: Array.from(document.querySelectorAll('.reader-word.is-selected')).map((el) => el.textContent),
    calls: JSON.parse(document.getElementById('calls').textContent || '[]'),
    scrollTop: document.getElementById('scroller').scrollTop,
    probe: JSON.parse(document.getElementById('probe').textContent || '{}'),
  }));

  const out = {};
  const w = [await box(0), await box(1), await box(2), await box(3)];

  // ── A. Удержание + протяжка по четырём словам ──────────────────────────────
  await touch('touchStart', w[0]);
  await page.waitForTimeout(450);
  out.afterHold = await state();
  for (const target of [w[1], w[2], w[3]]) {
    await touch('touchMove', target);
    await page.waitForTimeout(60);
  }
  out.afterDrag = await state();
  // Пока жест наш — вертикальное движение НЕ должно прокручивать контейнер.
  await touch('touchMove', { x: w[3].x, y: w[3].y - 90 });
  await page.waitForTimeout(80);
  out.scrollWhileSelecting = (await state()).scrollTop;
  await touch('touchMove', w[3]);
  await touch('touchEnd', w[3]);
  await page.waitForTimeout(150);
  out.afterRelease = await state();

  // ── B. Без удержания палец сразу поехал вниз — это прокрутка ───────────────
  await page.evaluate(() => { document.getElementById('calls').textContent = '[]'; });
  await touch('touchStart', w[0]);
  for (let i = 1; i <= 6; i += 1) {
    await touch('touchMove', { x: w[0].x, y: w[0].y - i * 20 });
    await page.waitForTimeout(20);
  }
  await touch('touchEnd', { x: w[0].x, y: w[0].y - 120 });
  await page.waitForTimeout(200);
  out.scrollGesture = await state();

  // ── C. Обычный тап по слову ────────────────────────────────────────────────
  await page.evaluate(() => { document.getElementById('scroller').scrollTop = 0; document.getElementById('calls').textContent = '[]'; });
  await page.waitForTimeout(100);
  const w1 = await box(1);
  await touch('touchStart', w1);
  await page.waitForTimeout(80);
  await touch('touchEnd', w1);
  await page.waitForTimeout(250);
  out.tap = await state();

  return out;
}
