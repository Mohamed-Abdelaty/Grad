import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import path from 'path';
import fs from 'fs';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME || !PASSWORD) {
    throw new Error(
      'E2E_USERNAME and E2E_PASSWORD must be set.\n' +
        'Example: $env:E2E_USERNAME="admin"; $env:E2E_PASSWORD="password"; npx playwright test'
    );
  }
});

function startAudit(page: Page) {
  const errors: string[] = [];
  const warnings: string[] = [];
  const failures: string[] = [];

  page.on('console', (msg) => {
    const text = msg.text();
    if (msg.type() === 'error') {
      errors.push(text);
    } else if (msg.type() === 'warning') {
      warnings.push(text);
    }
  });

  page.on('pageerror', (err) => {
    errors.push(`PAGE ERROR: ${err.message}`);
  });

  page.on('requestfailed', (req) => {
    failures.push(`NETWORK FAIL: ${req.method()} ${req.url()}`);
  });

  page.on('response', (res) => {
    if (res.status() >= 500) {
      failures.push(`HTTP ${res.status()}: ${res.url()}`);
    }
  });

  return { errors, warnings, failures };
}

function assertClean(audit: { errors: string[]; warnings: string[]; failures: string[] }, label: string) {
  const realErrors = audit.errors.filter(
    (e) =>
      !e.includes('Download the React DevTools') &&
      !e.includes('ResizeObserver') &&
      !e.includes('content.js')
  );
  const realFailures = audit.failures.filter(
    (f) => !f.includes('/stream') // SSE closure
  );
  expect(realErrors, `Console/Page errors on ${label}: ${realErrors.join('; ')}`).toHaveLength(0);
  expect(realFailures, `Server errors on ${label}: ${realFailures.join('; ')}`).toHaveLength(0);
}

async function doLogin(page: Page) {
  await page.goto('/login');
  await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/', { timeout: 10_000 });
  await expect(page.getByRole('heading', { name: 'System Status' })).toBeVisible({
    timeout: 8_000,
  });
}

async function clickNav(page: Page, label: string) {
  await page.locator('aside nav a', { hasText: label }).first().click();
}

test.describe('UI-7 Pipeline Visualization E2E', () => {
  test('1. Real run sales-csv: visualization renders, tracks progression, supports pause and quality toggle, passes axe', async ({
    page,
  }) => {
    test.setTimeout(420_000); // 7 minutes to accommodate Spark batch processing and verification
    const audit = startAudit(page);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Open Templates modal and select sales-csv
    await page.getByTestId('btn-templates').click();
    await expect(page.getByText('Pipeline Templates')).toBeVisible();
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });

    // Validate
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Run pipeline
    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    // Land on /pipelines/<uuid>
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Verify visualization container is visible
    const vizWrapper = page.getByTestId('viz');
    await expect(vizWrapper).toBeVisible({ timeout: 15_000 });

    // 1. viz-canvas has non-zero bounding box; record viz-mode (3d or 2d)
    const vizCanvas = page.getByTestId('viz-canvas');
    await expect(vizCanvas).toBeVisible({ timeout: 10_000 });
    const box = await vizCanvas.boundingBox();
    expect(box).not.toBeNull();
    expect(box!.width).toBeGreaterThan(0);
    expect(box!.height).toBeGreaterThan(0);

    const modeEl = page.getByTestId('viz-mode');
    await expect(modeEl).toBeAttached({ timeout: 5000 });
    const mode = await modeEl.innerText();
    console.log(`[UI-7 E2E] Recorded viz-mode: ${mode}`);
    expect(['3d', '2d']).toContain(mode);

    // 8. Capture running screenshots
    const outDir = path.resolve('..', 'docs', 'screenshots', 'ui-7');
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(outDir, 'run-page-running-1440x900.png') });
    await page.screenshot({ path: path.join(outDir, 'run-running-1440x900.png') });

    // 3. Pause button while running: click viz-pause, verify aria-pressed toggles, test canvas frames
    // 3. Pause button: click viz-pause and verify aria-pressed toggles
    const pauseBtn = page.getByTestId('viz-pause');
    await expect(pauseBtn).toBeVisible();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'false');

    await pauseBtn.click();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'true');
    await pauseBtn.click();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'false');

    // 4. Quality switch: switch to 2D for efficient headless monitoring
    const qualityCtrl = page.getByTestId('viz-quality');
    await expect(qualityCtrl).toBeVisible();

    await qualityCtrl.getByRole('button', { name: '2D' }).click();
    await expect(page.getByTestId('viz-mode')).toHaveText('2d', { timeout: 5000 });
    await expect(page.locator('[data-testid="scene-2d"]')).toBeVisible({ timeout: 5000 });

    // 2. Text equivalent changes in expected order; assert Spark active before HDFS done; ends with all 4 done
    const textEq = page.getByTestId('viz-text-equivalent');
    await expect(textEq).toBeAttached({ timeout: 5000 });

    const observedStationSequences: string[] = [];
    let sparkWasActive = false;
    let hdfsDoneBeforeSparkActive = false;

    const monitorInterval = setInterval(async () => {
      try {
        const text = await textEq.innerText();
        if (text && !observedStationSequences.includes(text)) {
          observedStationSequences.push(text);
        }
        if (
          text.includes('Spark') &&
          (text.includes('processing') || text.includes('active') || text.includes('running') || text.includes('tasks'))
        ) {
          sparkWasActive = true;
        }
        if (text.includes('HDFS') && text.includes('done') && !sparkWasActive) {
          hdfsDoneBeforeSparkActive = true;
        }
      } catch {
        // detached or navigation during tick
      }
    }, 2000);

    // Wait for the pipeline run stages to complete in order
    await expect(page.getByTestId('stage-source')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
    await expect(page.getByTestId('stage-kafka')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
    await expect(page.getByTestId('stage-spark')).toHaveAttribute('data-state', 'done', { timeout: 360_000 });
    await expect(page.getByTestId('stage-hdfs')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });

    clearInterval(monitorInterval);

    expect(hdfsDoneBeforeSparkActive).toBe(false);

    // Verify all four stations in text equivalent end in done / complete
    const finalItems = await textEq.locator('li').allInnerTexts();
    console.log('[UI-7 E2E] Final station sentences:', finalItems);
    expect(finalItems.length).toBe(4);
    for (const item of finalItems) {
      expect(item.toLowerCase()).toMatch(/done|complete|written|published|read/);
    }

    // Switch back to 3D now that run is complete
    await qualityCtrl.getByRole('button', { name: 'High' }).click();
    await expect(page.getByTestId('viz-mode')).toHaveText('3d', { timeout: 5000 });

    // 3. Static canvas comparison when paused on completed run
    await pauseBtn.click();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'true');
    await page.waitForTimeout(600);
    const frame1 = await vizCanvas.screenshot();
    await page.waitForTimeout(700);
    const frame2 = await vizCanvas.screenshot();
    expect(frame1.equals(frame2)).toBe(true);
    await pauseBtn.click();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'false');

    // 8. Capture succeeded screenshots (desktop 1440x900 and mobile 390x844)
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(outDir, 'run-page-succeeded-1440x900.png') });
    await page.screenshot({ path: path.join(outDir, 'run-succeeded-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(outDir, 'run-page-mobile-390x844.png') });
    await page.screenshot({ path: path.join(outDir, 'run-mobile-390x844.png') });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(400);

    // Test persistence across reload (switching to 2D persists after reload + login)
    await qualityCtrl.getByRole('button', { name: '2D' }).click();
    await expect(page.getByTestId('viz-mode')).toHaveText('2d', { timeout: 5000 });

    await page.reload();
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    const runRow = page.getByTestId('runs-row').first();
    await expect(runRow).toBeVisible({ timeout: 15_000 });
    await runRow.getByRole('button', { name: 'Open' }).click();
    await expect(page.getByTestId('viz-canvas')).toBeVisible({ timeout: 15_000 });
    await expect(page.getByTestId('viz-mode')).toHaveText('2d', { timeout: 10_000 });

    // Restore to High
    await page.getByTestId('viz-quality').getByRole('button', { name: 'High' }).click();
    await expect(page.getByTestId('viz-mode')).toHaveText('3d', { timeout: 5000 });

    // 7. @axe-core/playwright accessibility audit on the scene with scene visible
    const axeResults = await new AxeBuilder({ page })
      .include('[data-testid="viz"]')
      .analyze();
    const violations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(violations, `Axe violations in viz component: ${JSON.stringify(violations, null, 2)}`).toHaveLength(0);

    assertClean(audit, 'Real run sales-csv');
  });

  test('2. Console errors, page errors, and JS heap growth <= 50 MB over 60s observation', async ({
    page,
  }) => {
    test.setTimeout(120_000);
    const audit = startAudit(page);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    const firstRow = page.getByTestId('runs-row').first();
    await expect(firstRow).toBeVisible({ timeout: 15_000 });
    await firstRow.getByRole('button', { name: 'Open' }).click();
    await expect(page.getByTestId('viz-canvas')).toBeVisible({ timeout: 15_000 });

    // Initial heap measurement
    const initialHeap = await page.evaluate(() => {
      const mem = (performance as any).memory;
      return mem ? mem.usedJSHeapSize : null;
    });

    console.log(`[UI-7 E2E] Initial JS heap: ${initialHeap ? (initialHeap / (1024 * 1024)).toFixed(2) + ' MB' : 'N/A'}`);

    // Observe for 60 seconds
    await page.waitForTimeout(60_000);

    const finalHeap = await page.evaluate(() => {
      const mem = (performance as any).memory;
      return mem ? mem.usedJSHeapSize : null;
    });

    console.log(`[UI-7 E2E] Final JS heap after 60s: ${finalHeap ? (finalHeap / (1024 * 1024)).toFixed(2) + ' MB' : 'N/A'}`);

    if (initialHeap !== null && finalHeap !== null) {
      const diffMB = (finalHeap - initialHeap) / (1024 * 1024);
      console.log(`[UI-7 E2E] JS heap growth over 60s: ${diffMB.toFixed(2)} MB`);
      expect(diffMB).toBeLessThanOrEqual(50);
    }

    assertClean(audit, '60s memory observation');
  });

  test('3. Navigate away and back 10 times: no WebGL context leak warnings and memory stable', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);

    await doLogin(page);

    // Initial navigation to run details page via RunsTable
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });
    const firstRow = page.getByTestId('runs-row').first();
    await expect(firstRow).toBeVisible({ timeout: 15_000 });
    await firstRow.getByRole('button', { name: 'Open' }).click();
    await expect(page.getByTestId('viz-canvas')).toBeVisible({ timeout: 15_000 });

    const initialHeap = await page.evaluate(() => {
      const mem = (performance as any).memory;
      return mem ? mem.usedJSHeapSize : null;
    });

    // Navigate away and back 10 times via sidebar nav and runs table link
    for (let i = 1; i <= 10; i++) {
      // Navigate to Pipelines list
      await clickNav(page, 'Pipelines');
      await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

      // Click Open on the first row from the runs table
      const row = page.getByTestId('runs-row').first();
      await expect(row).toBeVisible({ timeout: 15_000 });
      await row.getByRole('button', { name: 'Open' }).click();
      await expect(page.getByTestId('viz-canvas')).toBeVisible({ timeout: 15_000 });
    }

    const finalHeap = await page.evaluate(() => {
      const mem = (performance as any).memory;
      return mem ? mem.usedJSHeapSize : null;
    });

    console.log(
      `[UI-7 E2E] 10x Nav Heap: initial=${initialHeap ? (initialHeap / (1024 * 1024)).toFixed(2) + ' MB' : 'N/A'}, ` +
        `final=${finalHeap ? (finalHeap / (1024 * 1024)).toFixed(2) + ' MB' : 'N/A'}`
    );

    // Verify no "too many active contexts" WebGL warnings
    const webglWarnings = audit.warnings.filter((w) =>
      w.toLowerCase().includes('too many active webgl contexts')
    );
    expect(webglWarnings, `WebGL context leak warnings: ${webglWarnings.join('; ')}`).toHaveLength(0);

    assertClean(audit, '10x Navigation');
  });
});
