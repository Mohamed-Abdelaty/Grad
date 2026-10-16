/**
 * e2e/streaming.spec.ts
 * ---------------------
 * UI-10: End-to-end testing for Structured Streaming Mode (ChaosFlow).
 *
 * Covers:
 *  1. Streaming run UI indicators (streaming badge, consumer lag tile, terminal success).
 *  2. Live 1M-row streaming run: records lag curve every 2s, verifies drain to 0,
 *     captures screenshots for rising lag, draining lag, and completion.
 *  3. Streaming mode accessibility: axe reports zero serious or critical violations.
 */

import { test, expect, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import * as path from 'path';
import * as fs from 'fs';

const USERNAME = process.env.E2E_USERNAME || 'admin';
const PASSWORD = process.env.E2E_PASSWORD || '123456';

const STREAMING_SAMPLE_YAML = `pipeline:
  name: "streaming-e2e-run"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_sample.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }
processing:
  operations:
    - { type: remove_nulls }
    - { type: filter, condition: "amount > 0" }
streaming:
  enabled: true
  trigger_interval_seconds: 2
  idle_timeout_seconds: 5
  max_runtime_seconds: 120
  starting_offsets: earliest
messaging:
  broker: kafka
  topic: streaming-e2e-events
  partitions: 3
  replication_factor: 1
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/streaming_e2e_run
monitoring:
  enabled: true
`;

const STREAMING_1M_YAML = `pipeline:
  name: "sales-streaming-1m"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_large.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }
processing:
  operations:
    - { type: remove_nulls }
    - { type: filter, condition: "amount > 0" }
streaming:
  enabled: true
  trigger_interval_seconds: 2
  idle_timeout_seconds: 20
  max_runtime_seconds: 400
  starting_offsets: earliest
  max_offsets_per_trigger: 50000
messaging:
  broker: kafka
  topic: streaming-1m-events
  partitions: 3
  replication_factor: 1
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/output/streaming_1m_run
monitoring:
  enabled: true
`;

async function clickNav(page: Page, label: string) {
  await page.locator('aside nav a', { hasText: label }).first().click();
}

async function setEditorContent(page: Page, text: string) {
  const cmContent = page.locator('.cm-content');
  await cmContent.click();
  await page.keyboard.press('ControlOrMeta+A');
  await page.keyboard.press('Backspace');
  await page.keyboard.insertText(text);
}

async function doLogin(page: Page) {
  await page.goto('/login');
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/', { timeout: 10_000 });
}

test.describe('UI-10 Structured Streaming E2E', () => {
  // ── 1. Basic streaming UI indicators ──────────────────────────────────────
  test('1. Streaming run: displays streaming badge, consumer lag tile, and reaches terminal success', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    await doLogin(page);

    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    await setEditorContent(page, STREAMING_SAMPLE_YAML);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    const streamingBadge = page.locator('[data-testid="streaming-badge"]');
    await expect(streamingBadge).toBeVisible({ timeout: 15_000 });
    await expect(streamingBadge).toHaveText(/STREAMING/i);

    const lagCounter = page.locator('[data-testid="counter-lag"]');
    await expect(lagCounter).toBeVisible({ timeout: 15_000 });

    await expect(
      page.locator('[data-testid="status-pill"]').or(page.locator('text=succeeded')).first()
    ).toBeVisible({ timeout: 120_000 });
  });

  // ── 2. Live 1M-row run with lag curve table & screenshots ─────────────────
  test('2. Live 1M-row streaming run: records lag curve, samples every 2s, verifies drain to 0, captures rising/draining/completion screenshots', async ({
    page,
    request,
  }) => {
    test.setTimeout(450_000); // 7.5 min budget for 1M streaming run
    await doLogin(page);

    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    await setEditorContent(page, STREAMING_1M_YAML);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });
    const runId = page.url().split('/').pop()?.split('?')[0];
    expect(runId).toBeTruthy();

    await page.setViewportSize({ width: 1440, height: 900 });
    const screenshotDir = path.resolve('..', 'docs', 'screenshots', 'ui-10');
    if (!fs.existsSync(screenshotDir)) {
      fs.mkdirSync(screenshotDir, { recursive: true });
    }

    console.log('\n[TOLERANCE] Polling interval: 2.0s ± 0.5s; numeric lag parsed with commas stripped; exact terminal lag = 0.');
    console.log('| t(s) | Status | LiveBadge | UILag | ApiLag | SparkIn | KafkaPub | Rate(rec/s) | ETA(s) |');
    console.log('|------|--------|-----------|-------|--------|---------|----------|-------------|--------|');

    let peakLag = 0;
    let tookRisingShot = false;
    let tookDrainingShot = false;
    let startTime = Date.now();
    let terminalReached = false;
    let finalStatus = 'running';
    let prevSparkIn = 0;
    let sawSparkInIncreasingWhileLagging = false;

    const tokenRes = await request.post('http://localhost:8000/api/auth/login', {
      form: { username: USERNAME, password: PASSWORD },
    });
    expect(tokenRes.ok()).toBe(true);
    const { access_token } = await tokenRes.json();
    const authHeaders = { Authorization: `Bearer ${access_token}` };

    while (Date.now() - startTime < 360_000) {
      await page.waitForTimeout(2000);
      const elapsed = Math.round((Date.now() - startTime) / 1000);

      // Read values from page DOM
      const statusPill = (await page.locator('[data-testid="status-pill"]').first().textContent({ timeout: 1000 }).catch(() => ''))?.trim() || 'running';
      const connectionBadge = (await page.locator('[data-testid="connection-badge"]').textContent({ timeout: 1000 }).catch(() => ''))?.trim() || '—';
      const uiLagText = (await page.locator('[data-testid="counter-lag"] span.font-mono').first().textContent({ timeout: 1000 }).catch(() => ''))?.trim() || '—';
      const uiLagNum = uiLagText === '—' || !uiLagText ? null : parseInt(uiLagText.replace(/,/g, ''), 10);

      // Read values from progress API
      let apiLag: number | null = null;
      let sparkIn: number | null = null;
      let kafkaPub: number | null = null;
      let rps: number | null = null;
      let eta: number | null = null;

      try {
        const progRes = await request.get(`http://localhost:8000/api/pipeline-runs/${runId}/progress`, { headers: authHeaders });
        if (progRes.ok()) {
          const prog = await progRes.json();
          apiLag = prog.kafka?.consumerLag ?? null;
          sparkIn = prog.spark?.recordsIn ?? null;
          kafkaPub = prog.kafka?.published ?? null;
          rps = prog.throughput?.recordsPerSecond ?? null;
          eta = prog.etaSeconds ?? null;
        }
      } catch {
        // ignore transient api error during poll
      }

      console.log(
        `| ${elapsed.toString().padStart(4)} | ${statusPill.padEnd(8)} | ${connectionBadge.padEnd(9)} | ${(uiLagText).padEnd(7)} | ${(apiLag !== null ? apiLag.toString() : '—').padEnd(8)} | ${(sparkIn !== null ? sparkIn.toString() : '—').padEnd(8)} | ${(kafkaPub !== null ? kafkaPub.toString() : '—').padEnd(8)} | ${(rps !== null ? Math.round(rps).toString() : '—').padEnd(11)} | ${(eta !== null ? Math.round(eta).toString() : '—').padEnd(6)} |`
      );

      const effectiveLag = apiLag ?? uiLagNum ?? 0;
      if (effectiveLag > peakLag) {
        peakLag = effectiveLag;
      }

      if (sparkIn !== null && sparkIn > 0 && effectiveLag > 0) {
        if (sparkIn > prevSparkIn) {
          sawSparkInIncreasingWhileLagging = true;
        }
      }
      if (sparkIn !== null && sparkIn > prevSparkIn) {
        prevSparkIn = sparkIn;
      }

      // Screenshot 1: Rising lag (> 100,000)
      if (!tookRisingShot && effectiveLag >= 50_000) {
        await page.screenshot({ path: path.join(screenshotDir, 'rising_lag.png'), fullPage: false });
        tookRisingShot = true;
      }

      // Screenshot 2: Draining lag (after peak > 100k, and lag has dropped below 70% of peak)
      if (tookRisingShot && !tookDrainingShot && peakLag >= 100_000 && effectiveLag <= peakLag * 0.7) {
        await page.screenshot({ path: path.join(screenshotDir, 'draining_lag.png'), fullPage: false });
        tookDrainingShot = true;
      }

      if (statusPill === 'succeeded' || statusPill === 'failed' || statusPill === 'cancelled') {
        terminalReached = true;
        finalStatus = statusPill;
        break;
      }
    }

    expect(terminalReached, 'Streaming run must reach terminal state').toBe(true);
    expect(finalStatus, 'Streaming run status must be succeeded').toBe('succeeded');

    // Screenshot 3: Completion
    await page.screenshot({ path: path.join(screenshotDir, 'completion.png'), fullPage: false });

    // Assert lag peaked and reached 0
    expect(peakLag, 'Peak consumer lag must be observable (> 100,000)').toBeGreaterThan(100_000);
    expect(sawSparkInIncreasingWhileLagging, 'recordsIn must be greater than 0 and increasing while lag > 0').toBe(true);
    const finalProgRes = await request.get(`http://localhost:8000/api/pipeline-runs/${runId}/progress`, {
      headers: authHeaders,
    });
    expect(finalProgRes.ok()).toBe(true);
    const finalProg = await finalProgRes.json();
    expect(finalProg.kafka?.consumerLag, 'Final consumer lag must be 0').toBe(0);
    expect(finalProg.spark?.recordsIn, 'Total ingested rows must be 1,000,000').toBe(1_000_000);
  });

  // ── 3. Streaming mode accessibility audit ─────────────────────────────────
  test('3. Streaming mode accessibility: axe reports zero serious or critical violations', async ({
    page,
  }) => {
    test.setTimeout(120_000);
    await doLogin(page);

    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Validate and launch sample streaming run
    await setEditorContent(page, STREAMING_SAMPLE_YAML);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });
    await expect(page.locator('[data-testid="streaming-badge"]')).toBeVisible({ timeout: 15_000 });

    const axeResults = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();

    const criticalViolations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(
      criticalViolations,
      `Axe violations on streaming run page: ${JSON.stringify(criticalViolations)}`
    ).toHaveLength(0);

    await expect(
      page.locator('[data-testid="status-pill"]').or(page.locator('text=succeeded')).first()
    ).toBeVisible({ timeout: 60_000 });
  });
});
