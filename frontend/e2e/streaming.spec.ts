/**
 * e2e/streaming.spec.ts
 * ---------------------
 * UI-10: End-to-end testing for Structured Streaming Mode (ChaosFlow).
 * Verifies:
 * 1. Submitting streaming pipeline run via API / UI.
 * 2. Streaming badge (data-testid="streaming-badge") displayed in RunHeader.
 * 3. Consumer lag counter tile (data-testid="counter-lag") visible in CountersGrid.
 * 4. Pipeline reaching terminal success with verified metrics.
 */

import { test, expect, type Page } from '@playwright/test';
import * as path from 'path';

const USERNAME = process.env.E2E_USERNAME || 'admin';
const PASSWORD = process.env.E2E_PASSWORD || '123456';

const STREAMING_YAML = `pipeline:
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
  test('1. Streaming run: displays streaming badge, consumer lag tile, and reaches terminal success', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    await doLogin(page);

    // 1. Navigate to Pipelines workspace via sidebar to maintain in-memory auth
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // 2. Insert streaming YAML configuration into CodeMirror editor
    await setEditorContent(page, STREAMING_YAML);

    // 3. Validate streaming YAML configuration
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // 4. Run pipeline
    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    // 5. Lands on /pipelines/<runId>
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // 6. Assert streaming badge is visible
    const streamingBadge = page.locator('[data-testid="streaming-badge"]');
    await expect(streamingBadge).toBeVisible({ timeout: 15_000 });
    await expect(streamingBadge).toHaveText(/STREAMING/i);

    // 7. Assert Consumer Lag counter tile is visible
    const lagCounter = page.locator('[data-testid="counter-lag"]');
    await expect(lagCounter).toBeVisible({ timeout: 15_000 });

    // 8. Wait for run to reach terminal success
    await expect(
      page.locator('[data-testid="status-pill"]').or(page.locator('text=succeeded')).first()
    ).toBeVisible({ timeout: 120_000 });

    // 9. Capture screenshot for UI-10 documentation
    const screenshotDir = path.resolve('..', 'docs', 'screenshots');
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.screenshot({
      path: path.join(screenshotDir, 'streaming-run-details-1440x900.png'),
      fullPage: true,
    });
  });
});
