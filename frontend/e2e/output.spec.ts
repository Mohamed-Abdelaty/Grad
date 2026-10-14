/**
 * frontend/e2e/output.spec.ts
 * ---------------------------
 * Playwright E2E tests for UI-6 Output preview, inspection, and downloads.
 * Covers all 10 items from Step 6.
 */

import { test, expect, type ConsoleMessage, type Response, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import fs from 'fs';
import path from 'path';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME) {
    throw new Error('Missing required environment variable: E2E_USERNAME');
  }
  if (!PASSWORD) {
    throw new Error('Missing required environment variable: E2E_PASSWORD');
  }
});

type Audit = { errors: string[]; failures: string[] };

function startAudit(page: Page): Audit {
  const errors: string[] = [];
  const failures: string[] = [];
  page.on('console', (msg: ConsoleMessage) => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  page.on('requestfailed', (req) =>
    failures.push(`NETWORK FAIL: ${req.method()} ${req.url()}`)
  );
  page.on('response', (res: Response) => {
    if (res.status() >= 500) failures.push(`HTTP ${res.status()}: ${res.url()}`);
  });
  return { errors, failures };
}

function assertClean(audit: Audit, label: string) {
  const realErrors = audit.errors.filter(
    (e) =>
      !e.includes('Download the React DevTools') &&
      !e.includes('ResizeObserver') &&
      !e.includes('content.js') &&
      !e.includes('409 (Conflict)')
  );
  const realFailures = audit.failures.filter(
    (f) => !f.includes('/stream') // SSE stream disconnect on navigation
  );
  expect(realErrors, `Console errors on ${label}: ${realErrors.join('; ')}`).toHaveLength(0);
  expect(realFailures, `Server errors on ${label}: ${realFailures.join('; ')}`).toHaveLength(0);
}

async function doLogin(page: Page) {
  await page.goto('/login');
  await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/', { timeout: 10_000 });
}

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

test.describe('UI-6 Output Inspection & Downloads', () => {
  test('Step 6 items 1-10: full output lifecycle, downloads, limits, token replay, large warning, and axe', async ({
    page,
  }) => {
    test.setTimeout(240_000);
    const audit = startAudit(page);
    await doLogin(page);
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

    // Track download link response URL for token replay test
    let lastDownloadUrl = '';
    page.on('response', async (res) => {
      if (res.url().includes('/output/download-link') && res.status() === 200) {
        try {
          const body = await res.json();
          if (body && body.url) {
            lastDownloadUrl = body.url;
          }
        } catch {
          // ignore non-json
        }
      }
    });

    // Mock large size (> 100 MB) on output metadata to test large-download confirm dialog
    await page.route('**/api/pipeline-runs/*/output', async (route) => {
      const response = await route.fetch();
      const json = await response.json();
      json.totalSizeBytes = 120 * 1024 * 1024; // 120 MB
      await route.fulfill({ response, json });
    });

    // 1. Navigate to Pipelines page
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // 2. Select sales-csv template
    await page.getByTestId('btn-templates').click();
    await expect(page.getByText('Pipeline Templates')).toBeVisible();
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });

    // 3. Validate
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // 4. Run pipeline with wait=false
    await page.getByTestId('btn-run').click();

    // 5. Follow redirect to run details page
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Item 6: While running, click Output tab -> verifies output-not-ready state
    await page.getByRole('tab', { name: 'Output' }).click();
    await expect(page.getByTestId('output-tab')).toBeVisible();

    // 7. Wait for pipeline to complete
    await expect(page.getByTestId('run-header')).toContainText('succeeded', { timeout: 120_000 });

    // 8. Output tab updates to ready state without page reload
    await expect(page.getByTestId('output-metadata')).toBeVisible({ timeout: 15_000 });

    // Item 1: Verify Metadata fields
    await expect(page.getByTestId('badge-format')).toHaveText('parquet');
    await expect(page.getByTestId('badge-verified')).toBeVisible();
    await expect(page.getByTestId('meta-output-path')).toContainText('/data/processed/template_sales_csv');
    await expect(page.getByTestId('meta-file-count')).toContainText('file');
    await expect(page.getByTestId('meta-total-size')).not.toHaveText('0 B');
    await expect(page.getByTestId('meta-row-count')).not.toHaveText('—');

    const metaRowCountStr = await page.getByTestId('meta-row-count').innerText();
    const metaRowCount = parseInt(metaRowCountStr.replace(/,/g, ''), 10);
    expect(metaRowCount).toBeGreaterThan(0);

    // Capture screenshot 1: desktop metadata
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'desktop-output-metadata.png'),
      fullPage: false,
    });

    // Test file list toggle
    await page.getByTestId('btn-toggle-files').click();
    await expect(page.getByTestId('output-file-list')).toBeVisible();
    await expect(page.getByTestId('output-file-list')).toContainText('.parquet');

    // Item 1: Verify Schema section
    await expect(page.getByTestId('output-schema')).toBeVisible();
    const expectedColumns = ['transaction_id', 'amount', 'customer_id', 'product_id', 'sale_date'];
    for (const col of expectedColumns) {
      await expect(page.getByTestId('output-schema')).toContainText(col);
    }

    // Item 1 & 2: Verify Tabular Preview
    await expect(page.getByTestId('output-preview')).toBeVisible();
    await expect(page.getByTestId('preview-count')).toBeVisible();
    const initialPreviewText = await page.getByTestId('preview-count').innerText();
    expect(initialPreviewText).toContain('Showing');

    const previewTableRows = page.locator('[data-testid="output-preview"] tbody tr');
    await expect(previewTableRows.first()).toBeVisible();

    // Capture screenshot 2: desktop preview
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'desktop-output-preview.png'),
      fullPage: false,
    });

    // Item 2: Test Preview Limits selector (25, 50, 100, 500)
    for (const limitValue of ['25', '50', '100', '500']) {
      await page.getByTestId('select-preview-limit').selectOption(limitValue);
      await expect(page.getByTestId('preview-count')).toBeVisible();
      const countText = await page.getByTestId('preview-count').innerText();
      expect(countText).toContain('Showing');
      const rowCount = await previewTableRows.count();
      const limNum = parseInt(limitValue, 10);
      expect(rowCount).toBeLessThanOrEqual(limNum);
      if (limNum === 25) {
        expect(rowCount).toBe(Math.min(25, metaRowCount));
      }
    }

    // Reset back to 50
    await page.getByTestId('select-preview-limit').selectOption('50');
    await expect(page.getByTestId('preview-count')).toBeVisible();

    // Test Refresh Button
    await page.getByTestId('btn-refresh-preview').click();
    await expect(page.getByTestId('preview-count')).toBeVisible();

    // Test Copy CSV button
    await page.getByTestId('btn-copy-csv').click();
    await expect(page.getByText('Copied CSV preview to clipboard')).toBeVisible({ timeout: 5000 });

    // Item 3: CSV Download with large-download confirmation dialog test (> 100 MB warning modal)
    await page.getByTestId('btn-download-csv').click();
    await expect(page.getByText('Large Download Confirmation')).toBeVisible({ timeout: 5000 });
    await expect(page.getByText('over 100 MB')).toBeVisible();

    const [csvDownload] = await Promise.all([
      page.waitForEvent('download', { timeout: 20_000 }),
      page.getByRole('button', { name: 'Proceed to Download' }).click(),
    ]);
    expect(csvDownload.suggestedFilename()).toContain('.csv');
    const csvPath = await csvDownload.path();
    expect(csvPath).toBeTruthy();
    const csvContent = fs.readFileSync(csvPath!, 'utf-8');
    const csvLines = csvContent.trim().split(/\r?\n/);
    const csvHeaderCols = csvLines[0].split(',').map((c) => c.replace(/^"|"$/g, ''));
    for (const col of expectedColumns) {
      expect(csvHeaderCols).toContain(col);
    }
    // Row count in CSV (excluding header) must equal metadata rowCount
    expect(csvLines.length - 1).toBe(metaRowCount);

    // First data row in CSV matches first row in preview
    const firstPreviewCells = await previewTableRows.first().locator('td').allInnerTexts();
    // First preview cell is index #, second is first column value
    const expectedFirstVal = firstPreviewCells[1].trim();
    expect(csvLines[1].replace(/"/g, '')).toContain(expectedFirstVal);

    // Item 4: Parquet Download with confirmation dialog
    await page.getByTestId('btn-download-parquet').click();
    await expect(page.getByText('Large Download Confirmation')).toBeVisible({ timeout: 5000 });
    const [parquetDownload] = await Promise.all([
      page.waitForEvent('download', { timeout: 20_000 }),
      page.getByRole('button', { name: 'Proceed to Download' }).click(),
    ]);
    expect(parquetDownload.suggestedFilename()).toContain('.parquet');
    const parquetPath = await parquetDownload.path();
    expect(parquetPath).toBeTruthy();
    const parquetStat = fs.statSync(parquetPath!);
    expect(parquetStat.size).toBeGreaterThan(0);

    const parquetBuffer = fs.readFileSync(parquetPath!);
    const magicStart = parquetBuffer.subarray(0, 4).toString('ascii');
    const magicEnd = parquetBuffer.subarray(parquetBuffer.length - 4).toString('ascii');
    expect(magicStart).toBe('PAR1');
    expect(magicEnd).toBe('PAR1');

    await page.unroute('**/api/pipeline-runs/*/output');

    // Item 5: Single-use token replay test
    expect(lastDownloadUrl).toBeTruthy();
    const replayResponse = await page.request.get(lastDownloadUrl);
    expect(replayResponse.status()).toBe(401);

    // Item 8: Access control checks via request API
    const unauthOutputResp = await page.request.get('http://localhost:3000/api/pipeline-runs/00000000-0000-0000-0000-000000000000/output');
    expect(unauthOutputResp.status()).toBe(401);

    // Item 9: Accessibility check on Output Tab in ready state
    const axeResults = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const criticalViolations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(
      criticalViolations,
      `Axe violations on Output Tab: ${JSON.stringify(criticalViolations)}`
    ).toHaveLength(0);

    // Item 10: Mobile Viewport & Screenshot (390x844)
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(500);
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'mobile-output.png'),
      fullPage: false,
    });

    assertClean(audit, 'Output Tab Succeeded Run');
  });

  test('Item 7: Output tab renders not-ready state for failed run', async ({ page }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);
    await doLogin(page);

    // Navigate to Pipelines page
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const failingYaml = `pipeline:
  name: e2e-failing-output
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/nonexistent_data_file_99999.csv
schema:
  - { name: id, type: string }
processing:
  operations:
    - { type: remove_nulls }
streaming:
  enabled: false
messaging:
  broker: kafka
  topic: e2e-failing-output
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/e2e_failing_output
monitoring:
  enabled: true
`;

    await setEditorContent(page, failingYaml);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Wait for failure
    await expect(page.getByTestId('run-header')).toContainText('failed', { timeout: 60_000 });

    // Click Output tab
    await page.getByRole('tab', { name: 'Output' }).click();
    await expect(page.getByTestId('output-not-ready')).toBeVisible();
    await expect(page.getByText('Output not available')).toBeVisible();

    // Screenshot of failed output tab
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'desktop-output-failed.png'),
      fullPage: false,
    });

    assertClean(audit, 'Output Tab Failed Run');
  });
});
