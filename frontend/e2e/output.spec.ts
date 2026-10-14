/**
 * frontend/e2e/output.spec.ts
 * ---------------------------
 * Playwright E2E tests for UI-6 Output preview, inspection, and downloads.
 */

import { test, expect, type ConsoleMessage, type Response, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import path from 'path';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME || !PASSWORD) {
    throw new Error(
      'E2E_USERNAME and E2E_PASSWORD must be set.\n' +
      'Example: E2E_USERNAME=admin E2E_PASSWORD=REDACTED npm run e2e'
    );
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
      !e.includes('content.js')
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
  test('output tab renders metadata, schema, preview, and handles downloads on a succeeded run', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);
    await doLogin(page);
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

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

    // 4. Run pipeline
    await page.getByTestId('btn-run').click();

    // 5. Follow redirect to run page
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // 6. While running, click Output tab -> verifies output-not-ready state
    await page.getByRole('tab', { name: 'Output' }).click();
    await expect(page.getByTestId('output-tab')).toBeVisible();

    // 7. Wait for pipeline to complete
    await expect(page.getByTestId('run-header')).toContainText('succeeded', { timeout: 120_000 });

    // 8. Ensure we are on Output tab
    await page.getByRole('tab', { name: 'Output' }).click();
    await expect(page.getByTestId('output-metadata')).toBeVisible({ timeout: 15_000 });

    // 9. Verify Metadata fields
    await expect(page.getByTestId('badge-format')).toHaveText('parquet');
    await expect(page.getByTestId('badge-verified')).toBeVisible();
    await expect(page.getByTestId('meta-output-path')).toContainText('/data/processed/template_sales_csv');
    await expect(page.getByTestId('meta-file-count')).toContainText('file');
    await expect(page.getByTestId('meta-total-size')).not.toHaveText('0 B');
    await expect(page.getByTestId('meta-row-count')).not.toHaveText('—');

    // 10. Capture screenshot 1: desktop metadata
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'desktop-output-metadata.png'),
      fullPage: false,
    });

    // 11. Test file list toggle
    await page.getByTestId('btn-toggle-files').click();
    await expect(page.getByTestId('output-file-list')).toBeVisible();
    await expect(page.getByTestId('output-file-list')).toContainText('.parquet');

    // 12. Verify Schema section
    await expect(page.getByTestId('output-schema')).toBeVisible();
    await expect(page.getByTestId('output-schema')).toContainText('transaction_id');
    await expect(page.getByTestId('output-schema')).toContainText('amount');

    // 13. Verify Tabular Preview
    await expect(page.getByTestId('output-preview')).toBeVisible();
    await expect(page.getByTestId('preview-count')).toContainText('Showing');
    const previewRows = page.locator('[data-testid="output-preview"] tbody tr');
    await expect(previewRows.first()).toBeVisible();

    // 14. Capture screenshot 2: desktop preview
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'desktop-output-preview.png'),
      fullPage: false,
    });

    // 15. Test Limit Selector
    await page.getByTestId('select-preview-limit').selectOption('25');
    await expect(page.getByTestId('preview-count')).toContainText('Showing');

    // 16. Test Refresh Button
    await page.getByTestId('btn-refresh-preview').click();
    await expect(page.getByTestId('preview-count')).toBeVisible();

    // 17. Test Copy CSV button
    await page.getByTestId('btn-copy-csv').click();
    await expect(page.getByText('Copied CSV preview to clipboard')).toBeVisible({ timeout: 5000 });

    // 18. Test CSV Download
    const [csvDownload] = await Promise.all([
      page.waitForEvent('download', { timeout: 15_000 }),
      page.getByTestId('btn-download-csv').click(),
    ]);
    expect(csvDownload.suggestedFilename()).toContain('.csv');

    // 19. Test Parquet Download
    const [parquetDownload] = await Promise.all([
      page.waitForEvent('download', { timeout: 15_000 }),
      page.getByTestId('btn-download-parquet').click(),
    ]);
    expect(parquetDownload.suggestedFilename()).toContain('.parquet');

    // 20. Accessibility check on Output Tab
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

    // 21. Mobile Viewport & Screenshot
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(500);
    await page.screenshot({
      path: path.resolve('..', 'docs', 'screenshots', 'ui-6', 'mobile-output.png'),
      fullPage: false,
    });

    assertClean(audit, 'Output Tab Succeeded Run');
  });

  test('output tab renders not-ready state for failed run', async ({ page }) => {
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

    assertClean(audit, 'Output Tab Failed Run');
  });
});
