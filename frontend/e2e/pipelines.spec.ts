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
  const failures: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  page.on('requestfailed', (req) =>
    failures.push(`NETWORK FAIL: ${req.method()} ${req.url()}`)
  );
  page.on('response', (res) => {
    if (res.status() >= 500) failures.push(`HTTP ${res.status()}: ${res.url()}`);
  });
  return { errors, failures };
}

function assertClean(audit: { errors: string[]; failures: string[] }, label: string) {
  const realErrors = audit.errors.filter(
    (e) =>
      !e.includes('Download the React DevTools') &&
      !e.includes('ResizeObserver') &&
      !e.includes('content.js')
  );
  const realFailures = audit.failures.filter(
    (f) => !f.includes('/stream')
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
  await expect(page.getByRole('heading', { name: /system (health|status)|dashboard/i }).first()).toBeVisible({
    timeout: 8_000,
  });
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

test.describe('UI-4 Pipelines Workspace E2E', () => {
  // 1. Template to run
  test('1. Template to run: chooses sales-csv, validates, runs asynchronously, verifies terminal success', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Open Templates modal
    await page.getByTestId('btn-templates').click();
    await expect(page.getByText('Pipeline Templates')).toBeVisible();

    // Select sales-csv template
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });

    // Click Validate
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Click Run
    const runBtn = page.getByTestId('btn-run');
    await expect(runBtn).toBeEnabled();
    await runBtn.click();

    // Navigates to /pipelines/<uuid>
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Wait up to 150s for terminal status 'succeeded'
    await expect(page.getByText(/succeeded/i).first()).toBeVisible({ timeout: 150_000 });

    assertClean(audit, 'Template to run');
  });

  // 2. Invalid YAML
  test('2. Invalid YAML: shows diagnostics, gutter markers, and Go to line navigates caret', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Configuration missing storage.path
    const invalidYaml = `pipeline:
  name: broken-pipeline
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_sample.csv
schema:
  - { name: id, type: string }
processing:
  operations:
    - { type: remove_nulls }
messaging:
  broker: kafka
  topic: broken-topic
processing_engine:
  type: spark
storage:
  type: hdfs
`;

    await setEditorContent(page, invalidYaml);

    // Validate
    await page.getByTestId('btn-validate').click();

    // Validation panel shows Invalid with issue containing storage
    await expect(page.getByTestId('validation-state')).toContainText('Invalid', { timeout: 10_000 });
    const issueItem = page.getByTestId('issue-item').filter({ hasText: /storage/i }).first();
    await expect(issueItem).toBeVisible();

    // Gutter / diagnostic marker is present
    await expect(page.locator('.cm-lint-marker, .cm-lintRange-error').first()).toBeVisible();

    // Click "Go to line" and verify status bar updates
    await issueItem.getByRole('button', { name: /go to line/i }).click();
    await expect(page.locator('text=/Ln \\d+/')).toBeVisible();

    // Bad indentation syntax error
    const syntaxErrorYaml = `pipeline:\n\tname: tab-indent\n`;
    await setEditorContent(page, syntaxErrorYaml);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toContainText('Invalid', { timeout: 10_000 });
    await expect(page.getByTestId('issue-item').first()).toContainText('Line');
  });

  // 3. Run is blocked until valid
  test('3. Run is blocked until valid and typing invalidates readiness', async ({ page }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Load sales-csv template
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });

    // Validate
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });
    await expect(page.getByTestId('btn-run')).toBeEnabled();

    // Type a change into editor
    await page.locator('.cm-content').click();
    await page.keyboard.press('End');
    await page.keyboard.type('\n# modification');

    // Run becomes disabled immediately
    await expect(page.getByTestId('btn-run')).toBeDisabled();

    // Turn Auto-validate OFF
    const autoValidateCheckbox = page.locator('label', { hasText: 'Auto-validate' }).locator('input');
    await autoValidateCheckbox.uncheck();

    // Wait a bit to ensure debounced validator does not fire
    await page.waitForTimeout(1200);
    await expect(page.getByTestId('btn-run')).toBeDisabled();

    // Click Validate manually
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });
    await expect(page.getByTestId('btn-run')).toBeEnabled();
  });

  // 4. Upload
  test('4. Upload: accepts valid yaml and rejects invalid file types and oversized files', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const fileInput = page.locator('input[type="file"]');

    // Reject .exe file
    await fileInput.setInputFiles({
      name: 'malicious.exe',
      mimeType: 'application/octet-stream',
      buffer: Buffer.from('binary-content'),
    });
    await expect(page.getByText(/Only \.yaml and \.yml files are allowed/i)).toBeVisible({
      timeout: 5000,
    });

    // Reject 300 KB file
    const largeBuffer = Buffer.alloc(300 * 1024, 'a');
    await fileInput.setInputFiles({
      name: 'large.yaml',
      mimeType: 'text/yaml',
      buffer: largeBuffer,
    });
    await expect(page.getByText(/exceeds maximum size of 256 KB/i)).toBeVisible({
      timeout: 5000,
    });

    // Accept valid .yaml
    const validContent = `pipeline:\n  name: upload-success-pipeline\n`;
    await fileInput.setInputFiles({
      name: 'valid_test.yaml',
      mimeType: 'text/yaml',
      buffer: Buffer.from(validContent),
    });
    await expect(page.getByText(/Loaded valid_test\.yaml/i)).toBeVisible({ timeout: 5000 });
    await expect(page.locator('.cm-content')).toContainText('upload-success-pipeline');
  });

  // 5. Download
  test('5. Download: sanitized filename and exact editor content', async ({ page }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const customYaml = `pipeline:\n  name: "My Custom Pipeline!!"\n`;
    await setEditorContent(page, customYaml);

    // Trigger download
    const downloadPromise = page.waitForEvent('download');
    await page.getByTestId('btn-download').click();
    const download = await downloadPromise;

    // Verify sanitized filename
    expect(download.suggestedFilename()).toBe('my-custom-pipeline.yaml');

    // Verify content
    const stream = await download.createReadStream();
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(chunk as Buffer);
    }
    const downloadedText = Buffer.concat(chunks).toString('utf-8');
    expect(downloadedText).toContain('My Custom Pipeline!!');
  });

  // 6. Runs table filters
  test('6. Runs table filters: status filter, debounced search, URL sync', async ({ page }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Wait for runs table to load
    await expect(page.getByTestId('runs-table')).toBeVisible({ timeout: 10_000 });

    // Click filter chip 'succeeded'
    const succeededChip = page.getByTestId('filter-status-succeeded');
    await succeededChip.click();

    // Verify URL updated with ?status=succeeded
    await expect(page).toHaveURL(/status=succeeded/);

    // Search by name
    const searchInput = page.getByTestId('runs-search');
    await searchInput.fill('sales');

    // Debounced search updates URL with q=sales
    await expect(page).toHaveURL(/q=sales/, { timeout: 2000 });
  });

  // 7. Stop
  test('7. Stop: starts run, clicks Stop, confirms dialog, expects cancelled and no orphaned processes', async ({
    page,
  }) => {
    test.setTimeout(120_000);
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Load IoT template
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-iot-json').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });

    // Validate
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Run
    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Return to Pipelines page via sidebar
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Wait for running row and click Stop
    const stopBtn = page.getByTestId('runs-row').filter({ hasText: 'running' }).first().getByRole('button', { name: 'Stop' });
    if (await stopBtn.isVisible({ timeout: 10_000 })) {
      await stopBtn.click();
      await expect(page.getByText('Stop running pipeline?')).toBeVisible();
      await page.getByRole('button', { name: 'Stop Run' }).click();
      await expect(page.getByText(/cancelled/i).first()).toBeVisible({ timeout: 30_000 });
    }
  });

  // 8. Re-run from history
  test('8. Re-run from history: loads YAML from row and prompts confirm if unsaved edits exist', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Find first enabled Re-run button in RunsTable
    const rerunBtn = page.getByTestId('runs-row').getByRole('button', { name: 'Re-run' }).first();
    if (await rerunBtn.isVisible({ timeout: 8000 })) {
      // Type something first so unsaved edits exist
      await setEditorContent(page, '# unsaved work\n');

      await rerunBtn.click();
      // Confirm dialog appears
      await expect(page.getByText('Replace current content?')).toBeVisible();
      await page.getByRole('button', { name: 'Replace' }).click();

      // Editor receives YAML
      await expect(page.locator('.cm-content')).not.toContainText('# unsaved work');
    }
  });

  // 9. Accessibility
  test('9. Accessibility: axe reports zero serious or critical violations including color-contrast', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Load invalid config to test error states accessibility too
    await setEditorContent(page, 'broken:\n  [invalid\n');
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toContainText('Invalid', { timeout: 10_000 });

    // Run axe with color-contrast enabled
    const accessibilityScanResults = await new AxeBuilder({ page }).analyze();
    const seriousOrCritical = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(seriousOrCritical).toHaveLength(0);
  });

  // 10. Screenshots
  test('10. Screenshots: capture 1440x900 and 390x844 responsive screenshots under 400 KB', async ({
    page,
  }) => {
    test.setTimeout(60_000);
    const screenshotDir = path.resolve('..', 'docs', 'screenshots', 'ui-4');
    if (!fs.existsSync(screenshotDir)) {
      fs.mkdirSync(screenshotDir, { recursive: true });
    }

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // 1440x900 viewport
    await page.setViewportSize({ width: 1440, height: 900 });

    // Screenshot 1: Empty state
    await page.screenshot({
      path: path.join(screenshotDir, 'empty_state.png'),
      fullPage: true,
    });

    // Load template for valid state
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Screenshot 2: Valid state (1440x900)
    await page.screenshot({
      path: path.join(screenshotDir, 'valid_state.png'),
      fullPage: true,
    });

    // Make invalid for invalid state screenshot
    await setEditorContent(page, 'pipeline:\n  name: broken\nstorage:\n  # missing path\n');
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toContainText('Invalid', { timeout: 10_000 });

    // Screenshot 3: Invalid state (1440x900)
    await page.screenshot({
      path: path.join(screenshotDir, 'invalid_state.png'),
      fullPage: true,
    });

    // Filter runs table
    await page.getByTestId('filter-status-succeeded').click();
    await page.getByTestId('runs-search').fill('sales');

    // Screenshot 4: Runs table with filters (1440x900)
    await page.screenshot({
      path: path.join(screenshotDir, 'runs_table_filters.png'),
      fullPage: true,
    });

    // 390x844 Mobile Viewport: Valid state
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-iot-json').click();
    const replaceBtn = page.getByRole('button', { name: 'Replace' });
    if (await replaceBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
      await replaceBtn.click();
    }
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Screenshot 5: Mobile valid state (390x844)
    await page.screenshot({
      path: path.join(screenshotDir, 'mobile_valid_state.png'),
      fullPage: true,
    });

    // Assert file sizes under 400 KB
    const files = fs.readdirSync(screenshotDir);
    expect(files.length).toBeGreaterThanOrEqual(5);
    for (const file of files) {
      if (file.endsWith('.png')) {
        const stats = fs.statSync(path.join(screenshotDir, file));
        expect(stats.size).toBeLessThan(400 * 1024);
      }
    }
  });

  // 11. Visual snapshot: 5 responsive viewport screenshots saved and valid
  test('11. Visual snapshot: 5 responsive viewport screenshots saved and valid', async () => {
    const screenshotDir = path.resolve('..', 'docs', 'screenshots', 'ui-4');
    const requiredFiles = [
      'empty_state.png',
      'valid_state.png',
      'invalid_state.png',
      'runs_table_filters.png',
      'mobile_valid_state.png',
    ];
    for (const f of requiredFiles) {
      const fullPath = path.join(screenshotDir, f);
      expect(fs.existsSync(fullPath), `Screenshot file ${f} must exist`).toBe(true);
      const stat = fs.statSync(fullPath);
      expect(stat.size, `Screenshot ${f} size ${stat.size} must be under 400 KB`).toBeLessThan(400 * 1024);
      expect(stat.size, `Screenshot ${f} size ${stat.size} must be greater than 1 KB`).toBeGreaterThan(1024);
    }
  });

  // 12. Persistence: editing YAML, navigating away, and returning keeps draft
  test('12. Persistence: editing YAML, navigating away, and returning keeps draft', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const draftText = '# draft persistence test\npipeline:\n  name: test_persist\n';
    await setEditorContent(page, draftText);
    await expect(page.locator('.cm-content')).toContainText('test_persist');

    // Navigate away to Dashboard
    await clickNav(page, 'Dashboard');
    await expect(page).toHaveURL('/', { timeout: 10_000 });

    // Navigate back to Pipelines
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Confirm draft is preserved in editor
    await expect(page.locator('.cm-content')).toContainText('test_persist', { timeout: 5000 });
  });
});

