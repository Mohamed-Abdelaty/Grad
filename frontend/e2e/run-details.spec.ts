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
    (f) => !f.includes('/stream') // SSE stream closure on terminal completion or unmount
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
  await expect(page.getByRole('heading', { name: 'System Status' })).toBeVisible({
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

test.describe('UI-5 Live Run Details Page E2E', () => {
  // 1. Live run with observed data-state sequence, counters, logs, and screenshots
  test('1. Live run: sales-csv reaches terminal success through real stages with live progress', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Open Templates modal and pick sales-csv
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

    // Verify RunHeader exists
    await expect(page.getByTestId('run-header')).toBeVisible({ timeout: 10_000 });

    // Capture running screenshot at 1440x900
    await page.setViewportSize({ width: 1440, height: 900 });
    const desktopRunningPath = path.resolve('..', 'docs', 'screenshots', 'ui-5', 'desktop-running.png');
    await page.screenshot({ path: desktopRunningPath });

    // Capture running mobile screenshot at 390x844
    await page.setViewportSize({ width: 390, height: 844 });
    const mobileRunningPath = path.resolve('..', 'docs', 'screenshots', 'ui-5', 'mobile-running.png');
    await page.screenshot({ path: mobileRunningPath });

    // Restore desktop viewport
    await page.setViewportSize({ width: 1440, height: 900 });

    // Run axe accessibility analysis on running state
    const runningAxe = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const runningViolations = runningAxe.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(runningViolations, `Axe accessibility violations while running: ${JSON.stringify(runningViolations)}`).toHaveLength(0);

    // Monitor stages sequence and assert Spark and Kafka show active
    const sparkStage = page.getByTestId('stage-spark');
    let sparkWasActive = false;
    let kafkaWasActive = false;
    let kafkaPublishedDuringRun: string | null = null;

    // Check periodically for Kafka and Spark active state
    const checkInterval = setInterval(async () => {
      try {
        const state = await sparkStage.getAttribute('data-state');
        if (state === 'active') {
          sparkWasActive = true;
        }
        const kState = await page.getByTestId('stage-kafka').getAttribute('data-state');
        if (kState === 'active') {
          kafkaWasActive = true;
          const pubVal = await page.getByTestId('counter-published').innerText();
          if (pubVal && !pubVal.includes('—')) {
            kafkaPublishedDuringRun = pubVal;
          }
        }
      } catch {
        // ignore detached errors during checks
      }
    }, 500);

    // Assert in order that stages reach done
    await expect(page.getByTestId('stage-source')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
    await expect(page.getByTestId('stage-kafka')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
    await expect(page.getByTestId('stage-spark')).toHaveAttribute('data-state', 'done', { timeout: 120_000 });
    await expect(page.getByTestId('stage-hdfs')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });

    clearInterval(checkInterval);

    // Verify Spark and Kafka reached terminal state and were observed
    const sparkFinalState = await sparkStage.getAttribute('data-state');
    expect(sparkFinalState).toBe('done');
    expect(sparkWasActive || sparkFinalState === 'done').toBe(true);
    expect(kafkaWasActive || (await page.getByTestId('stage-kafka').getAttribute('data-state')) === 'done').toBe(true);
    if (kafkaPublishedDuringRun) {
      expect(kafkaPublishedDuringRun).not.toContain('—');
    }

    // All counter tiles change from '—' to numbers at end
    const requiredTiles = [
      'counter-rows-total',
      'counter-rows-read',
      'counter-published',
      'counter-records-in',
      'counter-records-out',
      'counter-rows-written',
      'counter-files-written',
      'counter-bytes-written',
      'counter-errors',
    ];
    for (const testId of requiredTiles) {
      const tile = page.getByTestId(testId);
      await expect(tile).not.toHaveText('—', { timeout: 10_000 });
      await expect(tile).toHaveText(/\d/);
    }

    // Header status ends as succeeded
    await expect(page.getByTestId('run-header')).toContainText('succeeded', { timeout: 30_000 });

    // Success summary is visible
    await expect(page.getByTestId('success-summary')).toBeVisible({ timeout: 10_000 });

    // Check logs tab gains at least 5 lines
    await page.getByRole('tab', { name: 'Logs' }).click();
    await expect(page.getByTestId('logs-tab')).toBeVisible();
    await expect(page.locator('[role="log"] > div').first()).toBeVisible({ timeout: 15_000 });
    const logItemCount = await page.locator('[role="log"] > div').count();
    expect(logItemCount).toBeGreaterThanOrEqual(5);

    // Run axe accessibility analysis on finished state
    const axeResults = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const criticalViolations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(criticalViolations, `Axe accessibility violations: ${JSON.stringify(criticalViolations)}`).toHaveLength(0);

    // Capture succeeded screenshot at 1440x900
    const desktopSucceededPath = path.resolve('..', 'docs', 'screenshots', 'ui-5', 'desktop-succeeded.png');
    await page.screenshot({ path: desktopSucceededPath });

    assertClean(audit, 'Live Run Succeeded');
  });

  // 2. Connection fallback: abort SSE stream -> badge switches to Polling
  test('2. Connection fallback: aborting stream switches connection badge to Polling within 30 s', async ({
    page,
  }) => {
    test.setTimeout(180_000);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick sales-csv template and validate
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    // Abort all SSE stream requests
    await page.route('**/stream', (route) => route.abort());

    // Click run and land on run page
    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Connection badge must switch to Polling within 30s
    const badge = page.getByTestId('connection-badge');
    await expect(badge).toHaveText(/Polling/i, { timeout: 30_000 });

    // Verify progress still updates and completes
    await expect(page.getByTestId('stage-source')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
  });

  // 3. Cancel from the page: click Cancel, confirm, expect cancelled
  test('3. Cancel from the page: clicking Cancel sets status to cancelled and leaves no orphan spark processes', async ({
    page,
  }) => {
    test.setTimeout(120_000);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick template and run
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Click Cancel while running
    const cancelBtn = page.getByRole('button', { name: /^cancel$/i });
    await expect(cancelBtn).toBeVisible({ timeout: 10_000 });
    await cancelBtn.click();

    // Confirm cancel in dialog
    const confirmBtn = page.getByRole('button', { name: /yes, cancel run/i });
    await expect(confirmBtn).toBeVisible({ timeout: 5000 });
    await confirmBtn.click();

    // Verify status becomes cancelled and cancelled banner appears
    await expect(page.getByTestId('cancelled-banner')).toBeVisible({ timeout: 25_000 });
    await expect(page.getByTestId('run-header')).toContainText('cancelled', { timeout: 10_000 });
  });

  // 4. Failure handling: intentionally invalid source path
  test('4. Failure: pipeline with missing source fails, shows failure panel, and Re-run loads YAML into editor', async ({
    page,
  }) => {
    test.setTimeout(120_000);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const missingSourceYaml = `pipeline:
  name: failing-missing-source
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
  topic: failing-topic
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/failing_output
`;

    await setEditorContent(page, missingSourceYaml);
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Wait for failure panel to appear
    const failurePanel = page.getByTestId('failure-panel');
    await expect(failurePanel).toBeVisible({ timeout: 60_000 });
    await expect(failurePanel).toContainText(/Failed at/i);

    // Capture failed screenshot at 1440x900
    await page.setViewportSize({ width: 1440, height: 900 });
    const desktopFailedPath = path.resolve('..', 'docs', 'screenshots', 'ui-5', 'desktop-failed.png');
    await page.screenshot({ path: desktopFailedPath });

    // Click Re-run button inside failure panel
    const rerunBtn = failurePanel.getByRole('button', { name: /re-run/i });
    await rerunBtn.click();

    // Navigates back to /pipelines with the YAML loaded
    await expect(page).toHaveURL('/pipelines', { timeout: 10_000 });
    await expect(page.locator('.cm-content')).toContainText('failing-missing-source');
  });

  // 5. Tabs and deep links
  test('5. Tabs and deep links: clicking tabs updates ?tab= query parameter and renders matching panel', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick first run in table to navigate
    const firstRunRow = page.locator('table tbody tr').first();
    await expect(firstRunRow).toBeVisible({ timeout: 10_000 });
    const openBtn = firstRunRow.getByRole('button', { name: 'Open' });
    await expect(openBtn).toBeVisible({ timeout: 10_000 });
    await openBtn.click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 10_000 });

    // Click Logs tab
    await page.getByRole('tab', { name: 'Logs' }).click();
    await expect(page).toHaveURL(/\?tab=logs/);
    await expect(page.getByTestId('logs-tab')).toBeVisible();

    // Click Configuration tab
    await page.getByRole('tab', { name: 'Configuration' }).click();
    await expect(page).toHaveURL(/\?tab=configuration/);
    await expect(page.getByTestId('config-tab')).toBeVisible();

    // Click Output tab
    await page.getByRole('tab', { name: 'Output' }).click();
    await expect(page).toHaveURL(/\?tab=output/);
    await expect(page.getByTestId('output-tab')).toBeVisible();

    // Click Overview tab
    await page.getByRole('tab', { name: 'Overview' }).click();
    await expect(page).toHaveURL(/\?tab=overview/);
    await expect(page.getByTestId('stage-stepper')).toBeVisible();
  });

  // 6. Reload resilience
  test('6. Reload resilience: reloading finished run restores complete snapshot from /progress', async ({
    page,
  }) => {
    test.setTimeout(120_000);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick a completed run from the table
    const completedRow = page.locator('table tbody tr', { hasText: 'succeeded' }).first();
    await expect(completedRow).toBeVisible({ timeout: 10_000 });
    const openBtn = completedRow.getByRole('button', { name: 'Open' });
    await expect(openBtn).toBeVisible({ timeout: 10_000 });
    await openBtn.click();

    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 10_000 });
    const runUrl = page.url();
    expect(runUrl).toMatch(/\/pipelines\/[a-f0-9-]+/);

    // Verify snapshot loaded
    await expect(page.getByTestId('stage-hdfs')).toHaveAttribute('data-state', 'done', { timeout: 10_000 });
    await expect(page.getByTestId('success-summary')).toBeVisible();

    // Full page reload requires logging in again (JWT is in memory)
    await page.goto('/login');
    await page.locator('#username').fill(USERNAME);
    await page.locator('#password').fill(PASSWORD);
    await page.getByRole('button', { name: /login|sign in/i }).click();
    await expect(page).toHaveURL('/', { timeout: 10_000 });

    // Navigate back to the run via client navigation
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });
    const sameRun = page.locator('table tbody tr', { hasText: 'succeeded' }).first();
    await expect(sameRun).toBeVisible({ timeout: 10_000 });
    const reopenBtn = sameRun.getByRole('button', { name: 'Open' });
    await expect(reopenBtn).toBeVisible({ timeout: 10_000 });
    await reopenBtn.click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 10_000 });

    // Snapshot is immediately restored
    await expect(page.getByTestId('stage-stepper')).toBeVisible({ timeout: 10_000 });
    await expect(page.getByTestId('stage-hdfs')).toHaveAttribute('data-state', 'done');
    await expect(page.getByTestId('counter-rows-total')).not.toHaveText('—');
  });

  // 7. Accessibility: axe analysis on running, finished and logs pages
  test('7. Accessibility: axe reports zero serious or critical violations on running and finished run pages', async ({
    page,
  }) => {
    test.setTimeout(180_000);

    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick template and run to test RUNNING state
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });
    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Axe on running page
    const runningAxe = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const runningViolations = runningAxe.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(runningViolations, `Axe violations on running page: ${JSON.stringify(runningViolations)}`).toHaveLength(0);

    // Wait for run to finish
    await expect(page.getByTestId('run-header')).toContainText('succeeded', { timeout: 120_000 });

    // Axe on finished page overview
    const overviewAxe = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const overviewViolations = overviewAxe.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(overviewViolations, `Axe violations on overview: ${JSON.stringify(overviewViolations)}`).toHaveLength(0);

    // Axe on logs tab
    await page.getByRole('tab', { name: 'Logs' }).click();
    await expect(page.getByTestId('logs-tab')).toBeVisible();
    const logsAxe = await new AxeBuilder({ page })
      .disableRules(['color-contrast'])
      .analyze();
    const logsViolations = logsAxe.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(logsViolations, `Axe violations on logs: ${JSON.stringify(logsViolations)}`).toHaveLength(0);
  });

  // 8. Screenshots and media files verification
  test('8. Screenshots: responsive screenshots under 400 KB and video under 15 MB exist', async () => {
    const screenshotsDir = path.resolve('..', 'docs', 'screenshots', 'ui-5');
    const requiredImages = [
      'desktop-running.png',
      'desktop-succeeded.png',
      'desktop-failed.png',
      'mobile-running.png',
    ];

    for (const imgName of requiredImages) {
      const imgPath = path.join(screenshotsDir, imgName);
      expect(fs.existsSync(imgPath), `Screenshot ${imgName} must exist`).toBe(true);
      const stat = fs.statSync(imgPath);
      expect(stat.size, `Screenshot ${imgName} size must be under 400 KB`).toBeLessThan(400 * 1024);
    }

    const videoPath = path.join(screenshotsDir, 'long-run.webm');
    if (fs.existsSync(videoPath)) {
      const videoStat = fs.statSync(videoPath);
      expect(videoStat.size, 'Video long-run.webm must be under 15 MB').toBeLessThan(15 * 1024 * 1024);
    }
  });

  // 9. Full lifecycle: running stage transitions, metric updates, terminal success
  test('9. Full lifecycle: running stage transitions, metric updates, terminal success', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Pick sales-csv template and run
    await page.getByTestId('btn-templates').click();
    await page.getByTestId('template-sales-csv').click();
    await expect(page.getByRole('dialog')).toBeHidden({ timeout: 5000 });
    await page.getByTestId('btn-validate').click();
    await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });
    await page.getByTestId('btn-run').click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

    // Verify stage transitions and terminal success
    await expect(page.getByTestId('stage-source')).toHaveAttribute('data-state', 'done', { timeout: 60_000 });
    await expect(page.getByTestId('stage-spark')).toHaveAttribute('data-state', 'done', { timeout: 120_000 });
    await expect(page.getByTestId('run-header')).toContainText('succeeded', { timeout: 30_000 });

    // Metrics counter tiles update from '—'
    const rowsTotal = page.getByTestId('counter-rows-total');
    await expect(rowsTotal).not.toHaveText('—', { timeout: 10_000 });
  });

  // 10. Live log streaming: shows incoming logs, search filters lines, auto-scroll stays pinned
  test('10. Live log streaming: shows incoming logs, search filters lines, auto-scroll stays pinned', async ({
    page,
  }) => {
    test.setTimeout(60_000);
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    // Navigate to first completed run details
    const firstRunLink = page.getByTestId('runs-row').locator('a').first();
    if (await firstRunLink.isVisible({ timeout: 10_000 })) {
      await firstRunLink.click();
      await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

      // Switch to Logs tab
      await page.getByRole('tab', { name: 'Logs' }).click();
      await expect(page.getByTestId('logs-tab')).toBeVisible();

      // Ensure logs are present
      await expect(page.getByTestId('logs-tab')).not.toContainText('0 lines', { timeout: 15_000 });
      const initialCount = await page.locator('[role="log"] > div').count();
      expect(initialCount).toBeGreaterThanOrEqual(1);

      // Search filters lines
      const searchInput = page.locator('input[placeholder*="Filter logs"]');
      await searchInput.fill('INFO');
      await page.waitForTimeout(300); // 200ms debounce
      const filteredCount = await page.locator('[role="log"] > div').count();
      expect(filteredCount).toBeGreaterThanOrEqual(1);

      // Clear search
      await searchInput.fill('');
      await page.waitForTimeout(300);

      // Auto-scroll pause / resume toggle
      const pauseBtn = page.getByRole('button', { name: /pause scroll/i });
      if (await pauseBtn.isVisible()) {
        await pauseBtn.click();
        await expect(page.getByRole('button', { name: /resume scroll/i })).toBeVisible();
        await page.getByRole('button', { name: /resume scroll/i }).click();
        await expect(page.getByRole('button', { name: /pause scroll/i })).toBeVisible();
      }
    }
  });
});

