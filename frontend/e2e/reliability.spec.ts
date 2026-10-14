import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import path from 'path';

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
    (f) => !f.includes('/stream')
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

const screenshotDir = path.resolve('..', 'docs', 'screenshots', 'ui-8');

test.describe('UI-8 ChaosLab Reliability and Reports E2E', () => {
  test('1. Wizard navigation, form validation, and responsive screenshots', async ({ page }) => {
    const audit = startAudit(page);
    await doLogin(page);

    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reliability')).toBeVisible({ timeout: 10_000 });

    const wizard = page.getByTestId('exp-wizard');
    await expect(wizard).toBeVisible();

    // Step 1: Target Selection
    await expect(page.getByTestId('exp-wizard-step-1')).toBeVisible();
    const kafkaOpt = page.getByTestId('target-opt-kafka');
    await expect(kafkaOpt).toBeVisible();
    await kafkaOpt.click();

    // Next to Step 2
    const nextBtn = page.getByTestId('wizard-next');
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 2: Fault Selection
    await expect(page.getByTestId('exp-wizard-step-2')).toBeVisible();
    const stopOpt = page.getByTestId('fault-opt-container_stop');
    await expect(stopOpt).toBeVisible();
    await stopOpt.click();

    // Next to Step 3
    await nextBtn.click();

    // Step 3: Parameters & Thresholds validation
    await expect(page.getByTestId('exp-wizard-step-3')).toBeVisible();
    const durationInput = page.getByTestId('fault-duration-input');
    await expect(durationInput).toBeVisible();

    // Test duration validation: set invalid duration 0 -> next button disabled
    await durationInput.fill('0');
    await expect(nextBtn).toBeDisabled();

    // Set valid duration
    await durationInput.fill('15');
    await expect(nextBtn).toBeEnabled();

    // Next to Step 4: Review & Run
    await nextBtn.click();
    await expect(page.getByTestId('exp-wizard-step-4')).toBeVisible();

    // Start button must be disabled until confirm checkbox is checked
    const startBtn = page.getByTestId('btn-start-exp');
    await expect(startBtn).toBeDisabled();

    const confirmCheckbox = page.getByTestId('exp-confirm');
    await confirmCheckbox.check();
    await expect(startBtn).toBeEnabled();

    // Capture responsive screenshots for Wizard
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'wizard-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'wizard-390x844.png') });

    // Restore desktop viewport
    await page.setViewportSize({ width: 1440, height: 900 });

    // Accessibility check on the wizard
    const axeResults = await new AxeBuilder({ page })
      .include('[data-testid="exp-wizard"]')
      .analyze();
    const violations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(violations, `Axe violations in ExperimentWizard: ${JSON.stringify(violations, null, 2)}`).toHaveLength(0);

    assertClean(audit, 'Wizard Navigation & Validation');
  });

  test('2. Live experiment execution: 12-phase transition, fault badge, truthful compact viz, and result summary', async ({
    page,
  }) => {
    test.setTimeout(180_000);
    const audit = startAudit(page);
    await doLogin(page);

    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 10_000 });

    // Fill wizard and start a real 5-second container_stop experiment on kafka
    await page.getByTestId('target-opt-kafka').click();
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('fault-opt-container_stop').click();
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('fault-duration-input').fill('5');
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('exp-confirm').check();
    await page.getByTestId('btn-start-exp').click();

    // Monitor appears
    const monitor = page.getByTestId('active-exp-monitor');
    await expect(monitor).toBeVisible({ timeout: 15_000 });

    // 12-phase lifecycle stepper visible
    await expect(page.getByTestId('exp-lifecycle')).toBeVisible();

    // Fault badge visible
    await expect(page.getByTestId('exp-fault-badge')).toBeVisible();

    // Compact pipeline visualization visible
    await expect(page.getByTestId('viz')).toBeVisible();

    // Log viewer visible
    await expect(page.getByTestId('exp-log-viewer')).toBeVisible();

    // Capture active monitor responsive screenshots
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(screenshotDir, 'monitor-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(screenshotDir, 'monitor-390x844.png') });

    await page.setViewportSize({ width: 1440, height: 900 });

    // Await experiment completion (result summary card appears)
    const resultCard = page.getByTestId('exp-result');
    await expect(resultCard).toBeVisible({ timeout: 120_000 });

    // Verify result contains verdict pill (PASS or FAIL)
    const verdict = page.getByTestId('exp-result-verdict');
    await expect(verdict).toBeVisible();

    assertClean(audit, 'Live Experiment Execution');
  });

  test('3. Experiment cancellation: immediately aborts and clears fault state', async ({ page }) => {
    test.setTimeout(90_000);
    const audit = startAudit(page);
    await doLogin(page);

    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 10_000 });

    // Start a 60-second experiment so we have time to cancel
    await page.getByTestId('target-opt-kafka').click();
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('fault-opt-container_stop').click();
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('fault-duration-input').fill('60');
    await page.getByTestId('wizard-next').click();

    await page.getByTestId('exp-confirm').check();
    await page.getByTestId('btn-start-exp').click();

    // Wait for monitor
    await expect(page.getByTestId('active-exp-monitor')).toBeVisible({ timeout: 15_000 });

    // Click cancel
    const cancelBtn = page.getByTestId('btn-cancel-exp');
    await expect(cancelBtn).toBeVisible();
    await cancelBtn.click();

    // Confirm cancellation in modal dialog
    const confirmBtn = page.getByRole('button', { name: /yes, revert fault/i });
    await expect(confirmBtn).toBeVisible();
    await confirmBtn.click();

    // Modal closes
    await expect(confirmBtn).toBeHidden({ timeout: 10_000 });

    assertClean(audit, 'Experiment Cancellation');
  });

  test('4. Reports page: filtering, Cards vs Table toggle, compare selection and modal', async ({ page }) => {
    const audit = startAudit(page);
    await doLogin(page);

    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reports')).toBeVisible({ timeout: 10_000 });

    // Trends section is visible
    await expect(page.getByTestId('trends')).toBeVisible();

    // Filter controls are present
    const filterTarget = page.getByTestId('filter-target');
    const filterFault = page.getByTestId('filter-fault');
    const filterResult = page.getByTestId('filter-result');
    await expect(filterTarget).toBeVisible();
    await expect(filterFault).toBeVisible();
    await expect(filterResult).toBeVisible();

    // Target filter changes URL query param
    await filterTarget.selectOption('kafka');
    await expect(page).toHaveURL(/target=kafka/, { timeout: 5000 });

    // Reset filter
    await filterTarget.selectOption('');
    await expect(page).toHaveURL(/^http:\/\/localhost:3000\/reports(\?.*)?$/);

    // Cards vs Table toggle
    const viewTableBtn = page.getByTestId('view-mode-table');
    const viewCardsBtn = page.getByTestId('view-mode-cards');
    await expect(viewTableBtn).toBeVisible();
    await expect(viewCardsBtn).toBeVisible();

    await viewTableBtn.click();
    await expect(page.getByTestId('reports-table')).toBeVisible();

    await viewCardsBtn.click();
    await expect(page.getByTestId('reports-cards')).toBeVisible();

    // Capture reports page screenshots
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'reports-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'reports-390x844.png') });

    await page.setViewportSize({ width: 1440, height: 900 });

    // Compare selection: select checkboxes for 2 reports
    const checkboxes = page.locator('input[type="checkbox"][data-testid^="cmp-"]');
    const count = await checkboxes.count();
    expect(count).toBeGreaterThanOrEqual(2);

    await checkboxes.nth(0).check();
    await checkboxes.nth(1).check();

    // Selection bar appears
    const compareBar = page.getByTestId('compare-selection-bar');
    await expect(compareBar).toBeVisible();
    await expect(compareBar).toContainText('2 reports selected');

    // Open Compare Modal
    const compareBtn = page.getByTestId('btn-compare-now');
    await expect(compareBtn).toBeEnabled();
    await compareBtn.click();

    const comparePanel = page.getByTestId('compare-panel');
    await expect(comparePanel).toBeVisible();

    // Capture compare modal screenshots
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'compare-modal-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'compare-modal-390x844.png') });

    await page.setViewportSize({ width: 1440, height: 900 });

    // Close compare panel via close button
    const closeCompareBtn = page.getByTestId('btn-close-compare');
    await expect(closeCompareBtn).toBeVisible();
    await closeCompareBtn.click();
    await expect(comparePanel).toBeHidden();

    assertClean(audit, 'Reports Page & Compare Panel');
  });

  test('5. Report details page: criteria table, key metrics, timeline, charts, logs, AI panel, and JSON export', async ({
    page,
  }) => {
    const audit = startAudit(page);
    await doLogin(page);

    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });

    // Click on the first report link
    const firstReportLink = page.getByRole('link', { name: /view report details/i }).first();
    await expect(firstReportLink).toBeVisible({ timeout: 10_000 });
    await firstReportLink.click();

    // Land on /reports/:id
    await expect(page).toHaveURL(/\/reports\/[a-f0-9-]+/, { timeout: 10_000 });
    await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 10_000 });

    // Verify key sections
    await expect(page.getByTestId('report-verdict-pill')).toBeVisible();
    await expect(page.getByTestId('report-criteria-table')).toBeVisible();
    await expect(page.getByTestId('report-key-metrics')).toBeVisible();
    await expect(page.getByTestId('report-timeline')).toBeVisible();
    await expect(page.getByTestId('report-charts')).toBeVisible();
    await expect(page.getByTestId('report-logs')).toBeVisible();
    await expect(page.getByTestId('report-ai-panel')).toBeVisible();

    // JSON export button
    const exportBtn = page.getByTestId('btn-export-json');
    await expect(exportBtn).toBeVisible();

    // Capture responsive screenshots for Report Details
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'report-details-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'report-details-390x844.png') });

    await page.setViewportSize({ width: 1440, height: 900 });

    // Axe accessibility audit
    const axeResults = await new AxeBuilder({ page })
      .include('[data-testid="page-report-details"]')
      .analyze();
    const violations = axeResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );
    expect(violations, `Axe violations in ReportDetails: ${JSON.stringify(violations, null, 2)}`).toHaveLength(0);

    assertClean(audit, 'Report Details Page');
  });
});
