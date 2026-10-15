/**
 * e2e/demo.spec.ts
 * ----------------
 * Full 9-Stage Demonstration Scenario Test (@demo)
 *
 * Implements the live demonstration script defined in docs/demo-script.md:
 *  Stage 1: System Access & Authentication
 *  Stage 2: Health & KPI Audit
 *  Stage 3: Pipeline Authoring & Template Selection
 *  Stage 4: Schema Validation & Launch
 *  Stage 5: Live Pipeline Operations & Honest Counters
 *  Stage 6: ChaosLab Wizard & Target Configuration
 *  Stage 7: Fault Injection & Active Monitor
 *  Stage 8: Resilience Reports & Full Audit
 *  Stage 9: Shell Polish & Global Controls (Sign Out)
 *
 * Run:
 *   npx playwright test e2e/demo.spec.ts --trace=on --video=on
 */
import { test, expect, type Page } from '@playwright/test';

// Enforce full video and execution trace capture for demonstration evidence
test.use({
  video: 'on',
  trace: 'on',
});

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME || !PASSWORD) {
    throw new Error('Missing required environment variables: E2E_USERNAME / E2E_PASSWORD');
  }
});

async function clickNav(page: Page, label: string) {
  await page.locator('aside nav a', { hasText: label }).first().click();
}

test.describe('@demo ChaosFlow 9-Stage Demonstration Scenario', () => {
  test('Executes end-to-end demo flow through all 9 stages', async ({ page }) => {
    test.setTimeout(180_000); // 3 minutes allowance for full scenario

    // ── STAGE 1: System Access & Authentication ─────────────────────────────
    await page.goto('/login');
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
    await expect(page).toHaveTitle(/login/i);

    // Verify empty credentials
    await expect(page.locator('#username')).toHaveValue('');
    await expect(page.locator('#password')).toHaveValue('');

    // Authenticate
    await page.locator('#username').fill(USERNAME);
    await page.locator('#password').fill(PASSWORD);
    await page.getByRole('button', { name: /login|sign in/i }).click();

    // Verify landing on Dashboard
    await expect(page).toHaveURL('/', { timeout: 10_000 });
    await expect(page).toHaveTitle(/dashboard/i);

    // ── STAGE 2: Health & KPI Audit ─────────────────────────────────────────
    // Check 4 system health indicators
    const healthSection = page.locator('[data-testid="health-cards"]');
    await expect(healthSection).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('[data-testid="health-database"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-kafka"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-spark"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-hdfs"]')).toBeVisible();

    // Check KPI overview cards
    const kpiSection = page.locator('[data-testid="kpis"]');
    await expect(kpiSection).toBeVisible();
    await expect(page.locator('[data-testid="kpi-active-runs"]')).toBeVisible();
    await expect(page.locator('[data-testid="kpi-runs-24h"]')).toBeVisible();

    // ── STAGE 3: Pipeline Authoring & Template Selection ────────────────────
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 8_000 });
    await expect(page.getByRole('heading', { name: /pipelines/i }).first()).toBeVisible();

    // Open Templates Modal
    const templatesBtn = page.locator('[data-testid="btn-templates"]');
    await expect(templatesBtn).toBeVisible({ timeout: 5_000 });
    await templatesBtn.click();

    // Select first template or sales-csv
    const templateModal = page.getByRole('dialog');
    await expect(templateModal).toBeVisible();
    const templateCards = templateModal.locator('[data-testid^="template-"]');
    await expect(templateCards.first()).toBeVisible({ timeout: 8_000 });
    await templateCards.first().click();

    // Modal closes and content is populated
    await expect(templateModal).not.toBeVisible();

    // ── STAGE 4: Schema Validation & Launch ──────────────────────────────────
    // Click Validate
    const validateBtn = page.locator('[data-testid="btn-validate"]');
    await expect(validateBtn).toBeVisible();
    await validateBtn.click();

    // Wait for Ready to run / Valid status pill
    await expect(page.getByText(/ready to run|valid/i).first()).toBeVisible({ timeout: 8_000 });

    // Click Run
    const runBtn = page.locator('[data-testid="btn-run"]');
    await expect(runBtn).toBeEnabled({ timeout: 5_000 });
    await runBtn.click();

    // ── STAGE 5: Live Pipeline Operations & Honest Counters ─────────────────
    // Navigates to /pipelines/:runId
    await expect(page).toHaveURL(/\/pipelines\/.+/, { timeout: 15_000 });

    // Stage stepper must be present
    const stepper = page.locator('[data-testid="stage-stepper"]');
    await expect(stepper).toBeVisible({ timeout: 10_000 });
    await expect(stepper.getByText(/source/i).first()).toBeVisible();
    await expect(stepper.getByText(/kafka/i).first()).toBeVisible();
    await expect(stepper.getByText(/spark/i).first()).toBeVisible();
    await expect(stepper.getByText(/hdfs/i).first()).toBeVisible();

    // Check tabs (Logs, Configuration, Output)
    const logsTab = page.getByRole('tab', { name: /logs/i });
    await expect(logsTab).toBeVisible();
    await logsTab.click();
    await expect(page.getByRole('tabpanel')).toBeVisible();

    const configTab = page.getByRole('tab', { name: /configuration/i });
    await expect(configTab).toBeVisible();
    await configTab.click();
    await expect(page.getByRole('tabpanel')).toBeVisible();

    const overviewTab = page.getByRole('tab', { name: /overview/i });
    await expect(overviewTab).toBeVisible();
    await overviewTab.click();

    // ── STAGE 6: ChaosLab Wizard & Target Configuration ─────────────────────
    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 8_000 });
    await expect(page.getByRole('heading', { name: /reliability testing/i })).toBeVisible();

    // Experiment Wizard
    const wizard = page.locator('[data-testid="exp-wizard"]');
    await expect(wizard).toBeVisible({ timeout: 8_000 });

    // Step 1: Target selection
    const nextBtn = page.locator('[data-testid="wizard-next"]');
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 2: Fault selection
    await expect(wizard.getByText(/step 2 of 4/i)).toBeVisible();
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 3: Parameters & SLA Thresholds
    await expect(wizard.getByText(/step 3 of 4/i)).toBeVisible();
    const durationInput = page.locator('[data-testid="fault-duration-input"]');
    if (await durationInput.isVisible()) {
      await durationInput.fill('5');
    }
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 4: Review & Confirmation
    await expect(wizard.getByText(/step 4 of 4/i)).toBeVisible();
    const confirmCheckbox = page.locator('[data-testid="exp-confirm"]');
    await expect(confirmCheckbox).toBeVisible();
    await confirmCheckbox.check();

    // ── STAGE 7: Fault Injection & Active Monitor ────────────────────────────
    // If no experiment currently running, we can click Start Experiment, or inspect active monitor
    const startExpBtn = page.locator('[data-testid="btn-start-exp"]');
    const isStartEnabled = await startExpBtn.isEnabled();
    if (isStartEnabled) {
      await startExpBtn.click();
      // Wait for experiment to start
      await expect(page.getByText(/initiating|running|pending|active/i).first()).toBeVisible({
        timeout: 10_000,
      });
    }

    // Inspect Active Experiment Monitor / Live Architecture State
    await expect(page.getByText(/live architecture state|active experiment|recent experiments/i).first()).toBeVisible({
      timeout: 10_000,
    });

    // ── STAGE 8: Resilience Reports & Full Audit ─────────────────────────────
    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 8_000 });
    await expect(page.getByRole('heading', { name: /resilience reports/i })).toBeVisible();

    // Wait for reports to load
    await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => undefined);

    // Check if any report cards or table rows exist
    const reportLinks = page.locator('a[href^="/reports/"]');
    const reportCount = await reportLinks.count();

    if (reportCount > 0) {
      // Click first report
      await reportLinks.first().click();
      await expect(page).toHaveURL(/\/reports\/.+/, { timeout: 8_000 });

      // Verify report detail components
      await expect(page.locator('[data-testid="page-report-details"]')).toBeVisible({
        timeout: 10_000,
      });
      await expect(page.locator('[data-testid="report-verdict-pill"]')).toBeVisible();
      await expect(page.locator('[data-testid="btn-export-json"]')).toBeVisible();
    }

    // ── STAGE 9: Shell Polish & Global Controls (Sign Out) ───────────────────
    // Test sidebar collapse toggle
    const collapseBtn = page.locator('button[aria-label*="sidebar" i]').first();
    if (await collapseBtn.isVisible()) {
      await collapseBtn.click();
      await collapseBtn.click();
    }

    // Sign out
    const signOutBtn = page.locator('button[aria-label="Sign out"]');
    await expect(signOutBtn).toBeVisible();
    await signOutBtn.click();

    // Assert redirected back to /login
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
    await expect(page.locator('#username')).toHaveValue('');

    // Assert in-memory token cleared: navigating to / redirects to /login
    await page.goto('/');
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  });
});
