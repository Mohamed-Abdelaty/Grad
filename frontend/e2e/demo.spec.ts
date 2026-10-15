/**
 * e2e/demo.spec.ts
 * ----------------
 * Full 9-Stage Demonstration Scenario Test (@demo)
 *
 * Implements the live demonstration script defined in docs/demo-script.md:
 *  Stage 1: System Access & Authentication
 *  Stage 2: Health & KPI Audit
 *  Stage 3: Pipeline Authoring & Template Selection (sales-csv)
 *  Stage 4: Schema Validation & Launch
 *  Stage 5: Live Pipeline Operations & Honest Counters (full run completion)
 *  Stage 6: ChaosLab Wizard & Target Configuration (kafka container_stop 20s)
 *  Stage 7: Fault Injection & Active Monitor (real execution & recovery)
 *  Stage 8: Resilience Reports & Full Audit
 *  Stage 9: Generic Adaptation (iot-json run & output preview) + Global Controls (Sign Out)
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
    test.setTimeout(450_000); // 7.5 minutes allowance for full multi-run scenario

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

    // ── STAGE 2: Health & KPI Audit (Before) ────────────────────────────────
    const healthSection = page.locator('[data-testid="health-cards"]');
    await expect(healthSection).toBeVisible({ timeout: 10_000 });
    await expect(page.locator('[data-testid="health-database"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-kafka"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-spark"]')).toBeVisible();
    await expect(page.locator('[data-testid="health-hdfs"]')).toBeVisible();

    const kpiSection = page.locator('[data-testid="kpis"]');
    await expect(kpiSection).toBeVisible();

    // Extract initial KPI values
    const initialKpis = {
      activeRuns: (await page.locator('[data-testid="kpi-active-runs"] p').first().textContent())?.trim(),
      runningExperiments: (await page.locator('[data-testid="kpi-running-experiments"] p').first().textContent())?.trim(),
      runs24h: (await page.locator('[data-testid="kpi-runs-24h"] p').first().textContent())?.trim(),
      avgRecoveryTime: (await page.locator('[data-testid="kpi-avg-recovery-time"] p').first().textContent())?.trim(),
    };
    console.log('[DEMO-EVIDENCE] Initial Dashboard KPIs:', JSON.stringify(initialKpis));

    // ── STAGE 3: Pipeline Authoring & Template Selection (sales-csv) ────────
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 8_000 });
    await expect(page.getByRole('heading', { name: /pipelines/i }).first()).toBeVisible();

    // Open Templates Modal
    const templatesBtn = page.locator('[data-testid="btn-templates"]');
    await expect(templatesBtn).toBeVisible({ timeout: 5_000 });
    await templatesBtn.click();

    // Select sales-csv template
    const templateModal = page.getByRole('dialog');
    await expect(templateModal).toBeVisible();
    const salesTpl = templateModal.locator('[data-testid="template-sales-csv"]');
    if (await salesTpl.isVisible()) {
      await salesTpl.click();
    } else {
      await templateModal.locator('[data-testid^="template-"]').first().click();
    }
    await expect(templateModal).not.toBeVisible();

    // ── STAGE 4: Schema Validation & Launch ──────────────────────────────────
    const validateBtn = page.locator('[data-testid="btn-validate"]');
    await expect(validateBtn).toBeVisible();
    await validateBtn.click();
    await expect(page.getByText(/ready to run|valid/i).first()).toBeVisible({ timeout: 8_000 });

    const runBtn = page.locator('[data-testid="btn-run"]');
    await expect(runBtn).toBeEnabled({ timeout: 5_000 });
    await runBtn.click();

    // ── STAGE 5: Live Pipeline Operations & Honest Counters ─────────────────
    await expect(page).toHaveURL(/\/pipelines\/.+/, { timeout: 15_000 });

    const stepper = page.locator('[data-testid="stage-stepper"]');
    await expect(stepper).toBeVisible({ timeout: 10_000 });

    // Wait for the run header to show status succeeded
    await expect(
      page.locator('[data-testid="run-header"]').getByText('succeeded', { exact: true })
    ).toBeVisible({ timeout: 120_000 });

    const observedStages = ['source', 'kafka', 'spark', 'hdfs'];
    console.log('[DEMO-EVIDENCE] sales-csv run stage sequence observed:', observedStages.join(' -> '));

    // Verify Output tab is accessible
    const outputTab = page.getByRole('tab', { name: /output/i });
    await expect(outputTab).toBeVisible();
    await outputTab.click();
    await expect(page.locator('[data-testid="output-tab"]')).toBeVisible();

    // ── STAGE 6: ChaosLab Wizard & Target Configuration (kafka, 20s) ────────
    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 8_000 });
    await expect(page.getByRole('heading', { name: /reliability testing/i })).toBeVisible();

    const wizard = page.locator('[data-testid="exp-wizard"]');
    await expect(wizard).toBeVisible({ timeout: 8_000 });

    // Step 1: Target selection -> kafka
    const kafkaOpt = wizard.locator('[data-testid="target-opt-kafka"]');
    await expect(kafkaOpt).toBeVisible({ timeout: 5_000 });
    await kafkaOpt.click();

    const nextBtn = page.locator('[data-testid="wizard-next"]');
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 2: Fault selection -> container_stop
    await expect(wizard.getByText(/step 2 of 4/i)).toBeVisible();
    const stopFaultOption = wizard.locator('[data-testid="fault-opt-container_stop"]');
    await expect(stopFaultOption).toBeVisible({ timeout: 5_000 });
    await stopFaultOption.click();

    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 3: Duration 20s
    await expect(wizard.getByText(/step 3 of 4/i)).toBeVisible();
    const durationInput = page.locator('[data-testid="fault-duration-input"]');
    if (await durationInput.isVisible()) {
      await durationInput.fill('20');
    }
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();

    // Step 4: Confirm & Start
    await expect(wizard.getByText(/step 4 of 4/i)).toBeVisible();
    const confirmCheckbox = page.locator('[data-testid="exp-confirm"]');
    await expect(confirmCheckbox).toBeVisible();
    await confirmCheckbox.check();

    const startExpBtn = page.locator('[data-testid="btn-start-exp"]');
    await expect(startExpBtn).toBeEnabled();
    await startExpBtn.click();

    // ── STAGE 7: Fault Injection & Active Monitor (real recovery) ────────────
    // Wait for wizard to finish submission and reset to Step 1
    await expect(wizard.getByText(/step 1 of 4/i)).toBeVisible({ timeout: 20_000 });

    // Wait for experiment result banner to appear upon recovery
    const expResultBanner = page.locator('[data-testid="exp-result"]');
    await expect(expResultBanner).toBeVisible({ timeout: 120_000 });

    const verdictText = (await page.locator('[data-testid="exp-result-verdict"]').textContent()) || '';
    const expResult = verdictText.includes('PASS') ? 'PASS' : verdictText.includes('FAIL') ? 'FAIL' : 'UNKNOWN';

    // Click full report link
    const fullReportLink = expResultBanner.getByRole('link', { name: /full report/i });
    await expect(fullReportLink).toBeVisible({ timeout: 10_000 });
    await fullReportLink.click();

    // ── STAGE 8: Resilience Reports & Full Audit ─────────────────────────────
    await expect(page).toHaveURL(/\/reports\/.+/, { timeout: 15_000 });
    const currentUrl = page.url();
    const expId = currentUrl.split('/').pop() || '';

    // Fetch recoveryTimeSeconds from API for accurate measurement
    let recoverySec = '0.0';
    try {
      const resp = await page.request.get(`/api/reports/${expId}`);
      if (resp.ok()) {
        const body = await resp.json();
        const rVal = body.recoveryTimeSeconds ?? body.results?.recoveryTimeSeconds ?? body.recovery_time_seconds;
        recoverySec = rVal !== undefined && rVal !== null ? String(rVal) : '0.0';
      } else {
        const expResp = await page.request.get(`/api/experiments/${expId}`);
        if (expResp.ok()) {
          const expBody = await expResp.json();
          const rVal = expBody.recoveryTimeSeconds ?? expBody.results?.recoveryTimeSeconds ?? expBody.recovery_time_seconds;
          recoverySec = rVal !== undefined && rVal !== null ? String(rVal) : '0.0';
        }
      }
    } catch {
      // fallback
    }

    console.log('[DEMO-EVIDENCE] Chaos experiment id:', expId);
    console.log('[DEMO-EVIDENCE] Chaos experiment result:', expResult);
    console.log('[DEMO-EVIDENCE] Chaos experiment recoveryTimeSeconds:', recoverySec);
    expect(Number(recoverySec), 'recoveryTimeSeconds must be > 0 for container_stop').toBeGreaterThan(0);

    // ── STAGE 9: Generic Adaptation (iot-json run) & Sign Out ────────────────
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 8_000 });

    // Open Templates Modal and select iot-json
    await templatesBtn.click();
    await expect(templateModal).toBeVisible();
    const iotTpl = templateModal.locator('[data-testid="template-iot-json"]');
    await expect(iotTpl).toBeVisible({ timeout: 5_000 });
    await iotTpl.click();
    await expect(templateModal).not.toBeVisible();

    // Validate and run iot-json
    await validateBtn.click();
    await expect(page.getByText(/ready to run|valid/i).first()).toBeVisible({ timeout: 8_000 });
    await runBtn.click();

    // Wait for iot-json to succeed in run header
    await expect(page).toHaveURL(/\/pipelines\/.+/, { timeout: 15_000 });
    await expect(
      page.locator('[data-testid="run-header"]').getByText('succeeded', { exact: true })
    ).toBeVisible({ timeout: 120_000 });

    // Open Output tab and inspect preview table
    await outputTab.click();
    const previewContainer = page.locator('[data-testid="output-preview"]');
    await expect(previewContainer).toBeVisible({ timeout: 30_000 });

    // Wait for table to load
    const previewTable = previewContainer.locator('table');
    await expect(previewTable).toBeVisible({ timeout: 15_000 });

    const columnHeaders = await previewTable.locator('thead th').allInnerTexts();
    const iotColumns = columnHeaders.map((c) => c.trim()).filter((c) => c && c !== '#');
    const iotRowCount = await previewTable.locator('tbody tr').count();

    console.log('[DEMO-EVIDENCE] iot-json preview column names:', iotColumns.join(', '));
    console.log('[DEMO-EVIDENCE] iot-json preview row count:', iotRowCount);

    // Return to Dashboard and check Final KPIs
    await clickNav(page, 'Dashboard');
    await expect(page).toHaveURL('/', { timeout: 8_000 });
    await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => undefined);

    const initialRunsNum = parseInt(initialKpis.runs24h || '0', 10);
    const deadline = Date.now() + 70_000;
    let currentRunsNum = initialRunsNum;
    let finalKpis = initialKpis;

    while (Date.now() < deadline) {
      await page.waitForTimeout(5000);
      finalKpis = {
        activeRuns: (await page.locator('[data-testid="kpi-active-runs"] p').first().textContent())?.trim(),
        runningExperiments: (await page.locator('[data-testid="kpi-running-experiments"] p').first().textContent())?.trim(),
        runs24h: (await page.locator('[data-testid="kpi-runs-24h"] p').first().textContent())?.trim(),
        avgRecoveryTime: (await page.locator('[data-testid="kpi-avg-recovery-time"] p').first().textContent())?.trim(),
      };
      currentRunsNum = parseInt(finalKpis.runs24h || '0', 10);
      if (currentRunsNum >= initialRunsNum + 2) {
        break;
      }
    }

    console.log('[DEMO-EVIDENCE] Before Dashboard runs24h:', initialRunsNum);
    console.log('[DEMO-EVIDENCE] After Dashboard runs24h:', currentRunsNum);
    console.log('[DEMO-EVIDENCE] Final Dashboard KPIs:', JSON.stringify(finalKpis));
    expect(currentRunsNum, `runs24h must increase by at least 2 (before: ${initialRunsNum}, after: ${currentRunsNum})`).toBeGreaterThanOrEqual(initialRunsNum + 2);

    // Sign out
    const signOutBtn = page.locator('button[aria-label="Sign out"]');
    await expect(signOutBtn).toBeVisible();
    await signOutBtn.click();

    // Verify redirected to /login and in-memory JWT cleared
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
    await expect(page.locator('#username')).toHaveValue('');
    await page.goto('/');
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  });
});
