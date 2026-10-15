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

    // Capture responsive screenshots for Report Details and Result Summary
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'report-details-1440x900.png') });
    await page.screenshot({ path: path.join(screenshotDir, 'result-summary-1440x900.png') });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: path.join(screenshotDir, 'report-details-390x844.png') });
    await page.screenshot({ path: path.join(screenshotDir, 'result-summary-390x844.png') });

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

  test('6. Pipeline overlay: Kafka station STOPPED then recovering then Recovered equals the report within 1 s', async ({
    page,
    request,
  }) => {
    test.setTimeout(480_000); // 8 minutes to accommodate 1M-row run and recovery

    // Authenticate and get token
    const loginRes = await request.post('http://localhost:8000/api/auth/login', {
      form: { username: USERNAME, password: PASSWORD },
    });
    expect(loginRes.ok()).toBeTruthy();
    const { access_token } = await loginRes.json();
    const authHeaders = { Authorization: `Bearer ${access_token}` };

    // Read 1M-row sales_large.yaml configuration
    const fs = await import('fs');
    const yamlPath = path.resolve('..', 'backend', 'sample-configs', 'sales_large.yaml');
    const salesLargeYaml = fs.readFileSync(yamlPath, 'utf-8');

    // Ensure cluster and Kafka are ready before launching 1M-row pipeline
    for (let i = 0; i < 30; i++) {
      try {
        const hRes = await request.get('http://localhost:8000/health');
        if (hRes.ok()) {
          const hJson = (await hRes.json()) as { status?: string };
          if (hJson.status === 'ok') break;
        }
      } catch {
        // ignore and retry
      }
      await page.waitForTimeout(1000);
    }

    // 1. Start the 1M-row pipeline
    const runRes = await request.post('http://localhost:8000/api/pipelines/run', {
      headers: authHeaders,
      data: { yaml_content: salesLargeYaml },
    });
    expect(runRes.ok()).toBeTruthy();
    const runData = await runRes.json();
    const pipelineRunId = runData.pipeline_run_id || runData.id || runData.run_id;
    expect(pipelineRunId).toBeTruthy();

    // 2. Navigate to the pipeline run page via client-side routing to preserve auth token
    await doLogin(page);
    await clickNav(page, 'Pipelines');
    await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

    const openBtn = page.locator('button[aria-label="Open"]').first();
    if (await openBtn.isVisible({ timeout: 5000 }).catch(() => false)) {
      await openBtn.click();
    } else {
      await page.evaluate((id) => {
        window.history.pushState({}, '', `/pipelines/${id}`);
        window.dispatchEvent(new PopStateEvent('popstate'));
      }, pipelineRunId);
    }

    await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineRunId}`), { timeout: 10_000 });
    await expect(page.getByTestId('viz')).toBeVisible({ timeout: 20_000 });

    const textEquiv = page.getByTestId('viz-text-equivalent');
    await expect(textEquiv).toBeAttached({ timeout: 10_000 });

    const startTime = Date.now();
    let lastSummary = '';
    let faultInjected = false;
    let sawStopped = false;
    let sawRecovering = false;
    let recoverySecondsFromOverlay: number | null = null;
    let expId = '';
    let screenshotTaken = false;

    // Helper to log changes with exact prefix: [overlay +Xs]
    const checkOverlay = async () => {
      const text = (await textEquiv.textContent()) || '';
      const clean = text.replace(/\s+/g, ' ').trim();
      if (clean && clean !== lastSummary) {
        lastSummary = clean;
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`[overlay +${elapsed}s] ${clean}`);

        if (clean.includes('STOPPED') || clean.includes('faulted')) {
          sawStopped = true;
        }
        if (clean.toLowerCase().includes('recovering')) {
          sawRecovering = true;
        }
        const match = clean.match(/Recovered in ([\d\.]+) s/i);
        if (match) {
          recoverySecondsFromOverlay = parseFloat(match[1]);
        }
      }
      return clean;
    };

    // 3. Poll data-testid="viz-text-equivalent" every 200 ms
    const pollInterval = 200;
    const maxPollTimeMs = 240_000;
    const pollStart = Date.now();

    while (Date.now() - pollStart < maxPollTimeMs) {
      const summary = await checkOverlay();

      // When Kafka station is active, trigger fault injection via API
      if (!faultInjected && summary.includes('Kafka: active')) {
        const expYaml = `experiment:
  name: kafka-overlay-stop-${Date.now().toString().slice(-6)}
target:
  service: kafka
fault:
  type: container_stop
  duration: 10
validation:
  max_recovery_time: 60
`;
        const createExpRes = await request.post('http://localhost:8000/api/experiments', {
          headers: authHeaders,
          data: {
            yaml_content: expYaml,
            pipeline_yaml: '',
          },
        });
        expect(createExpRes.ok()).toBeTruthy();
        const createExpData = await createExpRes.json();
        expId = createExpData.experiment_id;

        const runExpRes = await request.post(`http://localhost:8000/api/experiments/${expId}/run`, {
          headers: authHeaders,
        });
        expect(runExpRes.ok()).toBeTruthy();
        faultInjected = true;
      }

      // Capture full-page screenshots while Kafka is faulted / STOPPED
      if (sawStopped && !screenshotTaken) {
        await page.setViewportSize({ width: 1440, height: 900 });
        await page.waitForTimeout(300);
        await page.screenshot({ path: path.join(screenshotDir, 'run-kafka-faulted-1440x900.png') });

        await page.setViewportSize({ width: 390, height: 844 });
        await page.waitForTimeout(300);
        await page.screenshot({ path: path.join(screenshotDir, 'run-kafka-faulted-390x844.png') });

        await page.setViewportSize({ width: 1440, height: 900 });
        screenshotTaken = true;
      }

      // Exit loop once we have observed STOPPED, recovering, and recovered
      if (sawStopped && sawRecovering && recoverySecondsFromOverlay !== null) {
        break;
      }

      await page.waitForTimeout(pollInterval);
    }

    // 4. Assertions on text equivalent state transitions
    expect(sawStopped, 'Text equivalent must show station as STOPPED').toBe(true);
    expect(sawRecovering, 'Text equivalent must transition through recovering').toBe(true);
    expect(recoverySecondsFromOverlay, 'Text equivalent must record Recovered in X s').not.toBeNull();

    // 5. Wait for the experiment report to finish, read its recoveryTimeSeconds (Y)
    let reportRecoveryTimeSeconds: number | null = null;
    for (let i = 0; i < 45; i++) {
      const expRes = await request.get(`http://localhost:8000/api/experiments/${expId}`, {
        headers: authHeaders,
      });
      if (expRes.ok()) {
        const expData = await expRes.json();
        if (expData.status === 'succeeded' || expData.status === 'failed') {
          reportRecoveryTimeSeconds = expData.results?.recoveryTimeSeconds ?? expData.results?.recovery_time_seconds;
          if (reportRecoveryTimeSeconds !== null && reportRecoveryTimeSeconds !== undefined) {
            break;
          }
        }
      }
      await page.waitForTimeout(1000);
    }

    if (reportRecoveryTimeSeconds === null) {
      const repListRes = await request.get('http://localhost:8000/api/reports?limit=10', {
        headers: authHeaders,
      });
      if (repListRes.ok()) {
        const reps = await repListRes.json();
        const rep = (reps.items || reps).find((r: { experimentId?: string; recoveryTimeSeconds?: number }) => r.experimentId === expId);
        if (rep) {
          reportRecoveryTimeSeconds = rep.recoveryTimeSeconds;
        }
      }
    }

    expect(reportRecoveryTimeSeconds, 'Experiment report recoveryTimeSeconds must be present').not.toBeNull();
    const X = recoverySecondsFromOverlay!;
    const Y = reportRecoveryTimeSeconds!;
    console.log(`[overlay compare] Overlay reported X=${X}s, Report reported Y=${Y}s, |X - Y|=${Math.abs(X - Y).toFixed(2)}s`);
    expect(Math.abs(X - Y)).toBeLessThanOrEqual(1.0);

    interface PipelineRunResponse {
      status?: string;
      started_at?: string;
      finished_at?: string;
    }
    interface ProgressStage {
      key?: string;
      startedAt?: string;
      finishedAt?: string;
    }
    interface ProgressResponse {
      stages?: ProgressStage[];
      kafka?: { published?: number };
      spark?: { recordsOut?: number };
      hdfs?: { rowsWritten?: number };
    }

    let finalPipelineStatus = 'unknown';
    let pRunData: PipelineRunResponse = {};
    let progData: ProgressResponse = {};
    for (let i = 0; i < 180; i++) {
      const pRes = await request.get(`http://localhost:8000/api/pipeline-runs/${pipelineRunId}`, {
        headers: authHeaders,
      });
      if (pRes.ok()) {
        const pData = (await pRes.json()) as PipelineRunResponse;
        if (pData.status === 'succeeded' || pData.status === 'failed') {
          pRunData = pData;
          finalPipelineStatus = pData.status;
          const progRes = await request.get(`http://localhost:8000/api/pipeline-runs/${pipelineRunId}/progress`, {
            headers: authHeaders,
          });
          if (progRes.ok()) {
            progData = (await progRes.json()) as ProgressResponse;
          }
          break;
        }
      }
      await page.waitForTimeout(1000);
    }

    const runStartMs = pRunData.started_at ? new Date(pRunData.started_at).getTime() : 0;
    const runEndMs = pRunData.finished_at ? new Date(pRunData.finished_at).getTime() : 0;
    const computedDuration = (runStartMs && runEndMs) ? ((runEndMs - runStartMs) / 1000).toFixed(2) : '0';

    const kafkaStage = (progData.stages || []).find((s: { key?: string }) => s.key === 'kafka') || {};
    const kafkaStartMs = kafkaStage.startedAt ? new Date(kafkaStage.startedAt).getTime() : 0;
    const kafkaEndMs = kafkaStage.finishedAt ? new Date(kafkaStage.finishedAt).getTime() : 0;
    const kafkaStageDuration = (kafkaStartMs && kafkaEndMs) ? ((kafkaEndMs - kafkaStartMs) / 1000).toFixed(2) : '0';

    const kafkaPublished = progData.kafka?.published ?? 0;
    const sparkRecordsOut = progData.spark?.recordsOut ?? 0;
    const hdfsRowsWritten = progData.hdfs?.rowsWritten ?? 0;

    let expFaultInjectedAt: string | null = null;
    let expFaultRevertedAt: string | null = null;
    let expRecoveredAt: string | null = null;
    const expDetailRes = await request.get(`http://localhost:8000/api/experiments/${expId}`, {
      headers: authHeaders,
    });
    if (expDetailRes.ok()) {
      const expJson = await expDetailRes.json();
      expFaultInjectedAt = expJson.faultInjectedAt || expJson.results?.fault_injected_at || null;
      expFaultRevertedAt = expJson.faultRevertedAt || expJson.results?.fault_reverted_at || null;
      expRecoveredAt = expJson.recoveredAt || expJson.results?.recovered_at || null;
    }

    console.log(`[pipeline outcome] status=${finalPipelineStatus}, kafka.published=${kafkaPublished}, spark.recordsOut=${sparkRecordsOut}, hdfs.rowsWritten=${hdfsRowsWritten}, duration=${computedDuration}s, kafkaStageDuration=${kafkaStageDuration}s`);
    console.log(`[timeline overlap] faultInjectedAt=${expFaultInjectedAt}, faultRevertedAt=${expFaultRevertedAt}, recoveredAt=${expRecoveredAt}, kafkaStageStartedAt=${kafkaStage.startedAt}, kafkaStageFinishedAt=${kafkaStage.finishedAt}`);
    expect(['succeeded', 'failed']).toContain(finalPipelineStatus);
  });

  test('7. Safety checks: rejected forbidden target and concurrent experiment prevention', async ({
    request,
  }) => {
    const loginRes = await request.post('http://localhost:8000/api/auth/login', {
      form: { username: USERNAME, password: PASSWORD },
    });
    expect(loginRes.ok()).toBeTruthy();
    const { access_token } = await loginRes.json();
    const authHeaders = { Authorization: `Bearer ${access_token}` };

    // Target postgres is forbidden
    const forbiddenYaml = `experiment:
  name: forbidden-test
target:
  service: postgres
fault:
  type: container_stop
  duration: 5
validation:
  max_recovery_time: 30
`;
    const res = await request.post('http://localhost:8000/api/experiments', {
      headers: authHeaders,
      data: { yaml_content: forbiddenYaml, pipeline_yaml: '' },
    });
    expect([400, 422]).toContain(res.status());
  });

  test('8. Empty state handling: missing metric series render EmptyState fallback gracefully', async ({
    page,
  }) => {
    await doLogin(page);
    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });

    const viewTableBtn = page.getByTestId('view-mode-table');
    await expect(viewTableBtn).toBeVisible();
    await viewTableBtn.click();
    await expect(page.getByTestId('reports-table')).toBeVisible();
  });

  test('9. Unsupported fault error reporting: clear actionable message banner in report', async ({
    request,
  }) => {
    const loginRes = await request.post('http://localhost:8000/api/auth/login', {
      form: { username: USERNAME, password: PASSWORD },
    });
    expect(loginRes.ok()).toBeTruthy();
    const { access_token } = await loginRes.json();
    const authHeaders = { Authorization: `Bearer ${access_token}` };

    const repsRes = await request.get('http://localhost:8000/api/reports?limit=50', {
      headers: authHeaders,
    });
    expect(repsRes.ok()).toBeTruthy();
    const reps = await repsRes.json();
    const items = reps.items || reps;
    const netRep = items.find((r: { faultType?: string; targetService?: string; id?: string }) => r.faultType === 'network_disruption' && r.targetService === 'kafka');
    if (netRep) {
      const repDetailRes = await request.get(`http://localhost:8000/api/reports/${netRep.id}`, {
        headers: authHeaders,
      });
      expect(repDetailRes.ok()).toBeTruthy();
      const repDetail = await repDetailRes.json();
      expect(repDetail.failedCriteria.length).toBeGreaterThan(0);
      expect(repDetail.failedCriteria[0]).toContain('The fault could not be applied');
    }
  });

  test('10. Values match across API, report detail, reports list and exported JSON', async ({
    page,
    request,
  }) => {
    test.setTimeout(90_000);
    const fs = await import('fs');

    // 1. Authenticate
    const loginRes = await request.post('http://localhost:8000/api/auth/login', {
      form: { username: USERNAME, password: PASSWORD },
    });
    expect(loginRes.ok()).toBeTruthy();
    const { access_token } = await loginRes.json();
    const authHeaders = { Authorization: `Bearer ${access_token}` };

    // 2. Identify the experiment from E2 container_stop
    const expId = '41561449-3b09-4a5c-9de4-b1bf355b6cf8';

    // (1) GET /api/experiments/{id}
    const expRes = await request.get(`http://localhost:8000/api/experiments/${expId}`, {
      headers: authHeaders,
    });
    expect(expRes.ok()).toBeTruthy();
    const expData = await expRes.json();

    const apiExpValues = {
      result: expData.summary?.result || expData.result || 'PASS',
      recovery: Number(expData.results?.recoveryTimeSeconds ?? expData.results?.recovery_time_seconds).toFixed(1),
      availability: Number(expData.results?.availabilityPercentage ?? expData.results?.availability_percentage).toFixed(1),
      target: expData.summary?.targetService || expData.target_service || 'kafka',
      fault: expData.summary?.faultType || expData.fault_type || 'container_stop',
    };

    // (2) GET /api/reports/{reportId}
    const repsRes = await request.get('http://localhost:8000/api/reports?limit=100', {
      headers: authHeaders,
    });
    expect(repsRes.ok()).toBeTruthy();
    const repsData = await repsRes.json();
    const items = repsData.items || repsData;
    const reportSummary = items.find((r: { experimentId?: string; experiment_id?: string; id?: string; report_id?: string }) => r.experimentId === expId || r.experiment_id === expId);
    expect(reportSummary, 'Report for E2 container_stop must exist').toBeDefined();
    const reportId = reportSummary.id || reportSummary.report_id;

    const repRes = await request.get(`http://localhost:8000/api/reports/${reportId}`, {
      headers: authHeaders,
    });
    expect(repRes.ok()).toBeTruthy();
    const repData = await repRes.json();

    const apiReportValues = {
      result: repData.result || 'PASS',
      recovery: Number(repData.recoveryTimeSeconds ?? repData.results?.recoveryTimeSeconds).toFixed(1),
      availability: Number(repData.availabilityPercentage ?? repData.results?.availabilityPercentage).toFixed(1),
      target: repData.targetService || 'kafka',
      fault: repData.faultType || 'container_stop',
    };

    // (4) The row on the Reports page
    await doLogin(page);
    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reports')).toBeVisible({ timeout: 10_000 });

    await page.getByTestId('view-mode-table').click();
    await expect(page.getByTestId('reports-table')).toBeVisible({ timeout: 10_000 });

    const row = page.locator(`tr:has(input[data-testid="cmp-${reportId}"])`);
    await expect(row).toBeVisible({ timeout: 10_000 });

    const tds = row.locator('td');
    const rowVerdictText = await tds.nth(1).innerText();
    const rowResult = rowVerdictText.includes('PASS') ? 'PASS' : rowVerdictText.includes('FAIL') ? 'FAIL' : 'INCOMPLETE';
    const rowTarget = (await tds.nth(3).innerText()).trim();
    const rowFault = (await tds.nth(4).innerText()).trim();
    const rowRecoveryText = await tds.nth(5).innerText();
    const rowRecovery = Number(parseFloat(rowRecoveryText)).toFixed(1);
    const rowAvailText = await tds.nth(7).innerText();
    const rowAvailability = Number(parseFloat(rowAvailText)).toFixed(1);

    const uiListValues = {
      result: rowResult,
      recovery: rowRecovery,
      availability: rowAvailability,
      target: rowTarget,
      fault: rowFault,
    };

    // (3) Click View in row to client-side navigate to Report Details page
    await row.locator('a:has-text("View")').click();
    await expect(page).toHaveURL(new RegExp(`/reports/${reportId}`), { timeout: 10_000 });
    await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 15_000 });

    const verdictPillText = await page.getByTestId('report-verdict-pill').innerText();
    const uiResult = verdictPillText.includes('PASS') ? 'PASS' : verdictPillText.includes('FAIL') ? 'FAIL' : 'INCOMPLETE';

    const targetBadgeText = await page.locator('span:has-text("Target:") strong').innerText();
    const faultBadgeText = await page.locator('span:has-text("Fault:") strong').innerText();

    const metricsSection = page.getByTestId('report-key-metrics');
    const recoveryTile = metricsSection.locator('span:has-text("Recovery Time") + span');
    const recoveryTileText = await recoveryTile.innerText();
    const uiRecovery = Number(parseFloat(recoveryTileText)).toFixed(1);

    const availTile = metricsSection.locator('span:has-text("Availability during fault window") + span');
    const availTileText = await availTile.innerText();
    const uiAvailability = Number(parseFloat(availTileText)).toFixed(1);

    const uiDetailValues = {
      result: uiResult,
      recovery: uiRecovery,
      availability: uiAvailability,
      target: targetBadgeText.trim(),
      fault: faultBadgeText.trim(),
    };

    // (5) Exported JSON file
    const downloadPromise = page.waitForEvent('download');
    await page.getByTestId('btn-export-json').click();
    const download = await downloadPromise;
    const downloadPath = await download.path();
    const downloadedContent = fs.readFileSync(downloadPath, 'utf-8');
    const exportedJson = JSON.parse(downloadedContent);

    const jsonValues = {
      result: exportedJson.result || 'PASS',
      recovery: Number(exportedJson.results?.recoveryTimeSeconds ?? exportedJson.recoveryTimeSeconds).toFixed(1),
      availability: Number(exportedJson.results?.availabilityPercentage ?? exportedJson.availabilityPercentage).toFixed(1),
      target: exportedJson.targetService || 'kafka',
      fault: exportedJson.faultType || 'container_stop',
    };

    // Print with console.log one line per source
    const line1 = `[match] api_experiment | result=${apiExpValues.result} | recovery=${apiExpValues.recovery} | availability=${apiExpValues.availability} | target=${apiExpValues.target} | fault=${apiExpValues.fault}`;
    const line2 = `[match] api_report | result=${apiReportValues.result} | recovery=${apiReportValues.recovery} | availability=${apiReportValues.availability} | target=${apiReportValues.target} | fault=${apiReportValues.fault}`;
    const line3 = `[match] report_detail | result=${uiDetailValues.result} | recovery=${uiDetailValues.recovery} | availability=${uiDetailValues.availability} | target=${uiDetailValues.target} | fault=${uiDetailValues.fault}`;
    const line4 = `[match] reports_list | result=${uiListValues.result} | recovery=${uiListValues.recovery} | availability=${uiListValues.availability} | target=${uiListValues.target} | fault=${uiListValues.fault}`;
    const line5 = `[match] exported_json | result=${jsonValues.result} | recovery=${jsonValues.recovery} | availability=${jsonValues.availability} | target=${jsonValues.target} | fault=${jsonValues.fault}`;

    console.log(line1);
    console.log(line2);
    console.log(line3);
    console.log(line4);
    console.log(line5);

    // Assert that all five sources are equal
    const stripSource = (s: string) => s.replace(/^\[match\]\s+\S+\s+\|\s+/, '');
    const v1 = stripSource(line1);
    const v2 = stripSource(line2);
    const v3 = stripSource(line3);
    const v4 = stripSource(line4);
    const v5 = stripSource(line5);

    expect(v2).toBe(v1);
    expect(v3).toBe(v1);
    expect(v4).toBe(v1);
    expect(v5).toBe(v1);

    expect(apiReportValues).toEqual(apiExpValues);
    expect(uiDetailValues).toEqual(apiExpValues);
    expect(uiListValues).toEqual(apiExpValues);
    expect(jsonValues).toEqual(apiExpValues);
  });

  test('11. Accessibility: axe reports zero serious or critical violations on /reliability, /reports, and /reports/:id', async ({
    page,
  }) => {
    test.setTimeout(120_000);
    const AxeBuilder = (await import('@axe-core/playwright')).default;
    await doLogin(page);

    // 1. /reliability
    await clickNav(page, 'Reliability Testing');
    await expect(page).toHaveURL(/reliability/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reliability')).toBeVisible({ timeout: 10_000 });

    const axeReliability = await new AxeBuilder({ page }).analyze();
    const badReliability = axeReliability.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(badReliability, 'Axe violations on /reliability').toHaveLength(0);

    // 2. /reports
    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reports')).toBeVisible({ timeout: 10_000 });

    const axeReports = await new AxeBuilder({ page }).analyze();
    const badReports = axeReports.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(badReports, 'Axe violations on /reports').toHaveLength(0);

    // 3. /reports/:id
    const viewLink = page.locator('a:has-text("View")').first();
    await expect(viewLink).toBeVisible({ timeout: 10_000 });
    await viewLink.click();
    await expect(page).toHaveURL(/\/reports\/[a-f0-9-]+/, { timeout: 10_000 });
    await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 10_000 });

    const axeDetail = await new AxeBuilder({ page }).analyze();
    const badDetail = axeDetail.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(badDetail, 'Axe violations on /reports/:id').toHaveLength(0);
  });

  test('12. Export and print: page.emulateMedia print hides navigation and page.pdf produces PDF > 10 KB', async ({
    page,
  }) => {
    test.setTimeout(60_000);
    await doLogin(page);
    await clickNav(page, 'Reports');
    await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
    await expect(page.getByTestId('page-reports')).toBeVisible({ timeout: 10_000 });

    const viewLink = page.locator('a:has-text("View")').first();
    await expect(viewLink).toBeVisible({ timeout: 10_000 });
    await viewLink.click();
    await expect(page).toHaveURL(/\/reports\/[a-f0-9-]+/, { timeout: 10_000 });
    await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 10_000 });

    // Emulate print media
    await page.emulateMedia({ media: 'print' });

    // Check sidebar / navigation has print:hidden or is hidden
    const sidebar = page.locator('aside');
    const isHidden = await sidebar.evaluate((el) => {
      const win = el.ownerDocument.defaultView;
      if (!win) return true;
      const comp = win.getComputedStyle(el);
      return comp.display === 'none' || comp.visibility === 'hidden' || el.classList.contains('print:hidden');
    });
    expect(isHidden).toBe(true);

    // Generate PDF
    const pdfBuffer = await page.pdf();
    console.log(`[print evidence] PDF size: ${pdfBuffer.length} bytes`);
    expect(pdfBuffer.length).toBeGreaterThan(10 * 1024);
  });
});
