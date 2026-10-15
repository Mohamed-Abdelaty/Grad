/**
 * frontend/scripts/lighthouse-flow.mjs
 * -------------------------------------
 * Lighthouse User Flow Audit for ChaosFlow.
 *
 * Runs desktop Lighthouse audits across core user journeys:
 *  - Navigation: /login (Performance, Accessibility, Best Practices)
 *  - Interaction timespan: Dashboard -> Pipelines navigation
 *  - Snapshots: Dashboard, Pipelines, Pipeline Details, Reports
 *
 * Outputs HTML report to docs/lighthouse/flow.html and prints summary scores.
 */
import fs from 'fs';
import path from 'path';
import puppeteer from 'puppeteer';
import { startFlow, desktopConfig } from 'lighthouse';

const envPath = path.resolve('../.env');
let username = 'admin';
let password = 'password';

if (fs.existsSync(envPath)) {
  const content = fs.readFileSync(envPath, 'utf-8');
  const uMatch = content.match(/^ADMIN_USERNAME=(.*)$/m);
  const pMatch = content.match(/^ADMIN_PASSWORD=(.*)$/m);
  if (uMatch) username = uMatch[1].trim();
  if (pMatch) password = pMatch[1].trim();
}

async function run() {
  console.log('Launching browser for Lighthouse User Flow audit...');
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--window-size=1440,900',
    ],
    defaultViewport: { width: 1440, height: 900 },
  });

  const page = await browser.newPage();

  console.log('Initializing Lighthouse User Flow with desktop preset...');
  const flow = await startFlow(page, {
    name: 'ChaosFlow Desktop User Flow',
    config: desktopConfig,
  });

  // 1. Navigation: /login
  console.log('Step 1: Navigating to /login...');
  await flow.navigate('http://localhost:3000/login', {
    stepName: 'Login Navigation',
  });

  // 2. Perform Login
  console.log('Performing login through UI...');
  await page.waitForSelector('#username', { visible: true });
  await page.type('#username', username);
  await page.type('#password', password);
  await Promise.all([
    page.click('button[type="submit"]'),
    page.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 }).catch(() => {}),
  ]);
  await page.waitForSelector('#main', { timeout: 10000 });

  // 3. Snapshot: Dashboard
  console.log('Step 2: Snapshot of Dashboard...');
  await flow.snapshot({ stepName: 'Dashboard' });

  // 4. Timespan: Dashboard -> Pipelines
  console.log('Step 3: Timespan for navigation to Pipelines...');
  await flow.startTimespan({ stepName: 'Navigate to Pipelines' });
  const pipelinesNav = await page.$('aside nav a[href="/pipelines"]');
  if (pipelinesNav) {
    await pipelinesNav.click();
    await page.waitForSelector('.cm-editor', { timeout: 15000 }).catch(() => {});
  }
  await flow.endTimespan();

  // 5. Snapshot: Pipelines
  console.log('Step 4: Snapshot of Pipelines...');
  await flow.snapshot({ stepName: 'Pipelines' });

  // 6. Snapshot: Pipeline Run Details (if run exists)
  console.log('Step 5: Snapshot of Run Details...');
  const runRowLink = await page.$('table tbody tr a');
  if (runRowLink) {
    await runRowLink.click();
    await page.waitForSelector('[data-testid="run-header"]', { timeout: 10000 }).catch(() => {});
    await flow.snapshot({ stepName: 'Pipeline Run Details' });
  }

  // 7. Snapshot: Reports
  console.log('Step 6: Snapshot of Reports...');
  const reportsNav = await page.$('aside nav a[href="/reports"]');
  if (reportsNav) {
    await reportsNav.click();
    await page.waitForSelector('h1', { timeout: 10000 }).catch(() => {});
    await flow.snapshot({ stepName: 'Reports' });
  }

  // Generate Report
  console.log('Generating Lighthouse User Flow HTML report...');
  const reportHtml = await flow.generateReport();
  const outDir = path.resolve('../docs/lighthouse');
  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }
  const reportPath = path.join(outDir, 'flow.html');
  fs.writeFileSync(reportPath, reportHtml);
  console.log(`Report successfully written to: ${reportPath}`);

  // Summary scores table
  console.log('\n================ LIGHTHOUSE SCORES ================');
  const flowResult = await flow.createFlowResult();
  for (const step of flowResult.steps) {
    console.log(`\nStep: "${step.name}" (${step.lhr.gatherMode})`);
    const cats = step.lhr.categories || {};
    for (const [catKey, catVal] of Object.entries(cats)) {
      const score = catVal.score !== null ? Math.round(catVal.score * 100) : 'N/A';
      console.log(`  - ${catVal.title}: ${score}`);
    }
  }
  console.log('\n===================================================');

  await browser.close();
}

run().catch((err) => {
  console.error('Lighthouse Flow Error:', err);
  process.exit(1);
});
