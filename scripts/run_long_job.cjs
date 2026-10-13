const fs = require('fs');
const path = require('path');
const http = require('http');
const { chromium } = require(path.resolve(__dirname, '..', 'frontend', 'node_modules', '@playwright/test'));
const { execSync } = require('child_process');

async function main() {
  const yamlPath = path.resolve(__dirname, '..', 'backend', 'sample-configs', 'sales_large.yaml');
  const yamlContent = fs.readFileSync(yamlPath, 'utf8');

  console.log('Submitting sales_large.yaml to http://localhost:8000/api/pipelines/run?wait=false...');
  
  // Submit async run
  const res = await fetch('http://localhost:8000/api/pipelines/run?wait=false', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ yaml_content: yamlContent }),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Failed to submit pipeline: ${res.status} ${errText}`);
  }

  const runData = await res.json();
  const runId = runData.pipeline_run_id;
  console.log(`Run started with ID: ${runId}`);

  // Setup video directory
  const videoDir = path.resolve(__dirname, '..', 'docs', 'screenshots', 'ui-5', 'temp_video');
  if (!fs.existsSync(videoDir)) {
    fs.mkdirSync(videoDir, { recursive: true });
  }

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    recordVideo: {
      dir: videoDir,
      size: { width: 1440, height: 900 },
    },
  });

  const page = await context.newPage();

  // Login
  console.log('Logging in...');
  await page.goto('http://localhost:3000/login');
  await page.locator('#username').fill('admin');
  await page.locator('#password').fill('REDACTED');
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await page.waitForURL('http://localhost:3000/');

  // Navigate to Pipelines page
  console.log('Navigating to Pipelines page...');
  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await page.waitForURL(/pipelines/);

  // Wait for table and open the run
  console.log('Opening run details for ' + runId + '...');
  const firstRow = page.locator('table tbody tr').first();
  await firstRow.waitFor({ state: 'visible', timeout: 15000 });
  const openBtn = firstRow.getByRole('button', { name: 'Open' });
  await openBtn.click();
  await page.waitForURL(new RegExp(`/pipelines/${runId}`));
  console.log('Landed on run details page.');

  const stageKeys = ['source', 'kafka', 'spark', 'hdfs'];
  const observedStates = {
    source: new Set(),
    kafka: new Set(),
    spark: new Set(),
    hdfs: new Set(),
  };

  const snapshots = {};
  const startTime = Date.now();
  let isFinished = false;
  let finalStatus = 'unknown';

  async function getCounters() {
    const counters = {};
    const ids = [
      'rows-total',
      'rows-written',
      'rows-dropped',
      'rows-read',
      'source-rate',
      'spark-records-read',
      'spark-records-written',
      'spark-rate',
      'hdfs-file-count',
      'hdfs-bytes',
      'hdfs-duration',
    ];
    for (const id of ids) {
      try {
        const el = page.getByTestId(`counter-${id}`);
        if (await el.isVisible()) {
          counters[id] = (await el.innerText()).trim();
        } else {
          counters[id] = '—';
        }
      } catch {
        counters[id] = '—';
      }
    }
    return counters;
  }

  // 0s snapshot
  snapshots['0s'] = await getCounters();
  console.log('Recorded snapshot at 0s:', JSON.stringify(snapshots['0s']));

  const targetTimes = [15, 30, 45];
  const recordedTargets = new Set();

  while (!isFinished && Date.now() - startTime < 300_000) {
    const elapsedSec = Math.floor((Date.now() - startTime) / 1000);

    for (const t of targetTimes) {
      if (!recordedTargets.has(t) && elapsedSec >= t) {
        snapshots[`${t}s`] = await getCounters();
        recordedTargets.add(t);
        console.log(`Recorded snapshot at ${t}s:`, JSON.stringify(snapshots[`${t}s`]));
      }
    }

    // Check stage states
    for (const key of stageKeys) {
      try {
        const el = page.getByTestId(`stage-${key}`);
        if (await el.isVisible()) {
          const state = await el.getAttribute('data-state');
          if (state) {
            observedStates[key].add(state);
          }
        }
      } catch {
        // ignore
      }
    }

    // Check terminal status
    try {
      const header = page.getByTestId('run-header');
      if (await header.isVisible()) {
        const text = await header.innerText();
        if (text.includes('succeeded')) {
          isFinished = true;
          finalStatus = 'succeeded';
        } else if (text.includes('failed')) {
          isFinished = true;
          finalStatus = 'failed';
        } else if (text.includes('cancelled')) {
          isFinished = true;
          finalStatus = 'cancelled';
        }
      }
    } catch {
      // ignore
    }

    if (!isFinished) {
      await page.waitForTimeout(1000);
    }
  }

  // Finish snapshot
  snapshots['finished'] = await getCounters();
  console.log('Recorded snapshot at finished:', JSON.stringify(snapshots['finished']));
  console.log(`Run terminated with status: ${finalStatus} in ${Math.round((Date.now() - startTime) / 1000)}s`);

  // Close context to save video
  await page.waitForTimeout(1000);
  const video = page.video();
  await context.close();
  await browser.close();

  const finalVideoPath = path.resolve(__dirname, '..', 'docs', 'screenshots', 'ui-5', 'long-run.webm');
  if (video) {
    const tempVideoPath = await video.path();
    if (fs.existsSync(finalVideoPath)) {
      fs.unlinkSync(finalVideoPath);
    }
    fs.renameSync(tempVideoPath, finalVideoPath);
    const stat = fs.statSync(finalVideoPath);
    console.log(`Video saved to: ${finalVideoPath} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`);
  }

  // Clean temp video directory if exists
  try {
    if (fs.existsSync(videoDir)) {
      fs.rmSync(videoDir, { recursive: true, force: true });
    }
  } catch {}

  console.log('\n--- OBSERVED DATA-STATE SEQUENCES ---');
  for (const [key, states] of Object.entries(observedStates)) {
    console.log(`Stage ${key}: [${Array.from(states).join(' -> ')}]`);
  }

  console.log('\n--- COUNTER SNAPSHOTS ---');
  console.table(snapshots);

  // Return runId for verification
  return runId;
}

main().then(
  (runId) => {
    console.log('\nRUN_ID=' + runId);
    process.exit(0);
  },
  (err) => {
    console.error(err);
    process.exit(1);
  }
);
