const fs = require('fs');
const path = require('path');
const { chromium } = require(path.resolve(__dirname, '..', 'frontend', 'node_modules', '@playwright/test'));

function getCredentials() {
  const u = process.env.E2E_USERNAME;
  const p = process.env.E2E_PASSWORD;
  if (!u || !p) {
    throw new Error('E2E_USERNAME and E2E_PASSWORD environment variables are required.');
  }
  return { username: u, password: p };
}

async function measureMode(page, qualityName, durationMs = 30000) {
  console.log(`\n========================================`);
  console.log(`Starting 30s measurement for [${qualityName}] Quality`);
  console.log(`========================================`);

  // Disable auto-downgrade during benchmark measurement
  await page.evaluate(() => {
    window.__DISABLE_AUTO_DOWNGRADE__ = true;
  });

  // Switch quality via UI button
  const qualityBtn = page.getByTestId('viz-quality').getByRole('button', { name: qualityName });
  await qualityBtn.click();
  await page.waitForTimeout(1000);

  // Set up long tasks observer
  await page.evaluate(() => {
    window.__LONG_TASKS__ = [];
    if (window.__LONG_TASK_OBSERVER__) {
      window.__LONG_TASK_OBSERVER__.disconnect();
    }
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        window.__LONG_TASKS__.push({
          startTime: entry.startTime,
          duration: entry.duration,
          name: entry.name,
        });
      }
    });
    observer.observe({ entryTypes: ['longtask'] });
    window.__LONG_TASK_OBSERVER__ = observer;
  });

  // Force GC before measurement
  await page.evaluate(() => {
    if (typeof window.gc === 'function') {
      window.gc();
    }
  });

  // Sample initial state
  const initial = await page.evaluate(() => {
    const gl = window.__THREE_GL__;
    const renderFrames = gl && gl.info && gl.info.render ? gl.info.render.frame : 0;
    const mem = performance.memory ? performance.memory.usedJSHeapSize : null;
    return {
      renderFrames,
      heapBytes: mem,
      time: performance.now(),
    };
  });

  console.log(`[${qualityName}] Initial Render Frame Count: ${initial.renderFrames}`);
  console.log(`[${qualityName}] Initial Heap: ${initial.heapBytes ? (initial.heapBytes / (1024 * 1024)).toFixed(3) + ' MB (' + initial.heapBytes + ' bytes)' : 'N/A'}`);

  // Wait 30 seconds
  console.log(`[${qualityName}] Observing rendered frames over 30 seconds...`);
  await page.waitForTimeout(durationMs);

  // Sample post-wait state
  const postWait = await page.evaluate(() => {
    const gl = window.__THREE_GL__;
    const renderFrames = gl && gl.info && gl.info.render ? gl.info.render.frame : 0;
    const memBeforeGC = performance.memory ? performance.memory.usedJSHeapSize : null;
    return {
      renderFrames,
      heapBytesBeforeGC: memBeforeGC,
      time: performance.now(),
    };
  });

  // Force GC after measurement
  await page.evaluate(() => {
    if (typeof window.gc === 'function') {
      window.gc();
    }
  });

  const finalState = await page.evaluate(() => {
    const memAfterGC = performance.memory ? performance.memory.usedJSHeapSize : null;
    const longTasks = window.__LONG_TASKS__ || [];
    return {
      heapBytesAfterGC: memAfterGC,
      longTasks,
    };
  });

  const elapsedSec = (postWait.time - initial.time) / 1000;
  const renderedFrames = postWait.renderFrames - initial.renderFrames;
  const fps = renderedFrames / elapsedSec;

  const rawHeapGrowthBytes = postWait.heapBytesBeforeGC - initial.heapBytes;
  const rawHeapGrowthMB = rawHeapGrowthBytes / (1024 * 1024);

  const retainedHeapGrowthBytes = finalState.heapBytesAfterGC - initial.heapBytes;
  const retainedHeapGrowthMB = retainedHeapGrowthBytes / (1024 * 1024);

  console.log(`[${qualityName}] Elapsed: ${elapsedSec.toFixed(2)} s`);
  console.log(`[${qualityName}] Frames Rendered (renderer.info.render.frame delta): ${renderedFrames}`);
  console.log(`[${qualityName}] Measured FPS: ${fps.toFixed(2)}`);
  console.log(`[${qualityName}] Heap Before Post-GC: ${postWait.heapBytesBeforeGC ? (postWait.heapBytesBeforeGC / (1024 * 1024)).toFixed(3) + ' MB (' + postWait.heapBytesBeforeGC + ' bytes)' : 'N/A'}`);
  console.log(`[${qualityName}] Raw Heap Growth (before GC): ${rawHeapGrowthMB.toFixed(3)} MB (${rawHeapGrowthBytes} bytes)`);
  console.log(`[${qualityName}] Retained Heap (after GC): ${finalState.heapBytesAfterGC ? (finalState.heapBytesAfterGC / (1024 * 1024)).toFixed(3) + ' MB (' + finalState.heapBytesAfterGC + ' bytes)' : 'N/A'}`);
  console.log(`[${qualityName}] Retained Heap Growth: ${retainedHeapGrowthMB.toFixed(3)} MB (${retainedHeapGrowthBytes} bytes)`);
  console.log(`[${qualityName}] Long Tasks (>50ms): ${finalState.longTasks.length}`);
  if (finalState.longTasks.length > 0) {
    console.log(`[${qualityName}] Long Tasks Detail:`, JSON.stringify(finalState.longTasks));
  }

  return {
    quality: qualityName,
    elapsedSec: elapsedSec.toFixed(2),
    renderedFrames,
    fps: Number(fps.toFixed(1)),
    rawHeapGrowthBytes,
    rawHeapGrowthMB: Number(rawHeapGrowthMB.toFixed(2)),
    retainedHeapGrowthBytes,
    retainedHeapGrowthMB: Number(retainedHeapGrowthMB.toFixed(2)),
    longTasksCount: finalState.longTasks.length,
  };
}

async function main() {
  const yamlPath = path.resolve(__dirname, '..', 'backend', 'sample-configs', 'sales_large.yaml');
  const yamlContent = fs.readFileSync(yamlPath, 'utf8');

  console.log('Submitting 1M-row sales_large pipeline to http://localhost:8000/api/pipelines/run?wait=false...');
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
  console.log(`1M-row pipeline run started with ID: ${runId}`);

  const browser = await chromium.launch({
    headless: true,
    args: [
      '--use-gl=angle',
      '--use-angle=swiftshader',
      '--enable-unsafe-swiftshader',
      '--enable-webgl',
      '--disable-frame-rate-limit',
      '--disable-gpu-vsync',
      '--js-flags=--expose-gc',
    ],
  });

  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
  });

  const page = await context.newPage();
  const { username, password } = getCredentials();

  console.log('Logging in...');
  await page.goto('http://localhost:3000/login');
  await page.locator('#username').fill(username);
  await page.locator('#password').fill(password);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await page.waitForURL('http://localhost:3000/');

  console.log(`Navigating to run details page for ${runId}...`);
  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await page.waitForURL(/pipelines/);

  const firstRow = page.locator('table tbody tr').first();
  await firstRow.waitFor({ state: 'visible', timeout: 15000 });
  await firstRow.getByRole('button', { name: 'Open' }).click();
  await page.waitForURL(new RegExp(`/pipelines/${runId}`));
  console.log('On run details page.');

  // Wait for 3D viz canvas
  await page.getByTestId('viz-canvas').waitFor({ state: 'visible', timeout: 60000 });
  console.log('Viz canvas is visible.');

  // Ensure High quality is selected first so 3D scene mounts
  const qualityCtrl = page.getByTestId('viz-quality');
  await qualityCtrl.getByRole('button', { name: 'High' }).click();
  console.log('Switched to High quality.');

  // Ensure __THREE_GL__ is attached
  await page.waitForFunction(() => typeof window.__THREE_GL__ !== 'undefined', { timeout: 25000 });
  console.log('Three.js WebGLRenderer handle verified on window.__THREE_GL__');

  // Measure High Quality
  const highMetrics = await measureMode(page, 'High', 30000);

  // Measure Low Quality
  const lowMetrics = await measureMode(page, 'Low', 30000);

  console.log('\n========================================');
  console.log('FINAL PERFORMANCE MEASUREMENT RESULTS:');
  console.log('========================================');
  console.table([highMetrics, lowMetrics]);

  await context.close();
  await browser.close();

  return { runId, highMetrics, lowMetrics };
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error('Fatal error during performance measurement:', err);
    process.exit(1);
  }
);
