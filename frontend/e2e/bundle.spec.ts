/**
 * e2e/bundle.spec.ts
 * ------------------
 * Verifies code-splitting and lazy-loading of heavy dependencies via network request monitoring.
 *
 * Rules:
 * - /login and / (Dashboard) must NOT download Three.js / Scene3D or CodeMirror / YamlEditor chunks.
 * - Opening /pipelines downloads the CodeMirror / YamlEditor chunk.
 * - Opening a run page downloads the Scene3D / Three.js chunk.
 */
import { test, expect } from '@playwright/test';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME || !PASSWORD) {
    throw new Error('E2E_USERNAME and E2E_PASSWORD must be set.');
  }
});

test('Bundle: heavy chunks (CodeMirror & Three.js/Scene3D) are strictly lazy-loaded on demand', async ({
  page,
}) => {
  const requestedScripts: string[] = [];
  page.on('request', (req) => {
    const url = req.url();
    if (url.endsWith('.js') || url.includes('/assets/')) {
      requestedScripts.push(url);
    }
  });

  // 1. Visit /login
  await page.goto('/login');
  await expect(page.locator('#username')).toBeVisible();

  const loginScripts = [...requestedScripts];
  const loginHasScene = loginScripts.some((s) => s.includes('Scene3D') || s.includes('three'));
  const loginHasYaml = loginScripts.some((s) => s.includes('YamlEditor') || s.includes('codemirror'));
  expect(loginHasScene, 'Login must not download Three.js/Scene3D chunk').toBe(false);
  expect(loginHasYaml, 'Login must not download CodeMirror/YamlEditor chunk').toBe(false);

  // 2. Login to Dashboard
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/');
  await expect(page.getByRole('heading', { name: /system (health|status)|dashboard/i }).first()).toBeVisible();

  const dashScripts = [...requestedScripts];
  const dashHasScene = dashScripts.some((s) => s.includes('Scene3D') || s.includes('three'));
  const dashHasYaml = dashScripts.some((s) => s.includes('YamlEditor') || s.includes('codemirror'));
  expect(dashHasScene, 'Dashboard must not download Three.js/Scene3D chunk').toBe(false);
  expect(dashHasYaml, 'Dashboard must not download CodeMirror/YamlEditor chunk').toBe(false);

  // 3. Navigate to /pipelines -> YamlEditor chunk MUST be downloaded, Scene3D must NOT
  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await expect(page).toHaveURL(/pipelines/);
  await expect(page.locator('.cm-editor')).toBeVisible({ timeout: 15_000 });

  const pipelineScripts = [...requestedScripts];
  const pipelinesHasYaml = pipelineScripts.some((s) => s.includes('YamlEditor'));
  const pipelinesHasScene = pipelineScripts.some((s) => s.includes('Scene3D') || s.includes('three'));
  expect(pipelinesHasYaml, 'Pipelines page must download YamlEditor chunk').toBe(true);
  expect(pipelinesHasScene, 'Pipelines page must not download Three.js/Scene3D chunk').toBe(false);

  // 4. Navigate to a run details page (or create one / click first recent run) -> Scene3D MUST be downloaded
  // Try navigating to the first run in the table or from dashboard
  const runLink = page.locator('table tbody tr a').first();
  if (await runLink.isVisible()) {
    await runLink.click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/);
    await expect(page.locator('[data-testid="run-header"]')).toBeVisible({ timeout: 10_000 });

    const runDetailsScripts = [...requestedScripts];
    const runHasScene = runDetailsScripts.some((s) => s.includes('Scene3D'));
    expect(runHasScene, 'Run details page must download Scene3D chunk').toBe(true);
  }
});
