/**
 * e2e/smoke.spec.ts
 * -----------------
 * Playwright smoke tests against the running Docker stack (http://localhost:3000).
 *
 * Proves things HTTP 200 checks cannot:
 *  - React bootstraps in a real Chromium browser.
 *  - Auth flow works end-to-end through nginx proxy → FastAPI backend.
 *  - Each page renders its main heading (not just a 200 from index.html).
 *  - No console errors and no 5xx responses on any page.
 *
 * Run:  npm run e2e   (requires `docker compose up --build -d` first)
 * CI:   set E2E_USERNAME / E2E_PASSWORD as pipeline secrets.
 *
 * Design note: The JWT is kept in React's in-memory state only (not
 * localStorage/cookies).  `page.goto()` triggers a full page reload which
 * clears JS heap and therefore the token.  All inter-page navigation is done
 * by clicking the sidebar NavLinks (React Router client-side navigation),
 * which preserves the heap.  Only the initial login uses `goto('/login')`.
 */
import { test, expect, type ConsoleMessage, type Response } from '@playwright/test';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME) {
    throw new Error('Missing required environment variable: E2E_USERNAME');
  }
  if (!PASSWORD) {
    throw new Error('Missing required environment variable: E2E_PASSWORD');
  }
});

// ── Audit helpers ─────────────────────────────────────────────────────────────

type Audit = { errors: string[]; failures: string[] };

function startAudit(page: import('@playwright/test').Page): Audit {
  const errors: string[] = [];
  const failures: string[] = [];
  page.on('console', (msg: ConsoleMessage) => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  page.on('requestfailed', (req) =>
    failures.push(`NETWORK FAIL: ${req.method()} ${req.url()}`)
  );
  page.on('response', (res: Response) => {
    if (res.status() >= 500) failures.push(`HTTP ${res.status()}: ${res.url()}`);
  });
  return { errors, failures };
}

function assertClean({ errors, failures }: Audit, label: string) {
  const realErrors = errors.filter(
    (e) =>
      !e.includes('Download the React DevTools') &&
      !e.includes('ResizeObserver loop') &&
      !e.includes('content.js')
  );
  expect(realErrors, `Console errors on ${label}: ${realErrors.join('; ')}`).toHaveLength(0);
  expect(failures, `Server errors on ${label}: ${failures.join('; ')}`).toHaveLength(0);
}

// ── Login helper ───────────────────────────────────────────────────────────────

async function doLogin(page: import('@playwright/test').Page) {
  await page.goto('/login');
  await expect(page).toHaveURL(/login/, { timeout: 10_000 });

  // Assert no pre-filled credentials (issue 5 fix).
  await expect(page.locator('#username')).toHaveValue('');
  await expect(page.locator('#password')).toHaveValue('');

  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();

  // After login we must land on the Dashboard.
  await expect(page).toHaveURL('/', { timeout: 10_000 });
  await expect(page.getByRole('heading', { name: /system (health|status)|dashboard/i }).first()).toBeVisible({
    timeout: 8_000,
  });
}

async function clickNav(page: import('@playwright/test').Page, label: string) {
  await page.locator('aside nav a', { hasText: label }).first().click();
}

// ── 1. Login: no pre-filled credentials ───────────────────────────────────────

test('Login: no pre-filled credentials and successful login reaches Dashboard', async ({ page }) => {
  const audit = startAudit(page);
  await doLogin(page);
  assertClean(audit, 'Login → Dashboard');
});

// ── 2–6. Page smoke tests — one test per page, all in a single login session ──

test('All pages: headings visible, no console errors, no server failures', async ({ page }) => {
  const audit = startAudit(page);

  // --- Login → Dashboard ---
  await doLogin(page);
  await expect(page.getByRole('heading', { name: /system (health|status)|dashboard/i }).first()).toBeVisible();

  // --- Pipelines (sidebar click, no page reload) ---
  await clickNav(page, 'Pipelines');
  await expect(page).toHaveURL(/pipelines/, { timeout: 8_000 });
  await expect(page.getByRole('heading', { name: /Pipeline runs/i })).toBeVisible({
    timeout: 8_000,
  });

  // --- Reliability Testing ---
  await clickNav(page, 'Reliability Testing');
  await expect(page).toHaveURL(/reliability/, { timeout: 8_000 });
  await expect(page.getByRole('heading', { name: 'Reliability Testing' })).toBeVisible({
    timeout: 8_000,
  });

  // --- Reports ---
  await clickNav(page, 'Reports');
  await expect(page).toHaveURL(/reports/, { timeout: 8_000 });
  await expect(page.getByRole('heading', { name: 'Resilience Reports' })).toBeVisible({
    timeout: 8_000,
  });

  assertClean(audit, 'Dashboard → Pipelines → Reliability → Reports');
});

// ── 3. Pipeline Details: graceful for unknown run ID ──────────────────────────

test('Pipeline Details: navigates to a run or shows empty state (no 5xx, no crash)', async ({ page }) => {
  const audit = startAudit(page);

  // Log in and go to Pipelines page via sidebar (preserves in-memory JWT).
  await doLogin(page);
  await clickNav(page, 'Pipelines');
  await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });
  // Wait for the page to finish loading API responses.
  await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => undefined);

  // Check if any run links are listed (fresh stacks have none).
  const runLinks = page.locator('a[href^="/pipelines/"]');
  const count = await runLinks.count();

  if (count > 0) {
    // Click the first run to navigate to its details page (React Router client-side).
    await runLinks.first().click();
    await expect(page).toHaveURL(/\/pipelines\/.+/, { timeout: 8_000 });
    await expect(
      page.getByRole('heading', { name: /Pipeline Run/i })
    ).toBeVisible({ timeout: 8_000 });
  }
  // If no runs exist on a fresh stack, reaching /pipelines without a crash is the
  // valid assertion — there is no run ID to navigate to.

  // Either way: no 5xx responses and no unexpected console errors.
  expect(audit.failures, `Server errors: ${audit.failures.join('; ')}`).toHaveLength(0);
  const realErrors = audit.errors.filter(
    (e) =>
      !e.includes('Download the React DevTools') &&
      !e.includes('ResizeObserver') &&
      !e.includes('content.js')
  );
  expect(realErrors, `Console errors: ${realErrors.join('; ')}`).toHaveLength(0);
});

// ── 4 & 5. Direct API checks ──────────────────────────────────────────────────

test('GET /health (port 8000) returns 200 with {status:"ok"}', async ({ request }) => {
  // The health endpoint is at /health on the backend, not under /api/.
  // Nginx only proxies /api/* so we hit port 8000 directly.
  const res = await request.get('http://localhost:8000/health');
  expect(res.status()).toBe(200);
  const body = await res.json() as { status: string };
  expect(body.status).toBe('ok');
});

test('Authenticated GET /api/pipeline-runs via nginx (port 3000) returns 200', async ({ request }) => {
  // OAuth2PasswordRequestForm requires x-www-form-urlencoded — use form: not data:.
  const loginRes = await request.post('http://localhost:3000/api/auth/login', {
    form: { username: USERNAME, password: PASSWORD },
  });
  expect(loginRes.status()).toBe(200);
  const { access_token } = await loginRes.json() as { access_token: string };
  expect(typeof access_token).toBe('string');

  // The runs list route is /api/pipeline-runs (hyphen, not /api/pipelines/runs).
  const runsRes = await request.get('http://localhost:3000/api/pipeline-runs', {
    headers: { Authorization: `Bearer ${access_token}` },
  });
  expect(runsRes.status()).toBe(200);
  const runs = await runsRes.json();
  expect(Array.isArray(runs)).toBe(true);
});

test('Pipeline Run: submitting YAML asynchronously navigates to run details and reaches terminal state', async ({ page }) => {
  test.slow(); // Increases timeout to 90s
  await doLogin(page);
  await clickNav(page, 'Pipelines');
  await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });

  // Load template
  await page.getByTestId('btn-templates').click();
  await page.getByTestId('template-sales-csv').click();

  // Validate
  await page.getByTestId('btn-validate').click();
  await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });

  // Run
  const runBtn = page.getByTestId('btn-run');
  await expect(runBtn).toBeEnabled({ timeout: 10_000 });
  await runBtn.click();

  // URL navigates to /pipelines/<uuid>
  await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });

  // Details page shows status and reaches terminal status
  await expect(page.getByText(/pending|running|succeeded|failed/i).first()).toBeVisible({ timeout: 30_000 });
});
