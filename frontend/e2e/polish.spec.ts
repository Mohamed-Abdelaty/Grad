/**
 * e2e/polish.spec.ts
 * ------------------
 * Global accessibility, polish, skip link, CSP, and login throttling verification.
 */
import { test, expect, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const USERNAME = process.env.E2E_USERNAME ?? '';
const PASSWORD = process.env.E2E_PASSWORD ?? '';

test.beforeAll(() => {
  if (!USERNAME || !PASSWORD) {
    throw new Error('E2E_USERNAME and E2E_PASSWORD must be set in environment.');
  }
});

async function doLogin(page: Page) {
  await page.goto('/login');
  await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/', { timeout: 10_000 });
  await expect(page.getByRole('heading', { name: /system (health|status)|dashboard/i }).first()).toBeVisible({
    timeout: 8_000,
  });
}

interface CspViolationItem {
  blockedURI: string;
  violatedDirective: string;
}

interface CspWindow {
  __cspViolations?: CspViolationItem[];
}

async function attachCspListener(page: Page) {
  await page.addInitScript(() => {
    (window as unknown as CspWindow).__cspViolations = [];
    window.addEventListener('securitypolicyviolation', (e) => {
      (window as unknown as CspWindow).__cspViolations?.push({
        blockedURI: e.blockedURI,
        violatedDirective: e.violatedDirective,
      });
    });
  });
}

async function assertNoCspViolations(page: Page, label: string) {
  const violations = await page.evaluate(
    () => (window as unknown as CspWindow).__cspViolations || []
  );
  expect(violations, `CSP violations on ${label}`).toHaveLength(0);
}

// ── 1. Skip Link Navigation ──────────────────────────────────────────────────
test('1. Skip link: visible on keyboard focus and moves focus to #main', async ({ page }) => {
  await doLogin(page);

  const skipLink = page.locator('a[href="#main"]');
  await expect(skipLink).toBeAttached();

  // Focus the skip link
  await skipLink.focus();
  await expect(skipLink).toBeVisible();

  // Activate skip link
  await page.keyboard.press('Enter');

  // Verify main content is focused
  const main = page.locator('#main');
  await expect(main).toBeFocused();
});

// ── 2. Document Titles ───────────────────────────────────────────────────────
test('2. Document titles follow "<Page> · ChaosFlow" format', async ({ page }) => {
  // Unauthenticated /login
  await page.goto('/login');
  await expect(page).toHaveTitle('Login · ChaosFlow');

  // 404 /not-found
  await page.goto('/unknown-random-route');
  await expect(page).toHaveTitle('Not Found · ChaosFlow');

  // Authenticated pages
  await doLogin(page);
  await expect(page).toHaveTitle('Dashboard · ChaosFlow');

  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await expect(page).toHaveURL(/pipelines/);
  await expect(page).toHaveTitle('Pipelines · ChaosFlow');

  await page.locator('aside nav a', { hasText: 'Reliability' }).first().click();
  await expect(page).toHaveURL(/reliability/);
  await expect(page).toHaveTitle('Reliability Testing · ChaosFlow');

  await page.locator('aside nav a', { hasText: 'Reports' }).first().click();
  await expect(page).toHaveURL(/reports/);
  await expect(page).toHaveTitle('Reports · ChaosFlow');
});

// ── 3. Heading Hierarchy ─────────────────────────────────────────────────────
test('3. Heading hierarchy: exactly one h1 per page and no skipped levels', async ({ page }) => {
  const testHeadingOrder = async (url: string, label: string) => {
    await expect(page.locator('h1')).toBeVisible({ timeout: 10_000 });
    const headings = await page.evaluate(() => {
      const elements = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
      return elements.map((el) => parseInt(el.tagName.replace('H', ''), 10));
    });

    const h1Count = headings.filter((level) => level === 1).length;
    expect(h1Count, `${label} should have exactly one h1`).toBe(1);

    // Verify no skipped levels (e.g. h1 -> h3 without h2)
    for (let i = 1; i < headings.length; i++) {
      const prev = headings[i - 1];
      const curr = headings[i];
      if (curr > prev) {
        expect(curr - prev, `${label} heading skipped from h${prev} to h${curr}`).toBeLessThanOrEqual(1);
      }
    }
  };

  // Login
  await page.goto('/login');
  await testHeadingOrder('/login', 'Login');

  // 404
  await page.goto('/unknown-page-route');
  await testHeadingOrder('/unknown-page-route', 'NotFound');

  // Dashboard & app routes
  await doLogin(page);
  await testHeadingOrder('/', 'Dashboard');

  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await expect(page).toHaveURL(/pipelines/);
  await testHeadingOrder('/pipelines', 'Pipelines');

  await page.locator('aside nav a', { hasText: 'Reliability' }).first().click();
  await expect(page).toHaveURL(/reliability/);
  await testHeadingOrder('/reliability', 'Reliability');

  await page.locator('aside nav a', { hasText: 'Reports' }).first().click();
  await expect(page).toHaveURL(/reports/);
  await testHeadingOrder('/reports', 'Reports');
});

// ── 4. 404 Page Not Found ────────────────────────────────────────────────────
test('4. 404 Page: displays error and button navigates to home/dashboard', async ({ page }) => {
  await page.goto('/some-completely-invalid-url');
  await expect(page.getByRole('heading', { name: 'Page Not Found' })).toBeVisible();
  const backLink = page.getByRole('link', { name: /dashboard/i });
  await expect(backLink).toBeVisible();
  await backLink.click();
  // Navigating to '/' when unauthenticated redirects to '/login' or loads dashboard
  await expect(page).toHaveURL(/\/|\/login/);
});

// ── 5. CSP Violations Check ──────────────────────────────────────────────────
test('5. Security: zero CSP violations reported across core routes', async ({ page }) => {
  await attachCspListener(page);

  await page.goto('/login');
  await assertNoCspViolations(page, '/login');

  await doLogin(page);
  await assertNoCspViolations(page, 'Dashboard');

  await page.locator('aside nav a', { hasText: 'Pipelines' }).first().click();
  await expect(page).toHaveURL(/pipelines/);
  await assertNoCspViolations(page, 'Pipelines');

  await page.locator('aside nav a', { hasText: 'Reliability' }).first().click();
  await expect(page).toHaveURL(/reliability/);
  await assertNoCspViolations(page, 'Reliability');
});

// ── 6. Login Rate Limiting (Sliding Window 429) ───────────────────────────────
test('6. Auth rate limit: 5 failed logins return 401, 6th returns 429 Too Many Requests', async ({
  request,
}) => {
  const testUser = `rate_test_user_${Date.now()}`;
  const formData = new URLSearchParams({
    username: testUser,
    password: 'wrong_password_123',
  }).toString();

  // First 5 attempts return 401
  for (let i = 0; i < 5; i++) {
    const res = await request.post('/api/auth/login', {
      data: formData,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Forwarded-For': '198.51.100.42',
      },
    });
    expect(res.status(), `Attempt ${i + 1} should be 401`).toBe(401);
  }

  // 6th attempt returns 429 with Retry-After header
  const blocked = await request.post('/api/auth/login', {
    data: formData,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'X-Forwarded-For': '198.51.100.42',
    },
  });
  expect(blocked.status(), '6th attempt must be 429').toBe(429);
  const retryAfter = blocked.headers()['retry-after'];
  expect(retryAfter, 'Must contain Retry-After header').toBeTruthy();
  expect(parseInt(retryAfter, 10)).toBeGreaterThan(0);
});

// ── 7. Axe-Core Full Audit (Desktop 1440x900 & Mobile 390x844) ───────────────
test('7. Accessibility: axe reports zero serious or critical violations at 1440x900 and 390x844', async ({
  page,
}) => {
  test.setTimeout(90_000);

  const viewports = [
    { name: 'Desktop', width: 1440, height: 900 },
    { name: 'Mobile', width: 390, height: 844 },
  ];

  for (const vp of viewports) {
    await page.setViewportSize({ width: vp.width, height: vp.height });

    // Check login page
    await page.goto('/login');
    await expect(page.locator('#username')).toBeVisible();
    const loginAxe = await new AxeBuilder({ page }).analyze();
    const loginViolations = loginAxe.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(
      loginViolations,
      `Axe violations on /login (${vp.name}): ${JSON.stringify(loginViolations, null, 2)}`
    ).toHaveLength(0);

    // Check 404 page
    await page.goto('/unknown-page-route');
    await expect(page.getByRole('heading', { name: 'Page Not Found' })).toBeVisible();
    const notFoundAxe = await new AxeBuilder({ page }).analyze();
    const notFoundViolations = notFoundAxe.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical'
    );
    expect(
      notFoundViolations,
      `Axe violations on /not-found (${vp.name}): ${JSON.stringify(notFoundViolations, null, 2)}`
    ).toHaveLength(0);

    // Login and check core app pages
    await doLogin(page);

    const pagesToCheck = [
      { name: 'Dashboard', path: '/' },
      { name: 'Pipelines', navText: 'Pipelines', pathUrl: /pipelines/ },
      { name: 'Reliability', navText: 'Reliability', pathUrl: /reliability/ },
      { name: 'Reports', navText: 'Reports', pathUrl: /reports/ },
    ];

    for (const p of pagesToCheck) {
      if (p.navText) {
        // Use sidebar nav if visible, else direct url
        const navLink = page.locator('aside nav a', { hasText: p.navText }).first();
        if (await navLink.isVisible()) {
          await navLink.click();
        } else {
          await page.goto(p.path || '/');
        }
        if (p.pathUrl) {
          await expect(page).toHaveURL(p.pathUrl, { timeout: 10_000 });
        }
      }
      await page.waitForLoadState('networkidle');

      const axeResults = await new AxeBuilder({ page })
        .exclude('.cm-scroller')
        .analyze();
      const violations = axeResults.violations.filter(
        (v) => v.impact === 'serious' || v.impact === 'critical'
      );
      expect(
        violations,
        `Axe violations on ${p.name} (${vp.name}): ${JSON.stringify(violations, null, 2)}`
      ).toHaveLength(0);
    }
  }
});
