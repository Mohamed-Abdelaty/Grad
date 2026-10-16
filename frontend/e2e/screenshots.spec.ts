import { test, expect, Page } from '@playwright/test';
import * as path from 'path';
import * as fs from 'fs';

const USERNAME = process.env.E2E_USERNAME || '';
const PASSWORD = process.env.E2E_PASSWORD || '';

const outDir = path.resolve('..', 'docs', 'screenshots', 'final');
if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

async function doLogin(page: Page) {
  await page.goto('/login');
  await page.locator('#username').fill(USERNAME);
  await page.locator('#password').fill(PASSWORD);
  await page.getByRole('button', { name: /login|sign in/i }).click();
  await expect(page).toHaveURL('/', { timeout: 10_000 });
}

async function clickNav(page: Page, href: string) {
  await page.locator(`aside nav a[href="${href}"]`).first().click({ force: true });
}

test('Capture 8 final delivery screenshots', async ({ page }) => {
  test.setTimeout(180_000);

  if (!USERNAME || !PASSWORD) {
    throw new Error('E2E_USERNAME and E2E_PASSWORD environment variables are required.');
  }

  // 1. desktop-dashboard.png (1440x900) on /
  await page.setViewportSize({ width: 1440, height: 900 });
  await doLogin(page);
  await expect(page.locator('[data-testid="health-cards"]')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'desktop-dashboard.png'), fullPage: false });

  // 2. desktop-pipelines-valid.png (1440x900) on /pipelines with sales-csv loaded and validated
  await clickNav(page, '/pipelines');
  await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });
  await page.locator('[data-testid="btn-templates"]').click();
  const templateModal = page.getByRole('dialog');
  await expect(templateModal).toBeVisible();
  await templateModal.locator('[data-testid="template-sales-csv"]').click();
  await expect(templateModal).toBeHidden({ timeout: 5000 });
  await page.locator('[data-testid="btn-validate"]').click();
  await expect(page.getByTestId('validation-state')).toHaveText('Valid', { timeout: 15_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'desktop-pipelines-valid.png'), fullPage: false });

  // 3. desktop-run-succeeded.png (1440x900) on succeeded run
  const runRow = page.locator('table tbody tr', { hasText: 'succeeded' }).first();
  await expect(runRow).toBeVisible({ timeout: 10_000 });
  await runRow.getByRole('button', { name: 'Open' }).click();
  await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });
  await expect(page.getByTestId('run-header')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'desktop-run-succeeded.png'), fullPage: false });

  // 4. desktop-reliability.png (1440x900) on /reliability
  await clickNav(page, '/reliability');
  await expect(page).toHaveURL(/reliability/, { timeout: 10_000 });
  await expect(page.getByTestId('page-reliability')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'desktop-reliability.png'), fullPage: false });

  // 5. desktop-report-passed.png (1440x900) on the report from Section 6
  await clickNav(page, '/reports');
  await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
  const viewReportLink = page.locator('a[href^="/reports/"]').first();
  await expect(viewReportLink).toBeVisible({ timeout: 10_000 });
  await viewReportLink.click();
  await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'desktop-report-passed.png'), fullPage: false });

  // Mobile screenshots (390x844)
  await page.setViewportSize({ width: 390, height: 844 });

  // 6. mobile-dashboard.png (390x844) on /
  await clickNav(page, '/');
  await expect(page).toHaveURL('/', { timeout: 10_000 });
  await expect(page.locator('[data-testid="health-cards"]')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'mobile-dashboard.png'), fullPage: false });

  // 7. mobile-run-succeeded.png (390x844) on the succeeded run
  await clickNav(page, '/pipelines');
  await expect(page).toHaveURL(/pipelines/, { timeout: 10_000 });
  const mobileRunRow = page.locator('table tbody tr', { hasText: 'succeeded' }).first();
  await expect(mobileRunRow).toBeVisible({ timeout: 10_000 });
  await mobileRunRow.getByRole('button', { name: 'Open' }).click();
  await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]+/, { timeout: 15_000 });
  await expect(page.getByTestId('run-header')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'mobile-run-succeeded.png'), fullPage: false });

  // 8. mobile-report-passed.png (390x844) on the report
  await clickNav(page, '/reports');
  await expect(page).toHaveURL(/reports/, { timeout: 10_000 });
  const mobileReportLink = page.locator('a[href^="/reports/"]').first();
  await expect(mobileReportLink).toBeVisible({ timeout: 10_000 });
  await mobileReportLink.click();
  await expect(page.getByTestId('page-report-details')).toBeVisible({ timeout: 10_000 });
  await page.waitForTimeout(600);
  await page.screenshot({ path: path.join(outDir, 'mobile-report-passed.png'), fullPage: false });
});
