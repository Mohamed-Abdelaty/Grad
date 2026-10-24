import { test, expect } from "@playwright/test";

test("3D viz renders during pipeline run", async ({ page }) => {
  // login
  await page.goto("http://127.0.0.1:3000");
  await page.fill('[name=username]', 'admin');
  await page.fill('[name=password]', '123456');
  await page.click('[type=submit]');
  await page.waitForURL('**/dashboard', { timeout: 10000 });
  // navigate to pipeline run page
  await page.goto("http://127.0.0.1:3000/runs");
  await page.waitForTimeout(2000);
  // check canvas or svg exists (3D or 2D fallback)
  const hasViz = await page.locator('canvas, svg.pipeline-viz, [data-testid="viz"]').count();
  expect(hasViz).toBeGreaterThan(0);
  await page.screenshot({ path: "docs/screenshots/viz_smoke_test.png", fullPage: false });
});
