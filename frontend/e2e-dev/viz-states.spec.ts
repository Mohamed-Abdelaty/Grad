import { test, expect } from '@playwright/test';
import path from 'path';

const PRESETS = [
  'pending',
  'kafka-40',
  'spark-active',
  'spark-unknown',
  'hdfs-writing',
  'succeeded',
  'failed-spark',
  'fault-injected',
  'fault-recovering',
  'fault-recovered',
  'streaming-lag',
  'cancelled',
] as const;

test.describe('Dev Visualization States & Visual QA', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 600 });
  });

  for (const preset of PRESETS) {
    test(`Visual QA & screenshots for preset: ${preset}`, async ({ page }) => {
      await page.goto('/dev/viz');
      await page.waitForSelector('[data-testid="viz-playground"]');

      // Click the preset button
      await page.click(`[data-testid="preset-${preset}"]`);
      await page.waitForTimeout(300);

      // Verify text equivalent has 4 station items
      const textEquiv = page.locator('[data-testid="viz-text-equivalent"]');
      await expect(textEquiv).toBeAttached();
      const liCount = await textEquiv.locator('li').count();
      expect(liCount).toBe(4);

      const viz = page.locator('[data-testid="viz"]');
      await expect(viz).toBeVisible();

      // 1. 3D Screenshot (or current mode if forced)
      const screenshot3dPath = path.resolve(
        process.cwd(),
        `../docs/screenshots/ui-7/${preset}-3d.png`
      );
      await viz.screenshot({ path: screenshot3dPath });

      // 2. Switch to 2D
      const btn2D = page.locator('[data-testid="viz-quality"] button', { hasText: '2D' });
      await btn2D.click();
      await page.waitForTimeout(300);

      const screenshot2dPath = path.resolve(
        process.cwd(),
        `../docs/screenshots/ui-7/${preset}-2d.png`
      );
      await viz.screenshot({ path: screenshot2dPath });
    });
  }

  test('Spark-active: animation runs when active, stops when paused', async ({ page }) => {
    await page.goto('/dev/viz');
    await page.click('[data-testid="preset-spark-active"]');
    await page.waitForTimeout(600);

    const canvas = (await page.locator('canvas').count()) > 0
      ? page.locator('canvas')
      : page.locator('[data-testid="viz-canvas"]');
    await expect(canvas).toBeVisible();

    // Frame 1
    const frame1 = await canvas.screenshot();
    await page.waitForTimeout(600);
    // Frame 2
    const frame2 = await canvas.screenshot();

    // Canvas pixels must differ between two frames 600ms apart
    expect(frame1.equals(frame2)).toBe(false);

    // Click Pause
    const pauseBtn = page.locator('[data-testid="viz-pause"]');
    await pauseBtn.click();
    await expect(pauseBtn).toHaveAttribute('aria-pressed', 'true');
    await page.waitForTimeout(1000);

    // Frame after pause
    const pausedFrame1 = await canvas.screenshot();
    await page.waitForTimeout(600);
    const pausedFrame2 = await canvas.screenshot();

    // Canvas pixels must be identical when paused
    expect(pausedFrame1.equals(pausedFrame2)).toBe(true);
  });

  test('prefers-reduced-motion forces 2D mode', async ({ page }) => {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/dev/viz');
    await page.waitForSelector('[data-testid="viz-playground"]');

    const mode = page.locator('[data-testid="viz-mode"]');
    await expect(mode).toHaveText('2d');
  });
});
