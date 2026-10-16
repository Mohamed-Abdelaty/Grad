import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  globalSetup: './e2e/global-setup.ts',
  testDir: './e2e',
  workers: 1,
  timeout: 30_000,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://localhost:3000',
    // Record all console messages and network responses for assertions.
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium-headless',
      use: { ...devices['Desktop Chrome'], headless: true },
    },
  ],
});
