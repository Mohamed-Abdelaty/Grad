import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './e2e-dev',
  workers: 1,
  timeout: 30_000,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'retain-on-failure',
    launchOptions: {
      // Hardware-accelerated or SwiftShader WebGL fallback for headless Chromium
      args: [
        '--use-gl=angle',
        '--use-angle=swiftshader',
        '--enable-unsafe-swiftshader',
        '--enable-webgl',
      ],
    },
  },
  webServer: {
    command: 'npm run dev -- --port 5173',
    url: 'http://localhost:5173',
    reuseExistingServer: true,
    timeout: 30_000,
  },
  projects: [
    {
      name: 'chromium-headless',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1280, height: 800 },
        headless: true,
      },
    },
  ],
});
