import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 90_000,             // generous – the app makes GPT calls for reports
  retries: 0,                  // CI: you can raise this
  use: {
    baseURL: 'http://localhost:5173', // Vite default
    headless: true,
    trace: 'retain-on-failure',
  },
});
