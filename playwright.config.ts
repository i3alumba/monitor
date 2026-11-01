import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "tests/browser",
  fullyParallel: true,
  workers: 4,
  use: {
    baseURL: "http://127.0.0.1:4179",
    launchOptions: {
      executablePath:
        process.env.CHROMIUM_PATH || "/usr/bin/google-chrome-stable",
    },
  },
  webServer: {
    command: "node tests/frontend-server.cjs",
    url: "http://127.0.0.1:4179",
    reuseExistingServer: false,
  },
});
