import { test, expect, type Page, type Route } from "@playwright/test";
const auth = "https://auth.i3alumba.ru/api/";
const uid = "10000000-0000-0000-0000-000000000001";
const other = "20000000-0000-0000-0000-000000000002";
const metrics: Record<string, number | null> = {
  "cpu.used_percent": 42,
  "memory.used_bytes": 1024,
  "memory.total_bytes": 2048,
  "memory.available_bytes": 1024,
  "swap.used_bytes": 0,
  "swap.total_bytes": 1024,
  "disk.sda.read_bytes_per_second": 512,
  "disk.sda.write_bytes_per_second": 1024,
  "disk.sda.busy_percent": 12,
  "net.eth0.rx_bytes_per_second": 1024,
  "net.eth0.tx_bytes_per_second": 256,
  "net.wg0.rx_bytes_per_second": 128,
  "net.wg0.tx_bytes_per_second": 64,
  "fs.probe_root.total_bytes": 4096,
  "fs.probe_root.available_bytes": 2048,
  "fs.probe_root.free_bytes": 3000,
  "gpu.nvidia.utilization_percent": null,
};
const nodes = [
  {
    uid,
    name: "alpha",
    lifecycle: "present",
    collection: "fresh",
    provider: "native",
    capabilities: ["Probe is not PVC quota"],
    latest: { metrics, gpu: { reason: "GPU driver unavailable" } },
  },
  {
    uid: other,
    name: "beta",
    lifecycle: "deleted",
    collection: "stale",
    provider: "kubernetes",
    capabilities: ["ARMv7 minimum"],
    latest: null,
  },
];
const history = (node = uid, value = 42) => ({
  node,
  bucket_ms: 10000,
  points: [
    {
      timestamp_ms: 100000,
      metrics: { ...metrics, "cpu.used_percent": value },
    },
    { timestamp_ms: 110000, metrics: {} },
    {
      timestamp_ms: 120000,
      metrics: { ...metrics, "cpu.used_percent": value },
    },
  ],
});
const fulfill = (route: Route, value: unknown, status = 200) =>
  route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(value),
  });
async function fixture(page: Page) {
  await page.route(auth + "**", (route) => {
    const path = route.request().url().slice(auth.length);
    if (path === "session/")
      return fulfill(route, { authenticated: true, csrfToken: "mock-csrf" });
    if (path === "session/token/" || path === "token/refresh/")
      return fulfill(route, { access: "mock-access", refresh: "mock-refresh" });
    if (path === "session/logout/") return fulfill(route, {});
    throw new Error("Unexpected Auth route: " + path);
  });
  await page.route("**/api/nodes", (route) =>
    fulfill(route, { nodes, discovery_error: null }),
  );
  await page.route("**/api/history?**", (route) =>
    fulfill(
      route,
      history(new URL(route.request().url()).searchParams.get("node")!),
    ),
  );
}
async function ready(page: Page) {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "CPU utilization" }),
  ).toBeVisible();
}
test.beforeEach(async ({ page }) => fixture(page));
test("automatic session restoration, CSRF, bearer and exact node labels", async ({
  page,
}) => {
  const requests: {
    url: string;
    headers: Record<string, string>;
    body: string | null;
  }[] = [];
  page.on("request", (request) => {
    requests.push({
      url: request.url(),
      headers: request.headers(),
      body: request.postData(),
    });
  });
  await ready(page);
  await expect(page.getByLabel("Node").locator("option")).toHaveText([
    "Select a node",
    "alpha",
    "beta",
  ]);
  expect(await page.getByLabel("Node").inputValue()).toBe(uid);
  expect(
    requests.find((request) => request.url === auth + "session/token/")
      ?.headers["x-csrftoken"],
  ).toBe("mock-csrf");
  expect(
    requests.find((request) => request.url.endsWith("/api/nodes"))?.headers
      .authorization,
  ).toBe("Bearer mock-access");
  expect(
    await page.evaluate(() => JSON.stringify({ ...localStorage })),
  ).not.toContain("mock-access");
  expect(await page.evaluate(() => document.cookie)).not.toContain(
    "mock-refresh",
  );
});
test("resource/device/history selectors limit chart count and never sum interfaces", async ({
  page,
}) => {
  await ready(page);
  expect(await page.locator("canvas").count()).toBe(1);
  await page.getByLabel("Resource", { exact: true }).selectOption("Memory");
  await expect(
    page.getByRole("heading", { name: "Memory", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Swap", exact: true }),
  ).toBeVisible();
  await page.getByLabel("Resource", { exact: true }).selectOption("Disk I/O");
  await expect(page.getByLabel("Device", { exact: true })).toHaveValue("sda");
  await expect(
    page.getByRole("heading", { name: "Disk utilization" }),
  ).toBeVisible();
  await page.getByLabel("Resource", { exact: true }).selectOption("Network");
  await expect(page.getByLabel("Interface")).toHaveValue("eth0");
  await expect(
    page.getByText("Receive: 1.0 KiB/s", { exact: true }),
  ).toBeVisible();
  await page.getByLabel("Interface").selectOption("wg0");
  await expect(
    page.getByText("Receive: 128.0 B/s", { exact: true }),
  ).toBeVisible();
  expect(await page.locator("canvas").count()).toBeLessThanOrEqual(3);
  const requested = page.waitForRequest((request) =>
    request.url().includes("hours=720"),
  );
  await page.getByLabel("History").selectOption("720");
  await requested;
});
test("GPU null readings are unavailable, filesystem probe warning explicit", async ({
  page,
}) => {
  await ready(page);
  await page.getByLabel("Resource", { exact: true }).selectOption("GPU");
  await expect(
    page.getByText("GPU telemetry unavailable.", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("canvas")).toHaveCount(0);
  await page
    .getByLabel("Resource", { exact: true })
    .selectOption("Filesystems");
  await expect(
    page.getByText("Probe capacity describes", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Filesystem capacity" }),
  ).toBeVisible();
});
for (const status of [401, 403, 503])
  test(`${status} clears protected measurements and inventory`, async ({
    page,
  }) => {
    await ready(page);
    await page.route("**/api/nodes", (route) => fulfill(route, {}, status));
    await page.getByRole("button", { name: "Retry / Refresh" }).click();
    await expect(page.locator("canvas")).toHaveCount(0);
    await expect(page.getByLabel("Node").locator("option")).toHaveCount(1);
    await expect(page.getByRole("status")).toContainText(/denied|unavailable/i);
  });
test("network error clears measurements; retry recovers", async ({ page }) => {
  await ready(page);
  await page.route("**/api/nodes", (route) => route.abort("failed"));
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await expect(page.locator("canvas")).toHaveCount(0);
  await page.unroute("**/api/nodes");
  await page.route("**/api/nodes", (route) =>
    fulfill(route, { nodes, discovery_error: null }),
  );
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await expect(page.locator("canvas")).toHaveCount(1);
});
test("late old-node response cannot replace selected UID history, including archived nodes", async ({
  page,
}) => {
  await ready(page);
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/api/history?**", async (route) => {
    const node = new URL(route.request().url()).searchParams.get("node")!;
    if (node === uid) await hold;
    await fulfill(route, history(node, node === uid ? 11 : 88)).catch(() => {});
  });
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await page.getByLabel("Node").selectOption(other);
  await expect(page.getByText("Used: 88.0 %", { exact: true })).toBeVisible();
  release();
  await page.waitForTimeout(100);
  await expect(page.getByText("Used: 88.0 %", { exact: true })).toBeVisible();
  await expect(page.getByText("deleted", { exact: true })).toBeVisible();
});
test("late old-time/resource responses are ignored", async ({ page }) => {
  await ready(page);
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/api/history?**", async (route) => {
    if (new URL(route.request().url()).searchParams.get("hours") === "1")
      await hold;
    await fulfill(route, history()).catch(() => {});
  });
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await page.getByLabel("History").selectOption("24");
  await page.getByLabel("Resource", { exact: true }).selectOption("Memory");
  await expect(
    page.getByRole("heading", { name: "Memory", exact: true }),
  ).toBeVisible();
  release();
  await expect(
    page.getByRole("heading", { name: "CPU utilization" }),
  ).toHaveCount(0);
});
test("logout waits for owned refresh rotation and blocks late data", async ({
  page,
}) => {
  await ready(page);
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  let logoutBody: unknown;
  let rotateCalls = 0;
  await page.route(auth + "token/refresh/", async (route) => {
    rotateCalls++;
    await hold;
    await fulfill(route, {
      access: "rotated-access",
      refresh: "rotated-refresh",
    });
  });
  await page.route(auth + "session/logout/", (route) => {
    logoutBody = route.request().postDataJSON();
    return fulfill(route, {});
  });
  await page.route("**/api/nodes", (route) => fulfill(route, {}, 401));
  const rotation = page.waitForRequest(auth + "token/refresh/");
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await rotation;
  await page.getByRole("button", { name: "Log out", exact: true }).click();
  release();
  await expect(page.getByRole("status")).toContainText(
    "Central session logged out",
  );
  expect(logoutBody).toEqual({ refresh: "rotated-refresh" });
  expect(rotateCalls).toBe(1);
  await expect(page.locator("canvas")).toHaveCount(0);
});
test("failed central logout reports local clearing honestly", async ({
  page,
}) => {
  await ready(page);
  await page.route(auth + "session/logout/", (route) =>
    fulfill(route, {}, 503),
  );
  await page.getByRole("button", { name: "Log out", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "central logout could not be confirmed",
  );
  await expect(page.locator("canvas")).toHaveCount(0);
});
test("denied session is explicit and can be restored", async ({ page }) => {
  await page.route(auth + "session/", (route) =>
    fulfill(route, { authenticated: false, csrfToken: "mock-csrf" }),
  );
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText(
    "Sign in through Dashboard",
  );
  await expect(page.locator("canvas")).toHaveCount(0);
  await page.unroute(auth + "session/");
  await page.getByRole("button", { name: "Restore session" }).click();
  await expect(page.locator("canvas")).toHaveCount(1);
});
test("shared theme cookie wins, cross-subdomain preference sync and denied storage fallback", async ({
  page,
}) => {
  await page.addInitScript(() => {
    localStorage.setItem("i3alumba-color-mode", "light");
    document.cookie = "i3alumba-theme=dark; Path=/";
  });
  await ready(page);
  await expect(
    page.getByRole("button", { name: "Switch to light theme" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Switch to light theme" }).click();
  await expect(
    page.getByRole("button", { name: "Switch to dark theme" }),
  ).toBeVisible();
  await page.evaluate(() => {
    document.cookie = "i3alumba-theme=dark; Path=/";
    window.dispatchEvent(new Event("focus"));
  });
  await expect(
    page.getByRole("button", { name: "Switch to light theme" }),
  ).toBeVisible();
});
test("theme still works with cookies and storage denied", async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(document, "cookie", {
      get() {
        throw new Error("denied");
      },
      set() {
        throw new Error("denied");
      },
    });
    Object.defineProperty(window, "localStorage", {
      get() {
        throw new Error("denied");
      },
    });
  });
  await ready(page);
  await page.getByRole("button", { name: /Switch to .* theme/ }).click();
  await expect(page.locator("canvas")).toHaveCount(1);
});
test("strict nonce CSP, no remote assets, mobile layout and keyboard navigation", async ({
  page,
}) => {
  const violations: string[] = [];
  const failures: string[] = [];
  await page.addInitScript(() => {
    (window as unknown as { violations: string[] }).violations = [];
    document.addEventListener("securitypolicyviolation", (event) =>
      (window as unknown as { violations: string[] }).violations.push(
        event.violatedDirective,
      ),
    );
  });
  page.on("pageerror", (error) => failures.push(error.message));
  await page.setViewportSize({ width: 360, height: 800 });
  await ready(page);
  await page.getByLabel("Resource", { exact: true }).selectOption("Network");
  await expect(
    page.getByRole("heading", { name: "Network throughput" }),
  ).toBeVisible();
  violations.push(
    ...(await page.evaluate(
      () => (window as unknown as { violations: string[] }).violations,
    )),
  );
  expect(violations).toEqual([]);
  expect(failures).toEqual([]);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  expect(
    await page
      .locator("style")
      .evaluateAll((styles) => styles.every((style) => !!style.nonce)),
  ).toBe(true);
  await page.keyboard.press("Tab");
  expect(await page.evaluate(() => document.activeElement?.tagName)).not.toBe(
    "BODY",
  );
});
test("null-only and empty histories never manufacture zero readings", async ({
  page,
}) => {
  await page.route("**/api/history?**", (route) =>
    fulfill(route, {
      node: uid,
      bucket_ms: 10000,
      points: [
        { timestamp_ms: 100000, metrics: { "cpu.used_percent": null } },
        { timestamp_ms: 110000, metrics: {} },
      ],
    }),
  );
  await page.goto("/");
  await expect(
    page.getByText("No readings for this resource", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("canvas")).toHaveCount(0);
  await expect(page.getByRole("status")).toContainText("No samples");
  await page.route("**/api/history?**", (route) =>
    fulfill(route, { node: uid, bucket_ms: 10000, points: [] }),
  );
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await expect(page.locator("canvas")).toHaveCount(0);
});
test("canvas uses real timestamps, breaks null and irregular gaps, hover chooses nearest timestamp", async ({
  page,
}) => {
  await page.addInitScript(() => {
    const original = CanvasRenderingContext2D.prototype.lineTo;
    (window as unknown as { dataLines: number[][] }).dataLines = [];
    CanvasRenderingContext2D.prototype.lineTo = function (
      x: number,
      y: number,
    ) {
      if (this.strokeStyle === "#2563eb")
        (window as unknown as { dataLines: number[][] }).dataLines.push([x, y]);
      return original.call(this, x, y);
    };
  });
  const points = [
    { timestamp_ms: 0, metrics: { "cpu.used_percent": 10 } },
    { timestamp_ms: 10000, metrics: { "cpu.used_percent": 20 } },
    { timestamp_ms: 20000, metrics: { "cpu.used_percent": null } },
    { timestamp_ms: 30000, metrics: { "cpu.used_percent": 30 } },
    { timestamp_ms: 100000, metrics: { "cpu.used_percent": 40 } },
  ];
  await page.route("**/api/history?**", (route) =>
    fulfill(route, { node: uid, bucket_ms: 10000, points }),
  );
  await ready(page);
  const lines = await page.evaluate(
    () => (window as unknown as { dataLines: number[][] }).dataLines,
  );
  expect(lines.length).toBeGreaterThan(0);
  const canvas = page.locator("canvas");
  const width = await canvas.evaluate((element) => element.clientWidth);
  const left = width < 480 ? 78 : 100;
  const plotWidth = width - 18 - left;
  expect(lines.every(([x]) => Math.abs(x - (left + plotWidth * 0.1)) < 1)).toBe(
    true,
  );
  const bounds = await canvas.boundingBox();
  expect(bounds).not.toBeNull();
  await page.mouse.move(
    bounds!.x + (bounds!.width * (left + plotWidth * 0.4)) / width,
    bounds!.y + 50,
  );
  await expect(page.getByText("Used: 30.0 %", { exact: true })).toBeVisible();
});
test("empty inventory, discovery failure and stale collection are explicit", async ({
  page,
}) => {
  await page.route("**/api/nodes", (route) =>
    fulfill(route, {
      nodes: [],
      discovery_error: "Fixture discovery unavailable",
    }),
  );
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText(
    "Fixture discovery unavailable",
  );
  await expect(page.locator("canvas")).toHaveCount(0);
  await page.route("**/api/nodes", (route) =>
    fulfill(route, { nodes: [nodes[1]], discovery_error: null }),
  );
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await expect(page.getByText("stale", { exact: true })).toBeVisible();
});
test("duplicate names remain separate UID history identities with exact menu names", async ({
  page,
}) => {
  await page.route("**/api/nodes", (route) =>
    fulfill(route, {
      nodes: [nodes[0], { ...nodes[1], name: "alpha" }],
      discovery_error: null,
    }),
  );
  await ready(page);
  await expect(page.getByLabel("Node").locator("option")).toHaveText([
    "Select a node",
    "alpha",
    "alpha",
  ]);
  const request = page.waitForRequest((request) =>
    request.url().includes("node=" + other),
  );
  await page.getByLabel("Node").selectOption(other);
  await request;
  await expect(page.getByText("deleted", { exact: true })).toBeVisible();
});
test("401 refresh clears charts immediately then restores using rotated access", async ({
  page,
}) => {
  await ready(page);
  let release!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  await page.route(auth + "token/refresh/", async (route) => {
    await hold;
    await fulfill(route, {
      access: "rotated-access",
      refresh: "rotated-refresh",
    });
  });
  await page.route("**/api/nodes", (route) => {
    calls++;
    return calls === 1
      ? fulfill(route, {}, 401)
      : fulfill(route, { nodes, discovery_error: null });
  });
  const rotation = page.waitForRequest(auth + "token/refresh/");
  await page.getByRole("button", { name: "Retry / Refresh" }).click();
  await rotation;
  await expect(page.locator("canvas")).toHaveCount(0);
  const retried = page.waitForRequest(
    (request) =>
      request.url().endsWith("/api/nodes") &&
      request.headers().authorization === "Bearer rotated-access",
  );
  release();
  await retried;
  await expect(page.locator("canvas")).toHaveCount(1);
});

test("chart timestamp labels fit canvas edges and dark marks match series", async ({
  page,
}) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.addInitScript(() => {
    type Label = { x: number; width: number; align: CanvasTextAlign };
    const state = window as unknown as {
      chartLabels: Label[];
      chartMarks: string[];
    };
    state.chartLabels = [];
    state.chartMarks = [];
    const originalText = CanvasRenderingContext2D.prototype.fillText;
    CanvasRenderingContext2D.prototype.fillText = function (text, x, y) {
      if (y === 235)
        state.chartLabels.push({
          x,
          width: this.measureText(text).width,
          align: this.textAlign,
        });
      originalText.call(this, text, x, y);
    };
    const originalMark = CanvasRenderingContext2D.prototype.fillRect;
    CanvasRenderingContext2D.prototype.fillRect = function (
      x,
      y,
      width,
      height,
    ) {
      if (width === 3 && height === 3)
        state.chartMarks.push(String(this.fillStyle));
      originalMark.call(this, x, y, width, height);
    };
  });
  await ready(page);
  const labels = await page.evaluate(
    () =>
      (
        window as unknown as {
          chartLabels: { x: number; width: number; align: string }[];
        }
      ).chartLabels,
  );
  expect(labels.length).toBeGreaterThan(0);
  for (const label of labels) {
    const left =
      label.align === "right"
        ? label.x - label.width
        : label.align === "center"
          ? label.x - label.width / 2
          : label.x;
    expect(left).toBeGreaterThanOrEqual(0);
    const width = await page
      .locator("canvas")
      .evaluate((element) => element.clientWidth);
    expect(left + label.width).toBeLessThanOrEqual(width);
  }
  await page.getByRole("button", { name: "Switch to dark theme" }).click();
  await expect
    .poll(() =>
      page.evaluate(() =>
        (window as unknown as { chartMarks: string[] }).chartMarks.at(-1),
      ),
    )
    .toBe("#60a5fa");
});

test("mobile charts keep readable height and high-DPI resolution across resize", async ({
  browser,
}, testInfo) => {
  const context = await browser.newContext({
    viewport: { width: 360, height: 800 },
    deviceScaleFactor: 2,
    colorScheme: "dark",
  });
  try {
    const page = await context.newPage();
    await fixture(page);
    await ready(page);
    const canvas = page.locator("canvas");
    await expect
      .poll(() =>
        canvas.evaluate(
          (element: HTMLCanvasElement) =>
            element.width === Math.round(element.clientWidth * 2),
        ),
      )
      .toBe(true);
    expect((await canvas.boundingBox())!.height).toBe(250);
    expect(
      await canvas.evaluate((element: HTMLCanvasElement) => element.height),
    ).toBe(500);
    await page.screenshot({
      path: testInfo.outputPath("monitor-mobile-dark.png"),
      fullPage: true,
    });
    await page.setViewportSize({ width: 1100, height: 900 });
    await expect
      .poll(() =>
        canvas.evaluate(
          (element: HTMLCanvasElement) =>
            element.clientWidth > 800 &&
            element.width === Math.round(element.clientWidth * 2),
        ),
      )
      .toBe(true);
    expect((await canvas.boundingBox())!.height).toBe(250);
    await page.screenshot({
      path: testInfo.outputPath("monitor-desktop-dark.png"),
      fullPage: true,
    });
  } finally {
    await context.close();
  }
});
