const { test } = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const ts = require("typescript");
const fs = require("node:fs");
function fixture(fetch) {
  const exports = {};
  const context = vm.createContext({
    exports,
    fetch,
    AbortSignal,
    DOMException,
    console,
  });
  const source = ts.transpileModule(
    fs.readFileSync("src/api/auth.ts", "utf8"),
    {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
      },
    },
  ).outputText;
  vm.runInContext(source, context);
  return exports;
}
const json = (data, status = 200) => ({
  ok: status === 200,
  status,
  json: async () => data,
});
test("auth refresh is single-flight for concurrent API requests and owned logout waits for rotation", async () => {
  let release,
    rotations = 0,
    body;
  const api = fixture(async (url, options) => {
    if (url.endsWith("session/"))
      return json({ authenticated: true, csrfToken: "csrf" });
    if (url.endsWith("session/token/"))
      return json({ access: "old-access", refresh: "old-refresh" });
    if (url.endsWith("token/refresh/")) {
      rotations++;
      return new Promise((resolve) => {
        release = () =>
          resolve(json({ access: "new-access", refresh: "new-refresh" }));
      });
    }
    if (url.endsWith("session/logout/")) {
      body = JSON.parse(options.body);
      return json({});
    }
    return json({}, 401);
  });
  await api.restore();
  const first = api.api("/api/nodes", new AbortController().signal);
  const second = api.api("/api/nodes", new AbortController().signal);
  await new Promise((resolve) => setImmediate(resolve));
  const logout = api.logout();
  release();
  const result = await Promise.allSettled([first, second, logout]);
  assert.equal(rotations, 1);
  assert.equal(body.refresh, "new-refresh");
  assert.equal(result[0].status, "rejected");
  assert.equal(result[1].status, "rejected");
  assert.match(result[2].value, /Central session logged out/);
});
test("logout waits for pending initial session/token and revokes the newly owned refresh", async () => {
  let release, body;
  const api = fixture(async (url, options) => {
    if (url.endsWith("session/"))
      return json({ authenticated: true, csrfToken: "csrf" });
    if (url.endsWith("session/token/"))
      return new Promise((resolve) => {
        release = () =>
          resolve(json({ access: "issued-access", refresh: "issued-refresh" }));
      });
    if (url.endsWith("session/logout/")) {
      body = JSON.parse(options.body);
      return json({});
    }
    throw new Error("unexpected");
  });
  const restore = api.restore();
  await new Promise((resolve) => setImmediate(resolve));
  const logout = api.logout();
  release();
  await Promise.all([restore, logout]);
  assert.equal(body.refresh, "issued-refresh");
  await assert.rejects(
    api.api("/api/nodes", new AbortController().signal),
    /Sign in/,
  );
});
test("late response after logout cannot expose telemetry", async () => {
  let release;
  const api = fixture(async (url) => {
    if (url.endsWith("session/"))
      return json({ authenticated: true, csrfToken: "csrf" });
    if (url.endsWith("session/token/"))
      return json({ access: "access", refresh: "refresh" });
    if (url.endsWith("session/logout/")) return json({});
    return new Promise((resolve) => {
      release = () => resolve(json({ protected: "data" }));
    });
  });
  await api.restore();
  const request = api.api("/api/nodes", new AbortController().signal);
  await api.logout();
  release();
  await assert.rejects(request, /Session changed/);
});
test("auth source has no token persistence or unsafe HTML sinks", () => {
  const source = fs.readFileSync("src/api/auth.ts", "utf8");
  assert.doesNotMatch(
    source,
    /localStorage|sessionStorage|document\.cookie|innerHTML/,
  );
  assert.match(source, /credentials: ?["']include["']/);
});
