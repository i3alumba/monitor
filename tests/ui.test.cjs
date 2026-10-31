const { test } = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const source = fs.readFileSync("assets/app.js", "utf8");
function context(fetch) {
  const elements = new Map();
  const element = () => ({
    textContent: "",
    value: "alpha",
    children: [],
    replaceChildren() {
      this.children = [];
    },
    append(...items) {
      this.children.push(...items);
    },
    classList: { toggle() {} },
  });
  const document = {
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, element());
      return elements.get(id);
    },
    createElement: element,
  };
  const ctx = vm.createContext({
    document,
    fetch,
    AbortSignal,
    setInterval() {},
    getComputedStyle: () => ({ color: "black" }),
    console,
  });
  // Omit only automatic bootstrap; tests explicitly drive session transitions.
  vm.runInContext(source.replace(/restore\(\);\s*$/, ""), ctx);
  return { ctx, elements, run: (code) => vm.runInContext(code, ctx) };
}
const json = (data, status = 200) => ({
  ok: status === 200,
  status,
  json: async () => data,
});
test("rotated refresh is single-flight and owned logout waits for it", async () => {
  let resolveRotation,
    calls = 0,
    logoutBody;
  const { run } = context(async (url, options) => {
    if (url.endsWith("token/refresh/")) {
      calls++;
      return new Promise((resolve) => {
        resolveRotation = () =>
          resolve(json({ access: "new-access", refresh: "new-refresh" }));
      });
    }
    if (url.endsWith("session/"))
      return json({ authenticated: true, csrfToken: "csrf" });
    if (url.endsWith("session/logout/")) {
      logoutBody = JSON.parse(options.body);
      return json({});
    }
    throw new Error("unexpected endpoint");
  });
  run('access="old-access"; refresh="old-refresh";');
  const first = run("rotate()"),
    second = run("rotate()");
  const logout = run('$("logout").onclick()');
  assert.equal(calls, 1);
  resolveRotation();
  await Promise.all([first, second, logout]);
  assert.equal(logoutBody.refresh, "new-refresh");
  assert.equal(run("access"), null);
  assert.equal(run("refresh"), null);
});
test("401, 403 and 503 clear charts; no persistence or unsafe HTML sinks", async () => {
  for (const status of [401, 403, 503]) {
    const { run, elements } = context(async () => json({}, status));
    run(
      'access="access"; lastPoints=[{timestamp_ms:1,metrics:{"cpu.used_percent":42}}];',
    );
    run('$("node").append({value:"protected-node"});');
    await run("load()");
    assert.equal(elements.get("node").children.length, 0);
    assert.equal(run("lastPoints.length"), 0);
    assert.notEqual(elements.get("status").textContent, "");
  }
  assert.doesNotMatch(
    source,
    /localStorage|sessionStorage|innerHTML|document\.cookie/,
  );
  assert.match(source, /v === null.*connected = false/s);
});

test("sign-in link targets the configured Dashboard origin", () => {
  const html = fs.readFileSync("assets/index.html", "utf8");
  assert.match(html, /href="https:\/\/main\.i3alumba\.ru"/);
  assert.doesNotMatch(html, /https:\/\/dashboard\.i3alumba\.ru/);
});

test("node selector uses discovered UIDs and displays pending/unsupported capabilities", async () => {
  const nodes = [
    {
      uid: "10000000-0000-0000-0000-000000000001",
      name: "same-name",
      lifecycle: "present",
      collection: "pending",
      provider: "kubernetes",
      capabilities: ["GPU unavailable"],
      latest: null,
    },
    {
      uid: "20000000-0000-0000-0000-000000000001",
      name: "same-name",
      lifecycle: "unsupported",
      collection: "blocked",
      provider: "kubernetes",
      capabilities: ["ARMv7 minimum"],
      latest: null,
    },
  ];
  const { run, elements } = context(async (url) =>
    url === "/api/nodes"
      ? json({ nodes, discovery_error: null })
      : json({ points: [], bucket_ms: 10000 }),
  );
  run('access="access";');
  await run("load()");
  assert.equal(elements.get("node").children.length, 2);
  assert.equal(elements.get("node").children[0].value, nodes[0].uid);
  assert.match(
    elements.get("node").children[1].textContent,
    /unsupported\/blocked/,
  );
  assert.match(
    elements.get("availability").textContent,
    /present\/pending.*GPU unavailable/,
  );
});
