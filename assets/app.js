"use strict";
const AUTH = "https://auth.i3alumba.ru/api/";
const $ = (id) => document.getElementById(id);
let access = null,
  refresh = null,
  csrf = null,
  refreshing = null;
let generation = 0,
  requestGeneration = 0,
  loading = false;
let lastPoints = [];
let loggingOut = false;
function clear(message, revokeInventory = false) {
  if (revokeInventory) $("node").replaceChildren();
  $("charts").replaceChildren();
  $("availability").textContent = "";
  lastPoints = [];
  $("status").textContent = message;
}
async function authFetch(path, body) {
  const response = await fetch(AUTH + path, {
    method: body === undefined ? "GET" : "POST",
    credentials: "include",
    cache: "no-store",
    headers:
      body === undefined
        ? {}
        : { "Content-Type": "application/json", "X-CSRFToken": csrf || "" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok) throw new Error("Auth unavailable or session denied");
  return response.json();
}
async function restore() {
  if (loggingOut) return;
  const epoch = ++generation;
  access = refresh = null;
  clear("Restoring central session…", true);
  try {
    const session = await authFetch("session/");
    if (epoch !== generation) return;
    csrf = session.csrfToken;
    if (!session.authenticated)
      throw new Error(
        "Sign in through Dashboard; administrator access required.",
      );
    const pair = await authFetch("session/token/", {});
    if (epoch !== generation) return;
    if (typeof pair.access !== "string" || typeof pair.refresh !== "string")
      throw new Error("Malformed Auth response");
    access = pair.access;
    refresh = pair.refresh;
    await load();
  } catch (error) {
    if (epoch === generation) clear(error.message, true);
  }
}
async function rotate() {
  if (refreshing) return refreshing;
  const epoch = generation;
  refreshing = (async () => {
    if (!refresh) throw new Error("Sign in through Dashboard.");
    const pair = await authFetch("token/refresh/", { refresh });
    if (epoch !== generation) throw new Error("Session changed");
    if (typeof pair.access !== "string" || typeof pair.refresh !== "string")
      throw new Error("Malformed refresh response");
    access = pair.access;
    refresh = pair.refresh;
  })().finally(() => {
    refreshing = null;
  });
  return refreshing;
}
async function api(path) {
  if (!access)
    throw new Error(
      "Sign in through Dashboard; administrator access required.",
    );
  let response = await fetch(path, {
    headers: { Authorization: `Bearer ${access}` },
    cache: "no-store",
    signal: AbortSignal.timeout(10000),
  });
  if (response.status === 401) {
    clear("Access expired; checking central session…");
    await rotate();
    response = await fetch(path, {
      headers: { Authorization: `Bearer ${access}` },
      cache: "no-store",
      signal: AbortSignal.timeout(10000),
    });
  }
  if (!response.ok) {
    if (response.status === 401 || response.status === 403) {
      access = null;
    }
    throw new Error(
      response.status === 403
        ? "Administrator access required (permission revoked or denied)."
        : response.status === 503
          ? "Auth or telemetry storage unavailable; charts cleared."
          : "Request denied or unavailable; charts cleared.",
    );
  }
  return response.json();
}
async function load() {
  if (loading || loggingOut) return;
  const epoch = generation,
    request = ++requestGeneration;
  loading = true;
  try {
    const inventory = await api("/api/nodes");
    if (epoch !== generation || request !== requestGeneration) return;
    const selected = $("node").value;
    $("node").replaceChildren();
    for (const entry of inventory.nodes) {
      const option = document.createElement("option");
      option.value = entry.uid;
      option.textContent = `${entry.name} · ${entry.uid.slice(0, 8)} · ${entry.lifecycle}/${entry.collection}`;
      $("node").append(option);
    }
    if (inventory.nodes.some((entry) => entry.uid === selected))
      $("node").value = selected;
    else if (inventory.nodes.length) $("node").value = inventory.nodes[0].uid;
    const node = $("node").value;
    if (!node) {
      clear(
        inventory.discovery_error || "No discovered nodes; coverage pending.",
      );
      return;
    }
    const history = await api(
      `/api/history?node=${encodeURIComponent(node)}&hours=${$("hours").value}`,
    );
    if (epoch !== generation || request !== requestGeneration) return;
    lastPoints = history.points;
    draw(lastPoints);
    const last = history.points
      .filter((p) => Object.values(p.metrics).some((v) => v !== null))
      .at(-1);
    $("status").textContent = last
      ? `Latest data bucket: ${new Date(last.timestamp_ms).toLocaleString()} (${Math.round(history.bucket_ms / 1000)}s averages)`
      : "No samples in this interval.";
    const entry = inventory.nodes.find((entry) => entry.uid === node);
    const latest = entry?.latest;
    $("availability").textContent =
      `${inventory.discovery_error ? "Discovery error: " + inventory.discovery_error + " · " : ""}${entry?.lifecycle}/${entry?.collection} · ${entry?.provider} · ${(entry?.capabilities || []).join("; ")} · ${latest ? latest.gpu?.reason || "" : "No samples yet"}`;
  } catch (error) {
    if (epoch === generation) clear(error.message, true);
  } finally {
    loading = false;
  }
}
function format(value, unit) {
  if (value === null || !Number.isFinite(value)) return "unavailable";
  if (unit === "bytes" || unit === "bytes/s") {
    let i = 0;
    const scales = ["B", "KiB", "MiB", "GiB", "TiB"];
    while (value >= 1024 && i < scales.length - 1) {
      value /= 1024;
      i++;
    }
    return `${value.toFixed(1)} ${scales[i]}${unit === "bytes/s" ? "/s" : ""}`;
  }
  return `${value.toFixed(1)} ${unit}`;
}
function unit(key) {
  if (key.endsWith("_bytes_per_second")) return "bytes/s";
  if (key.endsWith("_bytes")) return "bytes";
  if (key.endsWith("_percent")) return "%";
  if (key.endsWith("_celsius")) return "°C";
  if (key.endsWith("_watts")) return "W";
  if (key.endsWith("_per_second")) return "/s";
  return "";
}
function draw(points) {
  $("charts").replaceChildren();
  const keys = [
    ...new Set(points.flatMap((p) => Object.keys(p.metrics))),
  ].sort();
  for (const key of keys) {
    const section = document.createElement("section");
    section.className = "chart";
    const title = document.createElement("h2");
    title.textContent = key;
    const canvas = document.createElement("canvas");
    canvas.width = 720;
    canvas.height = 220;
    canvas.setAttribute("aria-label", key + " time series");
    canvas.setAttribute("role", "img");
    const legend = document.createElement("p");
    const values = points.map((p) => p.metrics[key] ?? null);
    const actual = values.filter((v) => v !== null);
    legend.textContent = `${key} · ${format(actual.at(-1) ?? null, unit(key))}`;
    section.append(title, canvas, legend);
    $("charts").append(section);
    const ctx = canvas.getContext("2d"),
      color = getComputedStyle(document.body).color;
    ctx.fillStyle = color;
    ctx.strokeStyle = color;
    ctx.font = "12px sans-serif";
    const left = 95,
      right = 710,
      top = 15,
      bottom = 175;
    const max = Math.max(1, ...actual);
    for (let i = 0; i <= 4; i++) {
      const y = bottom - ((bottom - top) * i) / 4;
      ctx.fillText(format((max * i) / 4, unit(key)), 2, y + 4);
      ctx.globalAlpha = 0.15;
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(right, y);
      ctx.stroke();
      ctx.globalAlpha = 1;
      if (points.length) {
        const index = Math.round(((points.length - 1) * i) / 4);
        ctx.fillText(
          new Date(points[index].timestamp_ms).toLocaleString([], {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
          }),
          left + ((right - left) * i) / 4 - 40,
          204,
        );
      }
    }
    const start = points[0]?.timestamp_ms ?? 0,
      end = points.at(-1)?.timestamp_ms ?? 1;
    const xFor = (t) =>
      left + ((right - left) * (t - start)) / Math.max(1, end - start);
    ctx.strokeStyle = "#239ed9";
    ctx.lineWidth = 2;
    ctx.beginPath();
    let connected = false;
    points.forEach((p, i) => {
      const v = values[i];
      if (v === null) {
        connected = false;
        return;
      }
      const x = xFor(p.timestamp_ms),
        y = bottom - ((bottom - top) * v) / max;
      if (connected) ctx.lineTo(x, y);
      else ctx.moveTo(x, y);
      connected = true;
    });
    ctx.stroke();
    canvas.addEventListener("mousemove", (event) => {
      const x =
        ((event.clientX - canvas.getBoundingClientRect().left) * canvas.width) /
        canvas.getBoundingClientRect().width;
      const index = Math.max(
        0,
        Math.min(
          points.length - 1,
          Math.round(((x - left) / (right - left)) * (points.length - 1)),
        ),
      );
      if (points[index]) {
        canvas.title = `${new Date(points[index].timestamp_ms).toLocaleString()}: ${format(values[index], unit(key))}`;
        legend.textContent = `${key} · ${canvas.title}`;
      }
    });
  }
}
$("restore").onclick = restore;
$("logout").onclick = async () => {
  if (loggingOut) return;
  loggingOut = true;
  ++requestGeneration;
  clear("Logging out…", true);
  if (refreshing) {
    try {
      await refreshing;
    } catch {
      /* Logout still clears local state. */
    }
  }
  ++generation;
  const ownedRefresh = refresh;
  access = refresh = null;
  clear("Logged out locally.", true);
  try {
    const session = await authFetch("session/");
    csrf = session.csrfToken;
    await authFetch(
      "session/logout/",
      ownedRefresh ? { refresh: ownedRefresh } : {},
    );
    clear(
      "Central session logged out. Previously issued access tokens expire on their normal lifetime.",
    );
  } catch {
    clear("Local tokens cleared; central logout could not be confirmed.");
  } finally {
    loggingOut = false;
  }
};
$("node").onchange = $("hours").onchange = () => {
  ++requestGeneration;
  clear("Loading…");
  load();
};
$("reload").onclick = load;
$("theme").onclick = () => {
  document.body.classList.toggle("dark");
  draw(lastPoints);
};
setInterval(() => {
  if (access) load();
}, 10000);
restore();
