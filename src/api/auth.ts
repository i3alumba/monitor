const AUTH = "https://auth.i3alumba.ru/api/";
let access: string | null = null;
let refresh: string | null = null;
let csrf: string | null = null;
let generation = 0;
let pending: Promise<void> | null = null;
let rotation: Promise<void> | null = null;
let loggingOut = false;
let notifyFailure = () => {};
export function onAccessFailure(callback: () => void) {
  notifyFailure = callback;
  return () => {
    if (notifyFailure === callback) notifyFailure = () => {};
  };
}
async function authFetch(
  path: string,
  body?: object,
): Promise<Record<string, unknown>> {
  const response = await fetch(AUTH + path, {
    method: body === undefined ? "GET" : "POST",
    credentials: "include",
    cache: "no-store",
    headers:
      body === undefined
        ? {}
        : { "Content-Type": "application/json", "X-CSRFToken": csrf ?? "" },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok)
    throw new Error(
      "Auth unavailable or session denied. Sign in through Dashboard, then retry.",
    );
  return response.json();
}
function pair(value: Record<string, unknown>) {
  if (
    typeof value.access !== "string" ||
    !value.access ||
    typeof value.refresh !== "string" ||
    !value.refresh
  )
    throw new Error("Malformed Auth response.");
  access = value.access;
  refresh = value.refresh;
}
export function restore(): Promise<void> {
  if (pending) return pending;
  if (loggingOut) return Promise.reject(new Error("Logout in progress."));
  const epoch = ++generation;
  access = refresh = null;
  pending = (async () => {
    const session = await authFetch("session/");
    if (epoch !== generation) throw new Error("Session changed.");
    if (typeof session.csrfToken !== "string")
      throw new Error("Missing CSRF protection.");
    csrf = session.csrfToken;
    if (session.authenticated !== true)
      throw new Error(
        "Sign in through Dashboard; administrator access required.",
      );
    const value = await authFetch("session/token/", {});
    // Logout waits for this operation so its newly issued refresh is owned and revoked.
    if (epoch !== generation) throw new Error("Session changed.");
    pair(value);
  })().finally(() => {
    pending = null;
  });
  return pending;
}
function rotate(): Promise<void> {
  if (rotation) return rotation;
  const epoch = generation;
  rotation = (async () => {
    if (!refresh || loggingOut) throw new Error("Sign in through Dashboard.");
    const value = await authFetch("token/refresh/", { refresh });
    if (epoch !== generation) throw new Error("Session changed.");
    pair(value);
  })().finally(() => {
    rotation = null;
  });
  return rotation;
}
export async function api<T>(path: string, signal: AbortSignal): Promise<T> {
  const epoch = generation;
  try {
    if (!access || loggingOut) throw new Error("Sign in through Dashboard.");
    const request = () =>
      fetch(path, {
        headers: { Authorization: `Bearer ${access}` },
        cache: "no-store",
        signal: AbortSignal.any([signal, AbortSignal.timeout(10000)]),
      });
    let response = await request();
    if (epoch !== generation || loggingOut || signal.aborted)
      throw new DOMException("Session changed", "AbortError");
    if (response.status === 401) {
      notifyFailure();
      await rotate();
      if (epoch !== generation || loggingOut || signal.aborted)
        throw new DOMException("Session changed", "AbortError");
      response = await request();
    }
    if (epoch !== generation || loggingOut || signal.aborted)
      throw new DOMException("Session changed", "AbortError");
    if (!response.ok) {
      if (response.status === 401 || response.status === 403) access = null;
      throw new Error(
        response.status === 403
          ? "Administrator access denied or revoked."
          : response.status === 503
            ? "Auth or telemetry storage unavailable; measurements cleared."
            : "Request denied or unavailable; measurements cleared.",
      );
    }
    const result: T = await response.json();
    if (epoch !== generation || loggingOut || signal.aborted)
      throw new DOMException("Session changed", "AbortError");
    return result;
  } catch (error) {
    if (!signal.aborted && epoch === generation) notifyFailure();
    throw error;
  }
}
export async function logout(): Promise<string> {
  if (loggingOut) return "Logout in progress.";
  loggingOut = true;
  notifyFailure();
  // Both session/token and refresh can issue a token while logout is clicked.
  await Promise.allSettled([pending, rotation]);
  ++generation;
  const ownedRefresh = refresh;
  access = refresh = null;
  try {
    const session = await authFetch("session/");
    if (typeof session.csrfToken !== "string")
      throw new Error("Missing CSRF protection");
    csrf = session.csrfToken;
    await authFetch(
      "session/logout/",
      ownedRefresh ? { refresh: ownedRefresh } : {},
    );
    return "Central session logged out. Previously issued access tokens expire normally.";
  } catch {
    return "Local tokens cleared; central logout could not be confirmed.";
  } finally {
    loggingOut = false;
  }
}
