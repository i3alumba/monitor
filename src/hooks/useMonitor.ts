import { useCallback, useEffect, useRef, useState } from "react";
import { api, logout, onAccessFailure, restore } from "../api/auth";
import type { History, Inventory } from "../types";
export function useMonitor(node: string, hours: string, selection: string) {
  const [inventory, setInventory] = useState<Inventory>({
    nodes: [],
    discovery_error: null,
  });
  const [loaded, setLoaded] = useState<{
    history: History;
    key: string;
  } | null>(null);
  const selectionKey = `${node}|${hours}|${selection}`;
  const history = loaded?.key === selectionKey ? loaded.history : null;
  const [status, setStatus] = useState("Restoring central session…");
  const [active, setActive] = useState(false);
  const [busy, setBusy] = useState(false);
  const [revision, setRevision] = useState(0);
  const epoch = useRef(0);
  const clear = useCallback(() => {
    setLoaded(null);
  }, []);
  useEffect(() => onAccessFailure(clear), [clear]);
  const connect = useCallback(async () => {
    const version = ++epoch.current;
    setActive(false);
    setInventory({ nodes: [], discovery_error: null });
    clear();
    setBusy(true);
    setStatus("Restoring central session…");
    try {
      await restore();
      if (version === epoch.current) {
        setActive(true);
        setStatus("Loading…");
      }
    } catch (error) {
      if (version === epoch.current)
        setStatus(
          error instanceof Error ? error.message : "Session unavailable.",
        );
    } finally {
      if (version === epoch.current) setBusy(false);
    }
  }, [clear]);
  useEffect(() => {
    const sessionEpoch = epoch;
    void connect();
    return () => {
      ++sessionEpoch.current;
    };
  }, [connect]);
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    let loading = false;
    clear();
    async function load() {
      if (loading) return;
      loading = true;
      setStatus("Loading…");
      try {
        const inventory = await api<Inventory>("/api/nodes", controller.signal);
        if (controller.signal.aborted) return;
        setInventory(inventory);
        const uid = inventory.nodes.some((entry) => entry.uid === node)
          ? node
          : "";
        if (!uid) {
          clear();
          setStatus(
            inventory.nodes.length
              ? "Select a node."
              : inventory.discovery_error ||
                  "No discovered nodes; coverage pending.",
          );
          return;
        }
        const result = await api<History>(
          `/api/history?node=${encodeURIComponent(uid)}&hours=${hours}`,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        setLoaded({ history: result, key: selectionKey });
        const last = result.points
          .filter((point) =>
            Object.values(point.metrics).some((value) => value !== null),
          )
          .at(-1);
        setStatus(
          last
            ? `Latest bucket: ${new Date(last.timestamp_ms).toLocaleString()} · ${result.bucket_ms / 1000}s averages`
            : "No samples in this interval.",
        );
      } catch (error) {
        if (!controller.signal.aborted) {
          clear();
          setInventory({ nodes: [], discovery_error: null });
          setStatus(
            error instanceof Error
              ? error.message
              : "Network unavailable; measurements cleared.",
          );
        }
      } finally {
        loading = false;
      }
    }
    void load();
    const timer = window.setInterval(() => void load(), 10000);
    return () => {
      controller.abort();
      window.clearInterval(timer);
    };
  }, [active, node, hours, selection, selectionKey, revision, clear]);
  const signOut = async () => {
    ++epoch.current;
    setActive(false);
    setBusy(true);
    clear();
    setInventory({ nodes: [], discovery_error: null });
    setStatus("Logging out…");
    setStatus(await logout());
    setBusy(false);
  };
  return {
    inventory,
    history,
    status,
    busy,
    connect,
    signOut,
    retry: () => {
      clear();
      setRevision((value) => value + 1);
    },
  };
}
