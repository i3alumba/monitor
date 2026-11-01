import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  AppBar,
  Box,
  Button,
  Chip,
  Container,
  Stack,
  TextField,
  Toolbar,
  Typography,
} from "@mui/material";
import type { Resource } from "../types";
import { useMonitor } from "../hooks/useMonitor";
import { ThemeToggle } from "../ui/ThemeToggle";
import { charts, devices, hasValues, resources } from "../components/resources";
import { MetricChart } from "../components/MetricChart";
export function Monitor() {
  const [node, setNode] = useState("");
  const [hours, setHours] = useState("1");
  const [resource, setResource] = useState<Resource>("CPU");
  const [device, setDevice] = useState("");
  const { inventory, history, status, busy, connect, signOut, retry } =
    useMonitor(node, hours, resource + "|" + device);
  useEffect(() => {
    if (!node && inventory.nodes.length) setNode(inventory.nodes[0].uid);
  }, [inventory.nodes, node]);
  const entry = inventory.nodes.find((entry) => entry.uid === node);
  const keys = useMemo(
    () => [
      ...new Set([
        ...(history?.points.flatMap((point) => Object.keys(point.metrics)) ??
          []),
        ...Object.keys(entry?.latest?.metrics ?? {}),
      ]),
    ],
    [history, entry],
  );
  const options = devices(keys, resource);
  const selectedDevice = options.includes(device) ? device : (options[0] ?? "");
  const specs = charts(resource, selectedDevice);
  const visible = history && hasValues(history.points, specs);
  return (
    <>
      <AppBar position="static">
        <Toolbar className="header">
          <Typography
            component="a"
            href="https://main.i3alumba.ru/"
            variant="h6"
            className="brand"
          >
            i3alumba
          </Typography>
          <Typography component="h1" variant="h6">
            Monitor
          </Typography>
          <Box className="header-space" />
          <ThemeToggle />
        </Toolbar>
      </AppBar>
      <Container maxWidth={false} className="app-container">
        <Stack spacing={3}>
          <Stack direction="row" className="actions" spacing={1}>
            <Button href="https://main.i3alumba.ru" variant="outlined">
              Dashboard / Sign in
            </Button>
            <Button disabled={busy} onClick={() => void connect()}>
              Restore session
            </Button>
            <Button disabled={busy} onClick={() => void signOut()}>
              Log out
            </Button>
            <Button disabled={busy} onClick={retry}>
              Retry / Refresh
            </Button>
          </Stack>
          <Box className="selectors">
            <TextField
              select
              label="Node"
              value={
                inventory.nodes.some((entry) => entry.uid === node) ? node : ""
              }
              onChange={(event) => setNode(event.target.value)}
              slotProps={{ select: { native: true } }}
            >
              <option value="" disabled>
                Select a node
              </option>
              {inventory.nodes.map((entry) => (
                <option key={entry.uid} value={entry.uid}>
                  {entry.name}
                </option>
              ))}
            </TextField>
            <TextField
              select
              label="Resource"
              value={resource}
              onChange={(event) => {
                setResource(event.target.value as Resource);
                setDevice("");
              }}
              slotProps={{ select: { native: true } }}
            >
              {resources.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </TextField>
            {["Disk I/O", "Filesystems", "Network"].includes(resource) && (
              <TextField
                select
                label={
                  resource === "Network"
                    ? "Interface"
                    : resource === "Filesystems"
                      ? "Filesystem probe"
                      : "Device"
                }
                value={selectedDevice}
                onChange={(event) => setDevice(event.target.value)}
                slotProps={{ select: { native: true } }}
              >
                {!options.length && (
                  <option value="">No devices available</option>
                )}
                {options.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </TextField>
            )}
            <TextField
              select
              label="History"
              value={hours}
              onChange={(event) => setHours(event.target.value)}
              slotProps={{ select: { native: true } }}
            >
              {[
                ["1", "1 hour"],
                ["6", "6 hours"],
                ["24", "24 hours"],
                ["168", "7 days"],
                ["720", "30 days"],
              ].map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </TextField>
          </Box>
          <Typography role="status">{status}</Typography>
          {inventory.discovery_error && (
            <Alert severity="warning">
              Discovery error: {inventory.discovery_error}
            </Alert>
          )}
          {entry && (
            <Box>
              <Stack direction="row" spacing={1} className="chips">
                <Chip label={entry.lifecycle} />
                <Chip
                  label={entry.collection}
                  color={entry.collection === "fresh" ? "success" : "warning"}
                />
                <Chip label={entry.provider} />
              </Stack>
              <details>
                <summary>Node details and collection limits</summary>
                <Typography variant="body2">UID: {entry.uid}</Typography>
                {entry.capabilities.map((value) => (
                  <Typography key={value} variant="body2">
                    {value}
                  </Typography>
                ))}
              </details>
            </Box>
          )}
          {resource === "Filesystems" && (
            <Alert severity="info">
              Probe capacity describes its containing filesystem, not
              necessarily root or a PVC quota. Free and unprivileged available
              space differ.
            </Alert>
          )}
          {resource === "Network" && (
            <Typography variant="body2">
              One interface at a time; interfaces are never summed to avoid
              double-counting physical and WireGuard traffic.
            </Typography>
          )}
          {history && !visible && (
            <Alert severity="info">
              {resource === "GPU"
                ? `GPU telemetry unavailable. ${entry?.latest?.gpu?.reason ?? "No supported readings in this interval."}`
                : "No readings for this resource in this interval. First samples, resets and missing counters are unavailable, not zero."}
            </Alert>
          )}
          {visible && history && (
            <Box className="chart-grid">
              {specs
                .filter((spec) => hasValues(history.points, [spec]))
                .map((spec) => (
                  <MetricChart
                    key={spec.title}
                    spec={spec}
                    points={history.points}
                    bucketMs={history.bucket_ms}
                  />
                ))}
            </Box>
          )}
          <Typography variant="caption">
            Gaps and null readings break lines. Longer ranges use averages that
            may conceal sub-bucket gaps. No collector or device controls.
          </Typography>
        </Stack>
      </Container>
    </>
  );
}
