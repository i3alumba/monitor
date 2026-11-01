import type { Point, Resource } from "../types";
export interface Series {
  key: string;
  label: string;
}
export interface ChartSpec {
  title: string;
  unit: string;
  series: Series[];
}
export const resources: Resource[] = [
  "CPU",
  "Memory",
  "Disk I/O",
  "Filesystems",
  "Network",
  "GPU",
];
export function devices(keys: string[], resource: Resource): string[] {
  const family =
    resource === "Disk I/O"
      ? "disk"
      : resource === "Filesystems"
        ? "fs"
        : resource === "Network"
          ? "net"
          : "";
  return family
    ? [
        ...new Set(
          keys
            .filter((key) => key.startsWith(family + "."))
            .map((key) => key.split(".")[1]),
        ),
      ].sort()
    : [];
}
export function charts(resource: Resource, device: string): ChartSpec[] {
  const chart = (
    title: string,
    unit: string,
    prefix: string,
    entries: [string, string][],
  ): ChartSpec => ({
    title,
    unit,
    series: entries.map(([key, label]) => ({ key: prefix + key, label })),
  });
  switch (resource) {
    case "CPU":
      return [
        chart("CPU utilization", "%", "cpu.", [["used_percent", "Used"]]),
      ];
    case "Memory":
      return [
        chart("Memory", "bytes", "memory.", [
          ["used_bytes", "Used"],
          ["available_bytes", "Available"],
          ["total_bytes", "Total"],
        ]),
        chart("Swap", "bytes", "swap.", [
          ["used_bytes", "Used"],
          ["total_bytes", "Total"],
        ]),
      ];
    case "Disk I/O":
      return device
        ? [
            chart("Disk throughput", "bytes/s", `disk.${device}.`, [
              ["read_bytes_per_second", "Read"],
              ["write_bytes_per_second", "Write"],
            ]),
            chart("Disk operations", "ops/s", `disk.${device}.`, [
              ["read_ops_per_second", "Read"],
              ["write_ops_per_second", "Write"],
            ]),
            chart("Disk utilization", "%", `disk.${device}.`, [
              ["busy_percent", "Busy"],
            ]),
          ]
        : [];
    case "Filesystems":
      return device
        ? [
            chart("Filesystem capacity", "bytes", `fs.${device}.`, [
              ["total_bytes", "Total"],
              ["free_bytes", "Free"],
              ["available_bytes", "Available to unprivileged users"],
            ]),
          ]
        : [];
    case "Network":
      return device
        ? [
            chart("Network throughput", "bytes/s", `net.${device}.`, [
              ["rx_bytes_per_second", "Receive"],
              ["tx_bytes_per_second", "Transmit"],
            ]),
            chart("Network packets", "packets/s", `net.${device}.`, [
              ["rx_packets_per_second", "Receive"],
              ["tx_packets_per_second", "Transmit"],
            ]),
            chart("Network errors and drops", "/s", `net.${device}.`, [
              ["rx_errors_per_second", "Receive errors"],
              ["tx_errors_per_second", "Transmit errors"],
              ["rx_drops_per_second", "Receive drops"],
              ["tx_drops_per_second", "Transmit drops"],
            ]),
          ]
        : [];
    case "GPU":
      return [
        chart("GPU utilization", "%", "gpu.nvidia.", [
          ["utilization_percent", "Used"],
        ]),
        chart("GPU memory", "bytes", "gpu.nvidia.", [
          ["memory_used_bytes", "Used"],
          ["memory_total_bytes", "Total"],
        ]),
        chart("GPU temperature", "°C", "gpu.nvidia.", [
          ["temperature_celsius", "Temperature"],
        ]),
        chart("GPU power", "W", "gpu.nvidia.", [["power_watts", "Power"]]),
      ];
  }
}
export function format(value: number | null | undefined, unit: string): string {
  if (value == null || !Number.isFinite(value)) return "unavailable";
  if (unit.startsWith("bytes")) {
    const scales = ["B", "KiB", "MiB", "GiB", "TiB"];
    let index = 0;
    while (value >= 1024 && index < scales.length - 1) {
      value /= 1024;
      index++;
    }
    return `${value.toFixed(1)} ${scales[index]}${unit.endsWith("/s") ? "/s" : ""}`;
  }
  return `${value.toFixed(1)} ${unit}`;
}
export function hasValues(points: Point[], specs: ChartSpec[]): boolean {
  return points.some((point) =>
    specs.some((spec) =>
      spec.series.some((series) => point.metrics[series.key] != null),
    ),
  );
}
