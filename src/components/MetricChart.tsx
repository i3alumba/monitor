import { useEffect, useRef, useState } from "react";
import {
  Card,
  CardContent,
  Stack,
  Typography,
  useColorScheme,
} from "@mui/material";
import type { Point } from "../types";
import { format, type ChartSpec } from "./resources";
const colors = ["#2563eb", "#0f766e", "#b45309", "#a855f7"];
const darkColors = ["#60a5fa", "#5eead4", "#fbbf24", "#c084fc"];
const plotHeight = 250;
export function MetricChart({
  spec,
  points,
  bucketMs,
}: {
  spec: ChartSpec;
  points: Point[];
  bucketMs: number;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const geometry = useRef({ width: 760, left: 100, right: 742 });
  const [hover, setHover] = useState<number | null>(null);
  const { mode, systemMode } = useColorScheme();
  useEffect(() => {
    const element = canvas.current;
    if (!element) return;
    const draw = () => {
      const width = Math.max(1, element.clientWidth);
      const ratio = Math.min(3, window.devicePixelRatio || 1);
      element.width = Math.round(width * ratio);
      element.height = Math.round(plotHeight * ratio);
      const ctx = element.getContext("2d");
      if (!ctx) return;
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      ctx.clearRect(0, 0, width, plotHeight);
      const dark = (mode === "system" ? systemMode : mode) === "dark";
      ctx.fillStyle = dark ? "#e6edf7" : "#16243a";
      ctx.strokeStyle = ctx.fillStyle;
      ctx.font = "12px system-ui";
      const left = width < 480 ? 78 : 100,
        right = width - 18,
        top = 16,
        bottom = 200;
      geometry.current = { width, left, right };
      const values = points
        .flatMap((point) =>
          spec.series.map((series) => point.metrics[series.key]),
        )
        .filter(
          (value): value is number => value != null && Number.isFinite(value),
        );
      const max = Math.max(1, ...values),
        start = points[0]?.timestamp_ms ?? 0,
        end = points.at(-1)?.timestamp_ms ?? start + 1;
      const ticks = Math.max(1, Math.min(4, Math.floor((right - left) / 130)));
      for (let i = 0; i <= 4; i++) {
        const y = bottom - ((bottom - top) * i) / 4;
        ctx.textAlign = "left";
        ctx.fillText(format((max * i) / 4, spec.unit), 2, y + 4);
        ctx.globalAlpha = 0.15;
        ctx.beginPath();
        ctx.moveTo(left, y);
        ctx.lineTo(right, y);
        ctx.stroke();
        ctx.globalAlpha = 1;
      }
      for (let i = 0; i <= ticks && points.length; i++) {
        const date = new Date(start + ((end - start) * i) / ticks);
        const label =
          width < 480
            ? end - start > 86400000
              ? date.toLocaleDateString([], { month: "short", day: "numeric" })
              : date.toLocaleTimeString([], {
                  hour: "2-digit",
                  minute: "2-digit",
                })
            : date.toLocaleString([], {
                month: "short",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
              });
        ctx.textAlign = i === 0 ? "left" : i === ticks ? "right" : "center";
        ctx.fillText(label, left + ((right - left) * i) / ticks, 235);
      }
      spec.series.forEach((series, index) => {
        ctx.strokeStyle = (dark ? darkColors : colors)[index % colors.length];
        ctx.fillStyle = ctx.strokeStyle;
        ctx.lineWidth = 2;
        ctx.beginPath();
        let connected = false,
          last = 0;
        points.forEach((point) => {
          const value = point.metrics[series.key];
          if (value == null || !Number.isFinite(value)) {
            connected = false;
            return;
          }
          const x =
              left +
              ((right - left) * (point.timestamp_ms - start)) /
                Math.max(1, end - start),
            y = bottom - ((bottom - top) * value) / max;
          // Null buckets and genuinely absent intervals must never be connected.
          if (connected && point.timestamp_ms - last <= bucketMs * 1.5)
            ctx.lineTo(x, y);
          else ctx.moveTo(x, y);
          connected = true;
          last = point.timestamp_ms;
          ctx.fillRect(x - 1.5, y - 1.5, 3, 3);
        });
        ctx.stroke();
      });
    };
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(element);
    window.addEventListener("resize", draw);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", draw);
    };
  }, [points, spec, bucketMs, mode, systemMode]);
  const point = hover === null ? points.at(-1) : points[hover];
  return (
    <Card variant="outlined">
      <CardContent>
        <Typography variant="h6" component="h2">
          {spec.title}
        </Typography>
        <canvas
          ref={canvas}
          width="760"
          height={plotHeight}
          role="img"
          aria-label={`${spec.title} time series; gaps indicate unavailable readings`}
          onMouseLeave={() => setHover(null)}
          onMouseMove={(event) => {
            const bounds = event.currentTarget.getBoundingClientRect();
            const { width, left, right } = geometry.current;
            const fraction = Math.max(
              0,
              Math.min(
                1,
                (((event.clientX - bounds.left) / bounds.width) * width -
                  left) /
                  Math.max(1, right - left),
              ),
            );
            const target =
              (points[0]?.timestamp_ms ?? 0) +
              fraction *
                ((points.at(-1)?.timestamp_ms ?? 0) -
                  (points[0]?.timestamp_ms ?? 0));
            let nearest = 0;
            points.forEach((point, index) => {
              if (
                Math.abs(point.timestamp_ms - target) <
                Math.abs(points[nearest].timestamp_ms - target)
              )
                nearest = index;
            });
            setHover(nearest);
          }}
        />
        <Stack spacing={0.5} aria-live="off">
          {spec.series.map((series, index) => (
            <Typography
              key={series.key}
              variant="body2"
              className={`series series-${index}`}
            >
              {series.label}: {format(point?.metrics[series.key], spec.unit)}
            </Typography>
          ))}
          {hover !== null && point && (
            <Typography variant="caption">
              {new Date(point.timestamp_ms).toLocaleString()}
            </Typography>
          )}
        </Stack>
        <details>
          <summary>Metric details</summary>
          {spec.series.map((series) => (
            <Typography variant="caption" component="p" key={series.key}>
              {series.key}
            </Typography>
          ))}
        </details>
      </CardContent>
    </Card>
  );
}
