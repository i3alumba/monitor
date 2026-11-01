export interface Point {
  timestamp_ms: number;
  metrics: Record<string, number | null>;
}
export interface History {
  node: string;
  bucket_ms: number;
  points: Point[];
}
export interface NodeEntry {
  uid: string;
  name: string;
  lifecycle: string;
  collection: string;
  provider: string;
  capabilities: string[];
  last_sample?: number | null;
  latest: {
    gpu?: { reason?: string; uuid?: string | null };
    metrics?: Record<string, number | null>;
  } | null;
}
export interface Inventory {
  nodes: NodeEntry[];
  discovery_error: string | null;
}
export type Resource =
  | "CPU"
  | "Memory"
  | "Disk I/O"
  | "Filesystems"
  | "Network"
  | "GPU";
