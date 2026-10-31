use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Metrics = BTreeMap<String, Option<f64>>;
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct GpuStatus {
    pub uuid: Option<String>,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub node: String,
    pub boot_id: String,
    pub process_id: String,
    pub seq: i64,
    pub timestamp_ms: i64,
    pub metrics: Metrics,
    #[serde(default)]
    pub gpu: GpuStatus,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub samples: Vec<Sample>,
}
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
pub fn known_node(s: &str) -> bool {
    matches!(s, "alpha" | "gamma")
}
pub fn valid_key(key: &str) -> bool {
    key.len() <= 96
        && key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        && ["cpu.", "memory.", "swap.", "fs.", "disk.", "net.", "gpu."]
            .iter()
            .any(|p| key.starts_with(p))
}
impl Sample {
    pub fn valid(&self, now: i64) -> bool {
        (known_node(&self.node) || uuid::Uuid::parse_str(&self.node).is_ok())
            && self.gpu.reason.len() <= 128
            && self.gpu.uuid.as_ref().is_none_or(|s| {
                s.len() <= 64
                    && s.starts_with("GPU-")
                    && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
            })
            && [&self.boot_id, &self.process_id]
                .iter()
                .all(|s| uuid::Uuid::parse_str(s).is_ok())
            && self.seq >= 0
            && self.timestamp_ms >= now - 120_000
            && self.timestamp_ms <= now + 30_000
            && self.metrics.len() <= 128
            && self
                .metrics
                .iter()
                .all(|(k, v)| valid_key(k) && v.is_none_or(|v| v.is_finite() && v >= 0.0))
    }
}
pub fn rate(previous: Option<u64>, current: u64, seconds: f64) -> Option<f64> {
    if !seconds.is_finite() || seconds <= 0.0 || seconds > 120.0 {
        return None;
    }
    previous
        .and_then(|p| current.checked_sub(p))
        .map(|d| d as f64 / seconds)
}
/// /proc/stat user/nice already include guest/guest_nice. Iowait is idle.
pub fn cpu(c: &[u64]) -> Option<(u64, u64)> {
    if c.len() < 8 {
        return None;
    }
    Some((c[..8].iter().sum(), c[3] + c[4]))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deltas_reset_first_and_monotonic() {
        assert_eq!(rate(None, 100, 10.), None);
        assert_eq!(rate(Some(100), 90, 10.), None);
        assert_eq!(rate(Some(0), 10, 0.), None);
        assert_eq!(rate(Some(0), 10, f64::NAN), None);
        assert_eq!(rate(Some(2), 4, 2.).map(|r| r * 512.), Some(512.));
    }
    #[test]
    fn no_guest_double_count() {
        assert_eq!(cpu(&[10, 2, 3, 20, 5, 1, 1, 0, 8, 2]), Some((42, 25)));
    }
    #[test]
    fn labels_bounded() {
        assert!(!valid_key("net.<script>"));
        assert!(!valid_key("unknown.x"));
    }
}
