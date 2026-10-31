use crate::domain::{cpu, rate, GpuStatus, Metrics};
use anyhow::{ensure, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::{Duration, Instant},
};

pub struct Collector {
    pub gpu_status: GpuStatus,
    previous: BTreeMap<String, u64>,
    observed: BTreeSet<String>,
    last: Option<Instant>,
    filesystems: BTreeMap<String, PathBuf>,
    interfaces: Vec<String>,
    node: String,
    automatic: bool,
    sources: Sources,
    pub diagnostics: Vec<String>,
}
#[derive(Clone)]
pub struct Sources {
    pub stat: PathBuf,
    pub memory: PathBuf,
    pub disk: PathBuf,
    pub network: PathBuf,
}
impl Sources {
    pub fn native() -> Self {
        Self {
            stat: "/proc/stat".into(),
            memory: "/proc/meminfo".into(),
            disk: "/proc/diskstats".into(),
            network: "/proc/net/dev".into(),
        }
    }
    pub fn kubernetes() -> Self {
        Self {
            stat: "/host/proc/stat".into(),
            memory: "/host/proc/meminfo".into(),
            disk: "/host/proc/diskstats".into(),
            network: "/host/proc/net_dev".into(),
        }
    }
}
fn read_source(path: &std::path::Path) -> String {
    use std::io::Read;
    let mut out = String::new();
    if let Ok(file) = std::fs::File::open(path) {
        if file.take(1_048_577).read_to_string(&mut out).is_err() || out.len() > 1_048_576 {
            out.clear();
        }
    }
    out
}
fn whole_disk(name: &str) -> bool {
    if name.len() > 24 {
        return false;
    }
    if let Some(rest) = name.strip_prefix("xvd") {
        return !rest.is_empty() && rest.bytes().all(|c| c.is_ascii_lowercase());
    }
    if let Some(rest) = name.strip_prefix("sd") {
        return !rest.is_empty() && rest.bytes().all(|c| c.is_ascii_lowercase());
    }
    if let Some(rest) = name.strip_prefix("nvme") {
        return rest.split_once('n').is_some_and(|(a, b)| {
            !a.is_empty()
                && !b.is_empty()
                && a.bytes().all(|c| c.is_ascii_digit())
                && b.bytes().all(|c| c.is_ascii_digit())
        });
    }
    if let Some(rest) = name.strip_prefix("mmcblk") {
        return !rest.is_empty() && rest.bytes().all(|c| c.is_ascii_digit());
    }
    name.strip_prefix("vd")
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|c| c.is_ascii_lowercase()))
}
fn host_interface(name: &str) -> bool {
    name.len() <= 16
        && ["eth", "en", "wl", "ww", "wg"]
            .iter()
            .any(|p| name.starts_with(p))
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}
impl Collector {
    pub fn new(
        node: String,
        filesystems: BTreeMap<String, PathBuf>,
        interfaces: Vec<String>,
    ) -> Result<Self> {
        ensure!(
            filesystems.len() <= 2
                && filesystems
                    .keys()
                    .all(|k| matches!(k.as_str(), "root" | "hdd" | "probe_root")),
            "only root/hdd probes supported"
        );
        ensure!(
            interfaces.len() <= 4
                && interfaces.iter().all(|n| !n.is_empty()
                    && n.len() <= 16
                    && n.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))),
            "invalid interface allowlist"
        );
        Ok(Self {
            gpu_status: GpuStatus::default(),
            automatic: false,
            sources: Sources::native(),
            diagnostics: Vec::new(),
            previous: BTreeMap::new(),
            observed: BTreeSet::new(),
            last: None,
            filesystems,
            interfaces,
            node,
        })
    }
    pub fn automatic(node: String) -> Result<Self> {
        let mut collector = Self::new(
            node,
            BTreeMap::from([("probe_root".into(), "/host/probes/root".into())]),
            vec![],
        )?;
        collector.automatic = true;
        collector.sources = Sources::kubernetes();
        Ok(collector)
    }
    fn delta(&mut self, key: &str, current: u64, seconds: f64) -> Option<f64> {
        self.observed.insert(key.into());
        let previous = self.previous.insert(key.into(), current);
        rate(previous, current, seconds)
    }
    fn finish_observation(&mut self) {
        // A counter delta is valid only across adjacent successful observations.
        self.previous.retain(|key, _| self.observed.contains(key));
        self.observed.clear();
    }
    pub async fn sample(&mut self) -> Metrics {
        let now = Instant::now();
        let seconds = self
            .last
            .replace(now)
            .map(|last| now.duration_since(last).as_secs_f64())
            .unwrap_or(0.);
        let mut out = Metrics::new();
        out.insert("cpu.used_percent".into(), None);
        self.diagnostics.clear();
        let stat = read_source(&self.sources.stat);
        if !stat.is_empty() {
            if let Some(line) = stat.lines().next() {
                let values: Vec<u64> = line
                    .split_whitespace()
                    .skip(1)
                    .filter_map(|s| s.parse().ok())
                    .collect();
                if let Some((total, idle)) = cpu(&values) {
                    let total = self.delta("cpu.total", total, seconds);
                    let idle = self.delta("cpu.idle", idle, seconds);
                    out.insert(
                        "cpu.used_percent".into(),
                        total
                            .zip(idle)
                            .and_then(|(t, i)| (t > 0. && i <= t).then_some(100. * (t - i) / t)),
                    );
                }
            }
        }
        let memory = read_source(&self.sources.memory);
        out.extend(memory_metrics(&memory));
        for (label, path) in &self.filesystems {
            for field in ["total_bytes", "free_bytes", "available_bytes"] {
                out.insert(format!("fs.{label}.{field}"), None);
            }
            if let Ok(values) = filesystem(path) {
                for (field, value) in ["total_bytes", "free_bytes", "available_bytes"]
                    .into_iter()
                    .zip(values)
                {
                    out.insert(format!("fs.{label}.{field}"), Some(value as f64));
                }
            }
        }
        let disks = read_source(&self.sources.disk);
        let mut devices: Vec<String> = if self.automatic {
            disks
                .lines()
                .filter_map(|l| l.split_whitespace().nth(2))
                .filter(|n| whole_disk(n))
                .map(str::to_owned)
                .collect()
        } else {
            vec!["nvme0n1".into(), "sda".into()]
        };
        devices.sort();
        devices.dedup();
        if devices.len() > 8 {
            self.diagnostics
                .push("block device discovery truncated at eight".into());
            devices.truncate(8);
        }
        for disk in &devices {
            for field in [
                "read_bytes_per_second",
                "write_bytes_per_second",
                "read_ops_per_second",
                "write_ops_per_second",
                "busy_percent",
            ] {
                out.insert(format!("disk.{disk}.{field}"), None);
            }
        }
        for line in disks.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 14 || !devices.iter().any(|d| d == f[2]) {
                continue;
            }
            for (index, name, factor) in [
                (3, "read_ops_per_second", 1.),
                (5, "read_bytes_per_second", 512.),
                (7, "write_ops_per_second", 1.),
                (9, "write_bytes_per_second", 512.),
                (12, "busy_percent", 0.1),
            ] {
                if let Ok(value) = f[index].parse() {
                    let key = format!("disk.{}.{name}", f[2]);
                    let rate = self.delta(&key, value, seconds).map(|v| v * factor);
                    out.insert(key, rate);
                }
            }
        }
        let net = read_source(&self.sources.network);
        let mut interfaces = if self.automatic {
            net.lines()
                .filter_map(|l| l.split_once(':'))
                .map(|(name, _)| name.trim())
                .filter(|n| host_interface(n))
                .map(str::to_owned)
                .collect::<Vec<_>>()
        } else {
            self.interfaces.clone()
        };
        interfaces.sort();
        interfaces.dedup();
        if interfaces.len() > 4 {
            self.diagnostics
                .push("interface discovery truncated at four".into());
            interfaces.truncate(4);
        }
        for interface in interfaces {
            let fields = [
                (0, "rx_bytes_per_second"),
                (1, "rx_packets_per_second"),
                (2, "rx_errors_per_second"),
                (3, "rx_drops_per_second"),
                (8, "tx_bytes_per_second"),
                (9, "tx_packets_per_second"),
                (10, "tx_errors_per_second"),
                (11, "tx_drops_per_second"),
            ];
            for (_, field) in fields {
                out.insert(format!("net.{interface}.{field}"), None);
            }
            if let Some(line) = net
                .lines()
                .filter_map(|l| l.split_once(':'))
                .find(|(name, _)| name.trim() == interface)
            {
                let values: Vec<&str> = line.1.split_whitespace().collect();
                for (index, field) in fields {
                    if let Some(value) = values.get(index).and_then(|v| v.parse().ok()) {
                        let key = format!("net.{interface}.{field}");
                        let rate = self.delta(&key, value, seconds);
                        out.insert(key, rate);
                    }
                }
            }
        }
        self.finish_observation();
        if self.automatic {
            let unreadable = [
                stat.is_empty(),
                memory.is_empty(),
                disks.is_empty(),
                net.is_empty(),
            ]
            .contains(&true);
            let reason = format!(
                "Baseline: {}{}GPU/extra FS unavailable; probe_root not necessarily /",
                if unreadable {
                    "host counters unavailable; "
                } else {
                    ""
                },
                if self.diagnostics.is_empty() {
                    ""
                } else {
                    "discovery truncated; "
                }
            );
            self.gpu_status = GpuStatus { uuid: None, reason };
            for field in [
                "utilization_percent",
                "memory_used_bytes",
                "memory_total_bytes",
                "temperature_celsius",
                "power_watts",
            ] {
                out.insert(format!("gpu.automatic.{field}"), None);
            }
        } else {
            let (metrics, status) = gpu(&self.node).await;
            self.gpu_status = status;
            out.extend(metrics);
        }
        out
    }
}
pub fn memory_metrics(text: &str) -> Metrics {
    let values: BTreeMap<&str, u64> = text
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((
                k,
                v.split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()?
                    .saturating_mul(1024),
            ))
        })
        .collect();
    let mut out = Metrics::new();
    for (key, total, available) in [
        ("memory", "MemTotal", "MemAvailable"),
        ("swap", "SwapTotal", "SwapFree"),
    ] {
        out.insert(
            format!("{key}.total_bytes"),
            values.get(total).map(|v| *v as f64),
        );
        out.insert(
            format!("{key}.available_bytes"),
            values.get(available).map(|v| *v as f64),
        );
        out.insert(
            format!("{key}.used_bytes"),
            values
                .get(total)
                .zip(values.get(available))
                .and_then(|(t, a)| t.checked_sub(*a))
                .map(|v| v as f64),
        );
    }
    out
}
#[allow(clippy::useless_conversion)] // GNU statvfs field widths differ on 32-bit targets.
fn filesystem(path: &std::path::Path) -> Result<[u64; 3]> {
    let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: valid NUL-terminated path and writable output structure.
    if unsafe { libc::statvfs(name.as_ptr(), info.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful statvfs initialized info.
    let info = unsafe { info.assume_init() };
    Ok([
        u64::from(info.f_blocks).saturating_mul(u64::from(info.f_frsize)),
        u64::from(info.f_bfree).saturating_mul(u64::from(info.f_frsize)),
        u64::from(info.f_bavail).saturating_mul(u64::from(info.f_frsize)),
    ])
}
async fn gpu(node: &str) -> (Metrics, GpuStatus) {
    let mut out = Metrics::new();
    let fields = [
        "utilization_percent",
        "memory_used_bytes",
        "memory_total_bytes",
        "temperature_celsius",
        "power_watts",
    ];
    for field in fields {
        out.insert(format!("gpu.nvidia.{field}"), None);
    }
    out.insert("gpu.nvidia.available".into(), Some(0.));
    // Known broken alpha NVIDIA driver; Intel has no safe unprivileged utilization adapter.
    for field in fields {
        out.insert(format!("gpu.intel.{field}"), None);
    }
    let mut status = GpuStatus {
        uuid: None,
        reason: if node == "alpha" {
            "NVIDIA driver unavailable; Intel utilization unsupported"
        } else {
            "NVIDIA query failed or timed out; Intel utilization unsupported"
        }
        .into(),
    };
    if node != "gamma" {
        return (out, status);
    }
    let mut command = tokio::process::Command::new("/usr/bin/nvidia-smi");
    command
        .args([
            "--query-gpu=uuid,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
            "--format=csv,noheader,nounits",
        ])
        .env_clear()
        .env("PATH", "/usr/bin")
        .env("LC_ALL", "C")
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Read a bounded output and wait inside one deadline; child killed on cancellation.
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        use tokio::io::AsyncReadExt;
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("stdout"))?;
        let mut bytes = Vec::new();
        stdout.take(4097).read_to_end(&mut bytes).await?;
        if bytes.len() > 4096 {
            return Err(std::io::Error::other("GPU output limit"));
        }
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((status, bytes))
    })
    .await;
    if let Ok(Ok((exit_status, bytes))) = result {
        if exit_status.success() {
            if let Ok(text) = std::str::from_utf8(&bytes) {
                let lines: Vec<_> = text.lines().collect();
                if lines.len() == 1 {
                    let cols: Vec<_> = lines[0].split(',').map(str::trim).collect();
                    if cols.len() == 6 && cols[0].starts_with("GPU-") {
                        status.uuid = Some(cols[0].into());
                        status.reason =
                            "NVIDIA telemetry available; Intel utilization unsupported".into();
                        out.insert("gpu.nvidia.available".into(), Some(1.));
                        for ((field, value), factor) in fields
                            .into_iter()
                            .zip(cols.iter().skip(1))
                            .zip([1., 1048576., 1048576., 1., 1.])
                        {
                            out.insert(
                                format!("gpu.nvidia.{field}"),
                                value
                                    .parse::<f64>()
                                    .ok()
                                    .filter(|v| v.is_finite() && *v >= 0.)
                                    .map(|v| v * factor),
                            );
                        }
                    }
                }
            }
        }
    }
    (out, status)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn automatic_profile_reads_host_network_not_pod_network() -> Result<()> {
        assert!(whole_disk("mmcblk0"));
        assert!(!whole_disk("mmcblk0p1"));
        let dir = tempfile::tempdir()?;
        let host = dir.path().join("net_dev");
        let pod = dir.path().join("pod_net_dev");
        std::fs::write(&host,"eth0: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0\nveth123: 90000 0 0 0 0 0 0 0 90000 0 0 0 0 0 0 0\n")?;
        std::fs::write(&pod, "eth0: 999999 0 0 0 0 0 0 0 999999 0 0 0 0 0 0 0\n")?;
        let mut collector = Collector::automatic("new-node".into())?;
        let stat = dir.path().join("stat");
        std::fs::write(&stat, "cpu 10 0 10 10 0 0 0 0\n")?;
        let memory = dir.path().join("meminfo");
        std::fs::write(&memory, "MemTotal: 1024 kB\nMemAvailable: 512 kB\n")?;
        let disk = dir.path().join("diskstats");
        std::fs::write(
            &disk,
            "8 0 sda 1 0 10 0 1 0 10 0 0 0 0\n8 1 sda1 1 0 100 0 1 0 100 0 0 0 0\n",
        )?;
        collector.sources = Sources {
            stat,
            memory,
            disk,
            network: host,
        };
        let metrics = collector.sample().await;
        assert_eq!(collector.previous["net.eth0.rx_bytes_per_second"], 1000);
        assert_eq!(metrics["net.eth0.rx_bytes_per_second"], None);
        assert!(!metrics
            .keys()
            .any(|k| k.contains("veth") || k.contains("sda1")));
        assert_eq!(metrics["gpu.automatic.utilization_percent"], None);
        assert!(collector
            .gpu_status
            .reason
            .contains("GPU/extra FS unavailable"));
        assert_eq!(
            Sources::kubernetes().network,
            PathBuf::from("/host/proc/net_dev")
        );
        Ok(())
    }
    #[test]
    fn missing_disk_and_network_counters_invalidate_baselines() -> Result<()> {
        let mut collector = Collector::new("alpha".into(), BTreeMap::new(), vec![])?;
        for key in [
            "disk.sda.read_bytes_per_second",
            "net.eth0.rx_bytes_per_second",
        ] {
            assert_eq!(collector.delta(key, 1000, 10.), None);
        }
        collector.finish_observation();
        // The next observation has neither counter, but global elapsed time advances.
        collector.finish_observation();
        for key in [
            "disk.sda.read_bytes_per_second",
            "net.eth0.rx_bytes_per_second",
        ] {
            assert_eq!(collector.delta(key, 3000, 10.), None);
        }
        collector.finish_observation();
        for key in [
            "disk.sda.read_bytes_per_second",
            "net.eth0.rx_bytes_per_second",
        ] {
            assert_eq!(collector.delta(key, 4000, 10.), Some(100.));
        }
        Ok(())
    }
    #[test]
    fn mem_available_not_free() {
        let m=memory_metrics("MemTotal: 100 kB\nMemAvailable: 40 kB\nMemFree: 5 kB\nSwapTotal: 10 kB\nSwapFree: 8 kB");
        assert_eq!(m["memory.used_bytes"], Some(60. * 1024.));
        assert_eq!(m["swap.used_bytes"], Some(2. * 1024.));
    }
    #[tokio::test]
    async fn alpha_gpu_unavailable() {
        let (m, status) = gpu("alpha").await;
        assert!(status.reason.contains("unavailable"));
        assert_eq!(m["gpu.nvidia.available"], Some(0.));
        assert_eq!(m["gpu.nvidia.utilization_percent"], None);
    }
}
