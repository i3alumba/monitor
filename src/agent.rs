use crate::{
    collect::Collector,
    config::{client, env, projected_token, secret},
    domain::{known_node, now_ms, Batch, Sample},
    kubernetes::{valid_name, Enrollment},
};
use anyhow::{ensure, Result};
use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
pub async fn run() -> Result<()> {
    let node = env("MONITOR_NODE", "");
    let mode = env("MONITOR_AGENT_MODE", "native");
    ensure!(
        matches!(mode.as_str(), "native" | "kubernetes"),
        "invalid agent mode"
    );
    let automatic = mode == "kubernetes";
    ensure!(
        if automatic {
            valid_name(&node)
        } else {
            known_node(&node)
        },
        "invalid MONITOR_NODE"
    );
    let endpoint = env(
        "MONITOR_ENDPOINT",
        "https://monitor-collector.public-page.svc.cluster.local:8443/v1/samples",
    );
    let url = reqwest::Url::parse(&endpoint)?;
    ensure!(
        url.scheme() == "https"
            && url.path()
                == if automatic {
                    "/v1/kubernetes/samples"
                } else {
                    "/v1/samples"
                }
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty(),
        "endpoint must be HTTPS /v1/samples"
    );
    let server_ip = env("MONITOR_SERVER_IP", "");
    ensure!(
        automatic || !server_ip.is_empty(),
        "native mode requires MONITOR_SERVER_IP"
    );
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("missing endpoint host"))?;
    let ca = reqwest::Certificate::from_pem(&std::fs::read(env(
        "MONITOR_CA_FILE",
        "/etc/public-page-monitor/ca.crt",
    ))?)?;
    let mut builder = client()
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca);
    if !server_ip.is_empty() {
        let ip: IpAddr = server_ip.parse()?;
        builder = builder.resolve(
            host,
            SocketAddr::new(ip, url.port_or_known_default().unwrap_or(8443)),
        );
    }
    let client = builder.build()?;
    let token_file = env(
        "MONITOR_TOKEN_FILE",
        if automatic {
            "/run/monitor-identity/token"
        } else {
            "/run/credentials/public-page-monitor.service/token"
        },
    );
    let mut enrollment_url = url.clone();
    enrollment_url.set_path("/v1/kubernetes/enroll");
    let seconds: u64 = env("MONITOR_INTERVAL_SECONDS", "10").parse()?;
    ensure!(
        (10..=60).contains(&seconds),
        "interval must be 10..60 seconds"
    );
    let filesystems = serde_json::from_str(&env(
        "MONITOR_FILESYSTEMS_JSON",
        r#"{"root":"/var/lib/public-page/monitor-probes/root","hdd":"/run/monitor-probes/hdd"}"#,
    ))?;
    let interfaces = env("MONITOR_INTERFACES", "")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let mut collector = if automatic {
        Collector::automatic(node.clone())?
    } else {
        Collector::new(node.clone(), filesystems, interfaces)?
    };
    let boot = std::fs::read_to_string(if automatic {
        "/host/proc/boot_id"
    } else {
        "/proc/sys/kernel/random/boot_id"
    })?
    .trim()
    .to_owned();
    let process = uuid::Uuid::new_v4().to_string();
    let mut seq = 0;
    let mut queue = VecDeque::new();
    let mut tick = tokio::time::interval(Duration::from_secs(seconds));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut failures = 0u32;
    let mut retry_at = tokio::time::Instant::now();
    loop {
        tick.tick().await;
        let identity = if automatic {
            let token = match projected_token(&token_file) {
                Ok(token) => token,
                Err(_) => {
                    eprintln!("projected credential unavailable");
                    continue;
                }
            };
            let assignment = match client
                .post(enrollment_url.clone())
                .bearer_auth(token)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    read_enrollment(response).await.ok()
                }
                _ => None,
            };
            match assignment {
                Some(assignment) if !assignment.collect => {
                    queue.clear();
                    continue;
                }
                Some(assignment)
                    if assignment.provider == "kubernetes"
                        && uuid::Uuid::parse_str(&assignment.node_uid).is_ok() =>
                {
                    assignment.node_uid
                }
                _ => {
                    queue.clear();
                    eprintln!("Kubernetes enrollment unavailable");
                    continue;
                }
            }
        } else {
            node.clone()
        };
        if queue
            .front()
            .is_some_and(|sample: &Sample| sample.node != identity)
        {
            queue.clear();
            collector = Collector::automatic(node.clone())?;
        }
        let timestamp_ms = now_ms();
        let metrics = collector.sample().await;
        let sample = Sample {
            node: identity,
            boot_id: boot.clone(),
            process_id: process.clone(),
            seq,
            timestamp_ms,
            metrics,
            gpu: collector.gpu_status.clone(),
        };
        seq += 1;
        enqueue(&mut queue, sample, now_ms());
        if tokio::time::Instant::now() < retry_at {
            continue;
        }
        let batch = Batch {
            samples: queue.iter().cloned().collect(),
        };
        match client
            .post(url.clone())
            .bearer_auth(if automatic {
                projected_token(&token_file)?
            } else {
                secret(&token_file)?
            })
            .json(&batch)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                queue.clear();
                failures = 0;
            }
            _ => {
                failures = (failures + 1).min(5);
                eprintln!("ingestion unavailable; bounded retry pending");
            }
        }
        // Small per-process jitter; no credentials or endpoint logged.
        let jitter = process.as_bytes()[0] as u64 % 3;
        retry_at = tokio::time::Instant::now()
            + Duration::from_secs(if failures == 0 {
                0
            } else {
                (1u64 << failures).min(30) + jitter
            });
    }
}

async fn read_enrollment(mut response: reqwest::Response) -> Result<Enrollment> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 16384,
            "enrollment response too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn enqueue(queue: &mut VecDeque<Sample>, sample: Sample, now: i64) {
    if queue.len() == 6 {
        queue.pop_front();
        eprintln!("retry queue overflow: dropped oldest sample");
    }
    queue.push_back(sample);
    while queue
        .front()
        .is_some_and(|s| now - s.timestamp_ms > 110_000 || s.timestamp_ms > now + 30_000)
    {
        queue.pop_front();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_queue_is_bounded_and_expired_data_dropped() {
        let mut queue = VecDeque::new();
        for seq in 0..10 {
            enqueue(
                &mut queue,
                Sample {
                    node: "alpha".into(),
                    boot_id: uuid::Uuid::nil().to_string(),
                    process_id: uuid::Uuid::nil().to_string(),
                    seq,
                    timestamp_ms: seq * 10000,
                    metrics: Default::default(),
                    gpu: Default::default(),
                },
                seq * 10000,
            );
        }
        assert_eq!(queue.len(), 6);
        assert_eq!(queue.front().map(|s| s.seq), Some(4));
        let mut new = queue.back().cloned().unwrap();
        new.timestamp_ms = 300_000;
        enqueue(&mut queue, new, 300_000);
        assert_eq!(queue.len(), 1);
    }
}
