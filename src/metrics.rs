use bollard::Docker;
use reqwest::Client;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

#[derive(Default)]
pub struct MetricsRegistry {
    pub restarts_succeeded: AtomicU64,
    pub restarts_failed: AtomicU64,
    pub listen_only_incidents: AtomicU64,
    pub stream_reconnects: AtomicU64,
    pub alerts_sent: AtomicU64,
    pub alerts_silenced_cooldown: AtomicU64,
    pub alerts_silenced_circuit_breaker: AtomicU64,
    pub per_container_restarts: RwLock<HashMap<String, u64>>,
    pub per_container_listen_incidents: RwLock<HashMap<String, u64>>,
}

impl MetricsRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn record_restart(&self, container_name: &str, success: bool) {
        if success {
            self.restarts_succeeded.fetch_add(1, Ordering::Relaxed);
            let mut map = self.per_container_restarts.write().await;
            *map.entry(container_name.to_string()).or_insert(0) += 1;
        } else {
            self.restarts_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub async fn record_listen_incident(&self, container_name: &str) {
        self.listen_only_incidents.fetch_add(1, Ordering::Relaxed);
        let mut map = self.per_container_listen_incidents.write().await;
        *map.entry(container_name.to_string()).or_insert(0) += 1;
    }

    pub fn record_reconnect(&self) {
        self.stream_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_alert_sent(&self) {
        self.alerts_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_alert_silenced(&self, reason: &str) {
        if reason == "circuit_breaker" {
            self.alerts_silenced_circuit_breaker.fetch_add(1, Ordering::Relaxed);
        } else {
            self.alerts_silenced_cooldown.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn render_prometheus(&self, docker_up: bool) -> String {
        let mut out = String::with_capacity(1024);

        out.push_str("# HELP autoheal_docker_connected Whether the Docker socket is reachable (1 = up, 0 = down).\n");
        out.push_str("# TYPE autoheal_docker_connected gauge\n");
        out.push_str(&format!("autoheal_docker_connected {}\n\n", if docker_up { 1 } else { 0 }));

        out.push_str("# HELP autoheal_restarts_total Total container restart attempts.\n");
        out.push_str("# TYPE autoheal_restarts_total counter\n");
        out.push_str(&format!(
            "autoheal_restarts_total{{status=\"success\"}} {}\n",
            self.restarts_succeeded.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "autoheal_restarts_total{{status=\"failed\"}} {}\n\n",
            self.restarts_failed.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP autoheal_listen_only_incidents_total Total incidents detected in listen-only mode.\n");
        out.push_str("# TYPE autoheal_listen_only_incidents_total counter\n");
        out.push_str(&format!(
            "autoheal_listen_only_incidents_total {}\n\n",
            self.listen_only_incidents.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP autoheal_stream_reconnects_total Total Docker event stream reconnections.\n");
        out.push_str("# TYPE autoheal_stream_reconnects_total counter\n");
        out.push_str(&format!(
            "autoheal_stream_reconnects_total {}\n\n",
            self.stream_reconnects.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP autoheal_alerts_sent_total Total notifications successfully sent.\n");
        out.push_str("# TYPE autoheal_alerts_sent_total counter\n");
        out.push_str(&format!(
            "autoheal_alerts_sent_total {}\n\n",
            self.alerts_sent.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP autoheal_alerts_silenced_total Total notifications silenced to prevent alert storms.\n");
        out.push_str("# TYPE autoheal_alerts_silenced_total counter\n");
        out.push_str(&format!(
            "autoheal_alerts_silenced_total{{reason=\"cooldown\"}} {}\n",
            self.alerts_silenced_cooldown.load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "autoheal_alerts_silenced_total{{reason=\"circuit_breaker\"}} {}\n\n",
            self.alerts_silenced_circuit_breaker.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP autoheal_container_restarts_total Restarts broken down by container name.\n");
        out.push_str("# TYPE autoheal_container_restarts_total counter\n");
        let map = self.per_container_restarts.read().await;
        for (name, count) in map.iter() {
            out.push_str(&format!(
                "autoheal_container_restarts_total{{container=\"{}\"}} {}\n",
                name.replace('"', "\\\""),
                count
            ));
        }

        let listen_map = self.per_container_listen_incidents.read().await;
        if !listen_map.is_empty() {
            out.push_str("\n# HELP autoheal_container_listen_incidents_total Incidents in listen-only mode broken down by container name.\n");
            out.push_str("# TYPE autoheal_container_listen_incidents_total counter\n");
            for (name, count) in listen_map.iter() {
                out.push_str(&format!(
                    "autoheal_container_listen_incidents_total{{container=\"{}\"}} {}\n",
                    name.replace('"', "\\\""),
                    count
                ));
            }
        }

        out
    }
}

pub async fn serve_http(addr: String, registry: Arc<MetricsRegistry>, docker: Docker) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            info!("Prometheus metrics & health server listening on http://{}", addr);
            l
        }
        Err(e) => {
            error!("Failed to bind metrics server on {}: {:?}", addr, e);
            return;
        }
    };

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            let reg = Arc::clone(&registry);
            let docker_cl = docker.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 512];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }

                let req = String::from_utf8_lossy(&buf[..n]);
                let docker_up = docker_cl.ping().await.is_ok();

                let response = if req.starts_with("GET /metrics") {
                    let body = reg.render_prometheus(docker_up).await;
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if req.starts_with("GET /healthz") {
                    if docker_up {
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK".to_string()
                    } else {
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 19\r\nConnection: close\r\n\r\nDocker Unreachable".to_string()
                    }
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\nConnection: close\r\n\r\nNot Found".to_string()
                };

                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    }
}

pub async fn run_dead_mans_switch(docker: Docker) {
    let Some(url) = env::var("HEARTBEAT_URL").ok().filter(|s| !s.is_empty()) else {
        return;
    };

    let interval_secs: u64 = env::var("HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    info!(
        "Dead-man's switch enabled (Interval: {}s -> {})",
        interval_secs, url
    );

    loop {
        match docker.ping().await {
            Ok(_) => {
                if let Err(e) = client.get(&url).send().await {
                    warn!("Failed to ping dead-man's switch URL: {:?}", e);
                }
            }
            Err(e) => {
                error!(
                    "Docker ping failed ({:?})! Skipping dead-man's switch heartbeat.",
                    e
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_recording_and_prometheus_rendering() {
        let registry = MetricsRegistry::new();
        registry.record_restart("redis-cache", true).await;
        registry.record_restart("redis-cache", true).await;
        registry.record_restart("api-gateway", false).await;
        registry.record_reconnect();
        registry.record_alert_sent();
        registry.record_alert_silenced("cooldown");
        registry.record_alert_silenced("circuit_breaker");
        registry.record_listen_incident("worker-queue").await;

        let prometheus = registry.render_prometheus(true).await;
        assert!(prometheus.contains("autoheal_docker_connected 1"));
        assert!(prometheus.contains("autoheal_restarts_total{status=\"success\"} 2"));
        assert!(prometheus.contains("autoheal_restarts_total{status=\"failed\"} 1"));
        assert!(prometheus.contains("autoheal_listen_only_incidents_total 1"));
        assert!(prometheus.contains("autoheal_stream_reconnects_total 1"));
        assert!(prometheus.contains("autoheal_alerts_sent_total 1"));
        assert!(prometheus.contains("autoheal_alerts_silenced_total{reason=\"cooldown\"} 1"));
        assert!(prometheus.contains("autoheal_alerts_silenced_total{reason=\"circuit_breaker\"} 1"));
        assert!(prometheus.contains("autoheal_container_restarts_total{container=\"redis-cache\"} 2"));
        assert!(prometheus.contains("autoheal_container_listen_incidents_total{container=\"worker-queue\"} 1"));

        let prometheus_down = registry.render_prometheus(false).await;
        assert!(prometheus_down.contains("autoheal_docker_connected 0"));
    }
}
