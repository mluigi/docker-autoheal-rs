mod metrics;
mod notifier;

use anyhow::{Context, Result};
use bollard::container::{ListContainersOptions, LogsOptions, RestartContainerOptions};
use bollard::system::EventsOptions;
use bollard::Docker;
use chrono::Utc;
use futures_util::StreamExt;
use metrics::MetricsRegistry;
use notifier::{AlertPayload, NotificationHub};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

struct AppConfig {
    target_label: String,
    restart_timeout: isize,
    cooldown: Duration,
    watch_crashes: bool,
    exclude_exit_codes: Vec<i64>,
    max_alerts: usize,
    alert_window: Duration,
}

#[derive(Default)]
pub struct ContainerAlertState {
    pub alert_timestamps: Vec<Instant>,
    pub circuit_breaker_active: bool,
}

pub enum AlertDecision {
    SendNormal,
    SendCircuitBreakerNotice { count: usize, window_secs: u64 },
    SuppressedCooldown,
    SuppressedCircuitBreaker,
}

fn evaluate_alert(
    state: &mut ContainerAlertState,
    cooldown: Duration,
    max_alerts: usize,
    window: Duration,
    now: Instant,
) -> AlertDecision {
    // 1. Prune timestamps older than the sliding window
    state
        .alert_timestamps
        .retain(|&t| now.duration_since(t) < window);

    // 2. Check if circuit breaker limit reached
    if max_alerts > 0 && state.alert_timestamps.len() >= max_alerts {
        if !state.circuit_breaker_active {
            state.circuit_breaker_active = true;
            return AlertDecision::SendCircuitBreakerNotice {
                count: state.alert_timestamps.len(),
                window_secs: window.as_secs(),
            };
        } else {
            return AlertDecision::SuppressedCircuitBreaker;
        }
    }

    state.circuit_breaker_active = false;

    // 3. Check minimum cooldown gap against last sent alert
    if let Some(&last_sent) = state.alert_timestamps.last() {
        if now.duration_since(last_sent) < cooldown {
            return AlertDecision::SuppressedCooldown;
        }
    }

    // 4. Allowed to send alert
    state.alert_timestamps.push(now);
    AlertDecision::SendNormal
}

#[derive(Clone, Debug)]
pub enum HealReason {
    UnhealthyHealthcheck,
    CrashOrExit { exit_code: i64 },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let exclude_exit_codes: Vec<i64> = env::var("AUTOHEAL_EXCLUDE_EXIT_CODES")
        .unwrap_or_else(|_| "0".into())
        .split(',')
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .collect();

    let config = Arc::new(AppConfig {
        target_label: env::var("AUTOHEAL_CONTAINER_LABEL").unwrap_or_else(|_| "autoheal".into()),
        restart_timeout: env::var("AUTOHEAL_DEFAULT_STOP_TIMEOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10),
        cooldown: Duration::from_secs(
            env::var("ALERT_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
        ),
        watch_crashes: env::var("AUTOHEAL_WATCH_CRASHES")
            .map(|v| v != "false")
            .unwrap_or(true),
        exclude_exit_codes,
        max_alerts: env::var("MAX_ALERTS_PER_CONTAINER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3),
        alert_window: Duration::from_secs(
            env::var("ALERT_WINDOW_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800),
        ),
    });

    let docker = Docker::connect_with_socket_defaults()
        .context("Failed to connect to /var/run/docker.sock")?;
    let notifier = Arc::new(NotificationHub::from_env());
    let metrics = MetricsRegistry::new();
    let alert_tracker: Arc<Mutex<HashMap<String, ContainerAlertState>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let in_flight_restarts: Arc<Mutex<HashMap<String, Instant>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let metrics_addr = env::var("METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".into());
    if !metrics_addr.is_empty() && metrics_addr != "disabled" {
        tokio::spawn(metrics::serve_http(
            metrics_addr,
            Arc::clone(&metrics),
            docker.clone(),
        ));
    }

    tokio::spawn(metrics::run_dead_mans_switch(docker.clone()));

    info!(
        "Starting docker-autoheal-rs (Label filter: '{}', Watch crashes: {}, Max alerts: {}/{}s, Cooldown: {}s)",
        config.target_label,
        config.watch_crashes,
        config.max_alerts,
        config.alert_window.as_secs(),
        config.cooldown.as_secs()
    );

    if let Err(e) = initial_sweep(
        &docker,
        &config,
        &notifier,
        &metrics,
        &alert_tracker,
        &in_flight_restarts,
    )
    .await
    {
        warn!("Initial sweep encountered an error: {:?}", e);
    }

    loop {
        let mut filters = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);

        let mut event_types = vec!["health_status: unhealthy".to_string()];
        if config.watch_crashes {
            event_types.push("die".to_string());
        }
        filters.insert("event".to_string(), event_types);

        if config.target_label != "all" {
            filters.insert(
                "label".to_string(),
                vec![format!("{}=true", config.target_label)],
            );
        }

        let mut event_stream = docker.events(Some(EventsOptions {
            filters,
            ..Default::default()
        }));

        info!("Subscribed to Docker Engine real-time health and crash events.");

        while let Some(event_result) = event_stream.next().await {
            match event_result {
                Ok(event) => {
                    let action = event.action.as_deref().unwrap_or("");
                    let actor = match event.actor {
                        Some(a) => a,
                        None => continue,
                    };
                    let id = match actor.id {
                        Some(i) => i,
                        None => continue,
                    };

                    let reason = if action == "health_status: unhealthy" {
                        HealReason::UnhealthyHealthcheck
                    } else if action == "die" {
                        // Check if this container was recently restarted by autoheal itself
                        {
                            let in_flight = in_flight_restarts.lock().await;
                            if let Some(&ts) = in_flight.get(&id) {
                                if ts.elapsed() < Duration::from_secs(15) {
                                    info!(
                                        "Ignoring 'die' event for {} caused by active autoheal restart.",
                                        id
                                    );
                                    continue;
                                }
                            }
                        }

                        let exit_code = actor
                            .attributes
                            .as_ref()
                            .and_then(|a| a.get("exitCode"))
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(0);

                        if config.exclude_exit_codes.contains(&exit_code) {
                            continue;
                        }

                        HealReason::CrashOrExit { exit_code }
                    } else {
                        continue;
                    };

                    let docker_cl = docker.clone();
                    let cfg_cl = Arc::clone(&config);
                    let notif_cl = Arc::clone(&notifier);
                    let met_cl = Arc::clone(&metrics);
                    let alert_cl = Arc::clone(&alert_tracker);
                    let in_flight_cl = Arc::clone(&in_flight_restarts);

                    tokio::spawn(async move {
                        if let Err(e) = heal_container(
                            &docker_cl,
                            &id,
                            reason,
                            &cfg_cl,
                            &notif_cl,
                            &met_cl,
                            &alert_cl,
                            &in_flight_cl,
                        )
                        .await
                        {
                            error!("Failed healing container {}: {:?}", id, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Docker event stream error: {:?}. Reconnecting in 5s...", e);
                    metrics.record_reconnect();
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn initial_sweep(
    docker: &Docker,
    config: &Arc<AppConfig>,
    notifier: &Arc<NotificationHub>,
    metrics: &Arc<MetricsRegistry>,
    alert_tracker: &Arc<Mutex<HashMap<String, ContainerAlertState>>>,
    in_flight_restarts: &Arc<Mutex<HashMap<String, Instant>>>,
) -> Result<()> {
    // Sweep 1: Check unhealthy containers
    let mut health_filters = HashMap::new();
    health_filters.insert("health".to_string(), vec!["unhealthy".to_string()]);
    if config.target_label != "all" {
        health_filters.insert(
            "label".to_string(),
            vec![format!("{}=true", config.target_label)],
        );
    }

    if let Ok(containers) = docker
        .list_containers(Some(ListContainersOptions {
            all: false,
            filters: health_filters,
            ..Default::default()
        }))
        .await
    {
        for c in containers {
            if let Some(id) = c.id {
                let _ = heal_container(
                    docker,
                    &id,
                    HealReason::UnhealthyHealthcheck,
                    config,
                    notifier,
                    metrics,
                    alert_tracker,
                    in_flight_restarts,
                )
                .await;
            }
        }
    }

    // Sweep 2: Check stopped/exited containers with non-zero exit code
    if config.watch_crashes {
        let mut exit_filters = HashMap::new();
        exit_filters.insert(
            "status".to_string(),
            vec!["exited".to_string(), "dead".to_string()],
        );
        if config.target_label != "all" {
            exit_filters.insert(
                "label".to_string(),
                vec![format!("{}=true", config.target_label)],
            );
        }

        if let Ok(containers) = docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: exit_filters,
                ..Default::default()
            }))
            .await
        {
            for c in containers {
                if let Some(id) = c.id {
                    if let Ok(inspect) = docker.inspect_container(&id, None).await {
                        let exit_code = inspect
                            .state
                            .as_ref()
                            .and_then(|s| s.exit_code)
                            .unwrap_or(0);
                        if !config.exclude_exit_codes.contains(&exit_code) {
                            let _ = heal_container(
                                docker,
                                &id,
                                HealReason::CrashOrExit { exit_code },
                                config,
                                notifier,
                                metrics,
                                alert_tracker,
                                in_flight_restarts,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn get_container_logs(docker: &Docker, container_id: &str, tail: usize) -> String {
    let mut stream = docker.logs(
        container_id,
        Some(LogsOptions::<String> {
            stdout: true,
            stderr: true,
            tail: tail.to_string(),
            ..Default::default()
        }),
    );

    let mut lines = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(500), stream.next()).await
    {
        if let Ok(log) = msg {
            lines.push(log.to_string());
        }
    }
    let combined = lines.join("");
    combined.trim().chars().take(800).collect()
}

#[allow(clippy::too_many_arguments)]
async fn heal_container(
    docker: &Docker,
    container_id: &str,
    reason: HealReason,
    config: &AppConfig,
    notifier: &NotificationHub,
    metrics: &MetricsRegistry,
    alert_tracker: &Mutex<HashMap<String, ContainerAlertState>>,
    in_flight_restarts: &Mutex<HashMap<String, Instant>>,
) -> Result<()> {
    let inspect = docker.inspect_container(container_id, None).await?;
    let name = inspect
        .name
        .unwrap_or_else(|| container_id.to_string())
        .trim_start_matches('/')
        .to_string();

    let image = inspect
        .config
        .as_ref()
        .and_then(|c| c.image.clone())
        .unwrap_or_else(|| "unknown".into());

    let last_health_log = inspect
        .state
        .as_ref()
        .and_then(|s| s.health.as_ref())
        .and_then(|h| h.log.as_ref())
        .and_then(|logs| logs.last().cloned())
        .and_then(|entry| entry.output)
        .map(|out| out.trim().chars().take(500).collect::<String>())
        .unwrap_or_default();

    let diagnostic_logs = if !last_health_log.is_empty() {
        last_health_log
    } else {
        get_container_logs(docker, container_id, 10).await
    };

    let timeout = inspect
        .config
        .as_ref()
        .and_then(|c| c.labels.as_ref())
        .and_then(|l| l.get("autoheal.stop.timeout").cloned())
        .and_then(|t| t.parse::<isize>().ok())
        .unwrap_or(config.restart_timeout);

    // Check for explicit opt-out label (useful when AUTOHEAL_CONTAINER_LABEL=all)
    if let Some(labels) = inspect.config.as_ref().and_then(|c| c.labels.as_ref()) {
        if labels.get("autoheal").map(|s| s.as_str()) == Some("false")
            || (config.target_label != "all"
                && labels.get(&config.target_label).map(|s| s.as_str()) == Some("false"))
        {
            info!(
                "Container '{}' ({}) has explicit opt-out label; skipping heal.",
                name,
                &container_id[..12.min(container_id.len())]
            );
            return Ok(());
        }
    }

    let (status_str, reason_desc) = match &reason {
        HealReason::UnhealthyHealthcheck => (
            "UNHEALTHY".to_string(),
            "Healthcheck reported unhealthy".to_string(),
        ),
        HealReason::CrashOrExit { exit_code } => (
            format!("CRASHED (Exit Code {})", exit_code),
            format!("Container exited unexpectedly with code {}", exit_code),
        ),
    };

    warn!(
        "Container '{}' ({}) is {}. Restarting (timeout: {}s)...",
        name,
        &container_id[..12.min(container_id.len())],
        status_str,
        timeout
    );

    // Register in-flight restart to ignore the Docker 'die' event emitted during restart
    in_flight_restarts
        .lock()
        .await
        .insert(container_id.to_string(), Instant::now());

    let restart_res = docker
        .restart_container(container_id, Some(RestartContainerOptions { t: timeout }))
        .await;

    let action_result = match &restart_res {
        Ok(_) => {
            info!("Container '{}' restarted successfully.", name);
            metrics.record_restart(&name, true).await;
            format!(
                "Successfully restarted (reason: {}, stop timeout: {}s)",
                reason_desc, timeout
            )
        }
        Err(e) => {
            error!("Container '{}' failed to restart: {:?}", name, e);
            metrics.record_restart(&name, false).await;
            format!("Restart failed: {}", e)
        }
    };

    let decision = {
        let mut map = alert_tracker.lock().await;
        let entry = map.entry(container_id.to_string()).or_default();
        evaluate_alert(
            entry,
            config.cooldown,
            config.max_alerts,
            config.alert_window,
            Instant::now(),
        )
    };

    match decision {
        AlertDecision::SendNormal => {
            let alert_title = match &reason {
                HealReason::UnhealthyHealthcheck => {
                    "⚠️ Container Unhealthy — Restart Triggered".to_string()
                }
                HealReason::CrashOrExit { exit_code } => format!(
                    "💥 Container Crashed (Code {}) — Restart Triggered",
                    exit_code
                ),
            };

            let payload = AlertPayload {
                alert_title,
                container_name: name.clone(),
                container_id: container_id.chars().take(12).collect(),
                image,
                status: status_str,
                action_result,
                last_health_log: diagnostic_logs,
                timestamp: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            };
            metrics.record_alert_sent();
            notifier.notify_all(payload).await;
        }
        AlertDecision::SendCircuitBreakerNotice { count, window_secs } => {
            warn!(
                "Container '{}' exceeded max alerts ({}/{}s). Tripping circuit breaker and sending notice.",
                name, count, window_secs
            );
            metrics.record_alert_sent();
            metrics.record_alert_silenced("circuit_breaker");

            let payload = AlertPayload {
                alert_title: "🛑 Restart Loop Detected — Alert Limit Reached (Silenced)".to_string(),
                container_name: name.clone(),
                container_id: container_id.chars().take(12).collect(),
                image,
                status: "CIRCUIT_BREAKER_ACTIVE".to_string(),
                action_result: format!(
                    "{}. WARNING: Container has failed {} times in {}s. Further alerts are silenced until container stabilizes.",
                    action_result, count, window_secs
                ),
                last_health_log: diagnostic_logs,
                timestamp: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            };
            notifier.notify_all(payload).await;
        }
        AlertDecision::SuppressedCooldown => {
            metrics.record_alert_silenced("cooldown");
            info!(
                "Suppressed notification for {} due to active cooldown.",
                name
            );
        }
        AlertDecision::SuppressedCircuitBreaker => {
            metrics.record_alert_silenced("circuit_breaker");
            warn!(
                "Suppressed notification for {} due to circuit breaker (max alerts reached).",
                name
            );
        }
    }

    restart_res?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_decision_normal_and_cooldown() {
        let mut state = ContainerAlertState::default();
        let cooldown = Duration::from_secs(10);
        let max_alerts = 3;
        let window = Duration::from_secs(60);
        let t0 = Instant::now();

        // 1. Initial alert sends normal
        let d1 = evaluate_alert(&mut state, cooldown, max_alerts, window, t0);
        assert!(matches!(d1, AlertDecision::SendNormal));
        assert_eq!(state.alert_timestamps.len(), 1);

        // 2. Alert before cooldown expires is suppressed
        let t1 = t0 + Duration::from_secs(5);
        let d2 = evaluate_alert(&mut state, cooldown, max_alerts, window, t1);
        assert!(matches!(d2, AlertDecision::SuppressedCooldown));
        assert_eq!(state.alert_timestamps.len(), 1);

        // 3. Alert after cooldown succeeds
        let t2 = t0 + Duration::from_secs(12);
        let d3 = evaluate_alert(&mut state, cooldown, max_alerts, window, t2);
        assert!(matches!(d3, AlertDecision::SendNormal));
        assert_eq!(state.alert_timestamps.len(), 2);
    }

    #[test]
    fn test_circuit_breaker_tripping_and_reset() {
        let mut state = ContainerAlertState::default();
        let cooldown = Duration::from_secs(5);
        let max_alerts = 2;
        let window = Duration::from_secs(30);
        let t0 = Instant::now();

        // Alert 1
        assert!(matches!(
            evaluate_alert(&mut state, cooldown, max_alerts, window, t0),
            AlertDecision::SendNormal
        ));

        // Alert 2
        let t1 = t0 + Duration::from_secs(6);
        assert!(matches!(
            evaluate_alert(&mut state, cooldown, max_alerts, window, t1),
            AlertDecision::SendNormal
        ));
        assert_eq!(state.alert_timestamps.len(), 2);

        // Alert 3: hits max_alerts within window -> trips circuit breaker notice
        let t2 = t0 + Duration::from_secs(12);
        let d3 = evaluate_alert(&mut state, cooldown, max_alerts, window, t2);
        match d3 {
            AlertDecision::SendCircuitBreakerNotice { count, window_secs } => {
                assert_eq!(count, 2);
                assert_eq!(window_secs, 30);
            }
            _ => panic!("Expected circuit breaker notice"),
        }
        assert!(state.circuit_breaker_active);

        // Alert 4: while tripped, alerts are suppressed by circuit breaker
        let t3 = t0 + Duration::from_secs(18);
        assert!(matches!(
            evaluate_alert(&mut state, cooldown, max_alerts, window, t3),
            AlertDecision::SuppressedCircuitBreaker
        ));

        // Alert 5: after window passes (>30s from t0 and t1), timestamps are pruned
        let t4 = t0 + Duration::from_secs(38);
        assert!(matches!(
            evaluate_alert(&mut state, cooldown, max_alerts, window, t4),
            AlertDecision::SendNormal
        ));
        assert!(!state.circuit_breaker_active);
    }

    #[test]
    fn test_circuit_breaker_disabled_when_zero() {
        let mut state = ContainerAlertState::default();
        let cooldown = Duration::from_secs(2);
        let max_alerts = 0; // disabled
        let window = Duration::from_secs(60);
        let t0 = Instant::now();

        for i in 0..5 {
            let t = t0 + Duration::from_secs(i * 3);
            let d = evaluate_alert(&mut state, cooldown, max_alerts, window, t);
            assert!(matches!(d, AlertDecision::SendNormal));
        }
    }
}
