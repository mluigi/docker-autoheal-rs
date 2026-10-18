# docker-autoheal-rs 🦀

An ultra-lightweight, zero-polling Rust replacement for `willfarrell/docker-autoheal`.

- **Real-Time Event Streaming:** Directly subscribes to Docker Engine's `/events` API (`health_status: unhealthy` and `die` crash events) via `/var/run/docker.sock` without costly polling loops.
- **Crash & Unhealthy Healing:** Restarts unhealthy containers as well as crashed containers (non-zero exit codes), respecting per-container stop timeouts (`autoheal.stop.timeout`).
- **Restart Loop Circuit Breaker:** Features a sliding-window alert limiter (`MAX_ALERTS_PER_CONTAINER`, `ALERT_WINDOW_SECS`) and per-container debouncing (`ALERT_COOLDOWN_SECS`) to prevent notification storms when containers enter flapping/crash loops.
- **Multi-Channel Alerting:** Concurrently alerts via **SMTP Email** (compiled-in responsive HTML + plain text multipart via `askama` and `lettre`), **Slack**, **Discord**, and **Generic Webhooks**.
- **Observability:** Built-in Prometheus `/metrics` and `/healthz` endpoints served on port `9090`, plus an optional Dead-Man's Switch heartbeat task (`HEARTBEAT_URL`).
- **Minimal Scratch Container:** Compiles with `musl` into a pure static binary and runs in a `FROM scratch` image under **9 MB** with pure-Rust TLS (`rustls`).

---

## Configuration

Configure `docker-autoheal-rs` using environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `AUTOHEAL_CONTAINER_LABEL` | `autoheal` | Target container label filter (`autoheal=true` or `all` to watch all containers). |
| `AUTOHEAL_DEFAULT_STOP_TIMEOUT` | `10` | Default timeout in seconds before force killing a container during restart. |
| `AUTOHEAL_WATCH_CRASHES` | `true` | Watch for unexpected container exit/crash events (`die`). |
| `AUTOHEAL_EXCLUDE_EXIT_CODES` | `0` | Comma-separated exit codes to ignore (e.g., graceful stops). |
| `ALERT_COOLDOWN_SECS` | `300` | Minimum seconds between consecutive alerts for the same container. |
| `MAX_ALERTS_PER_CONTAINER` | `3` | Maximum alerts allowed within `ALERT_WINDOW_SECS` before the circuit breaker trips (`0` to disable). |
| `ALERT_WINDOW_SECS` | `1800` | Sliding window duration in seconds for alert counting (30 mins). |
| `METRICS_ADDR` | `0.0.0.0:9090` | Address for Prometheus metrics and health check (`disabled` to turn off). |
| `HEARTBEAT_URL` | *None* | Optional URL to ping when Docker Engine is healthy. |
| `HEARTBEAT_INTERVAL_SECS` | `60` | Frequency in seconds to ping `HEARTBEAT_URL`. |
| `SMTP_HOST` | *None* | SMTP server hostname (enables email alerts if set). |
| `SMTP_PORT` | `587` | SMTP server port. |
| `SMTP_TLS` | `true` | Use STARTTLS for SMTP connections. |
| `SMTP_USER` | *None* | SMTP username. |
| `SMTP_PASS` | *None* | SMTP password. |
| `ALERT_FROM` | `Autoheal <alerts@localhost>` | Email `From` address. |
| `ALERT_TO` | *None* | Email recipient address. |
| `SLACK_WEBHOOK_URL` | *None* | Slack incoming webhook URL. |
| `DISCORD_WEBHOOK_URL` | *None* | Discord webhook URL. |
| `GENERIC_WEBHOOK_URL` | *None* | Generic webhook URL (JSON payload). |

---

## Monitoring Modes & Container Labels

You control which containers are monitored using the `AUTOHEAL_CONTAINER_LABEL` **environment variable** on `docker-autoheal-rs`:

### Mode 1: Selective Opt-In (Default)
Set `AUTOHEAL_CONTAINER_LABEL=autoheal` (or leave as default). Only containers labeled with `autoheal=true` will be watched:
```yaml
# On the target container:
labels:
  - "autoheal=true"
  - "autoheal.stop.timeout=5"   # Optional: custom stop timeout in seconds
```

### Mode 2: Watch Everything (`AUTOHEAL_CONTAINER_LABEL=all`)
Set `AUTOHEAL_CONTAINER_LABEL=all` in `docker-autoheal-rs` environment:
```yaml
# In docker-autoheal-rs:
environment:
  - AUTOHEAL_CONTAINER_LABEL=all
```
- In this mode, **all containers** on the Docker daemon are monitored automatically — **no labels required** on target containers.
- **Opt-Out:** If you want to exclude a specific container from auto-healing while in `all` mode, add `autoheal=false`:
  ```yaml
  # On the excluded container:
  labels:
    - "autoheal=false"
  ```

---

## Quick Start (Production)

```yaml
services:
  autoheal-rs:
    image: docker-autoheal-rs:latest
    build: .
    restart: unless-stopped
    ports:
      - "9090:9090"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    environment:
      - AUTOHEAL_CONTAINER_LABEL=autoheal
      - ALERT_COOLDOWN_SECS=300
      - MAX_ALERTS_PER_CONTAINER=3
      - ALERT_WINDOW_SECS=1800
      - SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

---

## End-to-End Testing

Spin up the self-contained test environment with Mailpit, an unhealthy container, and a crashing container:

```bash
# Build and run the test stack
docker compose -f docker-compose.test.yml up -d

# Inspect autoheal logs
docker compose -f docker-compose.test.yml logs autoheal-rs

# Inspect Prometheus metrics
curl http://localhost:9090/metrics

# Check caught alert emails via Mailpit web UI
open http://localhost:8025

# Tear down test stack
docker compose -f docker-compose.test.yml down
```
