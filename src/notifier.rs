use askama::Template;
use lettre::message::{header::ContentType, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use reqwest::Client;
use serde_json::json;
use std::env;
use tracing::{error, info};

#[derive(Template, Clone)]
#[template(path = "alert.html")]
pub struct AlertPayload {
    pub alert_title: String,
    pub container_name: String,
    pub container_id: String,
    pub image: String,
    pub status: String,
    pub action_result: String,
    pub last_health_log: String,
    pub timestamp: String,
}

#[derive(Clone)]
pub struct SmtpConfig {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
    to: String,
}

#[derive(Clone)]
pub struct NotificationHub {
    http: Client,
    smtp: Option<SmtpConfig>,
    slack_webhook: Option<String>,
    discord_webhook: Option<String>,
    generic_webhook: Option<String>,
}

impl NotificationHub {
    pub fn from_env() -> Self {
        let smtp = Self::init_smtp();
        let slack_webhook = env::var("SLACK_WEBHOOK_URL").ok().filter(|s| !s.is_empty());
        let discord_webhook = env::var("DISCORD_WEBHOOK_URL").ok().filter(|s| !s.is_empty());
        let generic_webhook = env::var("GENERIC_WEBHOOK_URL").ok().filter(|s| !s.is_empty());

        Self {
            http: Client::new(),
            smtp,
            slack_webhook,
            discord_webhook,
            generic_webhook,
        }
    }

    fn init_smtp() -> Option<SmtpConfig> {
        let host = env::var("SMTP_HOST").ok().filter(|s| !s.is_empty())?;
        let port: u16 = env::var("SMTP_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(587);
        let user = env::var("SMTP_USER").unwrap_or_default();
        let pass = env::var("SMTP_PASS").unwrap_or_default();
        let tls_enabled = env::var("SMTP_TLS").map(|v| v != "false").unwrap_or(true);
        let from = env::var("ALERT_FROM").unwrap_or_else(|_| "autoheal@localhost".into());
        let to = env::var("ALERT_TO").ok()?;

        let mut builder = if tls_enabled {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host).ok()?.port(port)
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host).port(port)
        };

        if !user.is_empty() {
            builder = builder.credentials(Credentials::new(user, pass));
        }

        Some(SmtpConfig {
            transport: builder.build(),
            from,
            to,
        })
    }

    pub async fn notify_all(&self, payload: AlertPayload) {
        tokio::join!(
            self.send_email(&payload),
            self.send_slack(&payload),
            self.send_discord(&payload),
            self.send_generic_webhook(&payload)
        );
    }

    async fn send_email(&self, payload: &AlertPayload) {
        let Some(smtp) = &self.smtp else { return };

        let html_body = match payload.render() {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to render HTML template: {:?}", e);
                return;
            }
        };

        let plain_body = format!(
            "{}\nContainer: {}\nID: {}\nImage: {}\nStatus: {}\nAction: {}\nTime: {}\n\nLogs/Output:\n{}",
            payload.alert_title,
            payload.container_name,
            payload.container_id,
            payload.image,
            payload.status,
            payload.action_result,
            payload.timestamp,
            payload.last_health_log
        );

        let email = Message::builder()
            .from(smtp.from.parse().unwrap())
            .to(smtp.to.parse().unwrap())
            .subject(format!("[Autoheal] {} is {}", payload.container_name, payload.status))
            .multipart(
                MultiPart::alternative()
                    .singlepart(
                        SinglePart::builder()
                            .header(ContentType::TEXT_PLAIN)
                            .body(plain_body),
                    )
                    .singlepart(
                        SinglePart::builder()
                            .header(ContentType::TEXT_HTML)
                            .body(html_body),
                    ),
            );

        match email {
            Ok(msg) => match smtp.transport.send(msg).await {
                Ok(_) => info!("Email alert sent for {}", payload.container_name),
                Err(e) => error!("SMTP send error: {:?}", e),
            },
            Err(e) => error!("Email builder error: {:?}", e),
        }
    }

    async fn send_slack(&self, p: &AlertPayload) {
        let Some(url) = &self.slack_webhook else { return };
        let body = json!({
            "text": format!("⚠️ *Autoheal Alert*: `{}` is *{}*", p.container_name, p.status),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!(
                            "*{}\n• *Name:* `{}` (`{}`)\n• *Image:* `{}`\n• *Status:* `{}`\n• *Action:* {}\n• *Output:* ```{}```",
                            p.alert_title, p.container_name, p.container_id, p.image, p.status, p.action_result,
                            if p.last_health_log.is_empty() { "N/A" } else { &p.last_health_log }
                        )
                    }
                }
            ]
        });

        if let Err(e) = self.http.post(url).json(&body).send().await {
            error!("Slack webhook failed: {:?}", e);
        }
    }

    async fn send_discord(&self, p: &AlertPayload) {
        let Some(url) = &self.discord_webhook else { return };
        let body = json!({
            "embeds": [{
                "title": format!("{}: {}", p.alert_title, p.container_name),
                "color": 15023678,
                "fields": [
                    { "name": "Container ID", "value": format!("`{}`", p.container_id), "inline": true },
                    { "name": "Image", "value": format!("`{}`", p.image), "inline": true },
                    { "name": "Status", "value": format!("`{}`", p.status), "inline": true },
                    { "name": "Action", "value": &p.action_result, "inline": false },
                    { "name": "Diagnostic Logs", "value": format!("```{}```", if p.last_health_log.is_empty() { "None" } else { &p.last_health_log }), "inline": false }
                ],
                "footer": { "text": format!("docker-autoheal-rs • {}", p.timestamp) }
            }]
        });

        if let Err(e) = self.http.post(url).json(&body).send().await {
            error!("Discord webhook failed: {:?}", e);
        }
    }

    async fn send_generic_webhook(&self, p: &AlertPayload) {
        let Some(url) = &self.generic_webhook else { return };
        let body = json!({
            "event": "container_autoheal",
            "alert_title": p.alert_title,
            "container_name": p.container_name,
            "container_id": p.container_id,
            "image": p.image,
            "status": p.status,
            "action_result": p.action_result,
            "last_health_log": p.last_health_log,
            "timestamp": p.timestamp,
        });

        if let Err(e) = self.http.post(url).json(&body).send().await {
            error!("Generic webhook failed: {:?}", e);
        }
    }
}
