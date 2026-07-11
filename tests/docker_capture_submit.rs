//! Opt-in command/reply smoke test for the checked-in Docker Compose deployment.
//!
//! Run this only against `deploy/docker/compose.yaml` after it reports healthy:
//! `CAMERA_ADAPTER_DOCKER_E2E=1 CAMERA_ADAPTER_DOCKER_E2E_HOST=broker cargo test --test docker_capture_submit`.

#![cfg(feature = "standalone")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use camera_adapter::COMPONENT_TOKEN;
use edgecommons::messaging::config::MessagingConfig;
use edgecommons::messaging::message::{Message, MessageBuilder};
use edgecommons::messaging::message_handler;
use edgecommons::messaging::provider::mqtt::MqttProvider;
use edgecommons::messaging::service::{DefaultMessagingService, MessagingService};
use serde_json::json;
use uuid::Uuid;

fn enabled() -> bool {
    std::env::var_os("CAMERA_ADAPTER_DOCKER_E2E").is_some()
}

fn messaging_config(client_id: &str) -> MessagingConfig {
    let host =
        std::env::var("CAMERA_ADAPTER_DOCKER_E2E_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("CAMERA_ADAPTER_DOCKER_E2E_PORT")
        .unwrap_or_else(|_| "1883".to_owned())
        .parse::<u16>()
        .expect("CAMERA_ADAPTER_DOCKER_E2E_PORT must be a valid TCP port");
    serde_json::from_value(json!({
        "messaging": {
            "local": { "host": host, "port": port, "clientId": client_id }
        }
    }))
    .expect("valid MQTT test-client configuration")
}

#[tokio::test]
async fn compose_capture_submit_returns_a_correlated_acceptance() {
    if !enabled() {
        eprintln!("skipping Docker capture smoke test (set CAMERA_ADAPTER_DOCKER_E2E=1)");
        return;
    }

    let device = "NOT_GREENGRASS";
    let reply_topic = format!("camera-adapter-e2e/reply/{}", Uuid::now_v7());
    let command_topic = format!("ecv1/{device}/{COMPONENT_TOKEN}/main/cmd/sb/capture-submit");
    let request_id = format!("docker-e2e-{}", Uuid::now_v7());
    let received = Arc::new(Mutex::new(Vec::<Message>::new()));
    let observer = Arc::new(
        MqttProvider::connect(&messaging_config(&format!(
            "camera-adapter-e2e-{}",
            Uuid::now_v7()
        )))
        .await
        .expect("connect test MQTT client"),
    );
    let service = Arc::new(DefaultMessagingService::new(observer));
    let received_handler = Arc::clone(&received);
    service
        .subscribe(
            &reply_topic,
            message_handler(move |_topic, message| {
                let received = Arc::clone(&received_handler);
                async move {
                    if let Ok(mut guard) = received.lock() {
                        guard.push(message);
                    }
                }
            }),
            8,
            1,
        )
        .await
        .expect("subscribe to command reply");
    tokio::time::sleep(Duration::from_millis(250)).await;

    let request = MessageBuilder::new("sb/capture-submit", "1.0")
        .reply_to(&reply_topic)
        .payload(json!({
            "instance": "onvif-simulator",
            "requestId": request_id,
            "captureProfile": "inspection"
        }))
        .build();
    let correlation_id = request.header.correlation_id.clone();
    service
        .publish(&command_topic, &request)
        .await
        .expect("publish capture-submit command");

    let reply = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(reply) = received
                .lock()
                .ok()
                .and_then(|guard| guard.first().cloned())
            {
                return reply;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("capture-submit reply timeout");

    assert_eq!(reply.header.name, "sb/capture-submit");
    assert_eq!(reply.header.correlation_id, correlation_id);
    assert_eq!(reply.body["ok"], true);
    assert!(reply.body["result"]["captureId"].as_str().is_some());
    assert!(
        matches!(
            reply.body["result"]["state"].as_str(),
            Some("ACCEPTED" | "QUEUED")
        ),
        "capture-submit must return a newly accepted job before terminal completion: {}",
        reply.body
    );
    service
        .unsubscribe(&reply_topic)
        .await
        .expect("unsubscribe reply topic");
}
