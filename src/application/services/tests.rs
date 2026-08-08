use std::path::Path;
use std::time::Duration;

use zero_api::{ApiErrorCode, CommandRequest, CommandService, ConfigApplyCommand};
use zero_config::RuntimeConfig;
use zero_engine::{Engine, EngineHandle};
use zero_proxy::{Proxy, ProxyHandle};

use super::ApplicationServices;

fn config_json(sink_path: &str, port: u16) -> String {
    serde_json::json!({
        "inbounds": [{
            "tag": "managed",
            "listen": { "address": "127.0.0.1", "port": port },
            "protocol": { "type": "direct" }
        }],
        "route": {
            "rules": [],
            "final": { "type": "direct" }
        },
        "api": {
            "event_sinks": [{
                "type": "jsonl",
                "tag": "audit",
                "path": sink_path,
                "events": ["engine.warning"]
            }]
        }
    })
    .to_string()
}

async fn wait_for_file_text(path: &Path, needle: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if std::fs::read_to_string(path)
                .map(|content| content.contains(needle))
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("event sink did not receive expected record");
}

fn count_event_type(path: &Path, event_type: &str) -> usize {
    let needle = format!(r#""event_type":"{event_type}""#);
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .matches(&needle)
        .count()
}

#[tokio::test]
async fn first_configured_dispatcher_delivers_engine_started_exactly_once() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config_path = directory.path().join("config.json");
    let first_sink = directory.path().join("first-events.jsonl");
    let replacement_sink = directory.path().join("replacement-events.jsonl");
    let mut initial: serde_json::Value =
        serde_json::from_str(&config_json("first-events.jsonl", port)).unwrap();
    initial["api"]["event_sinks"][0]["events"] =
        serde_json::json!(["engine.started", "engine.warning"]);
    std::fs::write(&config_path, serde_json::to_vec_pretty(&initial).unwrap()).unwrap();

    let initial = RuntimeConfig::load_from_path(&config_path).unwrap();
    let engine = Engine::new_with_config_path(initial, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let base_handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    let handle = base_handle.with_config_apply_reconciler(services.clone());
    let running = proxy.spawn();

    wait_for_file_text(&first_sink, "engine.started").await;
    assert_eq!(count_event_type(&first_sink, "engine.started"), 1);

    let mut candidate: serde_json::Value =
        serde_json::from_str(&config_json("replacement-events.jsonl", port)).unwrap();
    candidate["api"]["event_sinks"][0]["events"] =
        serde_json::json!(["engine.started", "engine.warning"]);
    handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: candidate,
        }))
        .await
        .expect("replace event dispatcher");

    engine.emit_warning("replacement-ready", "replacement-ready");
    wait_for_file_text(&replacement_sink, "replacement-ready").await;
    assert_eq!(count_event_type(&first_sink, "engine.started"), 1);
    assert_eq!(count_event_type(&replacement_sink, "engine.started"), 0);

    running.shutdown().await.unwrap();
    services.shutdown_status_monitor().await;
    engine.push_engine_stopped("test");
    tokio::task::yield_now().await;
    services.shutdown_dispatcher().await;
}

#[tokio::test]
async fn config_apply_rebuilds_event_dispatcher_without_process_restart() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config_path = directory.path().join("config.json");
    let old_sink = directory.path().join("old-events.jsonl");
    let new_sink = directory.path().join("new-events.jsonl");
    std::fs::write(&config_path, config_json("old-events.jsonl", port)).unwrap();

    let initial = RuntimeConfig::load_from_path(&config_path).unwrap();
    let engine = Engine::new_with_config_path(initial, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let base_handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    let handle = base_handle.with_config_apply_reconciler(services.clone());
    let running = proxy.spawn();

    engine.emit_warning("before-rebuild", "before-rebuild");
    wait_for_file_text(&old_sink, "before-rebuild").await;

    let candidate: serde_json::Value =
        serde_json::from_str(&config_json("new-events.jsonl", port)).unwrap();
    let response = handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: candidate,
        }))
        .await
        .expect("hot rebuild config.apply");
    assert_eq!(
        response.result.as_ref().unwrap()["application_components"],
        serde_json::json!(["event-dispatcher"])
    );

    engine.emit_warning("after-rebuild", "after-rebuild");
    wait_for_file_text(&new_sink, "after-rebuild").await;
    assert!(
        !std::fs::read_to_string(&old_sink)
            .unwrap()
            .contains("after-rebuild"),
        "replaced dispatcher continued writing the old sink"
    );

    running.shutdown().await.unwrap();
    services.shutdown_status_monitor().await;
    engine.push_engine_stopped("test");
    tokio::task::yield_now().await;
    services.shutdown_dispatcher().await;
}

#[tokio::test]
async fn config_apply_rejects_live_control_listener_or_credential_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let control_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let control_port = control_listener.local_addr().unwrap().port();
    drop(control_listener);
    let config_path = directory.path().join("config.json");
    std::fs::write(&config_path, config_json("events.jsonl", port)).unwrap();

    let initial = RuntimeConfig::load_from_path(&config_path).unwrap();
    let engine = Engine::new_with_config_path(initial, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let base_handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    let handle = base_handle.with_config_apply_reconciler(services.clone());
    let running = proxy.spawn();

    let mut candidate: serde_json::Value =
        serde_json::from_str(&config_json("events.jsonl", port)).unwrap();
    candidate["api"]["control"] = serde_json::json!({
        "enabled": true,
        "listen": { "address": "127.0.0.1", "port": control_port },
        "api_key": "replacement-key"
    });
    let error = handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: candidate,
        }))
        .await
        .expect_err("live control endpoint replacement must be rejected");

    assert_eq!(error.code, ApiErrorCode::InvalidArgument);
    assert!(error.message.contains("`api.control` cannot be changed"));
    assert!(!engine.config().api.control.enabled);
    assert!(
        !RuntimeConfig::load_from_path(&config_path)
            .unwrap()
            .api
            .control
            .enabled,
        "rejected control endpoint replacement reached the config file"
    );

    running.shutdown().await.unwrap();
    services.shutdown_status_monitor().await;
    engine.push_engine_stopped("test");
    tokio::task::yield_now().await;
    services.shutdown_dispatcher().await;
}

#[cfg(feature = "connector")]
#[tokio::test]
async fn config_apply_registers_complete_webhook_url_in_process() {
    use std::io::{Read, Write};

    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let receiver = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let receiver_address = receiver.local_addr().unwrap();
    let request = std::thread::spawn(move || {
        let (mut stream, _) = receiver.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 2048];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let header_end = header_end + 4;
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let content_length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                    .unwrap();
                if bytes.len() >= header_end + content_length {
                    break;
                }
            }
        }
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        String::from_utf8(bytes).unwrap()
    });
    let config_path = directory.path().join("config.json");
    std::fs::write(&config_path, config_json("events.jsonl", port)).unwrap();

    let initial = RuntimeConfig::load_from_path(&config_path).unwrap();
    let engine = Engine::new_with_config_path(initial, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let base_handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    let handle = base_handle.with_config_apply_reconciler(services.clone());
    let running = proxy.spawn();

    let mut registered: serde_json::Value =
        serde_json::from_str(&config_json("events.jsonl", port)).unwrap();
    registered["api"]["event_sinks"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "type": "webhook",
            "tag": "registered-receiver",
            "url": format!("http://{receiver_address}/custom/receiver/path"),
            "events": ["engine.warning"],
            "source_id": "node-hot-rebuild",
            "headers": {"x-receiver-token": "opaque-token"},
            "allow_insecure": true
        }));
    let response = handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: registered,
        }))
        .await
        .expect("register webhook by config.apply");
    assert_eq!(
        response.result.as_ref().unwrap()["application_components"],
        serde_json::json!(["event-dispatcher"])
    );

    engine.emit_warning("webhook-registered", "webhook-registered");
    let request = request.join().unwrap();
    assert!(request.starts_with("POST /custom/receiver/path HTTP/1.1\r\n"));
    assert!(request
        .to_ascii_lowercase()
        .contains("x-receiver-token: opaque-token"));
    assert!(request.contains("\"schema_id\":\"zero.event.v1\""));
    assert!(request.contains("\"event_type\":\"engine.warning\""));

    running.shutdown().await.unwrap();
    services.shutdown_status_monitor().await;
    engine.push_engine_stopped("test");
    tokio::task::yield_now().await;
    services.shutdown_dispatcher().await;
}
