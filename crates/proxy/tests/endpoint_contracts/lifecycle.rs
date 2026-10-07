//! Public queries/events agree with confirmed runtime ownership.
use crate::{management_support as management, support};
use management::{config, endpoint, handle, ready};
use support::{free_udp_port, spawn_engine};
use tokio::{net::UdpSocket, time::Duration};
use zero_api::{event_type, EndpointGetQuery, EndpointRuntimeState, EventFilter};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

fn states(proxy: &Proxy, tag: &str) -> Vec<String> {
    proxy
        .engine()
        .events_snapshot(&EventFilter::default())
        .into_iter()
        .filter(|event| {
            event.event_type == event_type::ENDPOINT_STATE_CHANGED
                && event.payload["endpoint_id"] == format!("endpoint:{tag}")
        })
        .map(|event| event.payload["state"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn ordinary_start_and_shutdown_confirm_facts_events_and_socket_release() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let proxy = Proxy::new(config(a, b)).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    assert_eq!(states(&proxy, "a"), ["starting", "running"]);
    running.shutdown().await.unwrap();
    for (tag, port) in [("a", a), ("b", b)] {
        let stopped = endpoint(&handle, tag);
        let engine = proxy
            .engine()
            .endpoint_snapshot(&EndpointGetQuery {
                endpoint_id: stopped.endpoint_id.clone(),
            })
            .unwrap();
        assert_eq!(stopped.state, EndpointRuntimeState::Stopped);
        assert_eq!(engine.state, stopped.state);
        assert!(
            stopped.enabled,
            "shutdown must preserve configuration intent"
        );
        assert!(stopped.started_at_unix_ms.is_none());
        assert!(!stopped.effective.inbound && !stopped.effective.outbound);
        assert_eq!(
            states(&proxy, tag),
            ["starting", "running", "stopping", "stopped"]
        );
        UdpSocket::bind(("127.0.0.1", port))
            .await
            .expect("confirmed release");
    }
    assert_eq!(endpoint(&handle, "a").generation, original.generation);
}

#[tokio::test]
async fn startup_failure_cleans_partial_listeners_and_preserves_failure_facts() {
    let a = free_udp_port();
    let occupied = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let proxy = Proxy::new(config(a, occupied.local_addr().unwrap().port())).unwrap();
    let handle = handle(&proxy);
    let error = proxy.run_until(std::future::pending()).await.unwrap_err();
    for tag in ["a", "b"] {
        let failed = endpoint(&handle, tag);
        assert_eq!(failed.state, EndpointRuntimeState::Failed);
        assert!(failed.enabled);
        assert!(failed
            .last_error
            .as_ref()
            .unwrap()
            .message
            .contains(&error.to_string()));
        assert_eq!(states(&proxy, tag), ["starting", "stopping", "failed"]);
        assert!(!failed.effective.inbound && !failed.effective.outbound);
    }
    UdpSocket::bind(("127.0.0.1", a))
        .await
        .expect("partial listener released before error return");
}

#[tokio::test]
async fn config_disable_reenable_and_removal_publish_transitions_with_isolated_generations() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let initial = config(a, b);
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    let untouched = endpoint(&handle, "b");
    let mut disabled = initial.clone();
    disabled.endpoints[0].enabled = false;
    handle
        .apply_runtime_config_and_wait(disabled, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(endpoint(&handle, "a").state, EndpointRuntimeState::Stopped);
    assert_eq!(endpoint(&handle, "a").generation, original.generation);
    handle
        .apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let recreated = endpoint(&handle, "a");
    assert!(recreated.generation > original.generation);
    assert_eq!(endpoint(&handle, "b").generation, untouched.generation);
    // Edit the canonical source. A parsed RuntimeConfig also contains derived
    // roles; retaining them would intentionally preserve legacy listeners.
    let mut source = serde_json::to_value(&initial).unwrap();
    source["endpoints"].as_array_mut().unwrap().remove(0);
    let removed = RuntimeConfig::parse(&source.to_string()).unwrap();
    handle
        .apply_runtime_config_and_wait(removed, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(proxy
        .engine()
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:a".into()
        })
        .is_err());
    UdpSocket::bind(("127.0.0.1", a))
        .await
        .expect("deleted resource socket is released before acknowledgement");
    assert_eq!(
        states(&proxy, "a"),
        [
            "starting", "running", "stopping", "stopped", "starting", "running", "stopping",
            "stopped"
        ]
    );
    handle
        .apply_runtime_config_and_wait(initial, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(endpoint(&handle, "a").generation > recreated.generation);
    assert_eq!(endpoint(&handle, "b").generation, untouched.generation);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn replacement_endpoint_reuses_removed_resource_port_without_affecting_other_resource() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let initial = config(a, b);
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    let untouched = endpoint(&handle, "b");
    let mut source = serde_json::to_value(&initial).unwrap();
    source["endpoints"][0]["tag"] = "replacement".into();
    let candidate = RuntimeConfig::parse(&source.to_string()).unwrap();
    handle
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
        .await
        .expect("removed listener's port can be reused by its replacement");
    let replacement = endpoint(&handle, "replacement");
    assert_eq!(replacement.state, EndpointRuntimeState::Running);
    assert!(replacement.generation > original.generation);
    assert_eq!(endpoint(&handle, "b").generation, untouched.generation);
    assert!(proxy
        .engine()
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:a".into(),
        })
        .is_err());
    running.shutdown().await.unwrap();
    UdpSocket::bind(("127.0.0.1", a))
        .await
        .expect("replacement socket release confirmed");
}

#[tokio::test]
async fn rejected_config_start_restores_stopped_fact_and_records_error_without_affecting_other_resource(
) {
    let (a, b) = (free_udp_port(), free_udp_port());
    let mut initial = config(a, b);
    initial.endpoints[0].enabled = false;
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    support::wait_for("b running", || {
        endpoint(&handle, "b").state == EndpointRuntimeState::Running
    })
    .await;
    let original = endpoint(&handle, "b");
    let occupied = UdpSocket::bind(("127.0.0.1", a)).await.unwrap();
    let mut candidate = initial;
    candidate.endpoints[0].enabled = true;
    assert!(handle
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
        .await
        .is_err());
    let restored = endpoint(&handle, "a");
    assert_eq!(restored.state, EndpointRuntimeState::Stopped);
    assert!(!restored.enabled);
    assert!(restored.last_error.is_some());
    assert_eq!(endpoint(&handle, "b").generation, original.generation);
    assert!(endpoint(&handle, "b").last_error.is_none());
    assert!(states(&proxy, "a")
        .windows(2)
        .any(|pair| pair == ["starting", "stopped"]));
    drop(occupied);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn same_id_key_replacement_reports_real_new_generation_without_rebuilding_another_endpoint() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let initial = config(free_udp_port(), free_udp_port());
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    let other = endpoint(&handle, "b");
    let mut source = serde_json::to_value(&initial).unwrap();
    source["endpoints"][0]["protocol"]["private_key"] = STANDARD.encode([91u8; 32]).into();
    handle
        .apply_runtime_config_and_wait(
            RuntimeConfig::parse(&source.to_string()).unwrap(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let rebuilt = endpoint(&handle, "a");
    assert!(rebuilt.generation > original.generation);
    assert_eq!(rebuilt.state, EndpointRuntimeState::Running);
    assert_eq!(endpoint(&handle, "b").generation, other.generation);
    assert!(states(&proxy, "a").ends_with(&["starting".into(), "running".into()]));
    running.shutdown().await.unwrap();
}
