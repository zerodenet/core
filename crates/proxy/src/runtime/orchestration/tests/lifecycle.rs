#![cfg(feature = "wireguard")]

use std::future::pending;

use tokio::net::UdpSocket;
use zero_api::{EndpointGetQuery, EndpointRuntimeState};
use zero_config::RuntimeConfig;
use zero_engine::EngineError;

use super::super::{lifecycle::run_loop, state::OrchestrationState};
use crate::runtime::Proxy;

async fn fixture() -> (Proxy, u16) {
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "endpoints":[{"tag":"wg", "directions":{"inbound":true,"outbound":true},
                "listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"wireguard", "private_key":"01".repeat(32),
                    "addresses":["10.0.0.1/32"], "peers":[{"public_key":"02".repeat(32),
                        "endpoint":"127.0.0.1:9", "allowed_ips":["10.0.0.0/24"]}]}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    (Proxy::new(config).unwrap(), port)
}

fn fact(proxy: &Proxy) -> zero_api::EndpointSnapshot {
    proxy
        .engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        })
        .unwrap()
}

#[tokio::test]
async fn unexpected_listener_failure_runs_cleanup_before_publishing_failed() {
    let (proxy, port) = fixture().await;
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let original = fact(&proxy);
    state
        .listeners
        .spawn(async { Err(EngineError::NoInbounds) });
    let result = run_loop(&proxy, &mut state, pending()).await;
    assert!(matches!(result, Err(EngineError::NoInbounds)));
    assert!(matches!(
        state.finish(&proxy, result).await,
        Err(EngineError::NoInbounds)
    ));
    UdpSocket::bind(("127.0.0.1", port))
        .await
        .expect("failure cleanup joins resource owner");
    let failed = fact(&proxy);
    assert_eq!(failed.state, EndpointRuntimeState::Failed);
    assert_eq!(failed.generation, original.generation);
    assert!(failed.started_at_unix_ms.is_none());
    assert!(failed.last_error.is_some());
    drop(state);
    assert_eq!(
        fact(&proxy).last_error,
        failed.last_error,
        "normal completion disarms interruption guard"
    );
}

#[tokio::test]
async fn listener_panic_becomes_failed_fact_after_other_owners_end() {
    let (proxy, port) = fixture().await;
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    state.listeners.spawn(async {
        panic!("injected listener panic");
    });
    let result = run_loop(&proxy, &mut state, pending()).await;
    let error = state.finish(&proxy, result).await.unwrap_err();
    assert!(error.to_string().contains("injected listener panic"));
    assert_eq!(fact(&proxy).state, EndpointRuntimeState::Failed);
    UdpSocket::bind(("127.0.0.1", port))
        .await
        .expect("other listener joined after panic");
}

#[tokio::test]
async fn unresponsive_task_is_aborted_and_joined_without_claiming_clean_shutdown() {
    // Real network/stack workers use std::time::Instant. Pausing only Tokio's
    // clock does not advance those workers and can prevent automatic advance.
    let (proxy, port) = fixture().await;
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    state.listeners.spawn(pending::<Result<(), EngineError>>());
    let error = state.finish(&proxy, Ok(())).await.unwrap_err();
    assert!(error.to_string().contains("shutdown timed out"));
    assert!(state.listeners.is_empty());
    assert_eq!(fact(&proxy).state, EndpointRuntimeState::Failed);
    UdpSocket::bind(("127.0.0.1", port))
        .await
        .expect("forced cleanup joined socket owner");
}

#[tokio::test]
async fn cancellation_guard_reports_unconfirmed_failure_instead_of_stale_running() {
    let (proxy, port) = fixture().await;
    let state = OrchestrationState::new(&proxy).await.unwrap();
    drop(state);
    let failed = fact(&proxy);
    assert_eq!(failed.state, EndpointRuntimeState::Failed);
    assert!(failed
        .last_error
        .unwrap()
        .message
        .contains("before shutdown confirmation"));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if UdpSocket::bind(("127.0.0.1", port)).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interruption_during_reload_also_terminates_new_candidate_transition() {
    let (proxy, _) = fixture().await;
    let state = OrchestrationState::new(&proxy).await.unwrap();
    let mut candidate = (*proxy.engine.config()).clone();
    let mut added = candidate.endpoints[0].clone();
    added.tag = "new".into();
    let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    added.listen.as_mut().unwrap().port = socket.local_addr().unwrap().port();
    candidate.endpoints.push(added);
    proxy.engine.stage_runtime_config(candidate).unwrap();
    state.publish_reload_transitions(&proxy, &proxy.engine.runtime_snapshot());
    drop(state);
    let endpoint = proxy
        .engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:new".into(),
        })
        .unwrap();
    assert_eq!(endpoint.state, EndpointRuntimeState::Failed);
    assert!(endpoint.generation.is_none());
    assert!(endpoint.last_error.is_some());
}

#[tokio::test]
async fn original_failure_does_not_hide_unconfirmed_cleanup_in_observation() {
    let (proxy, _) = fixture().await;
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    state.listeners.spawn(pending::<Result<(), EngineError>>());
    let result = state.finish(&proxy, Err(EngineError::NoInbounds)).await;
    assert!(matches!(result, Err(EngineError::NoInbounds)));
    assert!(fact(&proxy)
        .last_error
        .unwrap()
        .message
        .contains("shutdown timed out"));
}
