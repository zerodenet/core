//! Endpoint recovery publishes a new generation only after retiring originals.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_api::{
    CommandRequest, CommandService, EndpointGetQuery, EndpointPersistence, EndpointRuntimeState,
    EndpointSetStateCommand,
};

fn endpoints(a: u16, b: u16) -> RuntimeConfig {
    let endpoint = |tag, seed: u8, port, address| {
        serde_json::json!({
            "tag":tag,"directions":{"inbound":true,"outbound":true},
            "listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"wireguard","private_key":STANDARD.encode([seed;32]),
                "addresses":[address],"peers":[{
                    "public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([seed+1;32])).as_bytes()),
                    "endpoint":"127.0.0.1:9","allowed_ips":["10.0.0.0/24"]}]}
        })
    };
    RuntimeConfig::parse(
        &serde_json::json!({
            "endpoints":[endpoint("a",61,a,"10.0.0.1/32"),endpoint("b",63,b,"10.0.0.2/32")],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}

#[tokio::test]
async fn failed_endpoint_source_write_joins_and_closes_original_before_recovery_generation() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("source");
    std::fs::write(&parent, b"parent is a file").unwrap();
    let path = parent.join("zero.json");
    let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let config = endpoints(
        a.local_addr().unwrap().port(),
        b.local_addr().unwrap().port(),
    );
    let a_tag = config.endpoints[0].inbound_tag();
    let b_tag = config.endpoints[1].inbound_tag();
    let proxy =
        Proxy::from_engine(zero_engine::Engine::new_with_config_path(config, &path).unwrap())
            .unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let mut ready = proxy.orchestration_ready.subscribe();
    drop((a, b));
    let running = proxy.spawn();
    tokio::time::timeout(Duration::from_secs(5), ready.wait_for(|ready| *ready))
        .await
        .unwrap()
        .unwrap();
    let original = proxy.inbound_owners.owner(&a_tag).unwrap();
    let untouched = proxy.inbound_owners.owner(&b_tag).unwrap();
    original.wait_ready().await.unwrap();
    untouched.wait_ready().await.unwrap();
    let a_query = EndpointGetQuery {
        endpoint_id: "endpoint:a".into(),
    };
    let b_query = EndpointGetQuery {
        endpoint_id: "endpoint:b".into(),
    };
    let before = proxy.engine().endpoint_snapshot(&a_query).unwrap();
    let b_before = proxy.engine().endpoint_snapshot(&b_query).unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    let error = handle
        .execute_acknowledged(CommandRequest::EndpointSetState(EndpointSetStateCommand {
            endpoint_id: a_query.endpoint_id.clone(),
            enabled: false,
            persistence: EndpointPersistence::SourceFile,
            expected_intent_revision: Some(before.intent_revision),
            expected_core_instance_id: Some(before.core_instance_id.clone()),
        }))
        .await
        .unwrap_err();
    assert!(error.message.contains("restored previous"), "{error:?}");
    let terminal = original
        .terminal_for_test()
        .expect("the exact retired original");
    assert_eq!(terminal.identity(), original.identity());
    assert!(
        terminal.closed_io_receipts() > 0,
        "actual inbound native controller close"
    );
    assert!(
        terminal.joined_tasks() > 0,
        "actual original inbound Scope joined"
    );
    let restored = proxy.inbound_owners.owner(&a_tag).unwrap();
    restored.wait_ready().await.unwrap();
    assert_ne!(restored.identity(), original.identity());
    assert!(Arc::ptr_eq(
        &proxy.inbound_owners.owner(&b_tag).unwrap(),
        &untouched
    ));
    assert!(!untouched.task_access().cancellation().is_cancelled());
    assert!(untouched.terminal_for_test().is_none());
    let after = proxy.engine().endpoint_snapshot(&a_query).unwrap();
    assert_eq!(after.state, EndpointRuntimeState::Running);
    assert_eq!(after.intent_revision, before.intent_revision);
    assert!(after.generation > before.generation);
    assert_eq!(
        proxy
            .engine()
            .endpoint_snapshot(&b_query)
            .unwrap()
            .generation,
        b_before.generation
    );
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &snapshot));
    assert_eq!(proxy.engine().config_source_write_generation(), 0);
    assert_eq!(std::fs::read(&parent).unwrap(), b"parent is a file");
    proxy
        .engine()
        .configuration_source()
        .confirm_original_io_closed()
        .unwrap();
    running.shutdown().await.unwrap();
    for owner in [restored, untouched] {
        let terminal = owner
            .terminal_for_test()
            .expect("actual final owner cleanup");
        assert_eq!(terminal.identity(), owner.identity());
        assert!(terminal.closed_io_receipts() > 0);
        assert!(terminal.joined_tasks() > 0);
    }
}
