//! Actual cold candidate projection and physical Source replacement.
use crate::{Proxy, ProxyHandle, RunningProxy};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::component::ResourceDeclaration;
fn dimensions(
    resources: &[ResourceDeclaration],
) -> zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
    resources
        .iter()
        .find_map(|r| match r {
            ResourceDeclaration::MetadataDatagram { limits, .. } => Some(*limits),
            _ => None,
        })
        .expect("actual Source Metadata declaration")
}
async fn ready(proxy: &Proxy) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.inbound_owners.owner("source") {
                owner.wait_ready().await.unwrap();
                return owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn configuration(protocol: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({"runtime":{"metadata_max_probe_sockets":2,"metadata_max_probe_attempts":8,"metadata_max_send_calls":1,"quic_source_max_connections":2,"quic_shared_endpoint_max_users":16},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap()
}
fn declared(
    proxy: &Proxy,
    config: &RuntimeConfig,
    drivers: usize,
    users: usize,
    probes: usize,
    attempts: usize,
    sends: usize,
) {
    let prepared = proxy
        .protocols
        .prepare_inbound_bind_in_config(config.inbounds[0].clone(), config)
        .unwrap()
        .expect("ordinary factory");
    let limits = dimensions(prepared.resources());
    assert_eq!(
        (
            limits.main_sockets.get(),
            limits.probe_sockets.get(),
            limits.probe_attempts_per_initialization.get(),
            limits.concurrent_send_calls.get()
        ),
        (1, probes, attempts, sends)
    );
    assert_eq!(limits.write_pollers.get(), drivers + users + 1);
    assert_eq!(
        prepared.into_recipe().spec().task_limit.unwrap().get(),
        drivers
    );
}
fn closed(owner: &crate::runtime::inbound_owners::InboundOwner) {
    let closed = owner
        .terminal_for_test()
        .expect("actual original Source closure");
    assert_eq!(closed.identity(), owner.identity());
    assert!(closed.joined_tasks() > 0 && closed.closed_io_receipts() > 0);
    assert!(
        closed.failure().is_none()
            && closed.task_failures().is_empty()
            && closed.native_bound_failures().is_empty()
    );
}
#[tokio::test]
async fn metadata_source_config_changes_retire_actual_native_and_scope_before_fresh_source() {
    timeout(Duration::from_secs(25),async {
        let files=tempfile::tempdir().unwrap();
        let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert=files.path().join("cert.pem");let key=files.path().join("key.pem");
        std::fs::write(&cert,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
        for protocol in [serde_json::json!({"type":"vless","users":[],"quic":{"cert_path":cert,"key_path":key}}),serde_json::json!({"type":"hysteria2","password":"actual","cert_path":cert,"key_path":key})] {
            let mut config=configuration(protocol);
            let proxy=Proxy::new(config.clone()).unwrap();
            declared(&proxy,&config,3,16,2,8,1);
            let running:RunningProxy=proxy.clone().spawn();let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
            let old=ready(&proxy).await;
            let old_identity=old.identity();assert!(old.terminal_for_test().is_none());
            config.runtime.metadata_max_probe_sockets=Some(3);
            config.runtime.metadata_max_probe_attempts=Some(16);
            config.runtime.metadata_max_send_calls=Some(3);
            config.runtime.quic_source_max_connections=Some(3);
            config.runtime.quic_shared_endpoint_max_users=Some(24);
            handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.unwrap();
            closed(&old);
            let new=ready(&proxy).await;assert_ne!(new.identity(),old_identity);
            declared(&proxy,&config,4,24,3,16,3);
            // Real new native IO, rather than declaration-only replacement.
            let address=*new.original_listener_address.get().expect("actual rebound UDP endpoint");
            let mut client=quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            client.set_default_client_config(zero_transport::quic::client_config(true,None,&[b"h3".to_vec()],None).unwrap());
            let connection=timeout(Duration::from_secs(5),client.connect(address,"localhost").unwrap()).await.unwrap().unwrap();
            assert_eq!(connection.remote_address(),address);
            connection.close(0u32.into(),b"actual budget restart");client.close(0u32.into(),b"actual budget restart");client.wait_idle().await;
            running.shutdown().await.unwrap();closed(&new);
        }
    }).await.unwrap();
}
