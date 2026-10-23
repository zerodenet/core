#![cfg(feature = "vless")]
//! These use actual inventory-prepared VLESS operations and host lifetimes.
//! The wire fixture proves reconnect routing, not a component physical-close receipt.
use super::*;
use crate::{
    inventory::PreparedTcpOutbound, protocol_registry::OutboundAdapterContext, runtime::Proxy,
};
use zero_config::RuntimeConfig;
use zero_engine::RouteMode;
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";

fn value(port: u16) -> serde_json::Value {
    serde_json::json!({
        "outbounds":[
            {"tag":"first","protocol":{"type":"vless","server":"127.0.0.1","port":port,"id":ID}},
            {"tag":"last","protocol":{"type":"vless","server":"127.0.0.1","port":45873,"id":ID}},
            {"tag":"other","protocol":{"type":"direct"}}
        ],
        "outbound_groups":[{"tag":"chain","type":"relay","proxies":["first","last"]}],
        "route":{"final":{"type":"direct"}}
    })
}
fn config(value: &serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&value.to_string()).unwrap()
}
fn prepare(proxy: &Proxy) -> (TcpExecutionServices, PreparedTcpRelayPrefix) {
    let services = proxy.tcp_runtime_services();
    let target = services.snapshot().plan().target_id("chain").unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    // Exercise the real compatibility preparation explicitly. Native roles are
    // covered elsewhere; borrowing their slot would not authorize legacy pools.
    let prepared = proxy
        .protocols
        .prepare_tcp_outbound(
            OutboundAdapterContext::new(services.config()),
            &resolved,
            RouteMode::Flow,
            None,
        )
        .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
    let PreparedTcpOutbound::Relay(chain) = prepared else {
        panic!("expected legacy prepared relay")
    };
    let prefix = super::super::split_relay_prefix(chain).0;
    (services.execution(), prefix)
}
fn bound(services: &TcpExecutionServices, prefix: &PreparedTcpRelayPrefix) -> RetainedSource {
    let source = RetainedSource::capture(services, prefix);
    assert!(
        matches!(source, RetainedSource::Bound(_)),
        "all actual owners must be recognized"
    );
    source
}

#[test]
fn retained_connector_and_datagram_resolver_do_not_retain_engine_config_or_dns() {
    let proxy = Proxy::new(config(&value(34567))).unwrap();
    let source = proxy.current_service_source();
    let current_config = Arc::downgrade(&proxy.engine().config());
    let resolver = Arc::downgrade(&proxy.resolver);
    let (services, prefix) = prepare(&proxy);
    let retained = bound(&services, &prefix);
    let history = Arc::downgrade(&services.snapshot_handle());
    let carrier = super::super::prepare_lazy_tcp_relay_prefix(services, prefix.clone());
    let connector = carrier.connector().unwrap();
    drop(carrier); // Drop the one-shot future while retaining the real factory.
    drop(proxy);
    assert!(source.upgrade().is_none());
    assert!(current_config.upgrade().is_none());
    assert!(history.upgrade().is_none());
    assert!(
        resolver.upgrade().is_none(),
        "datagram resolver must also be weak"
    );
    assert!(retained.services(&prefix).is_err());
    drop(connector);
}

#[test]
fn selected_dependencies_allow_rule_and_unrelated_updates_without_reselection() {
    let mut input = value(34567);
    let proxy = Proxy::new(config(&input)).unwrap();
    let (services, prefix) = prepare(&proxy);
    let retained = bound(&services, &prefix);
    drop(services);
    input["route"] = serde_json::json!({"final":{"type":"reject"}});
    input["outbounds"][2]["dial"] = serde_json::json!({"address_family":"only_ipv4"});
    proxy
        .engine()
        .reload_runtime_config(config(&input))
        .unwrap();
    // Only kernel publication occurs here: actual owner retirement has its own
    // independent test below, and is never overridden by static equality.
    assert!(retained.services(&prefix).is_ok());
}

#[test]
fn committed_selected_option_aba_and_removal_cannot_revive_an_old_recipe() {
    for removal in [false, true] {
        let original = value(34567);
        let mut changed = original.clone();
        let proxy = Proxy::new(config(&original)).unwrap();
        let (services, prefix) = prepare(&proxy);
        let retained = bound(&services, &prefix);
        drop(services);
        if removal {
            changed["outbounds"].as_array_mut().unwrap().remove(0);
            changed["outbound_groups"] = serde_json::json!([]);
        } else {
            changed["outbounds"][0]["protocol"]["id"] = serde_json::json!("replacement-user");
        }
        proxy
            .engine()
            .reload_runtime_config(config(&changed))
            .unwrap();
        assert!(retained.services(&prefix).is_err());
        proxy
            .engine()
            .reload_runtime_config(config(&original))
            .unwrap();
        assert!(retained.services(&prefix).is_err());
    }
}

#[test]
fn actual_protocol_retirement_rejects_even_unchanged_static_configuration() {
    let input = value(34567);
    let proxy = Proxy::new(config(&input)).unwrap();
    let (services, prefix) = prepare(&proxy);
    let retained = bound(&services, &prefix);
    proxy.protocols.on_config_reloaded(services.config());
    assert!(retained.services(&prefix).is_err());
    assert!(
        matches!(
            RetainedSource::capture(&services, &prefix),
            RetainedSource::Rejected(_)
        ),
        "known retired owners must never fall back to the compatibility opener"
    );
}

#[test]
fn physical_egress_retirement_and_restore_do_not_reauthorize_the_old_recipe() {
    let proxy = Proxy::new(config(&value(34567))).unwrap();
    let (services, prefix) = prepare(&proxy);
    let retained = bound(&services, &prefix);
    proxy
        .egress_interface
        .mark_unavailable_for(false, "fixture physical egress retired");
    assert!(retained.services(&prefix).is_err());
    proxy.egress_interface.replace(None);
    assert!(retained.services(&prefix).is_err());
}

#[test]
fn unknown_owner_is_explicitly_unmigrated_without_default_authority() {
    let mut input = value(34567);
    input["outbounds"][0]["protocol"] = serde_json::json!({"type":"direct"});
    let proxy = Proxy::new(config(&input)).unwrap();
    // Direct is a real existing operation, but this batch does not borrow a
    // separate native role's grant to authorize its compatibility execution.
    let services = proxy.tcp_runtime_services();
    let target = services.snapshot().plan().target_id("first").unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    let prepared = proxy
        .protocols
        .prepare_tcp_outbound(
            OutboundAdapterContext::new(services.config()),
            &resolved,
            RouteMode::Flow,
            None,
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    let PreparedTcpOutbound::Single(first) = prepared else {
        panic!("expected concrete direct")
    };
    let (_, mut prefix) = prepare(&Proxy::new(config(&value(34567))).unwrap());
    prefix.first = first;
    assert!(matches!(
        check_owners(&prefix),
        Err(EngineError::Core(zero_core::Error::Unsupported(_)))
    ));
    assert!(matches!(
        RetainedSource::capture(&services.execution(), &prefix),
        RetainedSource::Legacy(_)
    ));
}

#[tokio::test]
async fn actual_vless_tcp_reconnect_preserves_selected_prefix_after_unrelated_publication() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut input = value(listener.local_addr().unwrap().port());
    let proxy = Proxy::new(config(&input)).unwrap();
    let (services, prefix) = prepare(&proxy);
    bound(&services, &prefix);
    let carrier = super::super::prepare_lazy_tcp_relay_prefix(services, prefix);
    let connector = carrier.connector().unwrap();
    drop(carrier);
    let peer = async {
        for payload in [7, 9] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut auth = [0; 18];
            stream.read_exact(&mut auth).await.unwrap();
            assert_eq!(auth[0], 0);
            assert_eq!(
                &auth[1..17],
                &[
                    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89,
                    0xab, 0xcd, 0xef
                ]
            );
            assert_eq!(auth[17], 0);
            let mut command = [0; 4];
            stream.read_exact(&mut command).await.unwrap();
            assert_eq!(&command[..3], &[1, (45873_u16 >> 8) as u8, 45873_u16 as u8]);
            assert_eq!(command[3], 2); // VLESS domain address, retained as a named node target.
            let len = stream.read_u8().await.unwrap();
            let mut host = vec![0; len as usize];
            stream.read_exact(&mut host).await.unwrap();
            assert_eq!(host, b"127.0.0.1");
            assert_eq!(stream.read_u8().await.unwrap(), payload);
            stream.write_all(&[0, 0, payload]).await.unwrap();
        }
    };
    let client = async {
        for payload in [7, 9] {
            let mut stream = connector.connect("127.0.0.1".into(), 45873).await.unwrap();
            stream.write_all(&[payload]).await.unwrap();
            stream.flush().await.unwrap();
            assert_eq!(stream.read_u8().await.unwrap(), payload);
            drop(stream);
            input["outbounds"][2]["dial"] = serde_json::json!({"address_family":"only_ipv4"});
            proxy
                .engine()
                .reload_runtime_config(config(&input))
                .unwrap();
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(peer, client);
    })
    .await
    .unwrap();
}

#[cfg(feature = "socks5")]
mod native;
