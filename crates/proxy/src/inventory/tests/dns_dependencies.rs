use crate::Proxy;
use std::time::Duration;
use tokio::time::Instant;
use zero_config::RuntimeConfig;

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"one","protocol":{"type":"socks5","server":"127.0.0.1","port":10001}},
            {"tag":"two","protocol":{"type":"socks5","server":"127.0.0.1","port":10002}}
        ],
        "outbound_groups":[{"tag":"dns-path","type":"relay","proxies":["one","two"]}],
        "runtime":{"dns":{"servers":{"remote":{"type":"udp","host":"127.0.0.1","detour":"dns-path"}},"default_server":"remote"}},
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
async fn started() -> Proxy {
    let proxy = Proxy::new(config()).unwrap();
    let config = proxy.engine().runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}

#[tokio::test]
async fn unrelated_revision_reuses_whole_actual_dns_prefix_but_second_hop_change_retires_it() {
    let proxy = started().await;
    let previous = proxy.engine().runtime_snapshot();
    let mut updated = (**previous.config()).clone();
    updated.runtime.event_log_capacity += 1;
    proxy.engine().stage_runtime_config(updated).unwrap();
    let same = proxy.engine().runtime_snapshot();
    let plan = proxy
        .protocols
        .prepare_components(same.config().clone())
        .unwrap();
    assert!(proxy
        .protocols
        .affected_dns_detours(proxy.engine(), &previous, &same, &plan)
        .is_empty());
    let mut updated = (**same.config()).clone();
    updated.outbounds[1].dial.address_family = zero_config::OutboundAddressFamily::OnlyIpv4;
    proxy.engine().stage_runtime_config(updated).unwrap();
    let changed = proxy.engine().runtime_snapshot();
    let plan = proxy
        .protocols
        .prepare_components(changed.config().clone())
        .unwrap();
    assert_eq!(
        proxy
            .protocols
            .affected_dns_detours(proxy.engine(), &previous, &changed, &plan),
        ["dns-path".to_owned()].into_iter().collect()
    );
}

#[tokio::test]
async fn same_native_owners_in_a_different_prefix_order_require_retirement() {
    let proxy = started().await;
    let previous = proxy.engine().runtime_snapshot();
    let mut updated = (**previous.config()).clone();
    let zero_config::OutboundGroupKind::Relay { proxies } = &mut updated.outbound_groups[0].group
    else {
        panic!("relay fixture")
    };
    proxies.reverse();
    proxy.engine().stage_runtime_config(updated).unwrap();
    let changed = proxy.engine().runtime_snapshot();
    let plan = proxy
        .protocols
        .prepare_components(changed.config().clone())
        .unwrap();
    assert_eq!(
        proxy
            .protocols
            .affected_dns_detours(proxy.engine(), &previous, &changed, &plan),
        ["dns-path".to_owned()].into_iter().collect()
    );
}
