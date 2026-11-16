use super::*;
use zero_config::RuntimeConfig;
mod production;

pub(super) fn context(
    proxy: &crate::runtime::Proxy,
    config: &RuntimeConfig,
) -> OutboundDevicePreparationContext {
    let services = proxy.tcp_runtime_services();
    OutboundDevicePreparationContext {
        direct_packet_device: None,
        mtu: config.runtime.network.mtu,
        stack_options: Default::default(),
        network_generation: zero_core::component::NetworkGeneration(
            services.upstream().egress_generation(),
        ),
        packet_paths: Arc::new(Default::default()),
    }
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn selected_source_without_its_candidate_never_enters_the_old_constructor() {
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[
                {"tag":"outer","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}},
                {"tag":"wg","protocol":{"type":"wireguard","private_key":"01".repeat(32),
                    "addresses":["10.0.0.1/32"],"outer_udp_proxy":"outer",
                    "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51820",
                        "allowed_ips":["0.0.0.0/0"]}]}}
            ],"route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let proxy = crate::runtime::Proxy::new(config.clone()).unwrap();
    let owner = WireguardAdapter::default();
    let actual = context(&proxy, &config);
    // Selection is metadata, never a native grant. With no same-candidate
    // graph it must fail before resolving/opening the outer operation.
    let result = owner
        .prepare_devices_for_candidate(
            &config,
            &config.outbounds.iter().collect::<Vec<_>>(),
            actual,
        )
        .await;
    let Err(source) = result else {
        panic!("missing selected Source was replaced by an old constructor")
    };
    assert!(source
        .to_string()
        .contains("same actual cold component candidate"));
    assert!(owner
        .pool
        .test_current_devices(&["wg".to_owned()])
        .is_empty());
}
