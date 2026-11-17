//! Existing layered pools use their actual same-pass owners at every shorter
//! prefix. These cold checks do not claim IO, task joins or native closure.
use super::*;

#[cfg(feature = "vless")]
#[test]
fn same_owner_vless_pools_remain_standard_when_reentered_through_a_shorter_prefix() {
    for transport in [
        serde_json::json!({"testpre":1}),
        serde_json::json!({"grpc":{"service_names":["native/Tun"]}}),
        serde_json::json!({"quic":{"insecure":true}}),
        serde_json::json!({"split_http":{"path":"/native"}}),
        serde_json::json!({"mux_concurrency":2,"xudp_concurrency":2}),
    ] {
        let mut protocol = serde_json::json!({"type":"vless","server":"127.0.0.1","port":443,
            "id":"11111111-2222-3333-4444-555555555555"});
        protocol
            .as_object_mut()
            .unwrap()
            .extend(transport.as_object().unwrap().clone());
        let config = RuntimeConfig::parse(
            &serde_json::json!({
                "outbounds":[{"tag":"pooled","protocol":protocol}],
                "outbound_groups":[{"tag":"chain","type":"relay","proxies":["pooled","pooled"]}],
                "route":{"final":{"type":"direct"}}
            })
            .to_string(),
        )
        .unwrap();
        let (_, factories, _) =
            crate::register::compiled_registries_with_background(Some(std::sync::Arc::new(
                crate::component::background::BackgroundNetwork::new(Default::default()),
            )));
        assert!(
            factories.has_factory_for_candidate("vless", &config.outbounds[0], Some(&config)),
            "the real owning factory must select the same complete production grammar: {transport}"
        );
    }
}

#[cfg(feature = "vmess")]
#[test]
fn vmess_inline_grpc_mux_uses_standard_prefix_assembly_without_a_second_grpc_pool() {
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[{"tag":"pooled","protocol":{"type":"vmess","server":"127.0.0.1",
                "port":443,"id":"11111111-2222-3333-4444-555555555555",
                "grpc":{"service_names":["native/Tun"]},"mux_concurrency":2}}],
            "outbound_groups":[{"tag":"chain","type":"relay","proxies":["pooled","pooled"]}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let (_, factories, _) =
        crate::register::compiled_registries_with_background(Some(std::sync::Arc::new(
            crate::component::background::BackgroundNetwork::new(Default::default()),
        )));
    assert!(factories.has_factory_for_candidate("vmess", &config.outbounds[0], Some(&config)));
}

#[cfg(feature = "vless")]
#[test]
fn layered_lower_owners_select_the_actual_grouped_production_assembly() {
    for transport in [
        serde_json::json!({"testpre":1,"grpc":{"service_names":["native/Tun"]}}),
        serde_json::json!({"testpre":1,"quic":{"insecure":true}}),
        serde_json::json!({"testpre":1,"split_http":{"path":"/native"}}),
        serde_json::json!({"testpre":1,"mux_concurrency":2,"xudp_concurrency":2}),
        serde_json::json!({"grpc":{"service_names":["native/Tun"]},"mux_concurrency":2,"xudp_concurrency":2}),
    ] {
        let mut protocol = serde_json::json!({"type":"vless","server":"127.0.0.1","port":443,
            "id":"11111111-2222-3333-4444-555555555555"});
        protocol
            .as_object_mut()
            .unwrap()
            .extend(transport.as_object().unwrap().clone());
        let config = RuntimeConfig::parse(
            &serde_json::json!({
                "outbounds":[{"tag":"pooled","protocol":protocol}],
                "outbound_groups":[{"tag":"chain","type":"relay","proxies":["pooled","pooled"]}],
                "route":{"final":{"type":"direct"}}
            })
            .to_string(),
        )
        .unwrap();
        let (_, factories, _) =
            crate::register::compiled_registries_with_background(Some(std::sync::Arc::new(
                crate::component::background::BackgroundNetwork::new(Default::default()),
            )));
        assert!(
            factories.has_factory_for_candidate("vless", &config.outbounds[0], Some(&config)),
            "{transport}"
        );
    }
}
