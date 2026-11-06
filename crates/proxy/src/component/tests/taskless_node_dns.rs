//! Real cold ordinary recipes keep Node DNS and replay as distinct dependencies.
use crate::Proxy;
use zero_platform_tokio::contract::component::{ResourceOwner, RoleKind};
#[test]
fn ordinary_domain_taskless_owners_bind_actual_node_dns_without_discarding_replay() {
    let id = "11111111-2222-3333-4444-555555555555";
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"outbounds":[
        {"tag":"vless","protocol":{"type":"vless","server":"node.example","port":443,"id":id}},
        {"tag":"vmess","protocol":{"type":"vmess","server":"node.example","port":443,"id":id}},
        {"tag":"trojan","protocol":{"type":"trojan","server":"node.example","port":443,"password":"p"}},
        {"tag":"ss","protocol":{"type":"shadowsocks","server":"node.example","port":443,"cipher":"2022-blake3-aes-128-gcm","password":"MDEyMzQ1Njc4OWFiY2RlZg=="}},
        {"tag":"socks","protocol":{"type":"socks5","server":"node.example","port":1080}},
        {"tag":"literal-socks","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}
    ],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let services = proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let mut common_node = None;
    for tag in ["vless", "vmess", "trojan", "ss", "socks", "literal-socks"] {
        let slot = plan.configured(tag).unwrap();
        assert!(slot.recipe.spec().roles.contains(&RoleKind::Stream));
        let dependencies = slot
            .recipe
            .spec()
            .dependencies
            .iter()
            .map(|id| plan.component(*id).unwrap())
            .collect::<Vec<_>>();
        let nodes=dependencies.iter().filter(|slot|matches!(&slot.recipe.spec().owner,ResourceOwner::Endpoint(name) if name.as_ref()=="proxy.dns_node_queries")).collect::<Vec<_>>();
        assert_eq!(
            nodes.len(),
            1,
            "{tag} must bind the same actual Node query owner"
        );
        let node = nodes[0];
        assert!(node.recipe.spec().roles.is_empty());
        assert!(node.recipe.spec().task_limit.is_some());
        if let Some(previous) = common_node {
            assert_eq!(previous, node.instance_id());
        } else {
            common_node = Some(node.instance_id());
        }
        if tag == "ss" {
            assert!(dependencies.iter().any(|slot|matches!(&slot.recipe.spec().owner,ResourceOwner::Endpoint(name) if name.as_ref()=="shadowsocks.tcp2022.replay")),"adding Node must retain the original security history/maintenance dependency");
            assert_eq!(dependencies.len(), 2);
        } else {
            assert_eq!(dependencies.len(), 1);
        }
        for ordinal in 0..slot.recipe.spec().resources.len() {
            assert!(
                plan.original_carrier_owner(slot.instance_id(), ordinal)
                    .is_none(),
                "cold metadata must not start any original"
            );
        }
    }
}
