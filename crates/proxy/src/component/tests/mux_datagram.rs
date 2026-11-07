//! Ordinary TCP and packet roles bind one original, finite MUX owner.
use zero_platform_tokio::contract::component::{ResourceDeclaration, RoleKind};
#[tokio::test]
async fn ordinary_mux_stream_and_datagram_declare_the_same_single_pool_scope_and_total_limit() {
    let mut protocols = Vec::new();
    #[cfg(feature="vless")]
    protocols.push(serde_json::json!({"type":"vless","server":"127.0.0.1","port":443,"id":"11111111-2222-3333-4444-555555555555","mux_concurrency":8,"xudp_concurrency":8}));
    #[cfg(feature="vmess")]
    protocols.push(serde_json::json!({"type":"vmess","server":"127.0.0.1","port":443,"id":"11111111-2222-3333-4444-555555555555","mux_concurrency":8}));
    #[cfg(feature="trojan")]
    protocols.push(serde_json::json!({"type":"trojan","server":"127.0.0.1","port":443,"password":"same-original-test","insecure":true,"mux_concurrency":8}));
    assert!(!protocols.is_empty());
    for protocol in protocols {
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_mux_max_carriers":2},"outbounds":[{"tag":"node","protocol":protocol}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        let services = proxy.tcp_runtime_services();
        let plan = services.components.as_ref().unwrap();
        let role = plan.configured("node").unwrap();
        let spec = role.recipe.spec();
        assert_eq!(spec.roles, vec![RoleKind::Stream, RoleKind::Datagram]);
        assert!(spec.task_limit.is_none() && spec.resources.is_empty());
        assert_eq!(role.dependency_instances().len(), 1);
        let pool = plan
            .component(role.dependency_instances()[0].component)
            .unwrap();
        assert_eq!(pool.instance_id(), role.dependency_instances()[0]);
        let spec = pool.recipe.spec();
        assert!(spec.roles.is_empty());
        assert_eq!(spec.task_limit.unwrap().get(), 2);
        assert_eq!(spec.resources.iter().filter(|resource|matches!(resource,
            ResourceDeclaration::ControlledCarrierPool{datagram:false,max_sockets,..} if max_sockets.get()==2)).count(),1);
        assert!(
            pool.original_carrier_owner(0).is_none(),
            "cold projection allocates no actual Scope/native owner"
        );
    }
}
