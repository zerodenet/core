use super::*;
#[tokio::test]
async fn route_source_known_owned_arrays_and_last_weak_header_remain_on_the_actual_host_metadata_root(
) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let root = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let start = crate::inbound::tun::managed::Start {
        name: None,
        raw_addr: String::new(),
        addresses: vec![],
        route_addresses: vec![(
            "10.77.0.1".parse().unwrap(),
            "255.255.255.0".parse().unwrap(),
        )],
        mtu: 1500,
        tag: "actual-route-meta".into(),
        options: crate::inbound::tun::TunRuntimeOptions {
            auto_route: true,
            include_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            exclude_cidrs: vec!["10.99.0.0/16".parse().unwrap()],
            dual_stack: false,
            strict_route: true,
            dns_hijack: true,
        },
        managed_config: None,
        dns_hijacked_queries: Arc::default(),
    };
    let layout = OwnedRoutes::layout(&start).unwrap();
    let credit = memory.reserve(layout).unwrap();
    let spec = RouteRuntimeSpec {
        id: 3,
        tun_name: String::new(),
        recovery_key: copy_key(&start.tag),
        primary_ipv6: false,
        addresses: copy_addresses(&start.route_addresses),
        include_cidrs: copy_prefixes(&start.options.include_cidrs),
        exclude_cidrs: copy_prefixes(&start.options.exclude_cidrs),
        dns_hijack: true,
        strict_route: true,
    };
    let actual = OwnedRoutes::from_prepaid(
        proxy.clone(),
        spec,
        Arc::default(),
        CancellationToken::new(),
        credit,
    )
    .unwrap();
    assert_eq!(
        memory.snapshot().occupied,
        baseline.checked_add(layout).unwrap()
    );
    assert!(actual.0.worker.get().is_none());
    assert!(actual.0.monitor.get().is_none());
    let weak = PacketMetadataArc::downgrade(&actual.0);
    drop(actual);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        memory.snapshot().occupied,
        baseline
            .checked_add(PacketMetadataArc::<Shared>::layout().unwrap())
            .unwrap(),
        "the actual old arrays are freed, but their surviving owner weak header still has custody"
    );
    drop(weak);
    assert_eq!(memory.snapshot().occupied, baseline);
}
