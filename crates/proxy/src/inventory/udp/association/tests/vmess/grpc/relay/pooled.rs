//! The final Datagram owns H2; each prefix retains its one real shared pool.
use super::pool_fixture::*;
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

#[cfg(feature = "shadowsocks")]
#[tokio::test]
async fn ordinary_vmess_grpc_mixed_relay_reuses_real_shadowsocks_replay_scope_and_datagram_dispatch(
) {
    timeout(Duration::from_secs(35), async {
        for cipher in ["aes-128-gcm", "2022-blake3-aes-128-gcm"] {
            let source = Pool::shadowsocks(cipher, "shared").await;
            let suffix = Prefix::new(Kind::Socks, false).await;
            let (proxy, server) = start_config_mode(
                true,
                true,
                "aes-128-gcm",
                "node",
                vec![b"mixed".to_vec()],
                1,
                true,
                false,
                Box::new(|config| {
                    configure(
                        config,
                        &[source.node.clone(), suffix.outbound("suffix")],
                        false,
                    )
                }),
            )
            .await;
            assert_standard_relay(&proxy);
            proxy
                .protocols
                .drive_component_supervision(async {
                    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                    let role = plan.configured("shared").unwrap();
                    assert_eq!(
                        role.dependency_instances().len(),
                        usize::from(cipher.starts_with("2022")),
                        "the role must retain the actual adapter-scoped replay dependency"
                    );
                    let mut flow = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("ordinary")
                        .await
                        .unwrap();
                    let id = flow.dispatch(input(53, b"mixed")).await.unwrap();
                    let packet = response(&mut flow).await;
                    assert_eq!(packet.2, b"mixed");
                    crate::runtime::udp_delivery::record_chain_udp_response_parts(
                        &proxy.udp_runtime_services(),
                        &flow,
                        packet.0,
                        packet.1,
                        packet.2,
                        packet.3,
                    );
                    let session = proxy
                        .engine()
                        .active_sessions()
                        .into_iter()
                        .find(|s| s.id == id)
                        .unwrap();
                    assert_eq!(session.outbound_tx_bytes, 14);
                    assert_eq!(session.outbound_rx_bytes, 5);
                    let registration = original(&proxy);
                    let view = registration.observe().unwrap();
                    assert!(view.slots()[0].native().is_some());
                    assert!(view.slots()[0].borrowed_origin().is_none());
                    let native = view.slots()[0].original().clone();
                    let driver = view.slots()[0].driver().unwrap().1.clone();
                    close_flow(&proxy, flow).await;
                    assert!(driver.is_joined());
                    assert!(native.matches_closed(&native.closed().unwrap()));
                    server.finish().await;
                    proxy.protocols.shutdown_components(end()).await.unwrap();
                })
                .await;
            suffix.close().await;
            source.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hosted_vmess_grpc_pooled_relay_real_mux_loans_share_original_and_preserve_tls_udp_accounting(
) {
    timeout(Duration::from_secs(45), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let pool = Pool::start(protocol, "pool").await;
            let (proxy, server) = start_config_mode(
                true,
                true,
                "aes-128-gcm",
                "node",
                vec![b"pooled".to_vec()],
                2,
                true,
                true,
                Box::new(|config| configure(config, &[pool.node.clone()], false)),
            )
            .await;
            assert_standard_relay(&proxy);
            proxy
                .protocols
                .drive_component_supervision(async {
                    let registration = pool_owner(&proxy, "pool");
                    let mut first = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("one")
                        .await
                        .unwrap();
                    let id = first.dispatch(input(53, b"pooled")).await.unwrap();
                    let packet = response(&mut first).await;
                    assert_eq!(packet.2, b"pooled");
                    crate::runtime::udp_delivery::record_chain_udp_response_parts(
                        &proxy.udp_runtime_services(),
                        &first,
                        packet.0,
                        packet.1,
                        packet.2,
                        packet.3,
                    );
                    let session = proxy
                        .engine()
                        .active_sessions()
                        .into_iter()
                        .find(|s| s.id == id)
                        .unwrap();
                    assert_eq!(session.outbound_tx_bytes, 15);
                    assert_eq!(session.outbound_rx_bytes, 6);
                    let observed = registration.observe().unwrap();
                    assert_eq!(observed.retained(), 1);
                    assert!(observed.slots()[0].native().is_some());
                    let physical = observed.slots()[0].original().clone();
                    let driver = observed.slots()[0].driver().unwrap().1.clone();
                    let consumer = original(&proxy);
                    let consumer_view = consumer.observe().unwrap();
                    assert!(consumer_view.slots()[0].native().is_none());
                    assert!(consumer_view.slots()[0]
                        .borrowed_origin()
                        .unwrap()
                        .owner()
                        .same_owner(&registration));
                    let first_control = consumer_view.slots()[0].original().clone();
                    let mut second = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("two")
                        .await
                        .unwrap();
                    second.dispatch(input(53, b"pooled")).await.unwrap();
                    assert_eq!(response(&mut second).await.2, b"pooled");
                    assert_eq!(
                        registration.observe().unwrap().retained(),
                        1,
                        "two real H2 clients must borrow one actual physical pool"
                    );
                    close_flow(&proxy, first).await;
                    assert!(first_control.closed().is_some());
                    assert!(!driver.is_joined());
                    assert!(
                        physical.closed().is_none(),
                        "logical return cannot certify donor native close"
                    );
                    close_flow(&proxy, second).await;
                    server.finish().await;
                    proxy.protocols.shutdown_components(end()).await.unwrap();
                    assert!(driver.matches_joined(&driver.try_joined().unwrap()));
                    assert!(physical.matches_closed(&physical.closed().unwrap()));
                    assert!(registration.closed().unwrap().matches(&registration));
                })
                .await;
            pool.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hosted_vmess_grpc_pooled_relay_mixed_prefixes_preserve_nested_original_lineage() {
    timeout(Duration::from_secs(35), async {
        let first = Prefix::new(Kind::Socks, false).await;
        let pool = Pool::start("vmess", "pool").await;
        let suffix = Prefix::new(Kind::Vmess, false).await;
        let (proxy, server) = start_config_mode(
            true,
            false,
            "chacha20-poly1305",
            "node",
            vec![b"pooled".to_vec()],
            1,
            true,
            true,
            Box::new(|config| {
                configure(
                    config,
                    &[
                        first.outbound("first"),
                        pool.node.clone(),
                        suffix.outbound("last"),
                    ],
                    false,
                )
            }),
        )
        .await;
        assert_standard_relay(&proxy);
        proxy
            .protocols
            .drive_component_supervision(async {
                let mut flow = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("mixed")
                    .await
                    .unwrap();
                flow.dispatch(input(53, b"pooled")).await.unwrap();
                assert_eq!(response(&mut flow).await.2, b"pooled");
                assert_eq!(first.count.load(std::sync::atomic::Ordering::SeqCst), 1);
                let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                let root_registration = plan
                    .original_carrier_owner(plan.configured("first").unwrap().instance_id(), 0)
                    .unwrap();
                let root_view = root_registration.observe().unwrap();
                assert!(root_view.slots()[0].native().is_some());
                let root_original = root_view.slots()[0].original().clone();
                let registration = pool_owner(&proxy, "pool");
                assert!(registration.observe().unwrap().slots()[0]
                    .borrowed_origin()
                    .unwrap()
                    .native_origin()
                    .owner()
                    .same_owner(&root_registration));
                let original_pool = registration.observe().unwrap().slots()[0]
                    .original()
                    .clone();
                let driver = registration.observe().unwrap().slots()[0]
                    .driver()
                    .unwrap()
                    .1
                    .clone();
                close_flow(&proxy, flow).await;
                assert!(original_pool.closed().is_none());
                server.finish().await;
                proxy.protocols.shutdown_components(end()).await.unwrap();
                assert!(driver.is_joined());
                assert!(original_pool.matches_closed(&original_pool.closed().unwrap()));
                assert!(root_original.matches_closed(&root_original.closed().unwrap()));
                assert!(root_registration
                    .closed()
                    .unwrap()
                    .matches(&root_registration));
            })
            .await;
        first.close().await;
        suffix.close().await;
        pool.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_pooled_relay_pending_suffix_replacement_waits_for_original_pool() {
    timeout(Duration::from_secs(35),async {
        let old=Pool::start("vmess","pool").await;let next=Pool::start("vmess","pool").await;
        let mut held=Prefix::new(Kind::Socks,true).await;let ready=Prefix::new(Kind::Socks,false).await;
        let (proxy,server)=start_config_mode(false,false,"aes-128-gcm","node",vec![b"owned".to_vec()],1,false,false,Box::new(|config|configure(config,&[old.node.clone(),held.outbound("suffix")],false))).await;
        assert_standard_relay(&proxy);
        let config=proxy.engine().runtime_snapshot().config().clone();let running=proxy.spawn();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait((*config).clone(),Duration::from_secs(5)).await.unwrap();
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();let prior=plan.configured("node").unwrap();let registration=pool_owner(&proxy,"pool");let consumer=original(&proxy);
        let mut flow=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("pending").await.unwrap();
        let opening=flow.dispatch(input(53,b"owned"));tokio::pin!(opening);
        tokio::select! {biased;_=&mut opening=>panic!("actual CONNECT reply is still withheld"),_=held.request()=>{}}
        poll_fn(|cx| {assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        let observed=registration.observe().unwrap();assert_eq!(observed.retained(),1);assert!(observed.slots()[0].native().is_some());let physical=observed.slots()[0].original().clone();let driver=observed.slots()[0].driver().unwrap().1.clone();
        let view=consumer.observe().unwrap();assert!(view.slots()[0].physical_view_live());assert!(view.slots()[0].driver().is_none());assert!(view.slots()[0].borrowed_origin().unwrap().owner().same_owner(&registration));assert!(physical.closed().is_none());
        let mut candidate=(*config).clone();candidate.outbounds[1]=next.node.clone();candidate.outbounds[2]=ready.outbound("suffix");
        let applying=tokio::spawn(async move {handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(10)).await});
        assert!(opening.await.is_err());applying.await.unwrap().unwrap();
        assert!(driver.is_joined());assert!(physical.matches_closed(&physical.closed().unwrap()));assert!(registration.closed().unwrap().matches(&registration));assert!(consumer.closed().unwrap().matches(&consumer));
        let fresh=proxy.tcp_runtime_services().components.clone().unwrap();assert_ne!(fresh.configured("node").unwrap().instance_id(),prior.instance_id());assert!(prior.datagram(end()).await.is_err());assert_eq!(pool_owner(&proxy,"pool").observe().unwrap().retained(),0,"new physical pool starts only on demand after confirmed old close");
        let mut flow=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("fresh").await.unwrap();flow.dispatch(input(53,b"owned")).await.unwrap();assert_eq!(response(&mut flow).await.2,b"owned");close_flow(&proxy,flow).await;
        server.finish().await;running.shutdown().await.unwrap();old.close().await;next.close().await;held.close().await;ready.close().await;
    }).await.unwrap();
}
