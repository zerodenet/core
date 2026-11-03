use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_relay_native_root_dns_and_network_replacement_wait_for_old_originals()
{
    timeout(Duration::from_secs(20), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let first = Prefix::new(Kind::Socks, false).await;
        let mut prefix = first.outbound("first");
        let zero_config::OutboundProtocolConfig::Socks5 { server, .. } = &mut prefix.protocol
        else {
            unreachable!()
        };
        *server = "root.node.test".into();
        let (proxy, server) = start_config(
            true,
            true,
            "aes-128-gcm",
            "node",
            vec![b"owned".to_vec()],
            2,
            false,
            |config| {
                configure(config, &[prefix], false);
                config.runtime.dns = Some(dns.config());
            },
        )
        .await;
        assert_standard_relay(&proxy);
        proxy
            .protocols
            .drive_component_supervision(async {
                let open = |proxy: Proxy, identity| async move {
                    let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("incoming")
                        .await
                        .unwrap();
                    let mut packet = input(53, b"owned");
                    packet.client_session_id = Some(identity);
                    dispatch.dispatch(packet).await.unwrap();
                    assert_eq!(response(&mut dispatch).await.2, b"owned");
                    dispatch
                };
                let opening = tokio::spawn(open(proxy.clone(), 1));
                let request = dns.request().await;
                let query = timeout(Duration::from_secs(3), async {
                    loop {
                        if let Some(query) = proxy
                            .resolver
                            .native_query_observations()
                            .into_iter()
                            .find(|q| q.io_pending && q.original.is_some())
                        {
                            break query;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                request.answer(false, false);
                let mut dispatch = opening.await.unwrap();
                query.task.wait().await;
                let dns_native = query.original.as_ref().unwrap();
                assert!(dns_native.closed().unwrap().matches(dns_native));
                let registration = original(&proxy);
                let old = registration.observe().unwrap().slots()[0]
                    .original()
                    .clone();
                proxy.egress_interface_control().invalidate_network();
                let mut packet = input(53, b"owned");
                packet.client_session_id = Some(2);
                assert!(dispatch.dispatch(packet).await.is_err());
                assert!(old.closed().is_none());
                dns.no_new_request();
                assert_eq!(first.count.load(std::sync::atomic::Ordering::SeqCst), 1);
                close_flow(&proxy, dispatch).await;
                assert!(old.matches_closed(&old.closed().unwrap()));
                let opening = tokio::spawn(open(proxy.clone(), 3));
                dns.request().await.answer(false, false);
                let dispatch = opening.await.unwrap();
                close_flow(&proxy, dispatch).await;
                server.finish().await;
                proxy
                    .protocols
                    .shutdown_components(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
            })
            .await;
        first.close().await;
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_relay_delegates_final_domain_to_prefix_and_preserves_ca_identity() {
    timeout(Duration::from_secs(15),async {
        let mut dns=crate::dns_fixture::Dns::new().await;let mut prefix=Prefix::new(Kind::Socks,false).await;
        let (proxy,server)=start_config(true,true,"aes-128-gcm","node",vec![b"owned".to_vec()],1,true,|config| {
            configure(config,&[prefix.outbound("first")],false);config.runtime.dns=Some(dns.config());
            let zero_config::OutboundProtocolConfig::Vmess{server,..}=&mut config.outbounds[0].protocol else{unreachable!()};*server="final.relay.test".into();
        }).await;
        assert_standard_relay(&proxy);
        proxy.protocols.drive_component_supervision(async {
            let mut dispatch=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("incoming").await.unwrap();dispatch.dispatch(input(53,b"owned")).await.unwrap();assert_eq!(response(&mut dispatch).await.2,b"owned");
            let target=prefix.request().await;assert_eq!(target.address,Address::Domain("final.relay.test".into()));
            dns.no_new_request();assert!(proxy.resolver.native_query_observations().is_empty(),"the final proxy name is resolved by the selected prefix, not by a root DNS escape");
            let registration=original(&proxy);let native=registration.observe().unwrap().slots()[0].original().clone();
            let completed=dispatch.finish_all();assert_eq!(completed[0].upstream.as_ref().unwrap().0,"final.relay.test");
            assert!(proxy.tcp_runtime_services().execution().retired_datagrams().progress_retired(Instant::now()+Duration::from_secs(3)).await.unwrap().is_complete());
            assert!(native.matches_closed(&native.closed().unwrap()));server.finish().await;proxy.protocols.shutdown_components(Instant::now()+Duration::from_secs(3)).await.unwrap();
        }).await;prefix.close().await;dns.close().await;
    }).await.unwrap();
}
