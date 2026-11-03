//! Ordinary catalog, real shared-prefix original and actual close credentials.
use super::pool_fixture::*;
use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_mux_prefix_uses_one_hosted_original_for_two_real_h2_clients() {
    timeout(Duration::from_secs(30), async {
        let pool = Pool::start("vmess", "pool").await;
        let (proxy, server) = start_config_mode(
            true,
            true,
            "aes-128-gcm",
            "node",
            vec![b"ordinary".to_vec()],
            2,
            true,
            false,
            Box::new(|config| {
                configure(config, &[pool.node.clone()], false);
                config.runtime.native_mux_max_carriers = Some(2);
            }),
        )
        .await;
        assert_standard_relay(&proxy);
        proxy
            .protocols
            .drive_component_supervision(async {
                let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                let slot = plan.configured("pool").unwrap();
                assert!(slot
                    .recipe
                    .spec()
                    .roles
                    .contains(&zero_platform_tokio::contract::component::RoleKind::Stream));
                assert!(
                    !slot
                        .recipe
                        .spec()
                        .roles
                        .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram),
                    "keep the established compatible packet codec"
                );
                let registration = pool_owner(&proxy, "pool");
                assert_eq!(registration.limit().get(), 2);
                let mut first = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("first")
                    .await
                    .unwrap();
                let id = first.dispatch(input(53, b"ordinary")).await.unwrap();
                let packet = response(&mut first).await;
                assert_eq!(packet.2, b"ordinary");
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
                assert_eq!(session.outbound_tx_bytes, 17);
                assert_eq!(session.outbound_rx_bytes, 8);
                let view = registration.observe().unwrap();
                assert_eq!(view.retained(), 1);
                let native = view.slots()[0].original().clone();
                let driver = view.slots()[0].driver().unwrap().1.clone();
                assert!(view.slots()[0].native().is_some());
                let consumer = original(&proxy);
                let view = consumer.observe().unwrap();
                assert!(view.slots()[0].native().is_none());
                assert!(view.slots()[0]
                    .borrowed_origin()
                    .unwrap()
                    .owner()
                    .same_owner(&registration));
                let loan = view.slots()[0].original().clone();
                let mut second = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("second")
                    .await
                    .unwrap();
                second.dispatch(input(53, b"ordinary")).await.unwrap();
                assert_eq!(response(&mut second).await.2, b"ordinary");
                assert_eq!(registration.observe().unwrap().retained(), 1);
                close_flow(&proxy, first).await;
                assert!(loan.closed().is_some());
                assert!(native.closed().is_none());
                assert!(!driver.is_joined());
                close_flow(&proxy, second).await;
                server.finish().await;
                proxy.protocols.shutdown_components(end()).await.unwrap();
                assert!(native.matches_closed(&native.closed().unwrap()));
                assert!(driver.matches_joined(&driver.try_joined().unwrap()));
                assert!(registration.closed().unwrap().matches(&registration));
            })
            .await;
        pool.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_mux_prefix_compat_udp_uses_actual_pool_without_blocking_other_legacy_node(
) {
    timeout(Duration::from_secs(35),async {
        let pool=Pool::start("vmess","pool").await;
        let echo=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();let port=echo.local_addr().unwrap().port();
        let stop=zero_platform_tokio::contract::CancellationToken::new();let cancel=stop.clone();
        let worker=tokio::spawn(async move {
            let mut buf=vec![0;65535];loop{tokio::select!{biased;_=cancel.cancelled()=>break,
                value=echo.recv_from(&mut buf)=>{let(n,peer)=value.unwrap();echo.send_to(&buf[..n],peer).await.unwrap();}
            }}
        });
        let (proxy,server)=start_config_mode(false,false,"aes-128-gcm","node",vec![b"shared".to_vec()],1,false,false,
            Box::new(|config| {
                configure(config,&[pool.node.clone()],false);
                let mut legacy=pool.node.clone();legacy.tag="legacy".into();config.outbounds.push(legacy);
                for tag in ["pool","legacy"] {
                    config.route.rules.push(serde_json::from_value(serde_json::json!({
                        "condition":{"type":"domain","values":[format!("{tag}.test")]},"action":{"type":"route","outbound":tag}
                    })).unwrap());
                }
            })).await;
        proxy.protocols.drive_component_supervision(async {
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();
            assert!(plan.configured("legacy").is_none());
            let registration=pool_owner(&proxy,"pool");assert_eq!(registration.limit().get(),4);
            let mut business=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("business").await.unwrap();
            business.dispatch(input(53,b"shared")).await.unwrap();assert_eq!(response(&mut business).await.2,b"shared");
            let view=registration.observe().unwrap();let original=view.slots()[0].original().clone();let driver=view.slots()[0].driver().unwrap().1.clone();
            for tag in ["legacy","pool"] {
                let mut dispatch=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch(tag).await.unwrap();
                // Darwin's real UDP send buffer rejects 16 KiB by default;
                // use the same 8 KiB wire baseline as existing native MUX tests.
                let mut sent=0_u64;let mut received=0_u64;
                for payload in [b"compatible".to_vec(),vec![],vec![7;8192]] {
                    let mut packet=input(port,&payload);packet.target=Address::Ipv4([127,0,0,1]);packet.route_target=Some(Address::Domain(format!("{tag}.test")));

                    let id=dispatch.dispatch(packet).await.unwrap();
                    let snapshot=proxy.engine().active_sessions().into_iter().find(|s|s.id==id).unwrap();
                    assert_eq!(snapshot.outbound_tag.as_deref(),Some(tag));

                    let packet=response(&mut dispatch).await;assert_eq!(packet.2,payload);
                    crate::runtime::udp_delivery::record_chain_udp_response_parts(&proxy.udp_runtime_services(),&dispatch,packet.0,packet.1,packet.2,packet.3);
                    let session=proxy.engine().active_sessions().into_iter().find(|s|s.id==id).unwrap();
                    sent+=payload.len() as u64+9;received+=payload.len() as u64;
                    assert_eq!(session.outbound_tx_bytes,sent);
                    assert_eq!(session.outbound_rx_bytes,received);
                }
                close_flow(&proxy,dispatch).await;
            }
            assert_eq!(registration.observe().unwrap().retained(),1,"compatible UDP must not spawn another pool driver");
            assert!(original.closed().is_none());assert!(!driver.is_joined());
            close_flow(&proxy,business).await;server.finish().await;
            proxy.protocols.shutdown_components(end()).await.unwrap();
            assert!(original.matches_closed(&original.closed().unwrap()));assert!(driver.is_joined());
        }).await;
        stop.cancel();worker.await.unwrap();pool.close().await;
    }).await.unwrap();
}

#[path = "production/vless_trojan.rs"]
mod vless_trojan;
