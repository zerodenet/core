//! Both newly admitted prefixes use actual remote listeners and original custody.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
fn node(pool: &Pool) -> zero_config::OutboundConfig {
    let mut input = pool.node.clone();
    if let zero_config::OutboundProtocolConfig::Vless {
        mux_concurrency,
        xudp_concurrency,
        ..
    } = &mut input.protocol
    {
        *xudp_concurrency = *mux_concurrency;
    }
    input
}
#[tokio::test]
async fn ordinary_vless_trojan_mux_prefix_loans_share_one_original_for_real_grpc_tls_udp() {
    timeout(Duration::from_secs(45), async {
        for protocol in ["vless", "trojan"] {
            let pool = Pool::start(protocol, "pool").await;
            let input_node = node(&pool);
            let (proxy, server) = start_config_mode(
                true,
                true,
                "aes-128-gcm",
                "node",
                vec![b"borrowed".to_vec()],
                2,
                true,
                false,
                Box::new(|config| {
                    configure(config, std::slice::from_ref(&input_node), false);
                    config.runtime.native_mux_max_carriers = Some(2);
                }),
            )
            .await;
            assert_standard_relay(&proxy);
            proxy
                .protocols
                .drive_component_supervision(async {
                    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                    assert_eq!(
                        plan.configured("pool").unwrap().recipe.spec().roles,
                        vec![zero_platform_tokio::contract::component::RoleKind::Stream]
                    );
                    let registration = pool_owner(&proxy, "pool");
                    assert_eq!(registration.limit().get(), 2);
                    let mut first = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("first")
                        .await
                        .unwrap();
                    let id = first.dispatch(input(53, b"borrowed")).await.unwrap();
                    let packet = response(&mut first).await;
                    assert_eq!(packet.2.as_ref(), b"borrowed");
                    crate::runtime::udp_delivery::record_chain_udp_response_parts(
                        &proxy.udp_runtime_services(),
                        &first,
                        packet.0,
                        packet.1,
                        packet.2,
                        packet.3,
                    );
                    let active = proxy
                        .engine()
                        .active_sessions()
                        .into_iter()
                        .find(|s| s.id == id)
                        .unwrap();
                    assert_eq!(active.outbound_tx_bytes, 17);
                    assert_eq!(active.outbound_rx_bytes, 8);
                    let observed = registration.observe().unwrap();
                    assert_eq!(observed.retained(), 1);
                    assert!(observed.slots()[0].native().is_some());
                    let physical = observed.slots()[0].original().clone();
                    let driver = observed.slots()[0].driver().unwrap().1.clone();
                    let consumer = original(&proxy);
                    let observed = consumer.observe().unwrap();
                    assert!(observed.slots()[0].native().is_none());
                    assert!(observed.slots()[0]
                        .borrowed_origin()
                        .unwrap()
                        .owner()
                        .same_owner(&registration));
                    let loan = observed.slots()[0].original().clone();
                    let mut second = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                        .new_dispatch("second")
                        .await
                        .unwrap();
                    second.dispatch(input(53, b"borrowed")).await.unwrap();
                    assert_eq!(response(&mut second).await.2.as_ref(), b"borrowed");
                    assert_eq!(registration.observe().unwrap().retained(), 1);
                    close_flow(&proxy, first).await;
                    assert!(loan.closed().is_some());
                    assert!(physical.closed().is_none());
                    assert!(!driver.is_joined());
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
async fn ordinary_vless_trojan_mux_prefix_udp_keeps_actual_pool_accounting_and_strict_packet_admission(
) {
    timeout(Duration::from_secs(50),async {
        for protocol in ["vless","trojan"] {
            let pool=Pool::start(protocol,"pool").await;let input_node=node(&pool);
            let echo=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();let port=echo.local_addr().unwrap().port();
            let stop=zero_platform_tokio::contract::CancellationToken::new();let cancel=stop.clone();
            let worker=tokio::spawn(async move {let mut buf=vec![0;65535];loop{tokio::select!{biased;_=cancel.cancelled()=>break,
                packet=echo.recv_from(&mut buf)=>{let(n,peer)=packet.unwrap();echo.send_to(&buf[..n],peer).await.unwrap();}
            }}});
            let (proxy,server)=start_config_mode(false,false,"aes-128-gcm","node",vec![b"shared".to_vec()],1,false,false,Box::new(|config| {
                configure(config,std::slice::from_ref(&input_node),false);let mut legacy=input_node.clone();legacy.tag="legacy".into();config.outbounds.push(legacy);
                for (name,tag,mode) in [("legacy","legacy","flow"),("pool","pool","flow"),("packet","pool","packet"),("legacy_packet","legacy","packet")] {
                    config.route.rules.push(serde_json::from_value(serde_json::json!({"condition":{"type":"domain","values":[format!("{name}.test")]},"action":{"type":"route","outbound":tag},"mode":mode})).unwrap());
                }
            })).await;
            proxy.protocols.drive_component_supervision(async {
                assert!(proxy.tcp_runtime_services().components.as_ref().unwrap().configured("legacy").is_none());
                let registration=pool_owner(&proxy,"pool");assert_eq!(registration.limit().get(),4);
                let mut business=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("business").await.unwrap();
                business.dispatch(input(53,b"shared")).await.unwrap();assert_eq!(response(&mut business).await.2.as_ref(),b"shared");
                let view=registration.observe().unwrap();let physical=view.slots()[0].original().clone();let driver=view.slots()[0].driver().unwrap().1.clone();
                let mut retained_sender=None;
                for name in ["legacy","pool"] {
                    let mut dispatch=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch(name).await.unwrap();
                    let mut payloads=vec![b"same-pool".to_vec(),vec![7;8192]];if protocol=="vless" {payloads.insert(1,vec![]);}
                    let mut total=0_u64;
                    for payload in payloads {
                        let mut packet=input(port,&payload);packet.target=Address::Ipv4([127,0,0,1]);packet.route_target=Some(Address::Domain(format!("{name}.test")));
                        let id=dispatch.dispatch(packet).await.unwrap();let packet=response(&mut dispatch).await;assert_eq!(packet.2.as_ref(),payload,"{protocol}/{name}");
                        crate::runtime::udp_delivery::record_chain_udp_response_parts(&proxy.udp_runtime_services(),&dispatch,packet.0,packet.1,packet.2,packet.3);
                        total+=payload.len() as u64;let active=proxy.engine().active_sessions().into_iter().find(|s|s.id==id).unwrap();
                        assert_eq!(active.outbound_tag.as_deref(),Some(if name=="legacy" {"legacy"} else {"pool"}));assert_eq!(active.outbound_tx_bytes,total);assert_eq!(active.outbound_rx_bytes,total);
                    }
                    if name=="pool" {retained_sender=Some(dispatch);} else {close_flow(&proxy,dispatch).await;}
                }
                for name in ["legacy_packet","packet"] {
                    let mut strict=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch(name).await.unwrap();
                    let mut packet=input(port,b"strict");packet.target=Address::Ipv4([127,0,0,1]);packet.route_target=Some(Address::Domain(format!("{name}.test")));
                    let error=strict.dispatch(packet).await.unwrap_err();assert!(error.to_string().contains("no executable UDP path for route mode Packet"));strict.finish_all();
                }
                let mut promoted=(**proxy.engine().runtime_snapshot().config()).clone();
                let zero_config::OutboundGroupKind::Relay{proxies}=&mut promoted.outbound_groups.iter_mut().find(|g|g.tag=="chain").unwrap().group else {unreachable!()};proxies[0]="legacy".into();
                let cold=proxy.protocols.prepare_components(std::sync::Arc::new(promoted)).unwrap();
                assert!(cold.configured("legacy").is_none(),"logical close/cache reuse cannot adopt an already exposed legacy pool");drop(cold);
                assert_eq!(registration.observe().unwrap().retained(),1,"no compatibility physical pool may be started");assert!(physical.closed().is_none());
                close_flow(&proxy,business).await;server.finish().await;proxy.protocols.shutdown_components(end()).await.unwrap();
                assert!(physical.matches_closed(&physical.closed().unwrap()));assert!(driver.is_joined());assert!(registration.closed().unwrap().matches(&registration));
                // The actual cached producer/flow remains held throughout the
                // original's close. Its old writer must not pin the native pool.
                let mut retained=retained_sender.unwrap();let mut stale=input(port,b"retired");stale.target=Address::Ipv4([127,0,0,1]);stale.route_target=Some(Address::Domain("pool.test".into()));
                assert!(retained.dispatch(stale).await.is_err());retained.finish_all();
            }).await;
            stop.cancel();worker.await.unwrap();pool.close().await;
        }
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_vless_trojan_mux_prefix_pending_suffix_replacement_confirms_original_before_successor(
) {
    timeout(Duration::from_secs(55),async {
        for protocol in ["vless","trojan"] {
            let old=Pool::start(protocol,"pool").await;let next=Pool::start(protocol,"pool").await;let old_node=node(&old);let mut next_node=node(&next);
            if protocol=="trojan" {
                let mut changed=serde_json::to_value(&old_node).unwrap();changed["protocol"]["ws"]["headers"]=serde_json::json!({"x-native-generation":"next"});
                next_node=serde_json::from_value(changed).unwrap();
            }
            let mut held=Prefix::new(Kind::Socks,true).await;let ready=Prefix::new(Kind::Socks,false).await;
            let (proxy,server)=start_config_mode(false,false,"aes-128-gcm","node",vec![b"replace".to_vec()],1,false,false,Box::new(|config|configure(config,&[old_node.clone(),held.outbound("suffix")],false))).await;
            assert_standard_relay(&proxy);let config=proxy.engine().runtime_snapshot().config().clone();let running=proxy.spawn();
            let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
            handle.apply_runtime_config_and_wait((*config).clone(),Duration::from_secs(5)).await.unwrap();
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();let old_role=plan.configured("node").unwrap();let registration=pool_owner(&proxy,"pool");let consumer=original(&proxy);
            let mut flow=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("pending").await.unwrap();let opening=flow.dispatch(input(53,b"replace"));tokio::pin!(opening);
            tokio::select!{biased;_=&mut opening=>panic!("actual CONNECT reply is withheld"),_=held.request()=>{}}
            poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            let view=registration.observe().unwrap();let physical=view.slots()[0].original().clone();let driver=view.slots()[0].driver().unwrap().1.clone();assert!(view.slots()[0].native().is_some());
            let view=consumer.observe().unwrap();assert!(view.slots()[0].physical_view_live());assert!(view.slots()[0].driver().is_none());assert!(view.slots()[0].borrowed_origin().unwrap().owner().same_owner(&registration));
            let mut candidate=(*config).clone();candidate.outbounds[1]=next_node.clone();candidate.outbounds[2]=ready.outbound("suffix");
            let applying=tokio::spawn(async move{handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(10)).await});assert!(opening.await.is_err());applying.await.unwrap().unwrap();
            assert!(physical.matches_closed(&physical.closed().unwrap()));assert!(driver.is_joined());assert!(registration.closed().unwrap().matches(&registration));assert!(consumer.closed().unwrap().matches(&consumer));
            assert!(old_role.datagram(end()).await.is_err());assert_eq!(pool_owner(&proxy,"pool").observe().unwrap().retained(),0);
            let mut fresh=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("fresh").await.unwrap();fresh.dispatch(input(53,b"replace")).await.unwrap();assert_eq!(response(&mut fresh).await.2.as_ref(),b"replace");close_flow(&proxy,fresh).await;
            server.finish().await;running.shutdown().await.unwrap();old.close().await;next.close().await;held.close().await;ready.close().await;
        }
    }).await.unwrap();
}
