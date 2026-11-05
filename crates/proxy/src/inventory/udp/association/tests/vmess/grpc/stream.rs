//! Ordinary TCP, UDP and Service use the same actual Node owner and Scope.
use super::*;
mod fixtures;
#[tokio::test]
async fn ordinary_vmess_grpc_stream_datagram_and_service_keep_one_managed_scope_and_matching_originals(
) {
    timeout(Duration::from_secs(20),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy=proxy_with_configure(serde_json::json!({
            "type":"vmess","server":"127.0.0.1","port":listener.local_addr().unwrap().port(),
            "id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["same.owner/Tun"],"multi_mode":true}
        }),serde_json::json!({"final":{"type":"route","outbound":"node"}}),true,None,|config| config.runtime.vmess_grpc_max_originals=Some(2)).await;
        assert_no_legacy_udp(&proxy);
        let slot=proxy.tcp_runtime_services().components.as_ref().unwrap().configured("node").unwrap();
        let business=original(&proxy);
        let ordinal=slot.recipe.spec().resources.iter().position(|resource|matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{..})).unwrap();
        let service=slot.original_carrier_owner(ordinal).unwrap();
        assert_eq!(business.instance(),service.instance());
        assert_eq!(business.observe().unwrap().retained(),0,"no preallocation");
        assert_eq!(service.observe().unwrap().retained(),0,"no preallocation");
        let (release,released)=tokio::sync::watch::channel(false);
        let peer=tokio::spawn(fixtures::server(listener,released));
        proxy.protocols.drive_component_supervision(async {
            let mut tcp=fixtures::business(&proxy).await;fixtures::exchange(&mut tcp).await;
            let mut control=fixtures::service(&proxy).await;fixtures::exchange(&mut control).await;
            let a=business.observe().unwrap();let b=service.observe().unwrap();
            assert_eq!(a.retained(),1);assert_eq!(b.retained(),1);
            assert_eq!(business.limit().get(),2);assert_eq!(service.limit().get(),2);
            assert_eq!(slot.recipe.spec().task_limit.unwrap().get(),2);
            let slots=a.slots().iter().chain(b.slots()).collect::<Vec<_>>();
            let scope=slots[0].driver().unwrap().1.scope_identity();
            let mut originals=slots.iter().map(|slot| {
                assert!(scope.same_scope(&slot.driver().unwrap().1.scope_identity()));
                assert!(slot.native().is_some());assert!(slot.borrowed_origin().is_none());
                (slot.original().clone(),slot.driver().unwrap().1.clone())
            }).collect::<Vec<_>>();
            let mut refused=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("refused").await.unwrap();
            let error=refused.dispatch(input(53,b"live")).await.unwrap_err();
            assert!(crate::transport::channel_failure(&error).is_some_and(|source|source.reason==zero_platform_tokio::contract::Reason::Capacity),"{error}");
            assert_eq!(business.observe().unwrap().retained(),1);
            assert_eq!(service.observe().unwrap().retained(),1,"cannot create a second budget for Service");
            assert!(originals.iter().all(|(native,driver)|native.closed().is_none()&&!driver.is_joined()),"refusal must not evict live users");
            refused.finish_all();
            drop(tcp);
            let mut udp=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("incoming").await.unwrap();
            udp.dispatch(input(53,b"live")).await.unwrap();assert_eq!(response(&mut udp).await.2,b"live");
            assert!(originals[0].0.matches_closed(&originals[0].0.closed().unwrap()));
            assert!(originals[0].1.matches_joined(&originals[0].1.try_joined().unwrap()));
            let next=business.observe().unwrap();assert_eq!(next.retained(),1);
            let next=&next.slots()[0];
            assert!(scope.same_scope(&next.driver().unwrap().1.scope_identity()));
            originals.push((next.original().clone(),next.driver().unwrap().1.clone()));
            drop(control);
            let retired=proxy.tcp_runtime_services().execution().retired_datagrams();udp.finish_all();
            assert!(retired.wait_closed(Instant::now()+Duration::from_secs(3)).await.unwrap().is_complete());
            proxy.protocols.shutdown_components(Instant::now()+Duration::from_secs(3)).await.unwrap();
            for (native,driver) in originals {
                assert!(native.matches_closed(&native.closed().unwrap()));
                assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            }
            assert!(business.closed().unwrap().matches(&business));
            assert!(business.users_closed().unwrap().matches(&business));
            assert!(service.closed().unwrap().matches(&service));
            assert!(service.users_closed().unwrap().matches(&service));
            release.send_replace(true);peer.await.unwrap();
        }).await;
    }).await.unwrap();
}

#[path = "stream/budget_replacement.rs"]
mod budget_replacement;
