//! Ordinary TCP, UDP and Service use the same actual Node owner and Scope.
use super::*;
use zero_platform_tokio::contract::{component::ResourceDeclaration, Reason};

#[path = "stream/fixtures.rs"]
mod fixtures;

#[tokio::test]
async fn ordinary_vmess_grpc_stream_datagram_and_service_keep_one_managed_scope_and_matching_originals(
) {
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy_with_configure(
            serde_json::json!({
                "type":"vmess","server":"127.0.0.1","port":listener.local_addr().unwrap().port(),
                "id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["same.owner/Tun"],"multi_mode":true}
            }),
            serde_json::json!({"final":{"type":"route","outbound":"node"}}),
            true,
            None,
            |config| config.runtime.vmess_grpc_max_originals = Some(2),
        )
        .await;
        assert_standard_datagram(&proxy);
        assert_no_legacy_udp(&proxy);
        let slot = proxy.tcp_runtime_services().components.as_ref().unwrap().configured("node").unwrap();
        let business = original(&proxy);
        let ordinal = slot.recipe.spec().resources.iter().position(|resource| {
            matches!(resource, ResourceDeclaration::ControlledServiceStream { .. })
        }).unwrap();
        let service = slot.original_carrier_owner(ordinal).unwrap();
        assert_eq!(business.instance(), service.instance());
        assert_eq!(business.observe().unwrap().retained(), 0);
        assert_eq!(service.observe().unwrap().retained(), 0);
        assert_eq!(business.limit().get(), 2);
        assert_eq!(service.limit().get(), 2);
        assert_eq!(slot.recipe.spec().task_limit.unwrap().get(), 2);

        let (release, released) = tokio::sync::watch::channel(false);
        let peer = tokio::spawn(fixtures::server(listener, released));
        let business_owner = business.clone();
        let service_owner = service.clone();
        let originals = crate::runtime::udp_ingress::test_source::run(
            &proxy,
            move |proxy, source| async move {
                let (return_tcp, tcp_source) = fixtures::business(&proxy).await;
                let mut control = fixtures::service(&proxy).await;
                fixtures::exchange(&mut control).await;

                let a = business_owner.observe().unwrap();
                let b = service_owner.observe().unwrap();
                assert_eq!(a.retained(), 1);
                assert_eq!(b.retained(), 1);
                let scope = a.slots()[0].driver().unwrap().1.scope_identity();
                let mut originals = Vec::new();
                for slot in a.slots().iter().chain(b.slots()) {
                    let driver = &slot.driver().unwrap().1;
                    assert!(scope.same_scope(&driver.scope_identity()));
                    assert!(slot.native().is_some());
                    assert!(slot.borrowed_origin().is_none());
                    originals.push((slot.original().clone(), driver.clone()));
                }

                let mut udp = source.new_dispatch("incoming").await.unwrap();
                let error = udp.dispatch(input(53, b"live")).await.unwrap_err();
                assert!(
                    crate::transport::channel_failure(&error)
                        .is_some_and(|source| source.reason == Reason::Capacity),
                    "{error}"
                );
                assert_eq!(business_owner.observe().unwrap().retained(), 1);
                assert_eq!(service_owner.observe().unwrap().retained(), 1);
                assert!(originals.iter().all(|(native, driver)| {
                    native.closed().is_none() && !driver.is_joined()
                }));

                return_tcp.send(()).unwrap();
                tcp_source.await.unwrap();
                let (native, driver) = &originals[0];
                assert!(driver.matches_joined(&driver.wait_registration_released().await));

                let mut packet = input(53, b"live");
                packet.client_session_id = Some(2);
                udp.dispatch(packet).await.unwrap();
                assert_eq!(response(&mut udp).await.2.as_ref(), b"live");
                assert!(native.matches_closed(&native.closed().unwrap()));
                let next = business_owner.observe().unwrap();
                assert_eq!(next.retained(), 1);
                let next = &next.slots()[0];
                let next_driver = &next.driver().unwrap().1;
                assert!(scope.same_scope(&next_driver.scope_identity()));
                originals.push((next.original().clone(), next_driver.clone()));

                drop(control);
                udp.finish_all();
                originals
            },
        )
        .await;

        proxy.protocols.shutdown_components(Instant::now() + Duration::from_secs(3)).await.unwrap();
        for (native, driver) in originals {
            assert!(native.matches_closed(&native.closed().unwrap()));
            assert!(driver.matches_joined(&driver.try_joined().unwrap()));
        }
        assert!(business.closed().unwrap().matches(&business));
        assert!(business.users_closed().unwrap().matches(&business));
        assert!(service.closed().unwrap().matches(&service));
        assert!(service.users_closed().unwrap().matches(&service));
        release.send_replace(true);
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[path = "stream/budget_replacement.rs"]
mod budget_replacement;
