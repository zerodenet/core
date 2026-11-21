use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_network_replaces_pending_dns_and_blocks_new_io_until_old_originals_close(
) {
    timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let (proxy, running, _) = start_domain(endpoint.config(&dns, false)).await;
        let server = endpoint.serve(false, 2, vec![b"owned".to_vec()]);
        // Observe the actual DNS socket suspended, rather than assuming a delay means pending I/O.
        let old = {
            let proxy = proxy.clone();
            tokio::spawn(async move {
                crate::runtime::udp_ingress::test_source::run(
                    &proxy,
                    move |_proxy, source| async move {
                        let mut dispatch = source.new_dispatch("old").await.unwrap();
                        let result = dispatch.dispatch(input(53, b"owned")).await;
                        dispatch.finish_all();
                        result.unwrap_err()
                    },
                )
                .await
            })
        };
        let old_request = dns.request().await;
        let prior = pending(&proxy).await;
        proxy.egress_interface_control().invalidate_network();
        let failure = old.await.unwrap();
        let zero_engine::EngineError::Io(error) = failure else {
            panic!("expected the original DNS network failure: {failure:?}");
        };
        let failure = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<zero_platform_tokio::contract::ChannelError>())
            .expect("original DNS network failure");
        use zero_platform_tokio::contract::{DeliveryState, Reason, Stage};
        assert_eq!(failure.stage, Stage::Dial);
        assert_eq!(failure.reason, Reason::StalePlan);
        assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
        let next = tokio::spawn(open(proxy.clone(), 2, b"owned"));
        let request = dns.request().await;
        dns_closed(&prior);
        let current = pending(&proxy).await;
        assert_eq!(
            prior.original.as_ref().unwrap().instance(),
            current.original.as_ref().unwrap().instance()
        );
        assert!(prior
            .task
            .scope_identity()
            .same_scope(&current.task.scope_identity()));
        request.answer(false, false);
        let mut dispatch = next.await.unwrap();
        current.task.wait().await;
        dns_closed(&current);
        let registration = original(&proxy);
        let observed = registration.observe().unwrap();
        let slot = &observed.slots()[0];
        let native = slot.original().clone();
        let driver = slot.driver().unwrap().1.clone();
        assert!(slot.user_view_live());
        proxy.egress_interface_control().invalidate_network();
        let error = dispatch.send(3, b"owned").await.unwrap_err();
        super::super::network::assert_epoch_fenced(error, &registration, &native);
        let closed = timeout(Duration::from_secs(3), async {
            loop {
                if let Some(closed) = native.closed() {
                    break closed;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same original native close while Datagram remains held");
        assert!(native.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(driver.matches_joined(&driver.try_joined().unwrap()));
        let error = dispatch.send(4, b"owned").await.unwrap_err();
        super::super::network::assert_epoch_fenced(error, &registration, &native);
        dns.no_new_request();
        retire(&proxy, dispatch).await;
        let next = tokio::spawn(open(proxy.clone(), 5, b"owned"));
        let request = dns.request().await;
        assert!(driver.try_joined().unwrap().registration_released());
        assert!(registration
            .observe()
            .unwrap()
            .slots()
            .iter()
            .all(|slot| !slot.original().same_original(&native)));
        request.answer(false, false);
        let dispatch = next.await.unwrap();
        retire(&proxy, dispatch).await;
        drop(old_request);
        server.await.unwrap();
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}
