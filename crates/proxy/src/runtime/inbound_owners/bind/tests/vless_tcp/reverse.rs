use super::*;
#[tokio::test]
async fn vless_tcp_reverse_control_uses_standard_production_owner_before_restart() {
    timeout(Duration::from_secs(15), async {
        let proxy = fixture::proxy(
            serde_json::json!({"type":"vless","users":[{"id":USER,"reverse_tag":"portal"}]}),
        )
        .await;
        for _ in 0..2 {
            let (owner, _stop, address) = fixture::install(&proxy);
            let scope = owner.original_scope_for_test();
            let census = owner
                .task_access()
                .peer_registrar()
                .retirement_observation()
                .unwrap();
            let business = async {
                owner.wait_ready().await.unwrap();
                let main = owner.task_access().source_main_witness().await.unwrap();
                let mut bridge = TcpStream::connect(address).await.unwrap();
                let mut header = vec![0];
                header.extend(vless::parse_uuid(USER).unwrap());
                header.extend([0, 4]);
                bridge.write_all(&header).await.unwrap();
                let mut response = [0; 2];
                bridge.read_exact(&mut response).await.unwrap();
                assert_eq!(response, [0, 0]);
                let size = bridge.read_u16().await.unwrap();
                let mut frame = vec![0; size as usize];
                bridge.read_exact(&mut frame).await.unwrap();
                assert_eq!(&frame[..5], &[0, 1, 1, 1, 2]); // the real reverse heartbeat's NEW/UDP
                let size = bridge.read_u16().await.unwrap();
                let mut control = vec![0; size as usize];
                bridge.read_exact(&mut control).await.unwrap();
                assert!(proxy.engine().active_sessions().is_empty());
                assert!(proxy.engine().completed_sessions().is_empty());
                let plan = proxy.tcp_runtime_services().components.clone().unwrap();
                let provider = plan
                    .configured("portal")
                    .unwrap()
                    .accepted_carrier_provider()
                    .unwrap();
                let originals = provider.originals();
                assert_eq!(originals.len(), 1);
                let original = originals.into_iter().next().unwrap();
                assert!(provider.sources().contains(&owner.identity()));
                assert!(!provider.source_originals_returned(owner.identity()));
                assert!(!original.accepted().unwrap().is_settled());
                assert!(original.closed().is_none());
                let mut byte = [0];
                let mut waiting = Box::pin(bridge.read(&mut byte));
                std::future::poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                owner.task_access().request_stop();
                assert_eq!(waiting.await.unwrap(), 0);
                (main, provider, original)
            };
            let (observed, (main, provider, original)) =
                tokio::join!(owner.observe_listener(), business);
            observed.unwrap();
            // The Main, accepted caller and Portal driver are the exact
            // registrations for this connection. All must release their ACK.
            fixture::retire(&proxy, 3, 2).await;
            let joined = main.try_joined().unwrap();
            assert!(main.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
            assert!(provider.sources().contains(&owner.identity()));
            assert!(provider.source_originals_returned(owner.identity()));
            let closed = original
                .closed()
                .expect("exact accepted TCP original closed");
            assert!(closed.matches(&original) && closed.had_native_acquisition());
            assert!(original.accepted().unwrap().is_settled());
            assert!(!scope.resource_open());
            assert!(census.is_drained());
        }
    })
    .await
    .unwrap();
}
