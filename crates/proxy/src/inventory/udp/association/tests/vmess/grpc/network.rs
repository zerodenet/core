use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_datagram_waits_for_old_original_before_acquiring_on_new_network() {
    timeout(Duration::from_secs(12), async {
        let (proxy, server) = start_count(
            false,
            false,
            "aes-128-gcm",
            "node",
            vec![b"network".to_vec()],
            2,
        )
        .await;
        proxy
            .protocols
            .drive_component_supervision(async {
                let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
                let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
                dispatch.dispatch(input(53, b"network")).await.unwrap();
                assert_eq!(response(&mut dispatch).await.2, b"network");
                let registration = original(&proxy);
                let first = registration.observe().unwrap().slots()[0]
                    .original()
                    .clone();
                proxy.egress_interface_control().invalidate_network();
                let mut packet = input(53, b"network");
                packet.client_session_id = Some(9);
                assert!(
                    dispatch.dispatch(packet).await.is_err(),
                    "new network cannot acquire ahead of old originals"
                );
                assert!(first.closed().is_none());
                assert_eq!(registration.observe().unwrap().slots().len(), 1);
                let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
                dispatch.finish_all();
                let closed = retired
                    .progress_retired(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
                assert!(closed.is_complete(), "{closed:?}");
                assert!(first.matches_closed(&first.closed().unwrap()));
                let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
                let mut packet = input(53, b"network");
                packet.client_session_id = Some(10);
                dispatch.dispatch(packet).await.unwrap();
                assert_eq!(response(&mut dispatch).await.2, b"network");
                dispatch.finish_all();
                assert!(retired
                    .progress_retired(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap()
                    .is_complete());
                server.finish().await;
                proxy
                    .protocols
                    .shutdown_components(Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
            })
            .await;
    })
    .await
    .unwrap();
}
