use super::*;

#[tokio::test]
async fn ordinary_vmess_grpc_datagram_keeps_native_admission_after_business_cancellation_until_original_close(
) {
    let (proxy, server) = start(false, true, "aes-128-gcm", "node", vec![b"cancel".to_vec()]).await;
    proxy
        .protocols
        .drive_component_supervision(async {
            let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
            let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
            let id = dispatch.dispatch(input(53, b"cancel")).await.unwrap();
            assert_eq!(response(&mut dispatch).await.2.as_ref(), b"cancel");
            let original = original(&proxy);
            let observed = original.observe().unwrap();
            let native = observed.slots()[0].original().clone();
            proxy.engine().close_flow(&id.to_string()).unwrap();
            assert!(
                native.closed().is_none(),
                "Engine cancellation is not original native closure"
            );
            let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
            dispatch.finish_all();
            let report = retired
                .wait_closed(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            assert!(report.is_complete(), "{report:?}");
            assert!(native.matches_closed(&native.closed().unwrap()));
            server.finish().await;
            proxy
                .protocols
                .shutdown_components(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            assert!(original.closed().unwrap().matches(&original));
        })
        .await;
}
