use super::*;
#[tokio::test]
async fn vless_tcp_reverse_control_uses_standard_production_owner_before_restart() {
    timeout(Duration::from_secs(15), async {
        let proxy = fixture::proxy(
            serde_json::json!({"type":"vless","users":[{"id":USER,"reverse_tag":"portal"}]}),
        )
        .await;
        for _ in 0..2 {
            let (owner, stop, address) = fixture::install(&proxy);
            let business = async {
                owner.wait_ready().await.unwrap();
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
                let mut byte = [0];
                let mut waiting = Box::pin(bridge.read(&mut byte));
                std::future::poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                stop.send_replace(true);
                assert_eq!(waiting.await.unwrap(), 0);
            };
            let (observed, ()) = tokio::join!(owner.observe_driver(), business);
            observed.unwrap();
            fixture::retire(&proxy, 4, 2).await;
        }
    })
    .await
    .unwrap();
}
