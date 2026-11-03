//! The existing host must retire the last caller in VMess-only builds.
use super::*;
use zero_platform_tokio::contract::Reason;

#[tokio::test]
async fn ordinary_vmess_datagram_host_reclaims_last_original_without_stopping_admission() {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy(
            listener.local_addr().unwrap().port(),
            serde_json::json!({"final":{"type":"route","outbound":"node"}}),
            "aes-128-gcm",
        )
        .await;
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
                ID.into(),
                "aes-128-gcm".into(),
                None::<String>,
                None,
                None,
                None,
                None,
                None,
            )])
            .unwrap();
            let (_, mut stream) = profile
                .accept_request_stream(
                    vmess::inbound::VmessInbound,
                    zero_platform_tokio::TokioSocket::new(stream),
                )
                .await
                .unwrap();
            let len = stream.read_u16().await.unwrap();
            let mut body = vec![0; len as usize];
            stream.read_exact(&mut body).await.unwrap();
            stream.write_u16(len).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.flush().await.unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
            eof_tx.send(()).unwrap();
            release_rx.await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
        let id = dispatch.dispatch(input(53, b"retire")).await.unwrap();
        {
            let (_, tasks, _) = dispatch.poll_sockets();
            let packet = tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
            assert_eq!(packet.2, b"retire");
        }
        let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
        // Fill only real admission reservations, not sockets or dummy controls.
        // Thus only the original VMess controller can free the missing slot.
        let mut metadata = Vec::new();
        let mut bounded = false;
        for _ in 0..10000 {
            match retired.reserve() {
                Ok(reservation) => metadata.push(reservation),
                Err(error) => {
                    assert_eq!(error.reason, Reason::Capacity);
                    bounded = true;
                    break;
                }
            }
        }
        assert!(bounded, "the actual host owner must have a finite budget");
        proxy.engine().close_flow(&id.to_string()).unwrap();
        assert!(!dispatch.finish_pending_cancellations());
        assert!(proxy.engine().active_sessions().is_empty());
        assert_eq!(retired.reserve().err().unwrap().reason, Reason::Capacity);
        let _ = dispatch.finish_all();
        // No UDP dispatcher/receiver remains. This is the same Proxy/host,
        // with its existing orchestration task, not a second protocol instance.
        let running = proxy.spawn();
        eof_rx.await.unwrap();
        assert_eq!(
            retired.reserve().err().unwrap().reason,
            Reason::Capacity,
            "shutdown and returned views cannot substitute for the original report"
        );
        release_tx.send(()).unwrap();
        let replacement = loop {
            match retired.reserve() {
                Ok(reservation) => break reservation,
                Err(error) => {
                    assert_eq!(error.reason, Reason::Capacity);
                    tokio::task::yield_now().await;
                }
            }
        };
        // Normal progress consumed the actual control's completed report. It
        // did not call the global stopping path or manufacture native evidence.
        drop(replacement);
        drop(metadata);
        server.await.unwrap();
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
