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
            drop(stream);
            let (fresh, _) = listener.accept().await.unwrap();
            echo_stream(fresh, "aes-128-gcm".into(), 53, vec![b"fresh".to_vec()]).await;
        });
        let mut flow = super::grpc::flow::open(proxy.clone(), 1, b"retire").await;
        let originals = capture_native_originals(&proxy);
        assert_eq!(originals.len(), 1);
        let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
        // Fill only actual host reservations, never dummy sockets or controls.
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
        assert!(bounded, "the actual host owner has a finite budget");
        proxy
            .engine()
            .close_flow(&flow.session().to_string())
            .unwrap();
        assert!(!flow.finish_pending_cancellations().await);
        assert!(proxy.engine().active_sessions().is_empty());
        assert_eq!(retired.reserve().err().unwrap().reason, Reason::Capacity);
        flow.retire().await;
        assert!(originals.iter().all(|original| original.closed().is_none()));
        assert_eq!(
            retired.reserve().err().unwrap().reason,
            Reason::Capacity,
            "the still-live original Source/RX keeps its actual custody debt"
        );
        let running = proxy.spawn();
        let returned = tokio::spawn(flow.finish_all());
        eof_rx.await.unwrap();
        release_tx.send(()).unwrap();
        returned.await.unwrap();
        for original in &originals {
            let closed = original
                .closed()
                .expect("same original native close before admission refund");
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        }
        let replacement = retired
            .reserve()
            .expect("actual owner consumed its same original report");
        drop(replacement);
        // Admission remains running under the same filled host budget. Prove
        // the refunded position with another real wire exchange, not a counter alone.
        let fresh = super::grpc::flow::open(proxy.clone(), 2, b"fresh").await;
        let fresh_originals = capture_native_originals(&proxy);
        assert_eq!(fresh_originals.len(), 1);
        assert!(originals
            .iter()
            .all(|old| !old.same_original(&fresh_originals[0])));
        fresh.finish_all().await;
        for original in fresh_originals {
            let closed = original
                .closed()
                .expect("same fresh native controller closed");
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        }
        drop(metadata);
        server.await.unwrap();
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
