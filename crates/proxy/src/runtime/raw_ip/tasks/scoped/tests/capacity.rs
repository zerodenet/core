//! A rejected real IO worker cannot stop existing IO or submit its own probe.
use super::*;
use zero_engine::EngineError;
#[tokio::test]
async fn source_scope_capacity_rejects_only_new_io_then_same_source_accepts_after_actual_ack() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut scope = hosted(1);
        let owner = ScopedSourceTasks::new("same bounded Source", 2, TaskClass::Critical);
        owner.bind(scope.registrar().downgrade()).unwrap();
        let mut guard = owner.lock().await.unwrap();
        let (read, mut writer) = tokio::io::duplex(1);
        let observed = Arc::new(Notify::new());
        let first = guard
            .spawn(ReadOnce {
                read,
                observed: observed.clone(),
            })
            .unwrap();
        observed.notified().await;
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let address = peer.local_addr().unwrap();
        let attempted = socket.clone();
        let refused = guard
            .spawn(async move {
                attempted.send_to(b"not-before-admission", address).await?;
                Ok::<(), io::Error>(())
            })
            .unwrap_err();
        assert!(super::super::super::capacity::before_work(&refused));
        assert!(!scope.registrar().cancellation().is_cancelled());
        assert_eq!(guard.len(), 1, "only the original accepted worker remains");
        let mut bytes = [0; 64];
        let mut receive = Box::pin(peer.recv_from(&mut bytes));
        pending(&mut receive).await;
        writer.write_u8(7).await.unwrap(); // Existing IO survived the rejection.
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), first);
        guard.join_next().await.unwrap().unwrap().unwrap();
        assert!(!receipt.joined().registration_released());
        pending(&mut receive).await;
        scope.acknowledge(&receipt).unwrap();
        let admitted = guard
            .spawn(async move {
                socket.send_to(b"after-original-ack", address).await?;
                Ok::<(), io::Error>(())
            })
            .unwrap();
        let (size, _) = receive.await.unwrap();
        assert_eq!(&bytes[..size], b"after-original-ack");
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), admitted);
        guard.join_next().await.unwrap().unwrap().unwrap();
        scope.acknowledge(&receipt).unwrap();
        assert_eq!(guard.len(), 0);
        assert!(owner.quiescent());
        // Only the exact outer admission fields can authorize rejection.
        let old_fault =
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(refused);
        let old_fault = EngineError::Io(io::Error::other(old_fault));
        assert!(!super::super::super::capacity::source_admission(&old_fault));
        drop(guard);
        owner
            .close(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
    })
    .await
    .unwrap();
}
