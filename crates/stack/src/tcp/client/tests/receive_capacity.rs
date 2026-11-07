//! Capacity is rejected before SYN, not a successful connection which can never RX.
use super::*;
#[tokio::test]
async fn actual_receive_backing_capacity_refuses_before_original_syn_submission() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let root = Arc::new(PacketReturnTotals::new_with_transmit(
            PacketMemoryLimit {
                frames: 1,
                bytes: 1,
            },
            PacketMemoryLimit {
                frames: 1,
                bytes: 128,
            },
            PacketMemoryLimit {
                frames: 4,
                bytes: 4096,
            },
        ));
        let read = root.native_grant(128).unwrap();
        let (send, mut packets) = tokio::sync::mpsc::channel::<zero_traits::PacketBuffer>(8);
        let send = crate::packet_output::PacketSender::from(send)
            .with_packet_memory_view(
                read.memory_view().unwrap(),
                crate::packet_output::memory::PacketWireMemory::PaddedEnvelope {
                    multiple: 16,
                    overhead: 32,
                },
            )
            .unwrap();
        let scope = Arc::new(Scope(StdMutex::new(JoinSet::new())));
        let local: IpAddr = "10.0.0.1".parse().unwrap();
        let client = Arc::new(
            ClientTcpStack::new_output_with_task_owner(vec![local], send, 1420, scope.clone())
                .unwrap(),
        );
        let opening = client.prepare_connect(local, "10.0.0.2:80".parse().unwrap(), None);
        let original = opening.control();
        assert!(matches!(
            opening.connect().await,
            Err(ClientTcpStackError::ReceiveMemory(
                zero_traits::PacketDeriveError::Capacity
            ))
        ));
        assert!(
            packets.try_recv().is_err(),
            "the actual SYN was never submitted"
        );
        let receipt = original.close().await;
        assert!(receipt.matches(&original));
        client.shutdown().await;
        drop(client);
        drop(packets);
        let mut tasks = std::mem::take(&mut *scope.0.lock().unwrap());
        while let Some(joined) = tasks.join_next().await {
            joined.unwrap();
        }
        assert_eq!(root.transmit().occupied.frames, 0);
        drop(read);
        assert_eq!(root.native().occupied.frames, 0);
    })
    .await
    .unwrap();
}
