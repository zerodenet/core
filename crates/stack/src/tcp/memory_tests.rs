//! Real TCP cache/FIFO operations may synchronously reenter memory waiters.
use super::*;
use std::{
    future::Future,
    sync::atomic::AtomicUsize,
    task::{Wake, Waker},
};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
struct CheckUnlocked {
    connections: Arc<Mutex<HashMap<ConnKey, Conn>>>,
    stream: Arc<UserTcpStream>,
    calls: AtomicUsize,
}
impl Wake for CheckUnlocked {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.connections.try_lock().is_ok(),
            "same actual connection map must be unlocked before memory wake"
        );
        assert!(
            self.stream.write.try_lock().is_ok(),
            "same actual TCP writer must be unlocked before memory wake"
        );
        assert!(
            self.stream.read.try_lock().is_ok(),
            "same actual TCP reader must unlock before its ACK memory wake"
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}
struct ReleaseOnEnqueue(std::sync::Mutex<Option<zero_traits::PacketBuffer>>);
impl Wake for ReleaseOnEnqueue {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let actual = self.0.lock().unwrap().take();
        // The real receiver notification synchronously returns another actual
        // packet allocation while the sending TCP poll is still on its stack.
        drop(actual);
    }
}
#[tokio::test]
async fn original_tcp_writer_and_connection_map_unlock_before_reentrant_root_notification() {
    let limit = PacketMemoryLimit {
        frames: 128,
        bytes: 65_536,
    };
    let root = Arc::new(PacketReturnTotals::new_with_transmit(limit, limit, limit));
    let grant = root.native_grant(128).unwrap();
    let view = grant.memory_view().unwrap();
    let (send, mut received) = mpsc::channel::<zero_traits::PacketBuffer>(256);
    let output = PacketSender::from(send)
        .with_packet_memory_view(
            view.clone(),
            crate::packet_output::memory::PacketWireMemory::Native,
        )
        .unwrap();
    let stack = UserTcpStack::new_output(output, 1460);
    let syn = client(packet::tcp_flags::SYN, 1000, 0, &[]);
    stack.feed(&syn).await;
    let syn_ack = received.recv().await.unwrap();
    let sequence = packet::parse_tcp(&syn_ack).unwrap().seq;
    drop(syn_ack);
    stack
        .feed(&client(
            packet::tcp_flags::ACK,
            1001,
            sequence.wrapping_add(1),
            &[],
        ))
        .await;
    let (stream, _, _) = stack.accept().await.unwrap();
    let stream = Arc::new(stream);
    let checks = Arc::new(CheckUnlocked {
        connections: stack.connections.clone(),
        stream: stream.clone(),
        calls: AtomicUsize::new(0),
    });
    let check_waker = Waker::from(checks.clone());
    let release = Arc::new(ReleaseOnEnqueue(std::sync::Mutex::new(Some(
        view.transmit_packet(128, 0, 0).unwrap(),
    ))));
    let release_waker = Waker::from(release.clone());
    {
        let notification = root.notified();
        tokio::pin!(notification);
        assert!(notification
            .as_mut()
            .poll(&mut Context::from_waker(&check_waker))
            .is_pending());
        let receive = received.recv();
        tokio::pin!(receive);
        assert!(receive
            .as_mut()
            .poll(&mut Context::from_waker(&release_waker))
            .is_pending());
        std::future::poll_fn(|cx| stream.poll_write_inner(cx, b"real-owned-tcp-output"))
            .await
            .unwrap();
        assert!(checks.calls.load(Ordering::Acquire) > 0);
        assert!(notification
            .as_mut()
            .poll(&mut Context::from_waker(&check_waker))
            .is_ready());
        let sent = match receive
            .as_mut()
            .poll(&mut Context::from_waker(&release_waker))
        {
            Poll::Ready(Some(packet)) => packet,
            _ => panic!("actual TCP payload was not enqueued"),
        };
        assert_eq!(
            packet::parse_tcp(&sent).unwrap().payload,
            b"real-owned-tcp-output"
        );
        drop(sent);
    }
    assert!(release.0.lock().unwrap().is_none());
    // The application read really frees receive-window space and emits an ACK
    // in that same poll. Its original FIFO receiver can return memory inline.
    let payload = [7_u8; 1460];
    stack
        .feed(&client(
            packet::tcp_flags::ACK | packet::tcp_flags::PSH,
            1001,
            sequence.wrapping_add(1 + "real-owned-tcp-output".len() as u32),
            &payload,
        ))
        .await;
    drop(received.recv().await.expect("actual payload ACK"));
    let read_release = Arc::new(ReleaseOnEnqueue(std::sync::Mutex::new(Some(
        view.transmit_packet(128, 0, 0).unwrap(),
    ))));
    let release_waker = Waker::from(read_release.clone());
    {
        let notification = root.notified();
        tokio::pin!(notification);
        assert!(notification
            .as_mut()
            .poll(&mut Context::from_waker(&check_waker))
            .is_pending());
        let receive = received.recv();
        tokio::pin!(receive);
        assert!(receive
            .as_mut()
            .poll(&mut Context::from_waker(&release_waker))
            .is_pending());
        let mut read = [0_u8; 1460];
        let mut target = ReadBuf::new(&mut read);
        assert!(matches!(
            stream.poll_read_inner(&mut Context::from_waker(&check_waker), &mut target),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(target.filled(), payload);
        assert!(read_release.0.lock().unwrap().is_none());
        assert!(notification
            .as_mut()
            .poll(&mut Context::from_waker(&check_waker))
            .is_ready());
        let ack = match receive
            .as_mut()
            .poll(&mut Context::from_waker(&release_waker))
        {
            Poll::Ready(Some(packet)) => packet,
            _ => panic!("actual receive-window ACK must be enqueued"),
        };
        assert!(packet::parse_tcp(&ack).unwrap().payload.is_empty());
        drop(ack);
    }
    drop(checks);
    drop(check_waker);
    drop(stream);
    stack.cleanup_idle(std::time::Duration::ZERO).await;
    grant.revoke();
}
fn client(flags: u8, sequence: u32, ack: u32, payload: &[u8]) -> Vec<u8> {
    packet::build_tcp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.1".parse().unwrap(),
        54321,
        443,
        sequence,
        ack,
        flags,
        payload,
    )
}
