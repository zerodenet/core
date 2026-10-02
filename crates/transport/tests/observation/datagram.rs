use super::*;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Mutex,
};
use zero_traits::IoObserver;

#[derive(Debug, Default)]
struct Counts {
    rx: AtomicU64,
    tx: AtomicU64,
    rx_packets: AtomicU64,
    tx_packets: AtomicU64,
    errors: AtomicU64,
}
impl IoObserver for Counts {
    fn received(&self, bytes: usize) {
        self.rx.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn sent(&self, bytes: usize) {
        self.tx.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn received_datagram(&self, bytes: usize) {
        self.received(bytes);
        self.rx_packets.fetch_add(1, Ordering::Relaxed);
    }
    fn sent_datagram(&self, bytes: usize) {
        self.sent(bytes);
        self.tx_packets.fetch_add(1, Ordering::Relaxed);
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn dropped(&self) {}
}
#[derive(Debug, Default)]
struct BatchSocket {
    send_result: AtomicUsize,
    receive: Mutex<Option<Vec<(usize, usize)>>>,
}
#[derive(Debug)]
struct Ready;
impl UdpPoller for Ready {
    fn poll_writable(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl AsyncUdpSocket for BatchSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Ready)
    }
    fn try_send(&self, _: &Transmit<'_>) -> io::Result<()> {
        match self.send_result.load(Ordering::Relaxed) {
            1 => Err(io::ErrorKind::WouldBlock.into()),
            2 => Err(io::ErrorKind::BrokenPipe.into()),
            _ => Ok(()),
        }
    }
    fn poll_recv(
        &self,
        _: &mut Context<'_>,
        _: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let Some(items) = self.receive.lock().unwrap().take() else {
            return Poll::Pending;
        };
        for ((len, stride), item) in items.iter().zip(meta.iter_mut()) {
            item.len = *len;
            item.stride = *stride;
        }
        Poll::Ready(Ok(items.len()))
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:12345".parse().unwrap())
    }
    fn max_transmit_segments(&self) -> usize {
        8
    }
    fn max_receive_segments(&self) -> usize {
        16
    }
    fn may_fragment(&self) -> bool {
        false
    }
}
#[tokio::test]
async fn datagram_batch_observation_counts_gso_gro_and_zero_packets_without_pending_sends() {
    let source = Arc::new(BatchSocket::default());
    let counts = Arc::new(Counts::default());
    let socket = wrap(source.clone(), Some(counts.clone()));
    assert_eq!(socket.max_transmit_segments(), 8);
    assert_eq!(socket.max_receive_segments(), 16);
    assert!(!socket.may_fragment());
    assert_eq!(socket.local_addr().unwrap(), source.local_addr().unwrap());
    let mut transmit = Transmit {
        destination: source.local_addr().unwrap(),
        contents: &[1; 10],
        segment_size: Some(4),
        ecn: None,
        src_ip: None,
    };
    source.send_result.store(1, Ordering::Relaxed);
    assert_eq!(
        socket.try_send(&transmit).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(counts.tx_packets.load(Ordering::Relaxed), 0);
    assert_eq!(counts.errors.load(Ordering::Relaxed), 0);
    source.send_result.store(0, Ordering::Relaxed);
    socket.try_send(&transmit).unwrap();
    transmit.contents = &[];
    transmit.segment_size = None;
    socket.try_send(&transmit).unwrap();
    assert_eq!(counts.tx.load(Ordering::Relaxed), 10);
    assert_eq!(counts.tx_packets.load(Ordering::Relaxed), 4);
    source.send_result.store(2, Ordering::Relaxed);
    assert!(socket.try_send(&transmit).is_err());
    assert_eq!(counts.tx_packets.load(Ordering::Relaxed), 4);
    assert_eq!(counts.errors.load(Ordering::Relaxed), 1);
    *source.receive.lock().unwrap() = Some(vec![(10, 4), (0, 0)]);
    let mut bytes_a = [0; 16];
    let mut bytes_b = [0; 16];
    let mut meta = [RecvMeta::default(); 2];
    std::future::poll_fn(|cx| {
        socket.poll_recv(
            cx,
            &mut [IoSliceMut::new(&mut bytes_a), IoSliceMut::new(&mut bytes_b)],
            &mut meta,
        )
    })
    .await
    .unwrap();
    assert_eq!(counts.rx.load(Ordering::Relaxed), 10);
    assert_eq!(counts.rx_packets.load(Ordering::Relaxed), 4);
}
