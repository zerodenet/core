use super::super::*;
use tokio::io::AsyncWriteExt;
fn data(sid: u16, payload: &[u8]) -> Vec<u8> {
    encode_keep_stream(sid, payload).unwrap()
}
fn end(sid: u16) -> Vec<u8> {
    encode_end_stream(sid).unwrap()
}
fn insert(
    map: &Arc<Mutex<HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>>,
    sid: u16,
    tx: mpsc::Sender<VmessMuxDownlink>,
) {
    map.lock().unwrap().insert(sid, tx);
}

#[tokio::test]
async fn actual_response_fifo_and_byte_credit_wait_then_deliver_the_complete_tail_and_eof() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut peer, reader) = tokio::io::duplex(4096);
        let expected: Vec<u8> = (0..1536).map(|n| (n % 251) as u8).collect();
        for chunk in expected.chunks(512) {
            peer.write_all(&data(7, chunk)).await.unwrap();
        }
        peer.write_all(&end(7)).await.unwrap();
        peer.shutdown().await.unwrap();
        let streams: Arc<Mutex<HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel(1);
        insert(&streams, 7, tx);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let mut driver = Box::pin(client_response::run_mux_read_relay_with_shutdown(
            reader,
            streams.clone(),
            closed,
            stop,
            stopping,
            None,
            backlog.clone(),
        ));
        core::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(rx.len(), 1, "same finite FIFO is full");
        assert_eq!(
            backlog.used(),
            1024,
            "queued and pending real DATA both hold byte credit"
        );
        let Some(VmessMuxDownlink::Data(first)) = rx.recv().await else {
            panic!("first original DATA");
        };
        let mut received = first.value().clone();
        core::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(rx.len(), 1);
        assert_eq!(
            backlog.used(),
            1024,
            "held response keeps its credit while next wire payload waits"
        );
        drop(first);
        let consume = async {
            while let Some(message) = rx.recv().await {
                match message {
                    VmessMuxDownlink::Data(data) => received.extend_from_slice(data.value()),
                    VmessMuxDownlink::Overflow => {
                        panic!("finite backpressure must not reject admissible DATA")
                    }
                }
            }
        };
        tokio::join!(driver, consume);
        assert_eq!(received, expected);
        assert_eq!(backlog.used(), 0);
        assert!(streams.lock().unwrap().is_empty());
    })
    .await
    .expect("original 8s full-tail deadline");
}

#[tokio::test]
async fn actual_response_receiver_drop_returns_queued_and_pending_original_byte_credit() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut peer, reader) = tokio::io::duplex(4096);
        peer.write_all(&data(7, &[3; 512])).await.unwrap();
        peer.write_all(&data(7, &[4; 512])).await.unwrap();
        peer.write_all(&end(7)).await.unwrap();
        peer.shutdown().await.unwrap();
        let streams: Arc<Mutex<HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(1);
        insert(&streams, 7, tx);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let mut driver = Box::pin(client_response::run_mux_read_relay_with_shutdown(
            reader,
            streams.clone(),
            closed,
            stop,
            stopping,
            None,
            backlog.clone(),
        ));
        core::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(rx.len(), 1);
        assert_eq!(backlog.used(), 1024);
        drop(rx);
        driver.await;
        assert_eq!(backlog.used(), 0);
        assert!(streams.lock().unwrap().is_empty());
    })
    .await
    .expect("original 8s receiver-return deadline");
}

#[tokio::test]
async fn actual_oversize_response_is_rejected_without_losing_the_next_sid_wire_framing() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut peer, reader) = tokio::io::duplex(4096);
        peer.write_all(&data(7, &[3; 1536])).await.unwrap();
        peer.write_all(&data(8, &[4; 512])).await.unwrap();
        peer.write_all(&end(8)).await.unwrap();
        peer.shutdown().await.unwrap();
        let streams: Arc<Mutex<HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (a, mut rejected) = mpsc::channel(1);
        insert(&streams, 7, a);
        let (b, mut received) = mpsc::channel(1);
        insert(&streams, 8, b);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let driver = client_response::run_mux_read_relay_with_shutdown(
            reader,
            streams.clone(),
            closed,
            stop,
            stopping,
            None,
            backlog.clone(),
        );
        let consume = async {
            assert!(matches!(
                rejected.recv().await,
                Some(VmessMuxDownlink::Overflow)
            ));
            assert!(rejected.recv().await.is_none());
            let Some(VmessMuxDownlink::Data(data)) = received.recv().await else {
                panic!("next SID original DATA")
            };
            assert_eq!(data.value().as_slice(), &[4; 512]);
            drop(data);
            assert!(received.recv().await.is_none());
        };
        tokio::join!(driver, consume);
        assert_eq!(backlog.used(), 0);
    })
    .await
    .expect("original 8s wire-framing deadline");
}

#[tokio::test]
async fn actual_partial_logical_read_keeps_byte_credit_until_its_last_byte_is_returned() {
    use tokio::io::AsyncReadExt;
    let backlog = MuxResponseBacklog::new(4);
    let (tx, rx) = mpsc::channel(1);
    assert!(tx
        .send(VmessMuxDownlink::Data(
            backlog.try_buffer(4, b"tail".to_vec()).ok().unwrap()
        ))
        .await
        .is_ok());
    let (write_tx, _write_rx) = mpsc::unbounded_channel();
    let mut stream = VmessMuxStream {
        session_id: 7,
        target: Address::Ipv4([127, 0, 0, 1]),
        port: 80,
        network: Network::Tcp,
        #[cfg(feature = "runtime")]
        native_writer: None,
        write_tx,
        read_rx: rx,
        write_buf: Vec::new(),
        write_pos: 0,
        read_buf: None,
        read_pos: 0,
        opened: true,
        ended: false,
        conn: None,
    };
    let mut first = [0; 1];
    stream.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"t");
    assert_eq!(
        backlog.used(),
        4,
        "the original partial frame is still held"
    );
    let mut rest = [0; 3];
    stream.read_exact(&mut rest).await.unwrap();
    assert_eq!(&rest, b"ail");
    assert_eq!(backlog.used(), 0);
}

#[tokio::test]
async fn pending_old_response_never_moves_to_a_reused_sid_receiver() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut peer, reader) = tokio::io::duplex(4096);
        peer.write_all(&data(7, &[3; 512])).await.unwrap();
        peer.write_all(&data(7, &[4; 512])).await.unwrap();
        let streams: Arc<Mutex<HashMap<u16, mpsc::Sender<VmessMuxDownlink>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, old_rx) = mpsc::channel(1);
        insert(&streams, 7, tx);
        let backlog = MuxResponseBacklog::new(512);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let mut driver = Box::pin(client_response::run_mux_read_relay_with_shutdown(
            reader,
            streams.clone(),
            closed,
            stop.clone(),
            stopping,
            None,
            backlog.clone(),
        ));
        core::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(old_rx.len(), 1);
        assert_eq!(backlog.used(), 512);
        // The second wire header already selected the old actual sender. The
        // pending original byte wait must not later target this new receiver.
        let (replacement, mut new_rx) = mpsc::channel(1);
        insert(&streams, 7, replacement.clone());
        drop(old_rx);
        core::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(streams
            .lock()
            .unwrap()
            .get(&7)
            .is_some_and(|current| current.same_channel(&replacement)));
        assert!(matches!(
            new_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(backlog.used(), 0);
        stop.send(true).unwrap();
        driver.await;
        assert!(streams.lock().unwrap().is_empty());
    })
    .await
    .expect("original 8s SID reuse deadline");
}
