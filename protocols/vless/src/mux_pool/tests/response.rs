use super::super::*;
use core::future::Future;
use tokio::io::AsyncWriteExt;
fn data(sid: u16, payload: &[u8]) -> Vec<u8> {
    crate::mux::encode_data_frame(sid, payload).unwrap()
}
fn end(sid: u16) -> Vec<u8> {
    crate::mux::encode_end_frame(sid).unwrap()
}
fn insert(
    map: &Arc<Mutex<HashMap<u16, MuxClientStreamState>>>,
    sid: u16,
    tx: mpsc::Sender<MuxDownlink<Vec<u8>>>,
) {
    map.lock().unwrap().insert(
        sid,
        MuxClientStreamState {
            downlink: MuxClientDownlink::Tcp(tx),
            target: None,
        },
    );
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
        let streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel(1);
        insert(&streams, 7, tx);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let mut driver = Box::pin(super::super::response::run_mux_read_relay(
            reader,
            streams.clone(),
            closed,
            (stop, stopping),
            None,
            backlog.clone(),
            false,
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
        let Some(MuxDownlink::Data(first)) = rx.recv().await else {
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
                    MuxDownlink::Data(data) => received.extend_from_slice(data.value()),
                    MuxDownlink::Overflow => {
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
        let streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, rx) = mpsc::channel(1);
        insert(&streams, 7, tx);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let mut driver = Box::pin(super::super::response::run_mux_read_relay(
            reader,
            streams.clone(),
            closed,
            (stop, stopping),
            None,
            backlog.clone(),
            false,
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
        let streams: Arc<Mutex<HashMap<u16, MuxClientStreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (a, mut rejected) = mpsc::channel(1);
        insert(&streams, 7, a);
        let (b, mut received) = mpsc::channel(1);
        insert(&streams, 8, b);
        let backlog = MuxResponseBacklog::new(1024);
        let closed = Arc::new(AtomicBool::new(false));
        let (stop, stopping) = watch::channel(false);
        let driver = super::super::response::run_mux_read_relay(
            reader,
            streams.clone(),
            closed,
            (stop, stopping),
            None,
            backlog.clone(),
            false,
        );
        let consume = async {
            assert!(matches!(rejected.recv().await, Some(MuxDownlink::Overflow)));
            assert!(rejected.recv().await.is_none());
            let Some(MuxDownlink::Data(data)) = received.recv().await else {
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
        .send(MuxDownlink::Data(
            backlog.try_buffer(4, b"tail".to_vec()).ok().unwrap()
        ))
        .await
        .is_ok());
    let (io, _peer) = tokio::io::duplex(64);
    let (conn, _driver) = MuxPoolConn::prepare(
        io,
        &[0; 16],
        1,
        None,
        MuxResponseBacklogPolicy::default(),
        false,
    );
    let mut stream = MuxStreamRelay {
        native_writer: None,
        native_initial: None,
        write_open: false,
        sid: 7,
        down_rx: Some(rx),
        read_pending: None,
        read_offset: 0,
        conn: Arc::new(conn),
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
