//! Real pending socket work remains in the original driver after HTTP EOF.
use super::*;
use std::future::Future;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;

async fn fixture() -> (
    PacketMetadataArc<Runtime>,
    TcpStream,
    Arc<AtomicBool>,
    Arc<AtomicBool>,
) {
    let metadata = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let runtime = Runtime::prepare(
        originals.buffer_budget(),
        metadata.driver_metadata_view(),
        1,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    let pending = Arc::new(AtomicBool::new(false));
    let complete = Arc::new(AtomicBool::new(false));
    let waiting = pending.clone();
    let finished = complete.clone();
    let work = async move {
        let read = socket.read_u8();
        tokio::pin!(read);
        let byte = std::future::poll_fn(|cx| {
            let result = read.as_mut().poll(cx);
            if result.is_pending() {
                waiting.store(true, Ordering::Release);
            }
            result
        })
        .await
        .unwrap();
        assert_eq!(byte, 0x5a);
        finished.store(true, Ordering::Release);
    };
    runtime
        .ledger
        .push(Work::Session(
            PaidFuture::prepare_returning(work, &runtime.memory, None).unwrap(),
        ))
        .unwrap();
    (runtime, peer, pending, complete)
}

#[tokio::test]
async fn normal_http_eof_drains_actual_pending_work_instead_of_cancelling_it() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (runtime, mut peer, pending, complete) = fixture().await;
        let mut drain = Box::pin(runtime.drain());
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            assert!(
                pending.load(Ordering::Acquire),
                "the actual socket read suspended"
            );
            Poll::Ready(())
        })
        .await;
        assert!(!complete.load(Ordering::Acquire));
        assert!(!runtime.stopped.is_cancelled());
        peer.write_all(&[0x5a]).await.unwrap();
        drain.await;
        assert!(complete.load(Ordering::Acquire));
        assert!(runtime.ledger.is_empty());
        assert!(!runtime.stopped.is_cancelled());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn original_stop_still_cancels_actual_pending_work_during_http_drain() {
    let (runtime, _peer, pending, complete) = fixture().await;
    let mut drain = Box::pin(runtime.drain());
    std::future::poll_fn(|cx| {
        assert!(drain.as_mut().poll(cx).is_pending());
        assert!(pending.load(Ordering::Acquire));
        Poll::Ready(())
    })
    .await;
    runtime.stop();
    tokio::time::timeout(std::time::Duration::from_secs(3), drain)
        .await
        .unwrap();
    assert!(!complete.load(Ordering::Acquire));
    assert!(runtime.ledger.is_empty());
}
