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

#[tokio::test]
async fn tcp_return_requests_logical_stop_but_retains_actual_io_until_its_acknowledgement() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let metadata = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let runtime = Runtime::prepare(
            originals.buffer_budget(),
            metadata.driver_metadata_view(),
            1,
        )
        .unwrap();
        let life = crate::split_http::io::LifetimeRef::prepare(&runtime.memory).unwrap();
        let returned = OwnedLogicalReturn::prepare(
            &runtime.memory,
            life.clone(),
            runtime.book.try_reserve(1).unwrap(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let waiting = Arc::new(AtomicBool::new(false));
        let observed = waiting.clone();
        let work_returned = returned.enter_upload().unwrap();
        let work = async move {
            let byte = {
                let read = socket.read_u8();
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    observed.store(result.is_pending(), Ordering::Release);
                    result
                })
                .await
                .unwrap()
            };
            assert_eq!(byte, 0x11);
            // Actual request EOF precedes the real peer's acknowledgement.
            socket.shutdown().await.unwrap();
            assert_eq!(socket.read_u8().await.unwrap(), 0x5a);
        };
        runtime
            .ledger
            .push(Work::Session(
                PaidFuture::prepare_returning(work, &runtime.memory, Some(work_returned)).unwrap(),
            ))
            .unwrap();
        let mut drain = Box::pin(runtime.drain());
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            assert!(waiting.load(Ordering::Acquire));
            Poll::Ready(())
        })
        .await;
        runtime.request_tcp_return();
        assert!(life.is_closed());
        assert!(!returned.is_returned());
        std::future::poll_fn(|cx| {
            assert!(drain.as_mut().poll(cx).is_pending());
            assert!(waiting.load(Ordering::Acquire));
            Poll::Ready(())
        })
        .await;
        peer.write_all(&[0x11]).await.unwrap();
        let acknowledge = async {
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            assert!(
                !returned.is_returned(),
                "EOF alone is not the original work's join"
            );
            peer.write_all(&[0x5a]).await.unwrap();
        };
        tokio::join!(drain, acknowledge);
        returned.wait_upload_returned().await;
        assert!(returned.is_returned());
        assert!(runtime.ledger.is_empty());
    })
    .await
    .unwrap();
}
