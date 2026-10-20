use super::*;
use std::future::{poll_fn, Future};
use std::task::Poll;
use zero_platform_tokio::contract::DeliveryState;

fn udp(destination: Destination, attempts: usize) -> ResourceDeclaration {
    ResourceDeclaration::DatagramListener {
        destination,
        max_bind_attempts: std::num::NonZeroUsize::new(attempts).unwrap(),
        max_pending_sends: std::num::NonZeroUsize::new(1).unwrap(),
    }
}
#[tokio::test]
async fn udp_failures_explicitly_close_originals_then_success_keeps_all_views_fenced_by_host() {
    tokio::time::timeout_at(deadline(), async {
        let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = occupied.local_addr().unwrap();
        let destination = endpoint(address);
        let grants = HostResources::new(
            ComponentId(104),
            &[udp(destination.clone(), 3)],
            CancellationToken::new(),
        )
        .unwrap();
        let _access = grants.activate(deadline()).unwrap();
        for _ in 0..2 {
            let error = grants
                .listen_datagram(destination.clone())
                .await
                .err()
                .unwrap();
            assert_eq!(error.delivery, DeliveryState::NotSubmitted);
            assert!(error.retry_permitted());
            let mut source: &(dyn Error + 'static) = &error;
            let mut original = None;
            loop {
                if let Some(io) = source.downcast_ref::<std::io::Error>() {
                    if io.raw_os_error().is_some() {
                        original = Some(io);
                    }
                }
                match source.source() {
                    Some(next) => source = next,
                    None => break,
                }
            }
            assert_eq!(original.unwrap().kind(), std::io::ErrorKind::AddrInUse);
        }
        drop(occupied); // The two failed originals were already actually closed.
        let mut view = grants.listen_datagram(destination.clone()).await.unwrap();
        assert_eq!(view.rx.local_addr().unwrap(), address);
        let tx = view.tx.clone();
        {
            let mut data = [0; 8];
            let mut receiving = Box::pin(view.rx.recv_from_addr(&mut data));
            poll_fn(|cx| {
                assert!(receiving.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.send_to(b"actual", address).await.unwrap();
        let mut bytes = [0; 6];
        assert_eq!(view.rx.recv_from_addr(&mut bytes).await.unwrap().0, 6);
        assert_eq!(&bytes, b"actual");
        tx.send_to_addr(b"reply", peer.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(peer.recv_from(&mut bytes).await.unwrap().0, 5);
        assert_eq!(
            grants
                .listen_datagram(destination)
                .await
                .err()
                .unwrap()
                .reason,
            Reason::Capacity
        );
        let report = grants.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert!(report.closed_io[0].contains("3 originals explicitly closed"));
        // Retain the real RX, clone TX and original TX during host closure.
        assert!(view.rx.recv_from_addr(&mut bytes).await.is_err());
        assert!(view.tx.send_to_addr(b"closed", address).await.is_err());
        assert!(tx.send_to_addr(b"closed", address).await.is_err());
        std::net::UdpSocket::bind(address).unwrap();
        assert!(grants
            .shutdown_io(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_attempt_consumes_exact_udp_budget_without_waiting_for_success() {
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let destination = endpoint(occupied.local_addr().unwrap());
    let grants = HostResources::new(
        ComponentId(105),
        &[udp(destination.clone(), 1)],
        CancellationToken::new(),
    )
    .unwrap();
    let _access = grants.activate(deadline()).unwrap();
    assert_eq!(
        grants
            .listen_datagram(destination.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Io
    );
    assert_eq!(
        grants
            .listen_datagram(destination)
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(report.closed_io[0].contains("1 originals explicitly closed"));
}

#[tokio::test]
async fn invalid_udp_sender_capacity_is_rejected_cold_and_tcp_failed_attempts_are_also_bounded() {
    let destination = endpoint("127.0.0.1:0".parse().unwrap());
    let invalid = ResourceDeclaration::DatagramListener {
        destination: destination.clone(),
        max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
        max_pending_sends: std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1)
            .unwrap(),
    };
    assert!(
        matches!(HostResources::new(ComponentId(106), &[invalid], CancellationToken::new()), Err(error) if error.reason == Reason::InvalidConfig)
    );
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let actual = occupied.local_addr().unwrap();
    let destination = endpoint(actual);
    let grants = HostResources::new(
        ComponentId(106),
        &[ResourceDeclaration::TcpListener {
            destination: destination.clone(),
            max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let _access = grants.activate(deadline()).unwrap();
    // Wrong endpoint is rejected before the sole native attempt is consumed.
    assert_eq!(
        grants
            .listen_tcp(endpoint("127.0.0.2:0".parse().unwrap()))
            .await
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    assert_eq!(
        grants
            .listen_tcp(destination.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Io
    );
    drop(occupied);
    assert_eq!(
        grants.listen_tcp(destination).await.err().unwrap().reason,
        Reason::Capacity
    );
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert_eq!(report.closed_io.len(), 1);
    std::net::TcpListener::bind(actual).unwrap();
}
