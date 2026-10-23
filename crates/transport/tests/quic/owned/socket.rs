use super::*;
use std::{io::IoSliceMut, task::Poll};

#[tokio::test]
async fn actual_metadata_socket_zero_ecn_and_payload_keep_native_close_separate_from_views() {
    let mut owner = socket();
    let socket = owner.socket();
    let local = socket.local_addr().unwrap();
    let mut writable = socket.clone().create_io_poller();
    for payload in [&b""[..], &b"actual abstract metadata"[..]] {
        let transmit = quinn::udp::Transmit {
            destination: local,
            ecn: Some(quinn::udp::EcnCodepoint::Ect0),
            contents: payload,
            segment_size: None,
            src_ip: Some(local.ip()),
        };
        poll_fn(|cx| loop {
            match writable.as_mut().poll_writable(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.unwrap(),
            }
            match socket.try_send(&transmit) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => return Poll::Ready(result),
            }
        })
        .await
        .unwrap();
        let mut bytes = [0; 128];
        let mut buffers = [IoSliceMut::new(&mut bytes)];
        let mut metadata = [quinn::udp::RecvMeta::default()];
        assert_eq!(
            poll_fn(|cx| socket.poll_recv(cx, &mut buffers, &mut metadata))
                .await
                .unwrap(),
            1
        );
        assert_eq!(&buffers[0][..metadata[0].len], payload);
        assert_eq!(metadata[0].addr, local);
        assert_eq!(metadata[0].dst_ip, Some(local.ip()));
        assert_eq!(metadata[0].ecn, Some(quinn::udp::EcnCodepoint::Ect0));
    }
    owner.revoke_io();
    assert!(
        socket.local_addr().is_ok(),
        "revoke is not a native close receipt"
    );
    let report = owner.close_io();
    assert!(report.is_complete());
    assert_eq!(report.main.confirmed_closed, 1);
    assert_eq!(
        socket.local_addr().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(owner.close_io().main.confirmed_closed, 0);
}
#[tokio::test]
async fn foreign_owner_with_equal_reservation_number_cannot_consume_actual_metadata_grant() {
    let mut first = native_owner();
    let first_grant = first.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut second = native_owner();
    let second_grant = second.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    assert_eq!(
        first_grant.control.reservation(),
        second_grant.control.reservation()
    );
    let mut rejected = match MetadataSocketOwner::new(first, second_grant) {
        Ok(_) => panic!("equal numeric reservation is not the same original owner"),
        Err(rejected) => rejected,
    };
    assert_eq!(rejected.error.kind(), io::ErrorKind::InvalidInput);
    assert!(rejected.grant.tx.local_addr().is_ok());
    assert!(first_grant.tx.local_addr().is_ok());
    assert_eq!(rejected.owner.close_all().main.confirmed_closed, 1);
    assert_eq!(second.close_all().main.confirmed_closed, 1);
}
#[tokio::test]
async fn actual_finite_pollers_report_capacity_and_native_send_drop_is_not_a_submission_receipt() {
    let mut owner = socket();
    let socket = owner.socket();
    let local = socket.local_addr().unwrap();
    let mut first = socket.clone().create_io_poller();
    let second = socket.clone().create_io_poller();
    let mut denied = socket.clone().create_io_poller();
    let error = poll_fn(|cx| denied.as_mut().poll_writable(cx))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(
        std::error::Error::source(&error).is_some(),
        "original finite-poller failure survives"
    );
    poll_fn(|cx| first.as_mut().poll_writable(cx))
        .await
        .unwrap();
    let oversized = vec![0; 65536];
    socket
        .try_send(&quinn::udp::Transmit {
            destination: local,
            contents: &oversized,
            ecn: None,
            segment_size: None,
            src_ip: None,
        })
        .unwrap();
    let diagnostic = owner.native_send_drops();
    assert_eq!(diagnostic.count, 1);
    assert!(!diagnostic.overflowed);
    assert!(diagnostic.first.as_ref().unwrap().raw_os_error().is_some());
    assert!(Arc::ptr_eq(
        diagnostic.first.as_ref().unwrap(),
        owner.native_send_drops().first.as_ref().unwrap()
    ));
    assert_eq!(
        socket
            .try_send(&quinn::udp::Transmit {
                destination: local,
                contents: b"bad",
                ecn: None,
                segment_size: Some(0),
                src_ip: None,
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(owner.native_send_drops().count, 1);
    drop(first);
    let mut replacement = socket.clone().create_io_poller();
    poll_fn(|cx| replacement.as_mut().poll_writable(cx))
        .await
        .unwrap();
    drop(second);
    assert_eq!(owner.close_io().main.confirmed_closed, 1);
    assert_eq!(
        socket
            .try_send(&quinn::udp::Transmit {
                destination: local,
                contents: b"retired",
                ecn: None,
                segment_size: None,
                src_ip: None,
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(owner.native_send_drops().count, 1);
}

#[derive(Debug)]
struct ReceiveReentry {
    socket: std::sync::Weak<dyn quinn::AsyncUdpSocket>,
    calls: std::sync::atomic::AtomicUsize,
}
impl zero_traits::IoObserver for ReceiveReentry {
    fn received(&self, _bytes: usize) {
        let socket = self.socket.upgrade().unwrap();
        let mut payload = [0; 16];
        let mut buffers = [IoSliceMut::new(&mut payload)];
        let mut meta = [quinn::udp::RecvMeta::default()];
        let waker = std::task::Waker::noop();
        match socket.poll_recv(
            &mut std::task::Context::from_waker(waker),
            &mut buffers,
            &mut meta,
        ) {
            Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::InvalidInput),
            other => panic!("concurrent RX must be refused without an outer lock: {other:?}"),
        }
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn sent(&self, _bytes: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn actual_receive_observer_reentry_has_no_outer_transport_mutex_or_second_receiver() {
    let mut owner = socket();
    let socket = owner.socket();
    let observer = Arc::new(ReceiveReentry {
        socket: Arc::downgrade(&socket),
        calls: Default::default(),
    });
    owner.set_observer(Some(observer.clone())).unwrap();
    let mut poller = socket.clone().create_io_poller();
    poll_fn(|cx| poller.as_mut().poll_writable(cx))
        .await
        .unwrap();
    socket
        .try_send(&quinn::udp::Transmit {
            destination: socket.local_addr().unwrap(),
            contents: b"reentry",
            ecn: None,
            segment_size: None,
            src_ip: None,
        })
        .unwrap();
    let mut payload = [0; 16];
    let mut buffers = [IoSliceMut::new(&mut payload)];
    let mut meta = [quinn::udp::RecvMeta::default()];
    assert_eq!(
        poll_fn(|cx| socket.poll_recv(cx, &mut buffers, &mut meta))
            .await
            .unwrap(),
        1
    );
    assert_eq!(&buffers[0][..meta[0].len], b"reentry");
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(owner.close_io().main.confirmed_closed, 1);
}
