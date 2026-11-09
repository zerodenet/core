use super::*;
use crate::controlled_tcp::ControlledTcpStream;
use tokio::{io::AsyncReadExt, net::TcpListener};
#[tokio::test]
async fn panic_after_taking_original_close_retains_same_unknown_source_without_false_confirmation()
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (socket, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (_io, mut control) =
        ControlledTcpStream::new(socket.unwrap(), tokio_util::sync::CancellationToken::new())
            .unwrap();
    let mut peer = peer.unwrap().0;
    let observer = control.close_observer();
    let shared = control.shared.clone();
    let unknown = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let action = shared.close_action(Instant::now() + Duration::from_secs(1));
        let CloseAction::Original(_original) = action else {
            panic!("expected actual uniquely owned socket");
        };
        assert!(shared.state().socket.is_none());
        assert!(shared.state().closing);
        assert!(!shared.state().closed);
        // Unwinding may let OS Drop dispose of the socket. That is deliberately
        // not an observed result from the original explicit native-close call.
        panic!("close operation did not return a disposition");
    }));
    assert!(unknown.is_err());
    let original = shared.state().unknown.clone().unwrap();
    let observation = observer.observe().unwrap();
    assert!(!observation.closed);
    assert!(Arc::ptr_eq(
        observation.disposition_unknown.as_ref().unwrap(),
        &original
    ));
    for _ in 0..2 {
        let failure = control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        let sources = control.close_sources().unwrap();
        assert!(Arc::ptr_eq(sources.unknown().unwrap(), &original));
        let canonical = failure.retained_source().unwrap();
        assert!(canonical
            .cleanup_failure_receipt()
            .unwrap()
            .acknowledge_retained(canonical));
        // Local ownership acceptance still grants no native close proof.
        assert!(!shared.state().closed);
        assert!(!control.reported);
    }
    assert_eq!(
        peer.read(&mut [0]).await.unwrap(),
        0,
        "actual OS drop is not explicit close proof"
    );
}

#[tokio::test]
async fn ready_native_close_uses_the_same_original_without_inventing_a_request_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (socket, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (view, mut control) =
        ControlledTcpStream::new(socket.unwrap(), tokio_util::sync::CancellationToken::new())
            .unwrap();
    let _peer = peer.unwrap().0;
    let observer = control.close_observer();
    let expired = control.wait_closed(Instant::now()).await.unwrap();
    assert!(!expired.is_complete());
    assert!(!observer.observe().unwrap().closed);
    let report = control.try_close_once().unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(observer.observe().unwrap().closed);
    assert!(control.try_close_once().unwrap().closed_io.is_empty());
    drop(view);
}

#[tokio::test]
async fn native_close_keeps_late_original_fault_until_same_receiver_accepts_it() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (socket, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (view, mut control) =
        ControlledTcpStream::new(socket.unwrap(), Default::default()).unwrap();
    let mut peer = peer.unwrap().0;
    let observer = control.close_observer();
    // Concrete OS errors injected at this original ledger's storage boundary;
    // actual native disposition is obtained independently below.
    let first = Arc::new(std::fs::File::open("/zero-no-such-original-tcp-close-file").unwrap_err());
    {
        control.shared.state().error = Some(first.clone());
    }
    let one = control.try_close_once().unwrap_err();
    let canonical = one.retained_source().unwrap().clone();
    let sources = control.close_sources().unwrap();
    assert!(Arc::ptr_eq(sources.native().unwrap(), &first));
    // Release only the injected storage gate, retain that same original source.
    control.shared.state().error = None;
    let two = control.try_close_once().unwrap_err();
    assert!(Arc::ptr_eq(two.retained_source().unwrap(), &canonical));
    assert!(observer.observe().unwrap().closed);
    assert!(!control.reported);
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(canonical
        .cleanup_failure_receipt()
        .unwrap()
        .acknowledge_retained(&canonical));
    assert_eq!(control.try_close_once().unwrap().closed_io.len(), 1);
    assert!(control.try_close_once().unwrap().closed_io.is_empty());
    drop(view);
    drop(control);
    assert!(Arc::ptr_eq(sources.native().unwrap(), &first));
}
