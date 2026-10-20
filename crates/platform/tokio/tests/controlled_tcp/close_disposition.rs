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
        ControlledTcpStream::new(socket.unwrap(), tokio_util::sync::CancellationToken::new());
    let mut peer = peer.unwrap().0;
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
    for _ in 0..2 {
        let failure = control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        let observed = std::error::Error::source(&failure)
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(observed, &original));
        assert!(!shared.state().closed);
        assert!(!control.reported);
    }
    assert_eq!(
        peer.read(&mut [0]).await.unwrap(),
        0,
        "actual OS drop is not explicit close proof"
    );
}
