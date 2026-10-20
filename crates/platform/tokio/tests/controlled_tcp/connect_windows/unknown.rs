//! Synthetic source-policy check, not evidence of a real registration failure.
use super::*;
use crate::contract::{lifecycle::ChannelControl, DeliveryState};
use crate::controlled_tcp::{socket::Socket, ControlledTcpControl, Shared, State};
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use windows_sys::Win32::Networking::WinSock::WSAEMFILE;
#[tokio::test]
async fn windows_consumed_registration_unknown_retains_source_identity_and_never_acknowledges_or_retries(
) {
    // No SOCKET is created or closed by this policy-only test.
    let unknown = ConsumedUnknown::new(42, io::Error::from_raw_os_error(WSAEMFILE));
    let source = unknown.error();
    assert!(source.to_string().contains("42"));
    assert!(!TokioSocket::close_is_retryable(&source));
    assert!(!TokioSocket::retry_close(&source).unwrap());
    let original = std::error::Error::source(source.get_ref().unwrap())
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(original.raw_os_error(), Some(WSAEMFILE));
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            socket: Some(Socket::Native(NativeSocket {
                status: Status::ConsumedUnknown(unknown),
                interface: None,
                observer: None,
                last_local: None,
            })),
            closed: false,
            error: None,
        }),
        cancel: CancellationToken::new(),
        label: "policy-only SOCKET 42 consumed by registration".into(),
    });
    // The unknown marker remains available if conversion unwinds while the
    // state lock is held. This is a synthetic poison-policy check only.
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = shared.state();
        panic!("policy-only registration unwind");
    }));
    assert!(poisoned.is_err());
    let mut control = ControlledTcpControl {
        shared: shared.clone(),
        reported: false,
    };
    let deadline = Instant::now() + Duration::from_secs(1);
    let first = control.wait_closed(deadline).await.err().unwrap();
    assert_eq!(first.delivery, DeliveryState::Unknown);
    let retained = shared.state().error.clone().unwrap();
    let again = control.wait_closed(deadline).await.err().unwrap();
    assert_eq!(again.delivery, DeliveryState::Unknown);
    assert!(Arc::ptr_eq(
        &retained,
        shared.state().error.as_ref().unwrap()
    ));
    assert!(!shared.state().closed);
    assert!(!control.reported);
    assert!(
        shared.state().socket.is_none(),
        "consumed identity is not reconstructed as a live socket"
    );
}
