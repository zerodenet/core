use super::*;
use crate::contract::{DeliveryState, Stage};
use std::error::Error;

#[tokio::test]
async fn consumed_handle_unknown_policy_retains_exact_source_and_never_confirms_or_retries() {
    // State-policy input, not evidence of inducing a native deregister failure.
    // There is deliberately no fabricated close success or reconstructed fd.
    let source = Arc::new(io::Error::from_raw_os_error(libc::EIO));
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            listener: None,
            retired: true,
            closed: false,
            error: Some(source.clone()),
        }),
        cancel: CancellationToken::new(),
        label: "policy-only consumed listener".into(),
    });
    let mut control = ControlledTcpListenerControl {
        shared: shared.clone(),
        reported: false,
    };
    for until in [Instant::now(), deadline(), deadline()] {
        let failure = control.wait_closed(until).await.unwrap_err();
        assert_eq!(failure.stage, Stage::Close);
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        let retained = failure
            .source()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(retained, &source));
        assert_eq!(retained.raw_os_error(), Some(libc::EIO));
        assert!(!control.reported);
        let state = shared.state.lock().unwrap();
        assert!(!state.closed);
        assert!(state.listener.is_none());
        assert!(Arc::ptr_eq(state.error.as_ref().unwrap(), &source));
    }
}
