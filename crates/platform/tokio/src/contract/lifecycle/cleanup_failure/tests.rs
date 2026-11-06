use super::*;
use crate::contract::{scope::TaskScope, Impact, Reason, Stage};
use std::num::NonZeroUsize;

#[tokio::test]
async fn same_original_fault_requires_real_local_receiver_before_host_registration_ack() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let original = Arc::new(ChannelError::new(
        Stage::Close,
        Reason::Io,
        Impact::Component,
    ));
    let failure = CleanupFailure::new(original.clone());
    let foreign = CleanupFailure::new(original.clone());
    let returned = failure.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move { Err(returned.error()) })
        .unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    let joined = witness.try_joined().unwrap();
    assert!(witness.matches_joined(&joined));
    assert!(!failure.is_accepted() && !joined.registration_released());
    assert_eq!(accept_retained_cleanup_failures(original.as_ref()), 0);
    assert!(
        !failure.is_accepted(),
        "equal original cause is not a delivery receipt"
    );
    // This is the real bounded local receiver, installed before acceptance.
    let receiver = vec![joined.result().unwrap_err()];
    witness.acknowledge_hosted_failure(&joined).unwrap();
    assert!(failure.is_accepted() && !foreign.is_accepted());
    assert!(Arc::ptr_eq(failure.original(), &original));
    assert_eq!(receiver[0].stage, original.stage);
    assert_eq!(receiver[0].reason, original.reason);
    assert!(
        !joined.registration_released(),
        "error delivery cannot return Host credit"
    );
    scope.acknowledge(&receipt).unwrap();
    assert!(joined.registration_released());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}

#[test]
fn nested_retained_causes_and_transparent_sources_keep_exact_delivery_identity() {
    #[derive(Debug)]
    struct Transparent(ChannelError);
    impl fmt::Display for Transparent {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }
    impl Error for Transparent {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }
    let inner = CleanupFailure::new(Arc::new(ChannelError::new(
        Stage::Close,
        Reason::Io,
        Impact::Component,
    )));
    let outer = CleanupFailure::new(Arc::new(inner.error()));
    let receiver = Arc::new(Transparent(outer.error()));
    assert_eq!(accept_retained_cleanup_failures(receiver.as_ref()), 2);
    assert!(outer.is_accepted() && inner.is_accepted());
    assert_eq!(accept_retained_cleanup_failures(receiver.as_ref()), 0);
    #[derive(Debug)]
    struct Cycle;
    impl fmt::Display for Cycle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("cycle")
        }
    }
    impl Error for Cycle {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self)
        }
    }
    assert_eq!(accept_retained_cleanup_failures(&Cycle), 0);
}
