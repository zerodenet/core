use super::*;
use zero_platform_tokio::contract::RetryClass;
#[test]
fn typed_rejection_keeps_never_retry_and_unknown_owner_boundary_wins_over_older_context() {
    for reason in [
        Reason::Revoked,
        Reason::Cancelled,
        Reason::StalePlan,
        Reason::Capacity,
    ] {
        let source = Arc::new(error(Stage::Prepare, reason, DeliveryState::NotSubmitted));
        assert_eq!(source.retry, RetryClass::Never);
        assert!(!source.retry_permitted());
        let retained = legacy(
            error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source.clone()),
        );
        assert!(
            !failure::logical_rejection(&retained),
            "unknown actual owner boundary is decisive"
        );
        let ordinary = std::io::Error::other(std::io::Error::other(source.clone()));
        let ordinary = EngineError::Io(ordinary);
        assert!(failure::logical_rejection(&ordinary));
        assert_eq!(source.stage, Stage::Prepare);
        assert_eq!(source.delivery, DeliveryState::NotSubmitted);
        assert_eq!(source.retry, RetryClass::Never);
    }
    assert!(!failure::logical_rejection(&EngineError::Io(
        std::io::Error::other("Capacity StalePlan words are not typed permission")
    )));
}
