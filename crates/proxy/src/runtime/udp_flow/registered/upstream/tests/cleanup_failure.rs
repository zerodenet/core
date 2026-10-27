use super::local_cleanup_failure;
use crate::{
    runtime::passive_relay_health::classify_outbound_establishment_failure,
    transport::{channel_failure, failure_origin, TransportFailureOrigin},
};
use std::{io, sync::Arc};
use zero_engine::{EngineError, PassiveRelayOutcome};
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

#[test]
fn close_error_metadata_and_original_source_survive_local_retirement_attribution() {
    // Negative error-provenance fixture; it never certifies any resource close.
    let source = Arc::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "original close source",
    ));
    let channel =
        ChannelError::new(Stage::Close, Reason::Io, Impact::Session).with_source(source.clone());
    let error = local_cleanup_failure(EngineError::Io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        channel,
    )));
    let original = channel_failure(&error).expect("original ChannelError was erased");
    assert_eq!(original.stage, Stage::Close);
    assert_eq!(original.reason, Reason::Io);
    let retained = std::error::Error::source(original)
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &source));
    assert_eq!(
        failure_origin(&error),
        Some(TransportFailureOrigin::LocalNetwork)
    );
    assert_eq!(
        classify_outbound_establishment_failure(&error, None),
        PassiveRelayOutcome::Neutral
    );
}

#[test]
fn incomplete_local_cleanup_does_not_mark_the_unattempted_outbound_unhealthy() {
    let error = local_cleanup_failure(EngineError::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "original closure is still pending",
    )));
    let EngineError::Io(io) = &error else {
        panic!("preserve the caller error kind")
    };
    assert_eq!(io.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        classify_outbound_establishment_failure(&error, None),
        PassiveRelayOutcome::Neutral
    );
}
