use std::time::Instant;

use zero_core::Session;
use zero_engine::{EngineError, SessionHandle};

use crate::logging::{log_session_failed, session_failure_observation};

pub(crate) fn finish_target_recovery_failure(
    handle: &mut SessionHandle,
    session: &Session,
    started_at: Instant,
    error: &EngineError,
) {
    let record = handle.finish_with_failure(
        "target_error",
        session_failure_observation("target_recovery", error, None),
    );
    log_session_failed(
        session,
        record.as_ref(),
        "target_recovery",
        started_at.elapsed(),
        error,
        None,
    );
}
