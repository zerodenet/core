//! A refusal counts only where this owner actually discards the packet.
use std::sync::Arc;
use zero_traits::{IoObserver, PacketDropReason};
pub(super) fn observe_discard(
    role: Option<&Arc<dyn IoObserver>>,
    boundary: Option<&Arc<dyn IoObserver>>,
    reason: PacketDropReason,
) {
    if let Some(role) = role {
        role.dropped_reason(reason);
    }
    if let Some(boundary) = boundary {
        if !role.is_some_and(|role| Arc::ptr_eq(role, boundary)) {
            boundary.dropped_reason(reason);
        }
    }
}
pub(super) fn observe_queue_drop<T>(
    role: Option<&Arc<dyn IoObserver>>,
    boundary: Option<&Arc<dyn IoObserver>>,
    error: &tokio::sync::mpsc::error::TrySendError<T>,
) {
    observe_discard(
        role,
        boundary,
        match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => PacketDropReason::QueueFull,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => PacketDropReason::QueueClosed,
        },
    );
}
