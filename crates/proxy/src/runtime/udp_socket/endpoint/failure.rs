//! Preserve the actual scoped failure at the temporary legacy error boundary.
use super::*;
use std::error::Error;

pub(super) fn logical_rejection(error: &EngineError) -> bool {
    channel_rejection(error, 0).unwrap_or(false)
}
fn channel_rejection(error: &(dyn Error + 'static), depth: u8) -> Option<bool> {
    if depth >= 64 {
        return Some(false);
    }
    let channel = error
        .downcast_ref::<ChannelError>()
        .or_else(|| error.downcast_ref::<Arc<ChannelError>>().map(Arc::as_ref));
    if let Some(channel) = channel {
        // Stop at the actual channel boundary: an Io/Unknown owner failure may
        // retain older contextual errors, which cannot turn it into rejection.
        return Some(matches!(
            channel.reason,
            Reason::Revoked | Reason::Cancelled | Reason::StalePlan | Reason::Capacity
        ));
    }
    if let Some(EngineError::Io(io)) = error.downcast_ref::<EngineError>() {
        return channel_rejection(io, depth + 1);
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if let Some(inner) = io.get_ref() {
            if let Some(result) = channel_rejection(inner, depth + 1) {
                return Some(result);
            }
        }
    }
    error
        .source()
        .and_then(|source| channel_rejection(source, depth + 1))
}
