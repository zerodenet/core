//! Error delivery only; original IO, users, joins and Host ACK stay independent.
use super::*;
use std::{
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// One original cleanup cause kept by its actual controller. Acceptance means
/// that a real receiver now retains the same returned cause. It grants no IO,
/// refund, replacement, native-close or task/registration completion.
#[derive(Debug)]
pub struct CleanupFailure {
    original: Arc<ChannelError>,
    accepted: AtomicBool,
}
impl CleanupFailure {
    pub fn new(original: Arc<ChannelError>) -> Arc<Self> {
        Arc::new(Self {
            original,
            accepted: AtomicBool::new(false),
        })
    }
    pub fn original(&self) -> &Arc<ChannelError> {
        &self.original
    }
    pub fn is_accepted(&self) -> bool {
        self.accepted.load(Ordering::Acquire)
    }
    /// Copy only decision fields. The diagnostic chain holds this same
    /// delivery cell and exact original Arc, not another failure identity.
    pub fn error(self: &Arc<Self>) -> ChannelError {
        let source = &self.original;
        let mut error = ChannelError::new(source.stage, source.reason, source.impact);
        error.component = source.component;
        error.session = source.session;
        error.delivery = source.delivery;
        error.retry = source.retry;
        error.with_source(Delivery(self.clone()))
    }
}
#[derive(Debug)]
struct Delivery(Arc<CleanupFailure>);
impl fmt::Display for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.original.as_ref(), f)
    }
}
impl Error for Delivery {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.original.as_ref())
    }
}

/// Call only AFTER the actual bounded receiver has durably retained this
/// outcome (for example its original Entry, TaskMonitor or Query fault row).
/// Merely polling a future, seeing a body finish or retrying never accepts it.
/// Call outside receiver/resource locks: foreign diagnostic source accessors
/// may execute arbitrary code. Only cells physically present in this exact
/// retained error chain are affected; equal messages/fields cannot substitute.
pub fn accept_retained_cleanup_failures(mut error: &(dyn Error + 'static)) -> usize {
    let mut accepted = 0;
    let mut visited: Vec<&(dyn Error + 'static)> = Vec::new();
    loop {
        if visited
            .iter()
            .any(|previous| std::ptr::eq(*previous, error))
        {
            break;
        }
        visited.push(error);
        if let Some(delivery) = error.downcast_ref::<Delivery>() {
            if !delivery.0.accepted.swap(true, Ordering::AcqRel) {
                accepted += 1;
            }
        }
        let Some(source) = error.source() else {
            break;
        };
        error = source;
    }
    accepted
}

#[cfg(test)]
mod tests;
