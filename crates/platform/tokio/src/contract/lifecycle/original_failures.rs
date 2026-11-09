//! The original protocol's finite fault positions. No controller/IO/Scope is
//! retained here, and local error reception is not native or registration ACK.
use super::*;
use std::sync::OnceLock;
#[derive(Default, Debug)]
pub struct OriginalProtocolFailures {
    poll: OnceLock<Arc<ChannelError>>,
    protocol: OnceLock<Arc<ChannelError>>,
    foreign: OnceLock<Arc<ChannelError>>,
    received: AtomicBool,
}
impl OriginalProtocolFailures {
    pub fn poll_panic(&self) -> Option<&Arc<ChannelError>> {
        self.poll.get()
    }
    pub fn protocol(&self) -> Option<&Arc<ChannelError>> {
        self.protocol.get()
    }
    /// A different, uncontracted source is retained, never discarded or used as
    /// native close. Its controller cannot mint unbounded faults by repolling.
    pub fn foreign_source(&self) -> Option<&Arc<ChannelError>> {
        self.foreign.get()
    }
    pub(crate) fn record(&self, poll: bool, source: Arc<ChannelError>) {
        let slot = if poll { &self.poll } else { &self.protocol };
        if let Err(source) = slot.set(source) {
            let stored = slot.get().unwrap();
            let same = same_original(stored, &source)
                || stored.cleanup_failure_receipt().is_some_and(|receipt| {
                    source
                        .cleanup_failure_receipt()
                        .is_some_and(|actual| receipt.same_receipt(actual))
                });
            if !same {
                assert!(
                    self.foreign.set(source).is_ok(),
                    "fenced original cannot mint further independent errors"
                );
            }
        }
        if self.received.load(Ordering::Acquire) {
            self.accept_sources();
        }
    }
    pub(crate) fn identity_failed(&self) -> bool {
        self.foreign.get().is_some()
    }
    pub(crate) fn accept(&self) {
        self.received.store(true, Ordering::Release);
        self.accept_sources();
    }
    fn accept_sources(&self) {
        for source in [self.poll.get(), self.protocol.get(), self.foreign.get()]
            .into_iter()
            .flatten()
        {
            if let Some(receipt) = source.cleanup_failure_receipt() {
                receipt.acknowledge_retained(source);
            }
        }
    }
}
impl std::fmt::Display for OriginalProtocolFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual original protocol fault sources")
    }
}
impl std::error::Error for OriginalProtocolFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.poll
            .get()
            .or_else(|| self.protocol.get())
            .map(|source| source.as_ref() as _)
    }
}

fn same_original(stored: &Arc<ChannelError>, observed: &Arc<ChannelError>) -> bool {
    let mut stored: &ChannelError = stored.as_ref();
    let mut observed: &ChannelError = observed.as_ref();
    while let Some(source) = stored.retained_diagnostic() {
        stored = source;
    }
    while let Some(source) = observed.retained_diagnostic() {
        observed = source;
    }
    std::ptr::eq(stored, observed)
}
