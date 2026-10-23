use std::sync::Arc;
use zero_core::component::SessionId;
use zero_platform_tokio::contract::context::{ConnectFact, ConnectObserver};

#[derive(Default)]
pub(super) struct Confirmation {
    observer: Option<Arc<dyn ConnectObserver>>,
    session: Option<SessionId>,
}
impl Confirmation {
    #[cfg(feature = "reality")]
    pub(super) fn new(
        session: Option<SessionId>,
        observer: Option<Arc<dyn ConnectObserver>>,
    ) -> Self {
        Self { session, observer }
    }
    pub(super) fn confirmed(&mut self) {
        if let Some(observer) = self.observer.take() {
            observer.record(self.session, ConnectFact::PeerConfirmed);
        }
    }
    pub(super) fn rejected(&mut self, error: std::io::Error) -> std::io::Error {
        if let Some(observer) = self.observer.take() {
            observer.record(self.session, ConnectFact::PeerRejected);
        }
        error
    }
}
