use super::*;
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_platform_tokio::EgressSelection;
use zero_traits::DialPolicy;
pub(super) struct Binding {
    source: Arc<dyn DatagramBindingAuthorization>,
    generation: NetworkGeneration,
    cancel: CancellationToken,
    revoked: AtomicBool,
    failure: Mutex<Option<SharedError>>,
}
impl Binding {
    pub(super) fn new(
        source: Arc<dyn DatagramBindingAuthorization>,
        generation: NetworkGeneration,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            source,
            generation,
            cancel,
            revoked: AtomicBool::new(false),
            failure: Mutex::new(None),
        }
    }
    pub(super) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }
    pub(super) fn retired(&self) -> bool {
        if self.revoked.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            return true;
        }
        if let Err(source) = self.source.check_current(self.generation) {
            self.failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| Arc::new(source));
            self.revoke();
            return true;
        }
        false
    }
}
impl DatagramBindingAuthorization for Binding {
    fn policy(&self) -> &DialPolicy {
        self.source.policy()
    }
    fn preferred_port(&self) -> Option<u16> {
        self.source.preferred_port()
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        if expected != self.generation {
            let mut failure = ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session);
            failure.delivery = DeliveryState::NotSubmitted;
            return Err(failure);
        }
        if self.retired() {
            if let Some(source) = self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                return Err(retained(source));
            }
            let mut failure = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session);
            failure.delivery = DeliveryState::NotSubmitted;
            return Err(failure);
        }
        self.source.check_current(expected)
    }
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<EgressSelection, ChannelError> {
        self.check_current(expected)?;
        let selected = self.source.select(peer, expected)?;
        self.check_current(expected)?;
        if selected.generation() != expected.0 {
            let mut failure = ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session);
            failure.delivery = DeliveryState::NotSubmitted;
            return Err(failure);
        }
        Ok(selected)
    }
}
