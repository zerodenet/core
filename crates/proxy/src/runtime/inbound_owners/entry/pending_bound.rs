//! Original rejected listeners and their persistent explicit-close progress.
mod tree;
use super::*;
pub(super) use tree::PendingNativeBound;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn begin_listener_installation(&self) {
        self.installation_complete.store(false, Ordering::Release);
    }
    pub(crate) fn seal_listener_installation(&self) {
        self.installation_complete.store(true, Ordering::Release);
    }
    /// The original object moves here before returning the preparation error.
    /// Cold TCP leaves close through their original native control. Other
    /// shapes remain held until their exact resource boundary is migrated.
    pub(crate) fn retain_unexecuted_bound(
        &self,
        bound: crate::protocol_registry::BoundInbound,
        source: EngineError,
    ) -> EngineError {
        let mut pending = self
            .pending_native_bound
            .lock()
            .expect("pending native listener poisoned");
        assert!(pending.is_none(), "one actual bound listener per instance");
        *pending = Some(Arc::new(PendingNativeBound::new(bound)));
        // Store the original first. Conversion replaces each leaf's control
        // inside that same persistent slot before dropping its accept view.
        pending.as_ref().unwrap().prepare(self.cancel.clone());
        let error = self.retain_failed_driver_installation(source);
        self.seal_listener_installation();
        error
    }
    /// A failed registration may have consumed its submitted future. Without a
    /// native receipt, retain Unknown and the unsealed installation forever;
    /// never infer close from that future's Drop or invent a joined TaskId.
    pub(crate) fn retain_failed_driver_installation(&self, source: EngineError) -> EngineError {
        let stopped = super::super::preparation_stopped(&source);
        let mut error =
            ChannelError::new(Stage::Close, Reason::Revoked, Impact::Component).with_source(source);
        error.component = Some(self.identity.component);
        error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
        let error = Arc::new(error);
        let cause = self
            .pending_native_cause
            .lock()
            .expect("pending native cause poisoned")
            .get_or_insert_with(|| error.clone())
            .clone();
        let mut failure = self.failure.lock().expect("inbound failure poisoned");
        if !stopped {
            failure.get_or_insert(error);
        }
        self.ready.wake();
        if let Some(failure) = failure.as_ref() {
            std::io::Error::other(retained(failure.clone())).into()
        } else {
            // Only the original request reports an expected phase stop. A
            // later unconfirmed cleanup still returns the retained Unknown;
            // an arbitrary Interrupted/native/task error cannot acquire this
            // private marker by appearing anywhere in an error source chain.
            std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                StoppedBeforeExecution(cause),
            )
            .into()
        }
    }
    pub(super) fn pending_native_result(&self) -> Result<(), EngineError> {
        self.result()?;
        match self
            .pending_native_cause
            .lock()
            .expect("pending native cause poisoned")
            .as_ref()
        {
            Some(cause) => Err(std::io::Error::other(retained(cause.clone())).into()),
            None => Ok(()),
        }
    }
}
#[derive(Debug)]
struct StoppedBeforeExecution(SharedError);
impl std::fmt::Display for StoppedBeforeExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("host stopped preparation; original native ownership remains retained")
    }
}
impl std::error::Error for StoppedBeforeExecution {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
pub(in crate::runtime::inbound_owners) fn pending_preparation_stopped(
    error: &std::io::Error,
) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<StoppedBeforeExecution>())
}
#[cfg(test)]
mod tests;
