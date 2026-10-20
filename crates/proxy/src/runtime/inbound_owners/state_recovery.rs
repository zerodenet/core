//! One pending transition's passive protocol state, tied to its actual old owner.
use super::*;
use crate::protocol_registry::{InboundRecovery, PreparedInboundBind};
use zero_config::{InboundConfig, RuntimeConfig};

pub(super) struct RetainedState {
    identity: ComponentInstanceId,
    last_good: InboundConfig,
    state: Arc<dyn InboundRecovery>,
    closed: Option<Arc<InboundTerminal>>,
}
impl InboundOwner {
    pub(crate) fn install_recovery(
        &self,
        state: Arc<dyn InboundRecovery>,
    ) -> Result<(), EngineError> {
        self.recovery
            .set(state)
            .map_err(|_| failure("actual listener recovery state was already installed"))
    }
}
impl InboundOwnerRegistry {
    /// Capture before StopOld, while the original operation still owns its state.
    /// Cancellation cannot turn a removed active-map entry into lost history.
    pub(crate) fn retain_recovery(
        &self,
        tag: &str,
        last_good: &InboundConfig,
    ) -> Result<(), EngineError> {
        if tag != last_good.tag {
            return Err(failure(
                "listener recovery tag differs from last-good facts",
            ));
        }
        let entries = self.entries.lock().expect("inbound entries poisoned");
        let mut states = self
            .retained_states
            .lock()
            .expect("listener recovery poisoned");
        if let Some(retained) = states.get(tag) {
            if retained.last_good != *last_good {
                return Err(failure(
                    "a different last-good listener transition remains pending",
                ));
            }
            return Ok(());
        }
        let Some(owner) = entries.get(tag) else {
            return Ok(());
        };
        let Some(state) = owner.recovery.get() else {
            return Ok(());
        };
        states.insert(
            tag.into(),
            RetainedState {
                identity: owner.identity(),
                last_good: last_good.clone(),
                state: state.clone(),
                closed: None,
            },
        );
        Ok(())
    }
    pub(super) fn confirm_recovery_origin(&self, tag: &str, terminal: &Arc<InboundTerminal>) {
        let mut states = self
            .retained_states
            .lock()
            .expect("listener recovery poisoned");
        if let Some(retained) = states.get_mut(tag) {
            if retained.identity == terminal.identity() {
                retained.closed = Some(terminal.clone());
            }
        }
    }
    /// A cancelled host pass may already have released the original entry.
    /// Only that bounded transition's exact last-good/instance terminal can be
    /// observed again; absence of an entry is never fabricated into a terminal.
    pub(crate) fn closed_recovery_origin(
        &self,
        tag: &str,
        last_good: &InboundConfig,
    ) -> Result<Option<Arc<InboundTerminal>>, EngineError> {
        let states = self
            .retained_states
            .lock()
            .expect("listener recovery poisoned");
        let Some(retained) = states.get(tag) else {
            return Ok(None);
        };
        if retained.last_good != *last_good {
            return Err(failure(
                "listener phase differs from retained last-good facts",
            ));
        }
        let Some(terminal) = &retained.closed else {
            return Err(failure("original listener terminal remains unconfirmed"));
        };
        if terminal.identity() != retained.identity {
            return Err(failure(
                "listener phase terminal belongs to another actual instance",
            ));
        }
        Ok(Some(terminal.clone()))
    }
    /// Host identity, last-good facts and the actual terminal precede invoking
    /// opaque protocol behavior. The capsule itself cannot manufacture a permit.
    pub(crate) fn prepare_recovery_bind(
        &self,
        inbound: InboundConfig,
        source_dir: Option<&std::path::Path>,
        restore: bool,
    ) -> Result<Option<PreparedInboundBind>, EngineError> {
        let states = self
            .retained_states
            .lock()
            .expect("listener recovery poisoned");
        let Some(retained) = states.get(&inbound.tag) else {
            return Ok(None);
        };
        let Some(terminal) = &retained.closed else {
            return Err(failure(
                "original listener recovery still requires its physical terminal",
            ));
        };
        if terminal.identity() != retained.identity {
            return Err(failure(
                "listener recovery terminal belongs to another instance",
            ));
        }
        let exact = retained.last_good == inbound;
        if restore && !exact {
            return Err(failure(
                "listener restoration differs from its exact last-good facts",
            ));
        }
        let state = retained.state.clone();
        drop(states);
        if restore || exact {
            state.prepare_restore(inbound, source_dir).map(Some)
        } else {
            state.prepare_replacement(inbound, source_dir)
        }
    }
    /// Commit may discard only confirmed old state whose replacement is really
    /// Ready, or whose listener was deliberately removed in this accepted config.
    pub(crate) fn finish_recovery_transition(&self, config: &RuntimeConfig) {
        let entries = self.entries.lock().expect("inbound entries poisoned");
        self.retained_states
            .lock()
            .expect("listener recovery poisoned")
            .retain(|tag, state| {
                if state.closed.is_none() {
                    return true;
                }
                match entries.get(tag) {
                    Some(owner) => {
                        owner.identity() == state.identity
                            || !owner.is_ready()
                            || !owner.is_running()
                    }
                    None => config.inbounds.iter().any(|inbound| inbound.tag == *tag),
                }
            });
    }
}
fn failure(message: &'static str) -> EngineError {
    std::io::Error::other(message).into()
}

#[cfg(test)]
mod tests;
