//! The actual establishing Aux is received independently of physical cleanup.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
pub(super) enum Registration {
    Pending,
    Refused,
    Actual(TaskJoinWitness),
}
impl Owners {
    pub fn set_actor(&self, actor: Option<TaskJoinWitness>) {
        let mut state = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (&*state, &actor) {
            (Registration::Actual(previous), Some(actor)) => {
                assert!(previous.same_registration(actor))
            }
            (Registration::Pending, _) => {
                *state = match actor {
                    Some(actor) => Registration::Actual(actor),
                    None => Registration::Refused,
                }
            }
            (Registration::Refused, None) => {}
            _ => panic!("one actual QUIC establishing registration"),
        }
        drop(state);
        self.actor_changed.notify_waiters();
    }
    pub async fn receive_actor_original(&self) -> Result<(), ChannelError> {
        let scope = self.tasks.retirement_observation();
        let actor = loop {
            let changed = self.actor_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let actor = {
                let state = self
                    .actor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*state {
                    Registration::Actual(actor) => Some(Some(actor.clone())),
                    Registration::Refused => Some(None),
                    Registration::Pending => None,
                }
            };
            if let Some(actor) = actor {
                break actor;
            }
            tokio::select! {
                _=&mut changed=>{},
                _=scope.wait_drained()=>return Ok(()),
            }
        };
        let Some(actor) = actor else { return Ok(()) };
        let joined = actor.wait().await;
        if !actor.matches_joined(&joined) {
            return Err(error(Stage::Close, Reason::StalePlan));
        }
        // Durable pure receipt/source is installed before local acceptance.
        // Only the actual Host later publishes this registration's ACK.
        {
            let mut original = self
                .actor_exit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            original.get_or_insert_with(|| joined.clone());
        }
        if let Err(source) = joined.result() {
            self.diagnostics.terminal(source);
            actor
                .acknowledge_hosted_failure(&joined)
                .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
        }
        Ok(())
    }
}

pub(in crate::component::quic) enum OriginalRegistration {
    Native(zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin),
    Borrowed(zero_platform_tokio::controlled_carrier::CarrierAdoption),
}
pub(in crate::component::quic) struct RegistrationGuard {
    entry: PacketMetadataArc<Entry>,
    original: OriginalRegistration,
    active: bool,
}
impl RegistrationGuard {
    pub fn new(entry: PacketMetadataArc<Entry>, original: OriginalRegistration) -> Self {
        Self {
            entry,
            original,
            active: true,
        }
    }
    pub fn finish(mut self, actor: Option<TaskJoinWitness>) {
        self.entry.owners.set_actor(actor);
        self.active = false;
    }
}
impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let actual = match &self.original {
            OriginalRegistration::Native(original) => original.initializer_registration(),
            OriginalRegistration::Borrowed(original) => original.initializer_registration(),
        };
        if let Some(actual) = actual {
            self.entry.owners.set_actor(Some(actual));
        }
        // An uncertain synchronous unwind is not proof of no registration.
        // The original registration ledger/Scope census remains charged until
        // its genuine supervisor resolves that uncertainty.
        self.entry.retirement.request_retirement();
    }
}
