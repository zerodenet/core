//! Existing Slot's setup Aux and its later actual HTTP driver, in one Scope.
mod handshake;
pub(in crate::component::quic::mixed) mod prepared;
mod ready;
pub(in crate::component::quic) mod setup;
use super::super::*;
use super::runtime::TcpGeneration;
use prepared::PreparedTcpOriginal;
use zero_platform_tokio::{
    contract::{context::OpenContext, scope::TaskJoinWitness},
    controlled_carrier::CarrierAdoption,
};
pub(in crate::component::quic::mixed) struct PendingTcpOriginal {
    generation: PacketMetadataArc<TcpGeneration>,
    entry: PacketMetadataArc<entry::Entry>,
    caller: TaskJoinWitness,
    context: OpenContext,
    pending: connect::Pending,
}
impl PreparedTcpOriginal {
    /// Both halves' constructors have already installed their actual controls.
    /// The only new task here is this original's declared setup Auxiliary.
    pub fn start(mut self) -> Result<PendingTcpOriginal, ChannelError> {
        let adoption = self.grant.adoption();
        let pending = connect::Pending(Some(self.entry.clone()));
        let registration = entry::actor::RegistrationGuard::new(
            self.entry.clone(),
            entry::actor::OriginalRegistration::Borrowed(adoption.clone()),
        );
        self.before.disarm();
        let context = self.context.clone();
        let generation = self.generation.clone();
        let entry = self.entry.clone();
        let (begin, started) = tokio::sync::oneshot::channel();
        let prepared::PreparedTcpOriginal {
            grant,
            view,
            preparation,
            upstream,
            call,
            ..
        } = self;
        let work_generation = generation.clone();
        let work_entry = entry.clone();
        let registered = adoption.spawn_auxiliary(&generation.tasks, async move {
            if started.await.is_err() {
                return Ok(());
            }
            let initializing = setup::Setup::initialize(
                &work_generation,
                &work_entry,
                grant,
                view,
                preparation,
                upstream,
                call,
            );
            let setup = tokio::select! { biased;
                _=work_entry.owners.stop.cancelled()=>None,
                _=work_generation.cancel.cancelled()=>None,
                result=initializing=>Some(result),
            };
            let terminal = setup.as_ref().and_then(|setup| match &setup.outcome {
                setup::Outcome::Rejected {
                    source,
                    staged_job: true,
                    ..
                } => Some(source.clone()),
                _ => None,
            });
            let discarded = {
                let mut state = work_entry.owners.state.lock().await;
                if state.cleanup_complete || work_entry.owners.stop.is_cancelled() {
                    setup
                } else {
                    assert!(state.tcp_setup.is_none());
                    state.tcp_setup = setup;
                    None
                }
            };
            // Actual IO/credit drops and notifications run outside State.
            drop(discarded);
            work_entry.owners.changed.notify_waiters();
            // A real staged job requires the original failed setup receipt.
            // Normal delivered dial/handshake rejection returns from its Aux
            // successfully; it is not a second component-wide task failure.
            match terminal {
                Some(source) => Err(ChannelError::retained(source)),
                None => Ok(()),
            }
        });
        let (_, caller) = match registered {
            Ok(actual) => actual,
            Err(source) => {
                registration.finish(adoption.initializer_registration());
                entry.retirement.request_retirement();
                return Err(source);
            }
        };
        registration.finish(Some(caller.clone()));
        let _ = begin.send(());
        Ok(PendingTcpOriginal {
            generation,
            entry,
            caller,
            context,
            pending,
        })
    }
}
