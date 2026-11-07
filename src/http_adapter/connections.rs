//! Actual accepted claims retain their global connection charge across awaits.
use super::*;
use std::{collections::BTreeMap, sync::Mutex};
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
#[derive(Default)]
pub(super) struct Connections {
    acquiring: Mutex<Option<ControlBudgetPermit>>,
    entries: Mutex<BTreeMap<u64, Arc<Original>>>,
}
pub(super) struct Original {
    claim: AcceptedTcpClaim,
    permit: ControlBudgetPermit,
    command: Mutex<Option<HeldCommand>>,
    worker: std::sync::OnceLock<zero_platform_tokio::contract::scope::TaskJoinWitness>,
}
struct HeldCommand {
    _request: Arc<super::input::HeldRequest>,
    observation: zero_runtime::ApplyObservation,
    _permit: zero_runtime::FrontendCommand,
}
impl Original {
    pub(super) fn register_worker(
        &self,
        actual: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> bool {
        self.worker.set(actual).is_ok()
    }
    pub(super) fn connection_budget(&self) -> zero_runtime::configuration::ControlConnectionBudget {
        self.permit
            .connection_budget()
            .expect("same actual HTTP connection issuer")
    }
    pub(super) fn reserve_command(
        &self,
        request: Arc<super::input::HeldRequest>,
    ) -> io::Result<zero_runtime::ApplyObservation> {
        let command = self
            .command
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if command.is_some() {
            return Err(io::Error::other("HTTP original already holds its command"));
        }
        drop(command);
        let permit = self
            .permit
            .connection_budget()
            .expect("same actual HTTP connection issuer")
            .reserve(zero_runtime::configuration::ControlConnectionCharge::PendingCommand)?;
        let actual = permit.command_owner().adopt_connection_command(permit)?;
        let worker = self
            .worker
            .get()
            .cloned()
            .ok_or_else(|| io::Error::other("HTTP original handler registration missing"))?;
        assert!(
            actual.register_scoped(worker),
            "same actual accepted HTTP task"
        );
        let observation = actual.observation();
        let mut command = self
            .command
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *command = Some(HeldCommand {
            _permit: actual,
            _request: request,
            observation: observation.clone(),
        });
        Ok(observation)
    }
    pub(super) async fn finish_command(&self) -> io::Result<()> {
        let observation = self
            .command
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|command| command.observation.clone());
        match observation {
            Some(observation) => {
                let work = crate::ipc::owner::wait_command_operation(&observation).await;
                let command = self
                    .command
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map(|command| command._permit.clone());
                let returned = match command {
                    Some(command) => command.finish_inline().await.map_err(io::Error::other),
                    None => Ok(()),
                };
                crate::ipc::owner::finish_work([work, returned])
            }
            None => Ok(()),
        }
    }
}
impl Connections {
    pub(super) fn before(&self, budget: &Arc<ControlBudget>) -> Result<(), ChannelError> {
        let mut acquiring = self
            .acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if acquiring.is_some() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        *acquiring = Some(budget.reserve(ControlCharge::Connection).map_err(|error| {
            ChannelError::new(
                Stage::Prepare,
                if error.kind() == io::ErrorKind::WouldBlock {
                    Reason::Capacity
                } else {
                    Reason::Revoked
                },
                Impact::Session,
            )
            .with_source(error)
        })?);
        Ok(())
    }
    pub(super) fn no_native(&self) {
        self.acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
    pub(super) fn installed(&self, claim: AcceptedTcpClaim) -> Arc<Original> {
        let permit = self
            .acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("charge installed before native accept");
        let original = Arc::new(Original {
            claim: claim.clone(),
            permit,
            command: Mutex::new(None),
            worker: std::sync::OnceLock::new(),
        });
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(claim.id(), original.clone());
        original
    }
    pub(super) async fn reap(
        &self,
        receiver: &zero_platform_tokio::AcceptedTcpTaskReceiver,
    ) -> io::Result<()> {
        let claims = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|original| original.claim.clone())
            .collect::<Vec<_>>();
        for claim in claims {
            if let Some(closed) = receiver
                .close_if_joined(&claim, crate::ipc::owner::close_deadline())
                .await
                .map_err(io::Error::other)?
            {
                if !closed.matches(&claim) {
                    return Err(io::Error::other("foreign HTTP accepted close"));
                }
                self.entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&claim.id());
            }
        }
        Ok(())
    }
    /// Called only after this same listener's entire native/accepted report and
    /// actual Scope joins are complete. Unknown accept charges stay until then.
    pub(super) fn confirm_all_closed(&self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        self.no_native();
    }
}
