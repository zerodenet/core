//! Actual accepted claims retain their global connection charge across awaits.
use super::*;
use std::{collections::BTreeMap, sync::Mutex};
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
#[derive(Default)]
pub(super) struct Connections {
    acquiring: Mutex<Option<ControlBudgetPermit>>,
    entries: Mutex<BTreeMap<u64, (AcceptedTcpClaim, ControlBudgetPermit)>>,
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
    pub(super) fn installed(&self, claim: AcceptedTcpClaim) {
        let permit = self
            .acquiring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("charge installed before native accept");
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(claim.id(), (claim, permit));
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
            .map(|(claim, _)| claim.clone())
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
