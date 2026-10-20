use super::*;
use crate::contract::{
    component::ResourceDeclaration,
    lifecycle::{ChannelControl, Unfinished},
};
impl ControlledTcpListenerOwner {
    /// Library opt-in only. No accepted default or canonical/config change.
    pub fn new_with_accepted_limit(
        declaration: &ResourceDeclaration,
        attempts: NonZeroUsize,
        parent: CancellationToken,
        accepted_limit: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        let mut owner = Self::new(declaration, attempts, parent)?;
        let shared = Arc::get_mut(&mut owner.control.shared).expect("cold unique listener owner");
        shared.accepted = Some(Arc::new(Pool::new(
            accepted_limit,
            shared.cancel.child_token(),
        )));
        Ok(owner)
    }
    /// None means no accepted authority/provider was installed, not empty true.
    pub fn accepted_evidence(&self) -> Option<AcceptedTcpEvidence> {
        self.control
            .shared
            .accepted
            .as_ref()
            .map(|pool| pool.evidence())
    }
    pub async fn close_accepted(
        &mut self,
        claim: &AcceptedTcpClaim,
        deadline: Instant,
    ) -> Result<Option<AcceptedTcpNativeClosed>, ChannelError> {
        let pool = self.control.shared.accepted.as_ref().ok_or_else(|| {
            failure(
                Stage::Close,
                Reason::Unsupported,
                "accepted TCP authority was not installed",
            )
        })?;
        pool.close(claim, deadline).await
    }
    /// Partial native receipts only, never a whole provider/user proof.
    pub fn take_accepted_report(&self) -> CloseReport {
        self.control
            .shared
            .accepted
            .as_ref()
            .map_or_else(CloseReport::default, |pool| pool.take_report())
    }
    /// Durable partial receipts when another child makes shutdown return Err.
    /// Completion of this extracted report alone never proves users/closure.
    pub fn take_native_report(&mut self) -> CloseReport {
        let mut report = std::mem::take(&mut self.pending_close);
        report.append(self.take_accepted_report());
        report
    }
    pub(in crate::controlled_listener) async fn wait_owned_children(
        &mut self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let listener = self.control.wait_closed(deadline).await;
        let mut current_unfinished = Vec::new();
        let listener_error = match listener {
            Ok(mut report) => {
                current_unfinished.append(&mut report.unfinished);
                self.pending_close.append(report);
                None
            }
            Err(error) => Some(error),
        };
        let pool = self
            .control
            .shared
            .accepted
            .clone()
            .expect("tracked library mode");
        let child_error = pool.shutdown(deadline).await;
        if let Some(error) = listener_error.or(child_error) {
            return Err(error);
        }
        let mut report = std::mem::take(&mut self.pending_close);
        report.append(pool.take_report());
        report.unfinished.append(&mut current_unfinished);
        let evidence = pool.evidence();
        if evidence.retained != 0 {
            report.unfinished.push(Unfinished::Io(
                "accepted TCP originals remain owned or Unknown".into(),
            ));
        }
        if evidence.users == AcceptedTcpUsers::Unaccounted {
            report.unfinished.push(Unfinished::Resource(
                "accepted TCP transferred users/tasks are Unaccounted".into(),
            ));
        }
        Ok(report)
    }
}
