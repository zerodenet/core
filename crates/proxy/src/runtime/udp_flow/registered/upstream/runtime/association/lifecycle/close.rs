use tokio::time::Instant as TokioInstant;
use zero_engine::EngineError;

use super::super::super::super::contract::{
    UpstreamAssociationCloseReason, UpstreamAssociationTarget, UpstreamAssociationTransport,
};
use super::super::model::UpstreamAssociationRuntime;

impl<T, A> UpstreamAssociationRuntime<T, A>
where
    T: UpstreamAssociationTarget,
    A: UpstreamAssociationTransport<T>,
{
    pub(crate) fn drop_after_send_error(&mut self, inbound_tag: &str, error: &EngineError) {
        if let Some(assoc) = self.upstream.take() {
            let (record, association) = assoc.into_parts();
            self.retire(association, UpstreamAssociationCloseReason::Dropped);
            let (outbound_tag, endpoint) = record.log_parts();
            if let Some((server, port)) = endpoint {
                crate::logging::log_udp_upstream_association_dropped(
                    inbound_tag,
                    outbound_tag,
                    server,
                    port,
                    error,
                );
            }
        }
        self.idle_deadline = None;
    }

    pub(crate) fn close_idle(&mut self) -> Option<T> {
        self.take_upstream().map(|association| {
            let (target, association) = association.into_parts();
            self.retire(association, UpstreamAssociationCloseReason::IdleTimeout);
            target
        })
    }

    pub(crate) fn close_dropped(&mut self) -> Option<T> {
        self.take_upstream().map(|association| {
            let (target, association) = association.into_parts();
            self.retire(association, UpstreamAssociationCloseReason::Dropped);
            target
        })
    }

    pub(crate) fn close_all_upstreams(&mut self) {
        if let Some(association) = self.upstream.take() {
            let (_, association) = association.into_parts();
            self.retire(association, UpstreamAssociationCloseReason::Closed);
        }
        self.idle_deadline = None;
    }

    pub(super) fn retire(&mut self, mut association: A, reason: UpstreamAssociationCloseReason) {
        association.request_close(reason);
        self.upstream.retire(association);
    }

    pub(super) async fn confirm_retired(
        &mut self,
        deadline: TokioInstant,
    ) -> Result<(), EngineError> {
        if let Some(original) = self.upstream.retiring_mut() {
            let report = original
                .wait_closed(deadline)
                .await
                .map_err(local_cleanup_failure)?;
            if !report.is_complete() {
                return Err(local_cleanup_failure(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "upstream Datagram original closure remains incomplete",
                ))));
            }
            self.upstream.release_confirmed();
        }
        Ok(())
    }
}

// No new upstream has been attempted. Preserve the actual close failure as the
// source while keeping local retirement out of selected-node failure scores.
fn local_cleanup_failure(source: EngineError) -> EngineError {
    let kind = match &source {
        EngineError::Io(error) => error.kind(),
        _ => std::io::ErrorKind::Other,
    };
    EngineError::Io(crate::transport::attributed_error(
        crate::transport::TransportFailureOrigin::LocalNetwork,
        "udp_upstream_cleanup",
        std::io::Error::new(kind, source),
    ))
}

#[cfg(test)]
#[path = "../../../tests/cleanup_failure.rs"]
mod tests;
