use super::*;
use zero_platform_tokio::contract::{Impact, Stage};
impl ControlledCandidates {
    async fn refresh_anchor(
        &self,
        source: CandidateRefresh,
    ) -> Result<(CarrierAdoption, Option<CarrierUserView>), ChannelError> {
        if source.candidate().generation() != self.generation
            || self.services.network_generation() != self.generation.0
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct DNS refresh belongs to an obsolete selected generation",
            ));
        }
        if let Some(original) = self
            .last_failed
            .lock()
            .expect("Direct original failed slot")
            .clone()
        {
            return Ok((original, None));
        }
        // Selection can fail before a native candidate exists. Reserve the
        // same finite owner for the actual refresh task; no FD claim is made.
        let mut grant = self.demand.grants.reserve()?;
        let view = grant.claim_user_view()?;
        let adoption = grant.adoption();
        self.demand.request.authorize_direct(
            source.candidate(),
            &self.binding(source.candidate().generation()),
            &grant,
        )?;
        self.keep_failure(adoption.clone());
        Ok((adoption, Some(view)))
    }
    pub(super) async fn refresh_owned(
        &self,
        work: RefreshFuture,
        source: CandidateRefresh,
    ) -> std::io::Result<Vec<SocketAddr>> {
        let (anchor, view) = self
            .refresh_anchor(source)
            .await
            .map_err(|error| self.fail(error).source)?;
        let (send, receive) = tokio::sync::oneshot::channel();
        let cancel = self.context.cancel.clone();
        let task = async move {
            let result = tokio::select! { biased;
                result = work => result,
                _ = cancel.cancelled() => Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "original Direct demand cancelled during DNS refresh")),
            }.map_err(Arc::new);
            let failure = result.as_ref().err().cloned();
            let _ = send.send(result);
            match failure {
                Some(source) => Err(
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session)
                        .with_source(RefreshSource(source)),
                ),
                None => Ok(()),
            }
        };
        anchor
            .spawn_auxiliary_weak(&self.demand.tasks, task)
            .map_err(|error| self.fail(error).source)?;
        let result = receive.await;
        // Actual same Scope must consume JoinSet before this becomes a receipt.
        // Persist the receipt/original failure in the original slot before ACK.
        anchor
            .accept_auxiliary()
            .await
            .map_err(|error| self.fail(error).source)?;
        drop(view);
        result
            .map_err(std::io::Error::other)?
            .map_err(|source| std::io::Error::new(source.kind(), RefreshSource(source)))
    }
}
#[derive(Debug)]
struct RefreshSource(Arc<std::io::Error>);
impl std::fmt::Display for RefreshSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for RefreshSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
