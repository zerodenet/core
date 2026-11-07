//! Original Direct Auxiliary, actual join/ACK, then one-way view delivery.
use super::{candidate::Candidates, *};
use zero_platform_tokio::{
    contract::{context::with_deadline, scope::TaskJoinWitness},
    controlled_carrier::{CarrierGrant, ControlledCarrier, NativeCarrierOpener},
};
impl Candidates {
    pub(super) async fn prepare(
        &self,
        slot: &Arc<ComponentSlot>,
        peer: std::net::SocketAddr,
        grant: CarrierGrant,
    ) -> Result<ControlledCarrier, ChannelError> {
        let adoption = grant.adoption();
        let (begin, started) = tokio::sync::oneshot::channel::<TaskJoinWitness>();
        let (send, receive) = tokio::sync::oneshot::channel();
        let owner = Arc::downgrade(slot);
        let endpoint = self.root.endpoint.clone();
        let binding = self.root.binding.clone();
        let call = self.request.context.clone();
        let dns = self.resolver.clone();
        let generation = self.dns_generation;
        let egress = Arc::new(self.root.services.egress_control());
        let observer = self.root.services.observer();
        let (_, witness) = adoption.spawn_auxiliary_weak(&self.request.tasks, async move {
            let Ok(caller) = started.await else {
                return Ok(());
            };
            let result = async {
                let slot = owner
                    .upgrade()
                    .ok_or_else(|| invalid(Reason::Revoked, "Direct initializer owner retired"))?;
                let ticket = slot
                    .issue_direct_background_carrier(caller, &endpoint, &binding, &grant, &call)?;
                let opener = NativeCarrierOpener::service_resolved(
                    ticket, peer, dns, generation, egress, observer,
                )?;
                opener
                    .prepare_controlled_stream(&binding, endpoint, call.clone(), grant)?
                    .open_with_deadline(call.deadline)
                    .await
            }
            .await
            .map_err(Arc::new);
            let fault = result.as_ref().err().cloned();
            let _ = send.send(result);
            match fault {
                Some(source) => Err(ChannelError::retained(source)),
                None => Ok(()),
            }
        })?;
        let _ = begin.send(witness.clone());
        let outcome=with_deadline(self.request.context.deadline,async {
            tokio::select!{biased;
                _=self.request.context.cancel.cancelled()=>Err(invalid(Reason::Cancelled,"Direct Service setup caller canceled")),
                result=receive=>result.map_err(|source|invalid(Reason::Revoked,"Direct setup did not return its original").with_source(source)),
            }
        }).await.map_err(|source|invalid(Reason::Timeout,"Direct Service setup absolute deadline").with_source(source))??;
        let original_error = outcome.as_ref().err().cloned();
        let joined = with_deadline(self.request.context.deadline, adoption.accept_auxiliary())
            .await
            .map_err(|source| {
                original_error.clone().map_or_else(
                    || invalid(Reason::Timeout, "Direct setup join deadline").with_source(source),
                    ChannelError::retained,
                )
            })?
            .map_err(|source| {
                original_error
                    .clone()
                    .map_or(source, ChannelError::retained)
            })?;
        if !joined
            .as_ref()
            .is_some_and(|joined| witness.matches_joined(joined))
        {
            return Err(original_error.map_or_else(
                || invalid(Reason::Internal, "Direct setup returned a foreign join"),
                ChannelError::retained,
            ));
        }
        let ack = with_deadline(self.request.context.deadline, async {
            tokio::select! {biased;
                _=self.request.context.cancel.cancelled()=>None,
                joined=witness.wait_registration_released()=>Some(joined),
            }
        })
        .await
        .map_err(|source| {
            original_error.clone().map_or_else(
                || invalid(Reason::Timeout, "Direct setup Host ACK deadline").with_source(source),
                ChannelError::retained,
            )
        })?;
        if !ack
            .as_ref()
            .is_some_and(|ack| witness.matches_joined(ack) && ack.registration_released())
        {
            return Err(original_error.map_or_else(
                || {
                    invalid(
                        Reason::Cancelled,
                        "Direct setup caller canceled before Host ACK",
                    )
                },
                ChannelError::retained,
            ));
        }
        outcome.map_err(ChannelError::retained)
    }
}
