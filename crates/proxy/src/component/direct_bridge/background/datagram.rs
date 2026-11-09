//! Direct UDP uses the same original native owner, setup Scope and N as TCP.
use super::*;
use zero_platform_tokio::{
    contract::{
        context::with_deadline, network::ControlledDatagramRequest, scope::TaskJoinWitness,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierAdoption, NativeCarrierOpener},
};
struct Opening(Option<CarrierAdoption>);
impl Drop for Opening {
    fn drop(&mut self) {
        if let Some(original) = &self.0 {
            original.request_retirement();
        }
    }
}
impl Root {
    pub(super) async fn open_datagram_loan(
        &self,
        binding: UpstreamBinding,
        mut request: ControlledDatagramRequest,
    ) -> Result<DatagramLoan, ChannelError> {
        let slot = self.owner()?;
        let call = self.call.as_ref().ok_or_else(|| {
            invalid(
                Reason::StalePlan,
                "Direct Datagram lacks its selected Service call",
            )
        })?;
        if binding != self.binding
            || request.destination != self.endpoint
            || request.context.session.is_some()
            || request.context.deadline != call.deadline
            || request.context.cancel.is_cancelled()
            || call.cancel.is_cancelled()
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct Datagram differs from the selected original request",
            ));
        }
        request.context = call.clone();
        slot.check_direct_background_owner(
            &binding.dial,
            &request.grants.registration(),
            &request.tasks.scope_identity(),
        )?;
        // DNS passes its actual selected numeric endpoint. Domains require the
        // existing Node/candidate path, never an unowned or second resolver.
        let peer = match &request.destination.address {
            zero_core::Address::Ipv4(ip) => std::net::SocketAddr::new(
                std::net::Ipv4Addr::from(*ip).into(),
                request.destination.port,
            ),
            zero_core::Address::Ipv6(ip) => std::net::SocketAddr::new(
                std::net::Ipv6Addr::from(*ip).into(),
                request.destination.port,
            ),
            zero_core::Address::Domain(_) => {
                return Err(invalid(
                    Reason::Unsupported,
                    "Direct Datagram requires its selected native candidate",
                ))
            }
        };
        let end = tokio::time::Instant::now()
            .checked_add(request.retirement)
            .ok_or_else(|| {
                invalid(
                    Reason::InvalidConfig,
                    "Direct Datagram retirement deadline overflow",
                )
            })?;
        let end = request.context.deadline.map_or(end, |bound| bound.min(end));
        request.grants.reap_returned(end).await?;
        let mut grant = request.grants.reserve()?;
        let (view, preparation) = grant.claim_preparing_transferred_view()?;
        let adoption = grant.adoption();
        let mut opening = Opening(Some(adoption.clone()));
        self.waiting
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "Direct Datagram has no actual pending receiver",
                )
            })?
            .retain(adoption.clone())?;
        let (begin, started) = tokio::sync::oneshot::channel::<TaskJoinWitness>();
        let (send, receive) = tokio::sync::oneshot::channel();
        let owner = Arc::downgrade(&slot);
        let endpoint = request.destination.clone();
        let binding = binding.clone();
        let context = request.context.clone();
        let egress = Arc::new(self.services.egress_control());
        let observer = self.services.observer();
        let (_, witness) = adoption.spawn_auxiliary_weak(&request.tasks, async move {
            let Ok(caller) = started.await else {
                return Ok(());
            };
            let result = (|| {
                let slot = owner
                    .upgrade()
                    .ok_or_else(|| invalid(Reason::Revoked, "Direct Datagram owner retired"))?;
                let ticket = slot.issue_direct_background_datagram(
                    caller, &endpoint, &binding, &grant, &context,
                )?;
                let opener = NativeCarrierOpener::service(ticket, egress, observer)?;
                zero_platform_tokio::contract::network::UpstreamOpener::prepare_controlled_datagram(
                    &opener, &binding, endpoint, context, grant,
                )
            })();
            // Native acquisition has returned its real disposition to the
            // installed row. Ordinary dial/prepare errors are the caller's
            // business result; Scope still owns any actual panic separately.
            // A lost result receiver drops the view, not its Root controller.
            let _ = send.send(result);
            Ok(())
        })?;
        let _ = begin.send(witness.clone());
        let outcome = with_deadline(request.context.deadline, async {
            tokio::select! { biased;
                _ = request.context.cancel.cancelled() => Err(invalid(Reason::Cancelled, "Direct Datagram caller cancelled")),
                result = receive => result.map_err(|source| invalid(Reason::Revoked, "Direct Datagram setup returned no original result").with_source(source)),
            }
        }).await.map_err(|source| invalid(Reason::Timeout, "Direct Datagram setup absolute deadline").with_source(source))??;
        let joined = with_deadline(request.context.deadline, adoption.accept_auxiliary())
            .await
            .map_err(|source| {
                invalid(
                    Reason::Timeout,
                    "Direct Datagram actual setup join deadline",
                )
                .with_source(source)
            })??;
        if !joined
            .as_ref()
            .is_some_and(|joined| witness.matches_joined(joined))
        {
            return Err(invalid(
                Reason::Internal,
                "Direct Datagram setup delivered a foreign join",
            ));
        }
        let ack = with_deadline(
            request.context.deadline,
            witness.wait_registration_released(),
        )
        .await
        .map_err(|source| {
            invalid(Reason::Timeout, "Direct Datagram setup Host ACK deadline").with_source(source)
        })?;
        if !witness.matches_joined(&ack) || !ack.registration_released() {
            return Err(invalid(
                Reason::Internal,
                "Direct Datagram setup lacks its actual Host ACK",
            ));
        }
        let io = outcome?;
        if io.peer() != peer || !view.matches(&adoption) {
            return Err(invalid(
                Reason::StalePlan,
                "Direct Datagram returned a foreign actual native endpoint",
            ));
        }
        preparation.publish_transferred()?;
        let loan = io.into_datagram_loan(view, request.association)?;
        opening.0.take();
        Ok(loan)
    }
}
