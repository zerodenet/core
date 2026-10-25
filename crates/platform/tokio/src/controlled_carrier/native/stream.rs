use super::*;
impl NativeCarrierOpener {
    pub(super) fn prepare(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        mut grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        grant.check()?;
        if let Some(business) = &self.business {
            business.check(&grant, binding, &destination, &context)?;
            business.bind_caller(&grant.slot)?;
        }
        if let Some(service) = &self.service {
            service.check(&grant, binding, &destination, &context)?;
            service.bind_caller(&grant.slot)?;
        }
        let bound = grant.slot.owner.upgrade().and_then(|owner| {
            let state = owner.state();
            state.binding.clone()
        });
        if bound.as_ref().is_some_and(|bound| {
            !bound.is_stream() || !bound.permits_native(binding, &destination, &context)
        }) {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if grant.instance() != self.grantee
            || binding != &self.binding
            || destination != self.endpoint
            || self.egress.generation() != binding.network_generation.0
            || (context.session.is_some() && self.business.is_none())
        {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if context.cancel.is_cancelled() {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::Cancelled,
                DeliveryState::NotSubmitted,
            ));
        }
        if context.deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::Timeout,
                DeliveryState::NotSubmitted,
            ));
        }
        let selection = self
            .egress
            .select_for_peer_with_policy(self.peer, &binding.dial)
            .map_err(|source| {
                error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Io,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
            })?;
        if selection.generation() != binding.network_generation.0 {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        // Host demand callbacks must run outside every carrier/native lock.
        let business_cancel = self
            .business
            .as_ref()
            .map(|business| business.cancellation())
            .or_else(|| self.service.as_ref().map(|service| service.cancellation()));
        {
            let mut state = grant.slot.state();
            if grant.slot.cancel.is_cancelled() || state.closed {
                return Err(error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                ));
            }
            state.opening = true;
            state.binding = Some(binding.clone());
            state.endpoint = Some(destination.clone());
            if state.business_cancel.is_none() {
                state.business_cancel = business_cancel;
            }
            // A synchronous acquisition panic/error must not return capacity
            // based on implicit Drop of an already created native socket.
            state.unknown = Some(Arc::new(
                error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Internal,
                    DeliveryState::Unknown,
                )
                .with_source(std::io::Error::other(
                    "native TCP preparation did not return an observed ownership result",
                )),
            ));
        }
        let (pending, control) = match PendingTcpConnect::prepare(
            self.peer,
            &binding.dial,
            &selection,
            grant.slot.cancel.clone(),
            self.observer.clone(),
        ) {
            Ok(value) => value,
            Err(source) => {
                let no_handle = source.stage() == "validate_dial_policy";
                let source = Arc::new(source);
                let failure = connect_error(grant.instance(), source.clone());
                grant.slot.state().unknown = if no_handle {
                    None
                } else {
                    Some(Arc::new(connect_error(grant.instance(), source)))
                };
                return Err(failure);
            }
        };
        {
            let mut state = grant.slot.state();
            // Sole original control is installed before PreparedCarrier exists
            // and before its inline connect can ever reach a first await.
            state.close_observer = Some(control.close_observer());
            state.control = Some(Arc::new(Mutex::new(control)));
            state.unknown = None;
        }
        super::super::users::acquired(&grant.slot);
        let end = Instant::now() + Duration::from_secs(10);
        let end = context.deadline.map_or(end, |caller| caller.min(end));
        grant.transferred = true;
        Ok(PreparedCarrier {
            pending,
            slot: grant.slot.clone(),
            egress: self.egress.clone(),
            generation: binding.network_generation,
            cancel: context.cancel,
            authorization: bound.as_ref().map(|bound| bound.cancellation()),
            end,
            endpoint_observer: None,
            business: self.business.clone(),
            service: self.service.clone(),
            session: context.session,
            transferred: false,
        })
    }
}
