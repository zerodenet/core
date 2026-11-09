//! Existing root selection and metadata, without new native authority.
use super::*;
impl UpstreamOpener for Root {
    fn protocol_original_observer(
        &self,
    ) -> Option<zero_platform_tokio::contract::network::PendingCarrierObserver> {
        self.waiting
            .as_ref()
            .and_then(|waiting| waiting.protocol_original_observer())
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.waiting.is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        let slot = self.owner().ok()?;
        if binding != &self.binding {
            return None;
        }
        self.plan
            .upgrade()?
            .pending_carrier_capacity(&[slot.role_id()])
            .ok()
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let slot = self.owner()?;
        if binding != &self.binding {
            return Err(invalid(Reason::StalePlan, "Direct pending binding changed"));
        }
        self.plan
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "Direct pending plan retired"))?
            .validate_carrier_adoption(slot.role_id(), adoption)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.owner()?;
        if binding != &self.binding {
            return Err(invalid(Reason::StalePlan, "Direct pending binding changed"));
        }
        // The same waiting caller retains its receiver before it borrows this
        // Direct donor. That receiver belongs to a later selected role, so it
        // cannot be validated as Direct's own native original. The actual
        // caller ledger revalidates its complete selected role graph below;
        // validate_pending_carrier remains the separate narrow donor check.
        self.waiting
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "Direct root has no foreign waiting ledger",
                )
            })?
            .retain(adoption)
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.owner().ok()?;
        Some(self.services.native_node_dns().generation())
    }
    fn supports_controlled_stream(&self) -> bool {
        self.call.is_some()
    }
    fn open_controlled_stream_loan(
        &self,
        binding: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move { self.open_loan(binding, request).await })
    }
    fn open_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let loan = self.open_loan(binding, request).await?;
            if !self.observe_carrier_origin(loan.owner.original()) {
                return Err(invalid(
                    Reason::StalePlan,
                    "Direct Service returned a foreign original",
                ));
            }
            Ok(crate::component::background::stateless::service_stream(
                loan,
            ))
        })
    }
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.reselect(binding, call, None)
    }
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.reselect(binding, call, Some(waiting))
    }
    fn observe_carrier_origin(&self, origin: &CarrierOrigin) -> bool {
        let origin = origin.native_origin();
        let Some(slot) = self.owner().ok() else {
            return false;
        };
        if slot.instance_id() != origin.instance()
            || !slot
                .original_carrier_owner(0)
                .is_some_and(|owner| origin.owner().same_owner(&owner))
            || !origin
                .selected_binding()
                .is_some_and(|(endpoint, selected)| {
                    endpoint == self.endpoint && selected == self.binding
                })
        {
            return false;
        }
        let Some(endpoints) = origin.endpoints() else {
            return false;
        };
        *self.evidence.lock().unwrap() = Some((endpoints.local(), endpoints.peer()));
        true
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "Direct loan selector has no bare socket executor",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "Direct stream loan selector has no datagram executor",
            ))
        })
    }
}

impl Root {
    fn reselect(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            self.owner()?;
            if binding.as_ref() != Some(&self.binding)
                || call.session.is_some()
                || call.cancel.is_cancelled()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "Direct Service root selection changed",
                ));
            }
            if let Some(business) = &self.business {
                zero_platform_tokio::controlled_carrier::CarrierBusinessDemand::check(business)?;
            } else if self
                .call
                .as_ref()
                .is_none_or(|call| call.cancel.is_cancelled())
            {
                return Err(invalid(
                    Reason::Revoked,
                    "Direct root original call retired",
                ));
            }
            let mut next = self.clone();
            next.business = None;
            next.call = Some(call);
            next.waiting = waiting;
            Ok(UpstreamTransport::new(next.binding.clone(), Arc::new(next)))
        })
    }
}
