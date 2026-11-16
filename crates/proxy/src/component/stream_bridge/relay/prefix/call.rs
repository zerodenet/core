//! Existing callback APIs transfer actual standard donor loans and controls.
use super::*;
impl PrefixOpener {
    fn callback_call(
        &self,
        binding: &UpstreamBinding,
        carrier: &CarrierContext,
    ) -> Result<CarrierContext, ChannelError> {
        if binding != &self.expected_binding
            || self.network.network_generation() != binding.network_generation.0
            || carrier.session != self.context.session_identity()?
        {
            return Err(self.callback_failure(self.interrupted(carrier, Reason::StalePlan)));
        }
        if let Some(business) = &self.business {
            zero_platform_tokio::controlled_carrier::CarrierBusinessDemand::check(business)
                .map_err(|error| self.callback_failure(error))?;
        }
        if self.context.cancel.is_cancelled() || carrier.cancel.is_cancelled() {
            return Err(self.callback_failure(self.interrupted(carrier, Reason::Cancelled)));
        }
        let service = self.context.session_identity()?.is_none();
        Ok(CarrierContext {
            session: None,
            // A sealed Service keeps its actual original facts, token and
            // deadline. Additional callback limits surround that same call.
            deadline: if service {
                self.context.deadline
            } else {
                self.callback_deadline(carrier)
            },
            cancel: if service {
                self.context.cancel.clone()
            } else {
                self.context.cancel.child_token()
            },
        })
    }
    fn callback_deadline(&self, carrier: &CarrierContext) -> Option<tokio::time::Instant> {
        match (self.context.deadline, carrier.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
    fn callback_identity(&self) -> Option<zero_platform_tokio::contract::context::RequestIdentity> {
        self.service_identity.clone().or_else(|| {
            matches!(
                &self.context.identity,
                zero_platform_tokio::contract::context::RequestIdentity::Service(_)
            )
            .then(|| self.context.identity.clone())
        })
    }

    fn callback_failure(&self, mut error: ChannelError) -> ChannelError {
        error.session = error.session.or(self.context.identity.session());
        if error.component.is_none() {
            error.component = self
                .expected_binding
                .prefix
                .last()
                .map(|role| role.component);
        }
        error
    }
    pub(super) fn open_stream_loan(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if destination != self.expected_endpoint {
                return Err(self.callback_failure(self.interrupted(&carrier, Reason::StalePlan)));
            }
            let call = self.callback_call(&binding, &carrier)?;
            let logical = if self.context.session_identity()?.is_none() {
                self.context.cancel.child_token()
            } else {
                call.cancel.clone()
            };
            let opening_stop = logical.clone().drop_guard();
            let opening = self.borrow_selected_in_context(
                binding,
                destination,
                call,
                None,
                self.callback_identity(),
            );
            let loan = tokio::select! {biased;
                _=carrier.cancel.cancelled()=>return Err(self.callback_failure(self.interrupted(&carrier,Reason::Cancelled))),
                result=with_deadline(self.callback_deadline(&carrier),opening)=>result.map_err(|error|self.callback_failure(self.interrupted(&carrier,Reason::Timeout).with_source(error)))?.map_err(|error|self.callback_failure(error))?,
            };
            // Keep the original owner with the view. Half-close only shuts down
            // writes; Drop/cancellation requests return, never native closure.
            opening_stop.disarm();
            Ok(stream::guard(
                crate::component::background::stateless::service_stream(loan),
                logical,
                carrier.cancel,
            ))
        })
    }
    pub(super) fn open_datagram_loan(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if !matches!(
                &request.targets,
                zero_core::request::DatagramTargets::Fixed(_)
            ) {
                return Err(self.callback_failure(self.interrupted(&carrier, Reason::Unsupported)));
            }
            let call = self.callback_call(&binding, &carrier)?;
            let logical = if self.context.session_identity()?.is_none() {
                self.context.cancel.child_token()
            } else {
                call.cancel.clone()
            };
            let opening_stop = logical.clone().drop_guard();
            let identity = self.callback_identity().unwrap_or_else(|| {
                zero_platform_tokio::contract::context::RequestIdentity::Service(
                    Arc::new(zero_core::request::ServiceRequestFacts {
                        origin: zero_core::request::ServiceOrigin::Runtime,
                        purpose: zero_core::request::ServicePurpose::BackgroundControl,
                    })
                    .into(),
                )
            });
            let opening = self.borrow_datagram_request(
                binding,
                self.expected_endpoint.clone(),
                request,
                identity,
                call,
            );
            let loan = tokio::select! {biased;
                _=carrier.cancel.cancelled()=>return Err(self.callback_failure(self.interrupted(&carrier,Reason::Cancelled))),
                result=with_deadline(self.callback_deadline(&carrier),opening)=>result.map_err(|error|self.callback_failure(self.interrupted(&carrier,Reason::Timeout).with_source(error)))?.map_err(|error|self.callback_failure(error))?,
            };
            opening_stop.disarm();
            Ok(datagram::guard(
                loan.into_session().0,
                logical,
                carrier.cancel,
            ))
        })
    }
}
