//! Solo Business cache and exclusive real association borrowing stay distinct.
use super::*;
impl DatagramOutbound for Outbound {
    type Tx = Pin<Box<dyn DatagramTx>>;
    type Rx = Pin<Box<dyn DatagramRx>>;
    type Control = Control;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramLoan, ChannelError> {
        let DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.admission.error(
                context.session_identity()?,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let association = request.association;
        let session = self.open(request, context).await?;
        let original = session.control.original.clone();
        Ok(DatagramLoan::new(
            zero_platform_tokio::contract::datagram::erase_session(session),
            original,
            association,
            endpoint,
        ))
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        if context.upstream.binding().dial != self.options.binding.dial
            || context.upstream.binding().network_generation
                != self.options.binding.network_generation
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let pool = self
            .options
            .actual(self.pool)
            .map_err(|error| self.admission.attach(error, session))?;
        if session.is_some() && context.upstream.binding().prefix.is_empty() {
            let flow = tokio::select! {biased;_=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),result=pool.open(context.deadline)=>result.map_err(|error|self.admission.attach(error,session))?};
            let original = pool.origin()?;
            context.upstream.observe_carrier_origin(&original);
            let logical =
                pool.register_logical(flow, original, self.admission.clone(), &context)?;
            let limits = DatagramLimits {
                max_payload: 65507,
                queued_bytes: 0,
                targets: request.targets,
            };
            let channel = logical.session(request.association, limits.clone());
            let original = channel.control.original().clone();
            return Ok(DatagramSession {
                tx: Box::pin(channel.tx),
                rx: Box::pin(channel.rx),
                control: Control {
                    inner: ControlKind::Cached(channel.control),
                    original,
                    admission: self.admission.clone(),
                    caller: context.cancel.clone(),
                },
                limits,
            });
        }
        let mut grant = self.grants.reserve()?;
        let view = grant.claim_transferred_view()?;
        let adoption = grant.adoption();
        let mut pending = Pending {
            original: adoption.clone(),
            armed: true,
        };
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(adoption.clone())?;
        }
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let opening = async {
            if context.upstream.binding().prefix.is_empty() {
                let io = if let Some((process, startup)) = self.process {
                    context.upstream.acquire_process_endpoint_datagram(zero_platform_tokio::contract::network::ProcessEndpointDatagramRequest {process,process_ordinal:0,endpoint:self.options.endpoint.clone(),context:call.clone(),grant,startup_deadline:context.deadline.map_or_else(||Instant::now()+startup,|end|end.min(Instant::now()+startup))}).await?
                } else {
                    let candidates = context
                        .upstream
                        .resolve_controlled_carrier(self.options.endpoint.clone(), call.clone())
                        .await?;
                    candidates.prepare_datagram(0, grant)?
                };
                io.into_datagram_loan(view, request.association)
            } else {
                let loan = context
                    .upstream
                    .borrow_datagram(self.options.endpoint.clone(), call.clone())
                    .await?;
                let io = grant.adopt_datagram_loan(
                    context.upstream.binding(),
                    self.options.endpoint.clone(),
                    &call,
                    loan,
                )?;
                io.into_datagram_loan(view, request.association)
            }
        };
        let loan = tokio::select! {biased;_=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),result=opening=>result?};
        let raw_endpoint = loan.io.endpoint().clone();
        let (raw, original) = loan.into_session();
        context.upstream.observe_carrier_origin(&original);
        let channel = codec::wrap(
            raw,
            request,
            raw_endpoint,
            Arc::new(self.options.resume.codec()),
            context.cancel.clone(),
            context.observer.clone(),
            session,
        );
        pending.armed = false;
        Ok(DatagramSession {
            tx: channel.tx,
            rx: channel.rx,
            control: Control {
                inner: ControlKind::Borrowed(channel.control),
                original,
                admission: self.admission.clone(),
                caller: context.cancel.clone(),
            },
            limits: channel.limits,
        })
    }
}
