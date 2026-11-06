//! UDP ASSOCIATE keeps its original TCP control beside its related packet slot.
use super::*;
use alloc::boxed::Box;
use association::Preparing;
use tokio::time::Instant;
use zero_platform_tokio::{contract::stream::BoxStream, controlled_carrier::loan::StreamLoan};

impl Outbound {
    pub(super) async fn open_controlled(
        &self,
        request: DatagramRequest,
        context: OpenContext,
        session: Option<SessionId>,
    ) -> Result<DatagramSession<tx::Tx, rx::Rx, control::Control>, ChannelError> {
        if !context.upstream.binding().prefix.is_empty() {
            // The existing packet-path capability can create a SOCKS UDP
            // association only as its root. A parsed Relay is not evidence of
            // a formerly supported non-root UDP capability.
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        let end = context
            .deadline
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(15));
        let carriers = self.carriers.as_ref().expect("actual association owner");
        self.associations
            .reap(end)
            .await
            .map_err(|error| self.admission.attach(error, session))?;
        carriers
            .reap_returned(end)
            .await
            .map_err(|error| self.admission.attach(error, session))?;
        let mut parent = carriers
            .reserve()
            .map_err(|error| self.admission.attach(error, session))?;
        // reserve has already installed this original in the actual CarrierOwner.
        // Claim its unique view before publishing a protocol cleanup record so
        // a synchronous claim failure cannot leave a record with no users proof.
        let view = parent.claim_transferred_view()?;
        let adoption = parent.adoption();
        let original = self
            .associations
            .install(
                adoption.clone(),
                parent.cancellation(),
                carriers.registration().limit().get(),
            )
            .map_err(|error| self.admission.attach(error, session))?;
        let _preparing = Preparing(original.clone());
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting
                .retain(adoption.clone())
                .map_err(|error| self.admission.attach(error, session))?;
        }
        // The physical control belongs to this association, including when it
        // serves packets from later business Sessions. Logical role facts above
        // still retain the real caller's business identity and accounting.
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let acquiring = context.upstream.acquire_controlled_stream(
            self.options.endpoint.clone(),
            call.clone(),
            parent,
        );
        let io = tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.failure(session,Stage::Dial,Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(self.failure(session,Stage::Dial,Reason::Cancelled)),
            result=acquiring=>result.map_err(|error|self.admission.attach(error,session))?,
        };
        let StreamLoan { io, owner } = io.into_stream_loan(view)?;
        context.upstream.observe_carrier_origin(owner.original());
        original.install_tcp(owner);
        let (tcp, relay) = self
            .establish_control(Box::pin(io) as BoxStream, &context, session)
            .await?;

        let mut child = carriers
            .reserve()
            .map_err(|error| self.admission.attach(error, session))?;
        let child_adoption = child.adoption();
        child.link_related_datagram(&adoption)?;
        let view = child.claim_transferred_view()?;
        original.install_child(child_adoption.clone());
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting
                .retain(child_adoption)
                .map_err(|error| self.admission.attach(error, session))?;
        }
        let opening = async {
            let candidates = context
                .upstream
                .resolve_related_datagram(adoption, relay.clone(), call.clone())
                .await?;
            candidates
                .prepare_datagram(0, child)?
                .into_datagram_loan(view, request.association)
        };
        let packet = tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.failure(session,Stage::Dial,Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(self.failure(session,Stage::Dial,Reason::Cancelled)),
            result=opening=>result.map_err(|error|self.admission.attach(error,session))?,
        };
        let (raw, origin) = packet.into_session();
        context.upstream.observe_carrier_origin(&origin);
        let endpoint = match &raw.limits.targets {
            DatagramTargets::Fixed(endpoint) => endpoint.clone(),
            DatagramTargets::Multiple => relay,
        };
        let limits = DatagramLimits {
            max_payload: raw.limits.max_payload.saturating_sub(10),
            targets: request.targets,
            queued_bytes: 0,
        };
        let shared = shared::Shared::new(tcp, raw.control, self.admission.clone(), session);
        original.publish(origin);
        Ok(DatagramSession {
            tx: tx::Tx::new(
                raw.tx,
                shared.clone(),
                request.association,
                endpoint.clone(),
                limits.clone(),
                raw.limits,
                context.cancel.clone(),
            ),
            rx: rx::Rx::new(
                raw.rx,
                shared.clone(),
                request.association,
                endpoint,
                context.cancel,
            ),
            control: control::Control::new(shared).with_original(original),
            limits,
        })
    }
}
