//! Packet boundaries and origin metadata on the actual reverse FIFO, without pumps.
use super::*;
impl DatagramOutbound for Outbound {
    type Tx = datagram::Tx<crate::mux_pool::native::DatagramIo, ownership::Lease>;
    type Rx = datagram::Rx<crate::mux_pool::native::DatagramIo, ownership::Lease>;
    type Control = datagram::Control<crate::mux_pool::native::DatagramIo, ownership::Lease>;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        self.open_actual(request, context, false)
            .await
            .map(|(session, _)| session)
    }
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let zero_core::request::DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        };
        let association = request.association;
        let (session, origin) = self.open_actual(request, context, true).await?;
        let original = origin.ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
                zero_platform_tokio::contract::datagram::erase_session(session),
                original,
                association,
                endpoint,
            ),
        )
    }
}
impl Outbound {
    async fn open_actual(
        &self,
        request: DatagramRequest,
        context: OpenContext,
        borrow: bool,
    ) -> Result<
        (
            DatagramSession<
                datagram::Tx<crate::mux_pool::native::DatagramIo, ownership::Lease>,
                datagram::Rx<crate::mux_pool::native::DatagramIo, ownership::Lease>,
                datagram::Control<crate::mux_pool::native::DatagramIo, ownership::Lease>,
            >,
            Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>,
        ),
        ChannelError,
    > {
        let session = self.admission.check(&context)?;
        let portal = self.portal(&context)?;
        let (worker, sid) = portal
            .reserve_worker()
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        if borrow && worker.origin.is_none() {
            worker.connection.release_stream(sid);
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        let lease = portal.lease(&worker).inspect_err(|_| {
            worker.connection.release_stream(sid);
        })?;
        let opened = crate::mux_pool::native::reverse_datagram(
            worker.connection.clone(),
            sid,
            Self::origin(&context, crate::mux::NETWORK_UDP),
            context.observer.clone(),
            session,
        );
        let io=tokio::select!{biased;
            _=self.admission.cancelled()=>return Err(error(Stage::Handshake,Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(error(Stage::Handshake,Reason::Cancelled)),
            result=zero_platform_tokio::contract::context::with_deadline(context.deadline,opened)=>result.map_err(|source|error(Stage::Handshake,Reason::Timeout).with_source(source))?,
        }.map_err(|source|error(Stage::Handshake,Reason::Io).with_source(source))?;
        let limits = DatagramLimits {
            max_payload: crate::mux::MUX_MAX_PAYLOAD,
            targets: request.targets.clone(),
            queued_bytes: 0,
        };
        let packet = datagram::session_with_lease(
            io,
            lease,
            self.admission.clone(),
            &context,
            request,
            limits,
        )?;
        if let Some(original) = &worker.origin {
            context.upstream.observe_carrier_origin(original);
        }
        Ok((packet, worker.origin.clone()))
    }
}
