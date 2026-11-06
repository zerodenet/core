//! Original non-MUX VLESS UDP framing; no socket, task or replacement pool.
use super::*;
use tokio::io::AsyncWriteExt;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    context::ConnectFact,
    datagram::{DatagramOutbound, DatagramRequest, DatagramSession},
};
impl DatagramOutbound for Outbound {
    type Tx = datagram::Tx;
    type Rx = datagram::Rx;
    type Control = datagram::Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let target = target.clone();
        if self.options.flow.is_some()
            || target.port == 0
            || context.upstream.binding().dial != self.options.dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let bytes = crate::shared::build_request_to(
            &target.address,
            target.port,
            &self.options.uuid,
            crate::shared::CMD_UDP,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let mut io = self.open_transport(&context, session).await?;
        let handshake = async {
            io.write_all(&bytes).await?;
            io.flush().await?;
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,handshake)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, bytes.len() as u64);
        }
        let response =
            datagram::response::read(&mut io, &context, &self.admission, session).await?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::PeerConfirmed);
            observer.protocol_traffic(session, response as u64, 0);
        }
        // Only the actual outer Transfer can provide native lineage. This
        // protocol framing control returns its exact raw stream, never forges it.
        Ok(datagram::session(
            Box::pin(io),
            None,
            None,
            request,
            context,
            self.admission.clone(),
            session,
            target,
        ))
    }
}
