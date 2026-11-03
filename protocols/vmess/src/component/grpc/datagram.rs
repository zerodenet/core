//! Same VMess packet session, plus its actual original H2/native close owner.
use super::super::datagram::{self, Rx, Tx};
use super::{connect::Outbound, state::Record, *};
use zero_platform_tokio::contract::{
    context::OpenContext,
    datagram::{DatagramOutbound, DatagramRequest, DatagramSession},
    lifecycle::{ChannelControl, CloseReport},
    CloseReason,
};

pub(super) struct Control {
    logical: datagram::Control,
    original: Arc<Record>,
}
impl DatagramOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
        let owner = &self.0;
        let (target, header_len) = datagram::prepare(&request, &owner.admission, &context)?;
        let (carrier, original) = owner.open_transport(&target, &context).await?;
        let session = datagram::open_carrier(
            &owner.options.base,
            owner.admission.clone(),
            request,
            context,
            carrier,
            header_len,
        )
        .await?;
        Ok(DatagramSession {
            tx: session.tx,
            rx: session.rx,
            control: Control {
                logical: session.control,
                original,
            },
            limits: session.limits,
        })
    }
}
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.logical.cancel(reason);
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        // Returning the wrapper ends custody only. UDP cancellation needs no
        // peer EOF: completion comes from the original H2 join/native provider.
        if !self.logical.return_carrier_before(deadline).await {
            return Ok(CloseReport {
                unfinished: vec![zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                    "VMess gRPC Datagram transferred views".into(),
                )],
                ..Default::default()
            });
        }
        self.original.stop.cancel();
        self.original.retire(deadline).await
    }
}
