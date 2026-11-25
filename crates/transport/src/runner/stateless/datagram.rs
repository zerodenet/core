//! Datagram-over-stream extension traits and role adapter for [`StatelessOutboundFactory`].

use super::{CarrierHandle, GuardedCall, HandshakeError, StatelessOutboundOptions};
use crate::TcpRelayStream;
use std::{future::Future, sync::Arc};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::OpenContext,
    datagram::{
        erase_datagram, DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession, DatagramTx,
        DynDatagramOutbound,
    },
    lifecycle::ChannelControl,
    ChannelError,
};

/// Optional pure datagram-over-stream protocol handshake plugged into
/// [`super::StatelessOutboundFactory`].
pub trait DatagramProtocolHandshake<O: StatelessOutboundOptions>:
    Clone + Send + Sync + 'static
{
    type Prepared: Send;
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;

    /// Whether datagram open enforces `context.upstream.binding().dial == options.dial()`.
    fn enforce_dial_policy(&self) -> bool {
        true
    }

    fn prepare_datagram(
        &self,
        options: &O,
        request: &DatagramRequest,
    ) -> Result<Self::Prepared, HandshakeError>;

    fn handshake_datagram(
        &self,
        options: &O,
        io: TcpRelayStream,
        prepared: Self::Prepared,
        request: DatagramRequest,
        context: OpenContext,
        admission: Arc<SessionAdmission>,
        session: Option<zero_core::component::SessionId>,
    ) -> impl Future<Output = Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError>>
           + Send;
}

/// Marker for stream-only stateless outbounds without a datagram role.
#[derive(Clone, Copy, Default)]
pub struct NoDatagram;

pub trait DatagramRoleBuilder<O: StatelessOutboundOptions>: Clone + Send + Sync + 'static {
    fn build_datagram_role(
        &self,
        carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>>;
}

impl<O: StatelessOutboundOptions> DatagramRoleBuilder<O> for NoDatagram {
    fn build_datagram_role(
        &self,
        _carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        None
    }
}

struct StatelessDatagramRole<O: StatelessOutboundOptions, D> {
    datagram: D,
    carrier: CarrierHandle<O>,
}

impl<O, D> DatagramOutbound for StatelessDatagramRole<O, D>
where
    O: StatelessOutboundOptions,
    D: DatagramProtocolHandshake<O>,
{
    type Tx = D::Tx;
    type Rx = D::Rx;
    type Control = D::Control;

    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let expected_dial = self
            .datagram
            .enforce_dial_policy()
            .then(|| self.carrier.options.dial());
        let call = GuardedCall::enter(&self.carrier.admission, &context, expected_dial)?;
        let prepared = self
            .datagram
            .prepare_datagram(&self.carrier.options, &request)
            .map_err(|err| err.into_channel_error(&call))?;
        let io = self.carrier.open_carrier(&call).await?;
        let session = call.session();
        drop(call);
        self.datagram
            .handshake_datagram(
                &self.carrier.options,
                io,
                prepared,
                request,
                context,
                self.carrier.admission.clone(),
                session,
            )
            .await
    }
}

/// Wrapper implementing [`DatagramRoleBuilder`] for any [`DatagramProtocolHandshake`].
#[derive(Clone, Copy)]
pub struct WithDatagram<D>(pub D);

impl<O, D> DatagramRoleBuilder<O> for WithDatagram<D>
where
    O: StatelessOutboundOptions,
    D: DatagramProtocolHandshake<O>,
{
    fn build_datagram_role(
        &self,
        carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        carrier.options.supports_datagram().then(|| {
            erase_datagram(StatelessDatagramRole {
                datagram: self.0.clone(),
                carrier,
            })
        })
    }
}
