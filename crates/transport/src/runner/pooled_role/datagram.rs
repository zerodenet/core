//! Datagram-over-pool extension traits and role adapter for [`PooledRoleFactory`].

use super::{GuardedCall, PooledRoleProtocol, RoleHandle};
use crate::runner::{HandshakeError, NoDatagram};
use crate::{
    pooled_driver::datagram::{self, Control, PacketIo, PooledDatagram, PooledSession, Rx, Tx},
    RuntimeError,
};
use std::{future::Future, sync::Arc};
use zero_core::request::DatagramTargets;
use zero_platform_tokio::{
    contract::{
        context::{ConnectFact, OpenContext},
        datagram::{
            erase_datagram, erase_session, DatagramLimits, DatagramOutbound, DatagramRequest,
            DatagramSession, DynDatagramOutbound,
        },
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierOrigin},
};

/// Optional datagram-over-pool hook for [`super::PooledRoleFactory`].
pub trait PooledDatagramProtocol<P: PooledRoleProtocol>: Clone + Send + Sync + 'static {
    type PacketIo: PacketIo;

    fn max_payload(&self, _options: &P::Options) -> usize {
        16 * 1024
    }

    fn validate_request(&self, _request: &DatagramRequest) -> Result<(), HandshakeError> {
        Ok(())
    }

    fn validate_leaf_request(
        &self,
        _leaf: &P::BoundLeaf,
        _request: &DatagramRequest,
    ) -> Result<(), HandshakeError> {
        Ok(())
    }

    fn open_datagram<'a>(
        &'a self,
        leaf: &'a P::BoundLeaf,
        request: &'a DatagramRequest,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledDatagram<Self::PacketIo>, RuntimeError>> + Send + 'a;
}

pub trait PooledDatagramRoleBuilder<P: PooledRoleProtocol>: Clone + Send + Sync + 'static {
    fn build_pooled_datagram(
        &self,
        shared: RoleHandle<P>,
        supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>>;
}

impl<P: PooledRoleProtocol> PooledDatagramRoleBuilder<P> for NoDatagram {
    fn build_pooled_datagram(
        &self,
        _shared: RoleHandle<P>,
        _supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        None
    }
}

/// Wrapper enabling datagram support on [`super::PooledRoleFactory`].
#[derive(Clone, Copy)]
pub struct WithPooledDatagram<D>(pub D);

impl<P, D> PooledDatagramRoleBuilder<P> for WithPooledDatagram<D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    fn build_pooled_datagram(
        &self,
        shared: RoleHandle<P>,
        supports_datagram: bool,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        supports_datagram.then(|| {
            erase_datagram(PooledDatagramRole {
                datagram: self.0.clone(),
                shared,
            })
        })
    }
}

struct PooledDatagramRole<P: PooledRoleProtocol, D> {
    datagram: D,
    shared: RoleHandle<P>,
}

impl<P, D> PooledDatagramRole<P, D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    async fn open_actual(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<(PooledSession<D::PacketIo>, CarrierOrigin), ChannelError> {
        let session = self.shared.admission.check(&context)?;
        {
            let call = GuardedCall::from_admitted(&self.shared.admission, &context, session);
            self.datagram
                .validate_request(&request)
                .map_err(|err| err.into_channel_error(&call))?;
        }
        let (options, leaf) = self.shared.selected(&context)?;
        let opened = {
            let call = GuardedCall::from_admitted(&self.shared.admission, &context, session);
            self.datagram
                .validate_leaf_request(&leaf, &request)
                .map_err(|err| err.into_channel_error(&call))?;
            let opened = call
                .run_io(
                    Stage::Handshake,
                    DeliveryState::Unknown,
                    self.datagram.open_datagram(&leaf, &request, &context),
                )
                .await?;
            if !context.upstream.observe_carrier_origin(&opened.origin) {
                return Err(self.shared.admission.error(
                    session,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::Unknown,
                ));
            }
            call.facts().record(ConnectFact::CarrierReady);
            opened
        };
        let limits = DatagramLimits {
            max_payload: self.datagram.max_payload(&options),
            targets: request.targets.clone(),
            queued_bytes: 0,
        };
        let original = opened.origin;
        let session = datagram::session(
            opened.io,
            opened.usage,
            self.shared.admission.clone(),
            &context,
            request,
            limits,
        )?;
        Ok((session, original))
    }
}

impl<P, D> DatagramOutbound for PooledDatagramRole<P, D>
where
    P: PooledRoleProtocol,
    D: PooledDatagramProtocol<P>,
{
    type Tx = Tx<D::PacketIo>;
    type Rx = Rx<D::PacketIo>;
    type Control = Control<D::PacketIo>;

    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramLoan, ChannelError> {
        let DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.shared.admission.error(
                context.session_identity()?,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let association = request.association;
        let (session, original) = self.open_actual(request, context).await?;
        Ok(DatagramLoan::new(
            erase_session(session),
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
        self.open_actual(request, context)
            .await
            .map(|(session, _)| session)
    }
}
