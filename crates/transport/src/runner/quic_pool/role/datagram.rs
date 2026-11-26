//! Optional Datagram role extension over a [`super::QuicPoolRoleProtocol`] session.

use super::{QuicPoolRoleProtocol, Role};
use crate::runner::quic_pool::{error, QuicPoolGeneration};
use std::{
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        context::OpenContext,
        datagram::{
            erase_session, DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession,
            DatagramTx,
        },
        lifecycle::{ChannelControl, CloseReport},
        BoxFuture, ChannelError, CloseReason, Reason, Stage,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierOrigin},
};

/// Boxed future returning an opened [`DatagramSession`] over a QUIC pool session.
pub type OpenedPoolDatagramFuture<Tx, Rx, C> =
    BoxFuture<'static, Result<DatagramSession<Tx, Rx, C>, ChannelError>>;

/// Optional Datagram role extension over a [`QuicPoolRoleProtocol`] session.
pub trait QuicPoolDatagramProtocol<P: QuicPoolRoleProtocol>: Send + Sync + 'static {
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;

    const ENABLED: bool;

    fn open_datagram(
        session: Arc<P::Session>,
        request: DatagramRequest,
        context: OpenContext,
        generation: Arc<QuicPoolGeneration<P>>,
        profile: usize,
    ) -> OpenedPoolDatagramFuture<Self::Tx, Self::Rx, Self::Control>;
}

/// Sentinel disabling `RoleKind::Datagram` on [`super::QuicPoolRoleFactory`].
pub struct NoQuicPoolDatagram;

pub enum NeverDatagramHalf {}

impl DatagramTx for NeverDatagramHalf {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &zero_core::packet::Datagram,
    ) -> Poll<
        Result<
            zero_platform_tokio::contract::SendReceipt,
            zero_platform_tokio::contract::SendFailure,
        >,
    > {
        match *self {}
    }
}

impl DatagramRx for NeverDatagramHalf {
    fn poll_recv(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<zero_core::packet::Datagram>, ChannelError>> {
        match *self {}
    }
}

impl ChannelControl for NeverDatagramHalf {
    fn cancel(&self, _: CloseReason) {
        match *self {}
    }
    async fn wait_closed(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        match *self {}
    }
}

impl<P: QuicPoolRoleProtocol> QuicPoolDatagramProtocol<P> for NoQuicPoolDatagram {
    type Tx = NeverDatagramHalf;
    type Rx = NeverDatagramHalf;
    type Control = NeverDatagramHalf;

    const ENABLED: bool = false;

    fn open_datagram(
        _: Arc<P::Session>,
        _: DatagramRequest,
        _: OpenContext,
        _: Arc<QuicPoolGeneration<P>>,
        _: usize,
    ) -> OpenedPoolDatagramFuture<Self::Tx, Self::Rx, Self::Control> {
        Box::pin(async { Err(error(Stage::Prepare, Reason::Unsupported)) })
    }
}

impl<P, D> DatagramOutbound for Role<P, D>
where
    P: QuicPoolRoleProtocol,
    D: QuicPoolDatagramProtocol<P>,
{
    type Tx = D::Tx;
    type Rx = D::Rx;
    type Control = D::Control;

    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramLoan, ChannelError> {
        let zero_core::request::DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        };
        let session = self.acquire(&context).await?;
        let origin = P::session_native_origin(&session)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let original = CarrierOrigin::from_metadata_pool(origin.clone());
        context.upstream.observe_carrier_origin(&original);
        let association = request.association;
        let generation = Weak::upgrade(&self.options.pool)
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .generation(self.generation)?;
        let opened =
            D::open_datagram(session, request, context, generation, self.options.profile).await?;
        Ok(DatagramLoan::new_metadata_pool(
            erase_session(opened),
            origin,
            association,
            endpoint,
        ))
    }

    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.acquire(&context).await?;
        let generation = Weak::upgrade(&self.options.pool)
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .generation(self.generation)?;
        D::open_datagram(session, request, context, generation, self.options.profile).await
    }
}
