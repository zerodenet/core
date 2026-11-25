//! Generic `ComponentFactory` for stream/datagram roles backed by a shared
//! `pooled_driver` physical pool component.
//!
//! Enforces the weak-reference fencing invariant automatically: cached role
//! handles retain only `Weak` pointers to options and bound protocol leaves so
//! they can never pin retired private resources after component revocation.

mod datagram;
mod factory;

pub use datagram::{PooledDatagramProtocol, PooledDatagramRoleBuilder, WithPooledDatagram};
pub use factory::PooledRoleFactory;

use super::GuardedCall;
use crate::{pooled_driver::PooledStream, RuntimeError};
use std::{
    future::Future,
    io,
    sync::{Arc, Mutex, Weak},
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::Destination,
};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::{ConnectFact, OpenContext},
    stream::{BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound},
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_traits::DialPolicy;

/// Configuration options for a pooled outbound role.
pub trait PooledRoleOptions: Send + Sync + 'static {
    fn pool_owner(&self) -> ComponentId;
    fn dial(&self) -> &DialPolicy;
    fn static_identity(&self) -> Arc<[u8]>;

    fn supports_datagram(&self) -> bool {
        false
    }
}

/// Bound protocol leaf handle tied to a specific `ComponentInstanceId` pool generation.
pub trait PooledBoundLeaf: Clone + Send + Sync + 'static {
    fn pool_instance(&self) -> Option<ComponentInstanceId>;

    fn open_stream<'a>(
        &'a self,
        destination: Destination,
        context: &'a OpenContext,
    ) -> impl Future<Output = Result<PooledStream, RuntimeError>> + Send + 'a;
}

/// Protocol hook for binding a role's options to the resolved pool generation.
pub trait PooledRoleProtocol: Clone + Send + Sync + 'static {
    type Options: PooledRoleOptions;
    type BoundLeaf: PooledBoundLeaf;

    fn bind_leaf(
        &self,
        options: &Self::Options,
        pool: ComponentInstanceId,
    ) -> Result<Self::BoundLeaf, RuntimeError>;
}

pub(super) struct Ready<L> {
    pub(super) leaf: Mutex<Option<L>>,
}

pub struct RoleHandle<P: PooledRoleProtocol> {
    pub(super) ready: Weak<Ready<P::BoundLeaf>>,
    pub(super) options: Weak<P::Options>,
    pub(super) admission: Arc<SessionAdmission>,
    pub(super) pool: ComponentInstanceId,
}

impl<P: PooledRoleProtocol> Clone for RoleHandle<P> {
    fn clone(&self) -> Self {
        Self {
            ready: self.ready.clone(),
            options: self.options.clone(),
            admission: self.admission.clone(),
            pool: self.pool,
        }
    }
}

impl<P: PooledRoleProtocol> RoleHandle<P> {
    pub(super) fn selected(
        &self,
        context: &OpenContext,
    ) -> Result<(Arc<P::Options>, P::BoundLeaf), ChannelError> {
        let session = self.admission.check(context)?;
        let options = self.options.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role recipe no longer retained"))
        })?;
        let ready = self.ready.upgrade().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role owner no longer retained"))
        })?;
        let leaf = ready.leaf.lock().unwrap().clone().ok_or_else(|| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
                .with_source(io::Error::other("pooled role was revoked before this open"))
        })?;
        if leaf.pool_instance() != Some(self.pool)
            || context.upstream.binding().dial != *options.dial()
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok((options, leaf))
    }

    async fn connect_actual(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<PooledStream, ChannelError> {
        let session = self.admission.check(&context)?;
        let (_, leaf) = self.selected(&context)?;
        let call = GuardedCall::from_admitted(&self.admission, &context, session);
        let io = call
            .run_io(
                Stage::Handshake,
                DeliveryState::Unknown,
                leaf.open_stream(request.destination, &context),
            )
            .await?;
        if !context.upstream.observe_carrier_origin(&io.origin) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        call.facts().record(ConnectFact::CarrierReady);
        Ok(io)
    }
}

pub(super) struct PooledStreamRole<P: PooledRoleProtocol> {
    pub(super) shared: RoleHandle<P>,
}

impl<P: PooledRoleProtocol> StreamOutbound for PooledStreamRole<P> {
    type Stream = BoxStream;

    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let io = self.shared.connect_actual(request, context).await?;
        Ok(Connected {
            io: io.io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }

    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        let session = context.session_identity()?;
        let io = self.shared.connect_actual(request, context).await?;
        let loan = io
            .into_stream_loan()
            .map_err(|error| self.shared.admission.attach(error, session))?;
        Ok(Connected {
            io: loan,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
