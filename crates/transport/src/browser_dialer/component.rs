//! Actual Browser Source dependency and retained per-call native custody.
mod acquire;
mod control;
mod delivery;
mod diagnostics;
mod entry;
mod factory;
#[cfg(feature = "split_http")]
mod http;
mod ordinary;
mod pending;
pub mod raw;
mod table;
use crate::profile::OwnedWebSocketProfile;
pub use factory::Owner;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::Destination,
};
use zero_platform_tokio::contract::{
    component::*, lifecycle::*, CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("validated Browser bound")
}
#[derive(Clone, Debug)]
pub enum CarrierProfile {
    WebSocket(OwnedWebSocketProfile),
    #[cfg(feature = "split_http")]
    SplitHttp(Box<crate::profile::OwnedSplitHttpProfile>),
}
pub struct Options {
    pub endpoint: Destination,
    pub dial: zero_traits::DialPolicy,
    pub server: String,
    pub secure: bool,
    pub carrier: CarrierProfile,
    pub provider: Arc<super::registry::ProviderOptions>,
    pub dependency: Option<ComponentId>,
    pub origins: NonZeroUsize,
}
impl Options {
    pub fn declare(&self, spec: &mut ComponentSpec) {
        spec.dependencies.extend(self.dependency);
        spec.resources
            .push(ResourceDeclaration::ControlledCarrierPool {
                endpoint: self.endpoint.clone(),
                dial: self.dial.clone(),
                max_sockets: self.origins,
                datagram: false,
            });
        spec.task_limit = Some(n(self
            .origins
            .get()
            .checked_mul(2)
            .expect("validated Browser original limit")));
    }
}
#[derive(Clone)]
pub struct Handle(PacketMetadataArc<factory::Shared>);
impl Handle {
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn closed_marker(&self) -> &PacketMetadataArc<std::sync::atomic::AtomicBool> {
        &self.0.closed
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.cancel.is_cancelled()
    }
    pub fn upstream(&self) -> &zero_platform_tokio::contract::network::UpstreamTransport {
        &self.0.upstream
    }
    pub fn memory(&self) -> &PacketDriverMetadataView {
        &self.0.memory
    }
    pub fn buffers(&self) -> &zero_platform_tokio::controlled_carrier::CarrierBufferBudget {
        &self.0.buffers
    }
    async fn acquire(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError> {
        match &self.0.options.carrier {
            CarrierProfile::WebSocket(_) => factory::Shared::acquire_ws(&self.0, context).await,
            #[cfg(feature = "split_http")]
            CarrierProfile::SplitHttp(_) => factory::Shared::acquire_http(&self.0, context).await,
        }
    }
    pub async fn open(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
        let loan = self.acquire(context).await?;
        context
            .upstream
            .observe_carrier_origin(loan.owner.original());
        Ok(zero_platform_tokio::TcpRelayStream::new(
            crate::pooled_driver::ExclusiveIo::new(ordinary::Ordinary(loan)),
        ))
    }
    /// Borrow the same original under a transformed protocol stream without
    /// immediately recording origin facts on `context.upstream`.
    pub async fn open_unobserved_parts(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<
        (
            zero_platform_tokio::TcpRelayStream,
            zero_platform_tokio::contract::lifecycle::BoxChannelControl,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        ChannelError,
    > {
        let credit = self
            .0
            .memory
            .reserve(PacketMetadataLayout::boxed::<control::Logical>())
            .map_err(capacity)?;
        let loan = self.acquire(context).await?;
        let original = loan.owner.original().clone();
        let io = crate::pooled_driver::ExclusiveIo::new(loan.io);
        let control = zero_platform_tokio::contract::lifecycle::BoxChannelControl::from_prepaid(
            control::Logical::Loan(loan.owner),
            credit,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        Ok((
            zero_platform_tokio::TcpRelayStream::new(io),
            control,
            original,
        ))
    }
    /// Borrow the same original under a transformed protocol stream. The paid
    /// controller retains its exact lower LoanOwner, never a new native grant.
    pub async fn open_parts(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<
        (
            zero_platform_tokio::TcpRelayStream,
            zero_platform_tokio::contract::lifecycle::BoxChannelControl,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        ChannelError,
    > {
        let (io, control, original) = self.open_unobserved_parts(context).await?;
        context.upstream.observe_carrier_origin(&original);
        Ok((io, control, original))
    }
}

impl Options {
    pub fn extend_identity(&self, identity: &mut Vec<u8>) {
        match &self.carrier {
            CarrierProfile::WebSocket(ws) => {
                identity.extend_from_slice(b"neutral.browser.ws.original.v1");
                identity.extend_from_slice(&self.provider.identity);
                identity.extend_from_slice(
                    ring::digest::digest(
                        &ring::digest::SHA256,
                        format!(
                            "{:?}{:?}{:?}{}{}",
                            self.endpoint, self.dial, ws, self.secure, self.origins
                        )
                        .as_bytes(),
                    )
                    .as_ref(),
                );
            }
            #[cfg(feature = "split_http")]
            CarrierProfile::SplitHttp(http) => {
                identity.extend_from_slice(b"neutral.browser.http.original.v1");
                identity.extend_from_slice(&self.provider.identity);
                identity.extend_from_slice(
                    ring::digest::digest(
                        &ring::digest::SHA256,
                        format!(
                            "{:?}{:?}{:?}{}{}",
                            self.endpoint, self.dial, http, self.secure, self.origins
                        )
                        .as_bytes(),
                    )
                    .as_ref(),
                );
            }
        }
    }
}
mod lifecycle;
pub use lifecycle::Combined;
