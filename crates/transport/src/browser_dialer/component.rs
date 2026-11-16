//! Actual Browser Source dependency and retained per-call native custody.
mod acquire;
mod delivery;
mod diagnostics;
mod entry;
mod factory;
mod ordinary;
mod pending;
mod table;
use crate::profile::OwnedWebSocketProfile;
pub use factory::Owner;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_core::{component::ComponentId, request::Destination};
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
pub struct Options {
    pub endpoint: Destination,
    pub dial: zero_traits::DialPolicy,
    pub server: String,
    pub secure: bool,
    pub ws: OwnedWebSocketProfile,
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
    pub async fn open(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
        let loan = factory::Shared::acquire_ws(&self.0, context).await?;
        context
            .upstream
            .observe_carrier_origin(loan.owner.original());
        Ok(zero_platform_tokio::TcpRelayStream::new(
            ordinary::Ordinary(loan),
        ))
    }
}

impl Options {
    pub fn extend_identity(&self, identity: &mut Vec<u8>) {
        identity.extend_from_slice(b"neutral.browser.ws.original.v1");
        identity.extend_from_slice(&self.provider.identity);
        identity.extend_from_slice(
            ring::digest::digest(
                &ring::digest::SHA256,
                format!(
                    "{:?}{:?}{:?}{}{}",
                    self.endpoint, self.dial, self.ws, self.secure, self.origins
                )
                .as_bytes(),
            )
            .as_ref(),
        );
    }
}
mod lifecycle;
pub use lifecycle::Combined;
