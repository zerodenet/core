//! One concrete owned H3 group. Its last driver lease requests original cleanup;
//! it never substitutes Drop, H3 EOF or cancellation for native/Scope ACK.
use super::*;
use crate::quic::owned::{h3::PendingHttp3, OwnedConnection, OwnedEndpointOwner};
use zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement;
pub struct PreparedXhttp3 {
    runtime: PacketMetadataArc<Runtime>,
    memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
}
pub struct PendingXhttp3 {
    original: PendingHttp3,
    runtime: PacketMetadataArc<Runtime>,
    retirement: CarrierProtocolRetirement,
    memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    origin: zero_platform_tokio::controlled_carrier::CarrierOrigin,
}
#[derive(Clone)]
pub struct OwnedXhttp3(PacketMetadataArc<Original>);
struct Original {
    sender: request::Sender,
    runtime: PacketMetadataArc<Runtime>,
    retirement: CarrierProtocolRetirement,
    task: zero_core::component::TaskId,
    origin: zero_platform_tokio::controlled_carrier::CarrierOrigin,
}
impl Drop for Original {
    fn drop(&mut self) {
        self.runtime.stop();
        self.retirement.request_retirement();
    }
}
impl PreparedXhttp3 {
    pub fn new(
        book: CarrierBufferBudget,
        memory: PacketDriverMetadataView,
        max_users: std::num::NonZeroUsize,
    ) -> io::Result<Self> {
        // Per accepted logical stream: two transfer directions, its streaming
        // producer, and at most two simultaneous request directions. Requests
        // and sessions share the original physical driver, not extra tasks.
        let limit = max_users
            .get()
            .checked_mul(5)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let header = memory
            .reserve(PacketMetadataArc::<Original>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        Ok(Self {
            runtime: Runtime::prepare(book, memory, limit)?,
            memory: Some(header),
        })
    }
    pub fn start(
        &mut self,
        owner: &OwnedEndpointOwner,
        connection: OwnedConnection,
        retirement: CarrierProtocolRetirement,
        origin: zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> Result<PendingXhttp3, zero_platform_tokio::contract::ChannelError> {
        let memory = self.memory.take().ok_or_else(|| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Component,
            )
        })?;
        let original = owner.prepare_xmux_http3(connection, self.runtime.clone())?;
        Ok(PendingXhttp3 {
            original,
            runtime: self.runtime.clone(),
            retirement,
            memory: Some(memory),
            origin,
        })
    }
    pub fn stop(&self) {
        self.runtime.stop();
    }
}
impl PendingXhttp3 {
    pub fn task_id(&self) -> zero_core::component::TaskId {
        self.original.task_id()
    }
    pub async fn ready(
        &mut self,
    ) -> Result<OwnedXhttp3, zero_platform_tokio::contract::ChannelError> {
        let sender = self.original.ready().await?;
        let credit = self.memory.take().expect("one actual prepared H3 sender");
        let original = PacketMetadataArc::from_prepaid(
            Original {
                sender: request::Sender::new(sender, &self.runtime),
                runtime: self.runtime.clone(),
                retirement: self.retirement.clone(),
                task: self.original.task_id(),
                origin: self.origin.clone(),
            },
            credit,
        )
        .unwrap_or_else(|_| panic!("H3 original lease header prepaid"));
        Ok(OwnedXhttp3(original))
    }
}
impl Drop for PendingXhttp3 {
    fn drop(&mut self) {
        if self.memory.is_some() {
            self.runtime.stop();
            self.retirement.request_retirement();
        }
    }
}
impl OwnedXhttp3 {
    pub(in crate::split_http) fn retirement(&self) -> CarrierProtocolRetirement {
        self.0.retirement.clone()
    }
    /// The exact native/borrowed original installed before this H3 driver.
    /// Observation only; this neither creates a receiver nor closes a resource.
    pub fn origin(&self) -> &zero_platform_tokio::controlled_carrier::CarrierOrigin {
        &self.0.origin
    }
    pub(crate) fn original_task(&self) -> zero_core::component::TaskId {
        self.0.task
    }
    pub(crate) fn names_retirement(&self, original: &CarrierProtocolRetirement) -> bool {
        self.0.retirement.same_original(original)
    }
    pub(in crate::split_http) fn sender(&self) -> request::Sender {
        self.0.sender.clone()
    }
    pub(in crate::split_http) fn runtime(&self) -> PacketMetadataArc<Runtime> {
        self.0.runtime.clone()
    }
}
