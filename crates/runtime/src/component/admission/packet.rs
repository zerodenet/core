//! Packet is admitted as actual endpoint facts, never a synthetic SessionId.
use super::*;
use zero_platform_tokio::contract::packet::{BoxPacketSession, DynPacketOutbound, PacketRequest};
pub(in crate::component) struct FencedPacket {
    pub(in crate::component) inner: std::sync::Weak<dyn DynPacketOutbound>,
    pub(in crate::component) host: Arc<HostAdmission>,
    pub(in crate::component) instance: ComponentInstanceId,
    pub(in crate::component) owner: Arc<()>,
    pub(in crate::component) users: Arc<[Arc<super::super::instance::users::Users>]>,
}
impl DynPacketOutbound for FencedPacket {
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async move {
            let facts = context.packet_facts()?;
            if request.endpoint != facts.endpoint {
                return Err(packet_failure(invalid(
                    Reason::InvalidConfig,
                    "Packet request differs from actual ingress endpoint",
                )));
            }
            self.host
                .check(self.instance, &self.owner, &context)
                .map_err(packet_failure)?;
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| packet_failure(super::retired_role(self.instance, None)))?;
            let Some(lease) = super::super::instance::users::Lease::enter(
                self.instance.component,
                &self.users,
                None,
            )
            .map_err(packet_failure)?
            else {
                return inner.open(request, context).await;
            };
            let session = tokio::select! { biased;
                _=lease.revoked()=> { let mut error=lease.failure(zero_platform_tokio::contract::Stage::Prepare);error.impact=zero_platform_tokio::contract::Impact::Packet;return Err(error); },
                result=inner.open(request,context)=>result?,
            };
            Ok(lease.packet(session))
        })
    }
}

fn packet_failure(mut error: ChannelError) -> ChannelError {
    error.impact = zero_platform_tokio::contract::Impact::Packet;
    error
}
