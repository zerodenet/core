//! The actual listener's child sets are installed before its driver can spawn.
use super::tasks::RetainedTasks;
use super::*;
use crate::inbound::tun::udp::OwnedTunUdpTasks;
use crate::runtime::inbound_owners::InboundInstallContext;

pub(super) struct RawIpChildren {
    pub(super) native: super::native_obligations::NativeObligations,
    pub(super) icmp: Option<crate::component::instance::HostIcmpProbeGrant>,
    pub(super) drivers: RetainedTasks<Result<(), EngineError>>,
    pub(super) proxies: RetainedTasks<()>,
    pub(super) connections: Option<Arc<crate::runtime::inbound_owners::InboundPeerTasks>>,
    pub(super) host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    pub(super) udp: OwnedTunUdpTasks,
    pub(super) stack_workers: RetainedTasks<()>,
    pub(super) echoes: RetainedTasks<()>,
}
impl RawIpChildren {
    pub(super) fn new() -> Self {
        let native = super::native_obligations::NativeObligations::new(
            super::tcp::MAX_CONNECTIONS,
            OwnedTunUdpTasks::MAX_ASSOCIATIONS,
            OwnedTunUdpTasks::MAX_DNS,
        );
        Self {
            native: native.clone(),
            icmp: None,
            drivers: RetainedTasks::new("raw-IP stack drivers joined", 2),
            proxies: RetainedTasks::new(
                "raw-IP outer carrier readers joined",
                crate::runtime::raw_ip::MAX_RAW_IP_DEVICES,
            ),
            connections: None,
            host: None,
            udp: OwnedTunUdpTasks::new(native),
            stack_workers: RetainedTasks::new(
                "raw-IP TCP retransmission and control workers joined",
                zero_stack::UserTcpStack::BACKGROUND_TASK_LIMIT,
            ),
            echoes: RetainedTasks::new(
                "raw-IP ICMP probes joined",
                RawIpInboundListenerOperation::MAX_ICMP_PROBES,
            ),
        }
    }
    pub(super) fn install(&mut self, context: &InboundInstallContext) -> Result<(), EngineError> {
        self.icmp = context.icmp_probe_grant();
        context.register(self.native.resource())?;
        context.register(self.stack_workers.resource())?;
        self.connections = Some(
            context
                .child_tasks(std::num::NonZeroUsize::new(super::tcp::MAX_CONNECTIONS).unwrap())?,
        );
        self.host = Some(context.tasks());
        self.udp.install(context)?;
        context.register(self.echoes.resource())?;
        context.register(self.proxies.resource())?;
        // Reverse cleanup first joins these actual parent loops. Their guard
        // release then permits the retained descendant sets to be drained.
        context.register(self.drivers.resource())?;
        Ok(())
    }
}
