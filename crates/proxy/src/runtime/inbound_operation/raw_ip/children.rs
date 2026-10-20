//! The actual listener's child sets are installed before its driver can spawn.
use super::tasks::RetainedTasks;
use super::*;
use crate::inbound::tun::udp::OwnedTunUdpTasks;
use crate::runtime::inbound_owners::InboundInstallContext;

pub(super) struct RawIpChildren {
    pub(super) drivers: RetainedTasks<Result<(), EngineError>>,
    pub(super) proxies: RetainedTasks<()>,
    pub(super) connections: RetainedTasks<Result<(), EngineError>>,
    pub(super) udp: OwnedTunUdpTasks,
    pub(super) stack_workers: RetainedTasks<()>,
    pub(super) echoes: RetainedTasks<()>,
}
impl RawIpChildren {
    pub(super) fn new() -> Self {
        Self {
            drivers: RetainedTasks::new("raw-IP stack drivers joined", 2),
            proxies: RetainedTasks::new(
                "raw-IP outer carrier readers joined",
                crate::runtime::raw_ip::MAX_RAW_IP_DEVICES,
            ),
            connections: RetainedTasks::new("raw-IP TCP connections joined", 4096),
            udp: OwnedTunUdpTasks::new(),
            stack_workers: RetainedTasks::new(
                "raw-IP TCP retransmission and control workers joined",
                zero_stack::UserTcpStack::BACKGROUND_TASK_LIMIT,
            ),
            echoes: RetainedTasks::new("raw-IP ICMP probes joined", super::icmp::MAX_ECHO_PROBES),
        }
    }
    pub(super) fn install(&self, context: &InboundInstallContext) -> Result<(), EngineError> {
        context.register(self.stack_workers.resource())?;
        context.register(self.connections.resource())?;
        self.udp.install(context)?;
        context.register(self.echoes.resource())?;
        context.register(self.proxies.resource())?;
        // Reverse cleanup first joins these actual parent loops. Their guard
        // release then permits the retained descendant sets to be drained.
        context.register(self.drivers.resource())?;
        Ok(())
    }
}
