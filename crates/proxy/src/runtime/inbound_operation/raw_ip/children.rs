//! The actual listener's child sets are installed before its driver can spawn.
use super::*;
use crate::inbound::tun::udp::OwnedTunUdpTasks;
use crate::runtime::inbound_owners::InboundInstallContext;
use crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks;
use zero_platform_tokio::contract::scope::TaskClass;

pub(super) struct RawIpChildren {
    pub(super) limits: RawIpSourceLimits,
    pub(super) packet_roles: crate::runtime::packet_route::PacketRoleSessions,
    pub(super) native: super::native_obligations::NativeObligations,
    pub(super) icmp: Option<crate::component::instance::HostIcmpProbeGrant>,
    pub(super) drivers: ScopedSourceTasks<Result<(), EngineError>>,
    pub(super) connections: Option<Arc<crate::runtime::inbound_owners::InboundPeerTasks>>,
    pub(super) host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    pub(super) udp: OwnedTunUdpTasks,
    pub(super) stack_workers: ScopedSourceTasks<()>,
    pub(super) echoes: ScopedSourceTasks<()>,
    pub(super) tcp_dns: Option<ScopedSourceTasks<Result<(), std::io::Error>>>,
}
impl RawIpChildren {
    pub(super) fn new(limits: RawIpSourceLimits) -> Self {
        let native =
            super::native_obligations::NativeObligations::new(limits.tcp, limits.udp, limits.dns);
        Self {
            limits,
            packet_roles: Default::default(),
            native: native.clone(),
            tcp_dns: None,
            icmp: None,
            drivers: ScopedSourceTasks::new("raw-IP stack drivers joined", 2, TaskClass::Critical),
            connections: None,
            host: None,
            udp: OwnedTunUdpTasks::new(native, limits.udp, limits.dns),
            stack_workers: ScopedSourceTasks::new(
                "raw-IP TCP retransmission and control workers joined",
                zero_stack::UserTcpStack::BACKGROUND_TASK_LIMIT,
                TaskClass::Critical,
            ),
            echoes: ScopedSourceTasks::new(
                "raw-IP ICMP probes joined",
                limits.icmp,
                TaskClass::Auxiliary,
            ),
        }
    }
    pub(super) fn device(limits: RawIpSourceLimits) -> Self {
        let mut children = Self::new(limits);
        children.tcp_dns = Some(ScopedSourceTasks::new(
            "System TUN TCP DNS connections joined",
            limits.tcp_dns,
            TaskClass::Auxiliary,
        ));
        children
    }
    pub(super) fn install(&mut self, context: &InboundInstallContext) -> Result<(), EngineError> {
        context.owner().prepare_packet_source_pause();
        self.native
            .bind_source(context.source_registrar().scope_identity())
            .map_err(std::io::Error::other)?;
        self.icmp = context.icmp_probe_grant();
        context.register_after_scope(self.packet_roles.resource())?;
        if let Some(tcp_dns) = &self.tcp_dns {
            tcp_dns
                .bind(context.source_registrar())
                .map_err(std::io::Error::other)?;
            context.register_after_scope(tcp_dns.resource())?;
        }
        self.drivers
            .bind(context.source_registrar())
            .map_err(std::io::Error::other)?;
        self.stack_workers
            .bind(context.source_registrar())
            .map_err(std::io::Error::other)?;
        self.echoes
            .bind(context.source_registrar())
            .map_err(std::io::Error::other)?;
        context.register_after_scope(self.native.resource())?;
        context.register_after_scope(self.stack_workers.resource())?;
        self.connections =
            Some(context.child_tasks(std::num::NonZeroUsize::new(self.limits.tcp).unwrap())?);
        self.host = Some(context.tasks());
        self.udp.install(context)?;
        context.register_after_scope(self.echoes.resource())?;
        // Reverse cleanup first joins these actual parent loops. Their guard
        // release then permits the retained descendant sets to be drained.
        context.register_after_scope(self.drivers.resource())?;
        Ok(())
    }
}
