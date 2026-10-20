//! The one canonical ICMP authority belongs to this actual listener instance.
use super::*;
use crate::component::instance::{HostIcmpProbeGrant, HostIcmpProbeOwner};
use zero_platform_tokio::contract::{
    component::ResourceDeclaration,
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle},
    ChannelError, CloseReason,
};
struct IcmpResource {
    native: HostIcmpProbeOwner,
    origin: Weak<InboundOwner>,
    identity: ComponentInstanceId,
    pending_report: CloseReport,
}
impl InboundOwner {
    pub(super) fn install_icmp_probe(
        self: &Arc<Self>,
        declaration: &ResourceDeclaration,
    ) -> Result<(), EngineError> {
        let native = HostIcmpProbeOwner::new(
            self.identity(),
            declaration,
            self.task_access().cancellation(),
        )
        .map_err(std::io::Error::other)?;
        let grant = native.grant();
        self.install_resource(
            self.identity(),
            erase_lifecycle(IcmpResource {
                native,
                origin: Arc::downgrade(self),
                identity: self.identity(),
                pending_report: CloseReport::default(),
            }),
        )?;
        self.icmp_probe
            .set(grant)
            .map_err(|_| io("ICMP authority was already installed for this listener"))
    }
}
impl InboundInstallContext {
    pub(crate) fn icmp_probe_grant(&self) -> Option<HostIcmpProbeGrant> {
        self.entry.icmp_probe.get().cloned()
    }
}
impl ResourceLifecycle for IcmpResource {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        self.native.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.native.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let result = self.native.shutdown(deadline).await;
        let mut progress = self.native.take_closed_io();
        let complete = self.native.physical_confirmed();
        if let Ok(report) = result.as_ref() {
            // Reports contain owned strings, so move the report below rather
            // than cloning proof. This branch only examines the disposition.
            debug_assert!(!complete || report.is_complete());
        }
        let result = match result {
            Ok(report) => {
                progress.append(report);
                Ok(())
            }
            Err(error) => Err(error),
        };
        // Current unfinished ownership is the retained resource, not receipt
        // history. Only confirmed progress belongs in the parent's ledger.
        progress.unfinished.clear();
        self.pending_report.append(progress);
        if let Some(owner) = self.origin.upgrade() {
            owner.record_native_progress(
                self.identity,
                std::mem::take(&mut self.pending_report),
                self.native.fault(),
            );
            if complete {
                return Ok(CloseReport::default());
            }
        } else {
            // The normal registry retains the actual owner throughout cleanup.
            // If this resource is independently driven, return its same proof.
            result?;
            return Ok(std::mem::take(&mut self.pending_report));
        }
        result?;
        Ok(CloseReport {
            unfinished: vec![zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                "original ICMP pool remains unconfirmed".into(),
            )],
            ..Default::default()
        })
    }
}
