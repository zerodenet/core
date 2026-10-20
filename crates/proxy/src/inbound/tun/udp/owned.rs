//! Raw-IP's actual child handles; the ordinary TUN entry keeps its legacy owner.
use super::*;
use crate::runtime::{
    inbound_operation::raw_ip::tasks::RetainedTasks, inbound_owners::InboundInstallContext,
};

type AssociationExit = (SocketAddress, u64, Result<(), EngineError>);
#[derive(Clone)]
pub(crate) struct OwnedTunUdpTasks {
    pub(super) associations: RetainedTasks<AssociationExit>,
    pub(super) dns: RetainedTasks<Result<(), std::io::Error>>,
}
impl OwnedTunUdpTasks {
    pub(crate) fn new() -> Self {
        Self {
            associations: RetainedTasks::new(
                "raw-IP UDP associations joined",
                association::MAX_ACTIVE_ASSOCIATIONS,
            ),
            dns: RetainedTasks::new("raw-IP UDP DNS queries joined", MAX_CONCURRENT_DNS_QUERIES),
        }
    }
    pub(crate) fn install(&self, context: &InboundInstallContext) -> Result<(), EngineError> {
        context.register(self.associations.resource())?;
        context.register(self.dns.resource())?;
        Ok(())
    }
}
