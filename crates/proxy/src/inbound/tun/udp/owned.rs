//! Raw-IP's actual child handles; the ordinary TUN entry keeps its legacy owner.
use super::*;
use crate::runtime::{
    inbound_operation::raw_ip::tasks::RetainedTasks, inbound_owners::InboundInstallContext,
};

type AssociationExit = (SocketAddress, u64, Result<(), EngineError>);
#[derive(Clone)]
pub(crate) struct OwnedTunUdpTasks {
    pub(super) native:
        crate::runtime::inbound_operation::raw_ip::native_obligations::NativeObligations,
    pub(super) scope: Arc<std::sync::OnceLock<super::scoped::AssociationScope>>,
    pub(super) completed: mpsc::Sender<AssociationExit>,
    pub(super) receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<AssociationExit>>>,
    pub(super) dns: RetainedTasks<Result<(), std::io::Error>>,
}
impl OwnedTunUdpTasks {
    pub(crate) const MAX_ASSOCIATIONS: usize = association::MAX_ACTIVE_ASSOCIATIONS;
    pub(crate) const MAX_DNS: usize = MAX_CONCURRENT_DNS_QUERIES;
    pub(crate) fn new(
        native: crate::runtime::inbound_operation::raw_ip::native_obligations::NativeObligations,
    ) -> Self {
        let (completed, receiver) = mpsc::channel(Self::MAX_ASSOCIATIONS);
        Self {
            native,
            scope: Arc::new(std::sync::OnceLock::new()),
            completed,
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            dns: RetainedTasks::new("raw-IP UDP DNS queries joined", MAX_CONCURRENT_DNS_QUERIES),
        }
    }
    pub(crate) fn install(&self, context: &InboundInstallContext) -> Result<(), EngineError> {
        let group =
            context.child_tasks(std::num::NonZeroUsize::new(Self::MAX_ASSOCIATIONS).unwrap())?;
        self.scope
            .set(super::scoped::AssociationScope {
                group,
                host: context.tasks(),
            })
            .map_err(|_| std::io::Error::other("raw-IP association Scope already installed"))?;
        context.register(self.dns.resource())?;
        Ok(())
    }
}

// The completion channel is metadata, not a join witness. If its consumer has
// stopped, send actual failure outputs through the same original Scope result.
pub(super) fn lost_completion(output: AssociationExit) -> Result<(), EngineError> {
    use crate::runtime::raw_ip::tasks::TaskOutput;
    output
        .into_failure()
        .map_or(Ok(()), |source| Err(std::io::Error::other(source).into()))
}

/// Explicit grants of the raw-IP association owner. The sole synchronous native
/// issuer permits one pending acquisition; address-family fallback is pre-I/O.
pub(super) fn direct_budget() -> crate::runtime::udp_dispatch::BoundDirectBudget {
    use crate::runtime::udp_socket::endpoint::DirectEndpointLimits;
    use std::num::NonZeroUsize;
    crate::runtime::udp_dispatch::BoundDirectBudget {
        limits: DirectEndpointLimits {
            logical: NonZeroUsize::new(256).unwrap(),
            native_sockets: NonZeroUsize::new(256).unwrap(),
            pending_native_opens: NonZeroUsize::MIN,
            pending_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            candidates_per_job: NonZeroUsize::new(1024).unwrap(),
        },
        max_bindings: NonZeroUsize::new(64).unwrap(),
        pre_io_bind_attempts: NonZeroUsize::new(2).unwrap(),
        start_deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}
