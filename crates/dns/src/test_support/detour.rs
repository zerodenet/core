//! Binding fixtures use the actual registered host query and its loan ledger.
use super::*;
use zero_platform_tokio::{
    contract::{scope::TaskJoinWitness, CancellationToken},
    controlled_carrier::loan::LoanIo,
};

pub struct NativeDetourStream {
    pub stream: LoanIo,
    pub binding: Arc<dyn crate::DnsCarrierBinding>,
    pub task: TaskJoinWitness,
    // Dropped after the actual stream. The original query then runs its real
    // provider finalizer; channel completion itself is not a close receipt.
    _returned: tokio::sync::oneshot::Sender<()>,
}

/// Open through the current provider inside its real query Scope. This fixture
/// neither builds a host nor grants an undeclared endpoint or protocol role.
pub async fn open_native_detour(
    dns: &DnsSystem,
    outbound: String,
    endpoint: std::net::SocketAddr,
    end: tokio::time::Instant,
) -> io::Result<NativeDetourStream> {
    let provider = dns
        .native_source
        .as_ref()
        .or_else(|| dns.query_coordinator.native_source())
        .ok_or_else(invalid)?
        .provider()?;
    let tasks = provider.tasks();
    let (ready, receiver) = tokio::sync::oneshot::channel();
    let (returned, return_wait) = tokio::sync::oneshot::channel();
    let cancel = CancellationToken::new();
    let caller_cancel = cancel.clone();
    tasks.spawn_in_network(
        async move {
            let opening = async {
                let caller = crate::OwnedDnsQueryTasks::current_witness()?;
                let (loan, binding) = crate::OwnedDnsQueryTasks::native_io(provider.detour_stream(
                    outbound,
                    endpoint,
                    caller.clone(),
                    caller_cancel,
                    end,
                ))
                .await??;
                crate::OwnedDnsQueryTasks::record_native(loan.owner.original())?;
                Ok((loan, binding, caller))
            }
            .await;
            match opening {
                Ok((loan, binding, caller)) => {
                    let owner = loan.owner;
                    let _ = ready.send(Ok(NativeDetourStream {
                        stream: loan.io,
                        binding,
                        task: caller,
                        _returned: returned,
                    }));
                    let _ = return_wait.await;
                    drop(owner);
                    // Actual originals/controls are retained by this same
                    // provider and settled by the original task's finalizer.
                }
                Err(source) => {
                    let _ = ready.send(Err(source));
                }
            }
        },
        cancel,
        Some(dns.egress_interface.generation()),
    )?;
    receiver.await.map_err(io::Error::other)?
}
