//! DNS borrows the selected pool through its existing bounded original ledger.
use super::*;
use crate::{inventory::service::ServiceNode, protocol_registry::TcpExecutionServices};

impl Provider {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn borrow_detour(
        &self,
        execution: &TcpExecutionServices,
        nodes: &[ServiceNode],
        peer: SocketAddr,
        binding: &UpstreamBinding,
        call: &CarrierContext,
        caller: TaskJoinWitness,
        upstream: UpstreamTransport,
    ) -> std::io::Result<StreamLoan> {
        let grants = &self
            .peers
            .iter()
            .find(|(address, packet, _)| *address == peer && !packet)
            .ok_or_else(|| std::io::Error::other("DNS borrowed endpoint was not declared"))?
            .2;
        let mut grant = grants.reserve().map_err(std::io::Error::other)?;
        let view = grant
            .claim_transferred_view()
            .map_err(std::io::Error::other)?;
        let entry = Arc::new(custody::NativeUse {
            task: caller,
            cancel: call.cancel.clone(),
            original: grant.adoption(),
            logical: Default::default(),
        });
        {
            let mut users = self.users.lock().expect("DNS users poisoned");
            if users.len() >= LIMIT {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "DNS original capacity exhausted",
                ));
            }
            users.push(entry);
        }
        let loan = crate::runtime::tcp_dispatch::service::connect_carrier_stream(
            execution,
            &destination(peer),
            nodes,
            zero_core::request::ServicePurpose::DnsQuery,
            call,
            upstream,
        )
        .await
        .map_err(std::io::Error::other)?;
        let origin = loan.owner.original().clone();
        // Persist the exact logical controller before validation or any await.
        // A stale or cancelled acquisition remains in the same retained slot.
        let io = grant
            .adopt_stream_loan(binding, destination(peer), call, loan)
            .map_err(std::io::Error::other)?;
        execution
            .components
            .as_ref()
            .map_err(|source| std::io::Error::other(source.clone()))?
            .validate_carrier_origin(nodes.last().unwrap().slot.role_id(), &origin)
            .map_err(std::io::Error::other)?;
        io.into_stream_loan(view).map_err(std::io::Error::other)
    }
}
