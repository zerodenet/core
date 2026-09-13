use super::super::{ClaimedRelayChain, ProtocolInventory};
use super::{PreparedTcpCandidate, PreparedTcpRelayHop};
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

#[derive(Clone)]
pub(crate) struct PreparedTcpRelayChain {
    pub(crate) first: PreparedTcpCandidate,
    #[cfg(feature = "udp-runtime")]
    pub(crate) datagram_prefixes: Vec<
        Option<crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier>,
    >,
    pub(crate) relay_hops: Vec<PreparedTcpRelayHop>,
}

/// A materialized route to one relay hop. Its relay hops and datagram plans
/// stop strictly before `final_server`.
#[derive(Clone)]
pub(crate) struct PreparedTcpRelayPrefix {
    pub(crate) first: PreparedTcpCandidate,
    #[cfg(feature = "udp-runtime")]
    pub(crate) datagram_prefixes: Vec<
        Option<crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier>,
    >,
    pub(crate) relay_hops: Vec<PreparedTcpRelayHop>,
    pub(crate) final_tag: String,
    pub(crate) final_protocol: String,
    pub(crate) final_server: String,
    pub(crate) final_port: u16,
}

impl ProtocolInventory {
    pub(crate) fn prepare_claimed_tcp_relay_chain<'a>(
        &self,
        ctx: OutboundAdapterContext,
        claimed_chain: &ClaimedRelayChain<'a>,
    ) -> Result<PreparedTcpRelayChain, TcpOutboundFailure> {
        let first_prepared = self.prepare_claimed_tcp_candidate(ctx, claimed_chain.first())?;
        let claimed_hops = claimed_chain.relay_hops();
        let mut prepared_hops = Vec::with_capacity(claimed_hops.len());
        #[cfg(feature = "udp-runtime")]
        let mut datagram_prefixes = {
            let first = claimed_chain
                .first()
                .prepare_udp_packet_path(ctx.source_dir())
                .and_then(
                    crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::new,
                );
            vec![first]
        };
        #[cfg(feature = "udp-runtime")]
        let mut current_datagram = datagram_prefixes[0].clone();

        for (index, next_hop) in claimed_hops.iter().enumerate() {
            let prepared = self
                .prepare_claimed_tcp_relay_hop(ctx, next_hop)
                .map_err(|error| TcpOutboundFailure {
                    stage: "relay_prepare",
                    error,
                    upstream_endpoint: None,
                    network: None,
                })?;

            #[cfg(feature = "udp-runtime")]
            if index + 1 < claimed_hops.len() {
                let prefix = PreparedTcpRelayPrefix {
                    first: first_prepared.clone(),
                    datagram_prefixes: datagram_prefixes.clone(),
                    relay_hops: prepared_hops.clone(),
                    final_tag: prepared.tag.clone(),
                    final_protocol: prepared.protocol.clone(),
                    final_server: prepared.server.clone(),
                    final_port: prepared.port,
                };
                current_datagram = next_hop
                    .prepare_udp_packet_path(ctx.source_dir())
                    .and_then(|operation| {
                        crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::extend(
                            current_datagram.take(),
                            operation,
                            prefix,
                        )
                    });
                datagram_prefixes.push(current_datagram.clone());
            }
            prepared_hops.push(prepared);
        }

        Ok(PreparedTcpRelayChain {
            first: first_prepared,
            #[cfg(feature = "udp-runtime")]
            datagram_prefixes,
            relay_hops: prepared_hops,
        })
    }
}
