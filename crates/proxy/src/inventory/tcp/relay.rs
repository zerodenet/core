use super::{PreparedTcpCandidate, PreparedTcpRelayHop};

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
