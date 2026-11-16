use super::{PreparedTcpCandidate, PreparedTcpRelayHop};

/// A materialized route to one relay hop. Its relay hops
/// stop strictly before `final_server`.
#[derive(Clone)]
pub(crate) struct PreparedTcpRelayPrefix {
    pub(crate) first: PreparedTcpCandidate,
    pub(crate) relay_hops: Vec<PreparedTcpRelayHop>,
    pub(crate) final_tag: String,
    pub(crate) final_protocol: String,
    pub(crate) final_server: String,
    pub(crate) final_port: u16,
}

impl PreparedTcpRelayPrefix {
    /// Reacquire this already-selected native prefix as exact neutral nodes.
    /// A missing generation is terminal; only an explicitly retained process
    /// compatibility donor selects the older executor.
    pub(crate) fn standard_nodes(
        &self,
    ) -> Result<
        Option<Vec<crate::inventory::service::ServiceNode>>,
        zero_platform_tokio::contract::ChannelError,
    > {
        use super::{PreparedTcpCandidateExecution, PreparedTcpRelayExecution};
        let PreparedTcpCandidateExecution::Native(first) = &self.first.execution else {
            return Ok(None);
        };
        if self
            .relay_hops
            .iter()
            .any(|hop| !matches!(hop.execution, PreparedTcpRelayExecution::Native(_)))
        {
            return Ok(None);
        }
        let mut nodes = Vec::with_capacity(self.relay_hops.len() + 1);
        let first = first.slot()?;
        let endpoint = self
            .first
            .endpoint
            .as_ref()
            .map(|(host, port)| crate::inventory::service::endpoint(host, *port));
        let direct=first.recipe.spec().resources.iter().any(|resource|matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..}
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectCarrier {..}));
        nodes.push(crate::inventory::service::ServiceNode {
            dial: first.dial.clone(),
            slot: first,
            tag: self.first.tag.clone(),
            wire_endpoint: self.first.endpoint.as_ref().map(|(host, port)| {
                zero_core::request::Destination::new(
                    zero_core::Address::Domain(host.clone()),
                    *port,
                )
            }),
            endpoint,
            role: if direct {
                zero_platform_tokio::contract::dns::DnsQueryRole::Direct
            } else {
                zero_platform_tokio::contract::dns::DnsQueryRole::Node
            },
        });
        for hop in &self.relay_hops {
            let PreparedTcpRelayExecution::Native(selected) = &hop.execution else {
                unreachable!()
            };
            let slot = selected.slot()?;
            let endpoint = crate::inventory::service::endpoint(&hop.server, hop.port);
            nodes.push(crate::inventory::service::ServiceNode {
                dial: slot.dial.clone(),
                slot,
                tag: Some(hop.tag.clone()),
                endpoint: Some(endpoint.clone()),
                wire_endpoint: Some(zero_core::request::Destination::new(
                    zero_core::Address::Domain(hop.server.clone()),
                    hop.port,
                )),
                role: zero_platform_tokio::contract::dns::DnsQueryRole::Node,
            });
        }
        Ok(Some(nodes))
    }
}
