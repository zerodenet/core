//! A standard Stream node never requires a compatibility TCP claimer.
use super::{
    PreparedTcpCandidate, PreparedTcpCandidateExecution, PreparedTcpRelayExecution,
    PreparedTcpRelayHop, PreparedTcpRelayPrefix,
};
use crate::{
    inventory::ProtocolInventory, protocol_registry::OutboundAdapterContext,
    transport::TcpOutboundFailure,
};
use zero_engine::{EngineError, ResolvedLeafOutbound, RouteMode};

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_tcp_relay<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        leaves: &[ResolvedLeafOutbound<'a>],
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Result<(PreparedTcpRelayPrefix, PreparedTcpRelayHop), TcpOutboundFailure> {
        let (receiver, donors) = leaves
            .split_last()
            .expect("validated relay has a final hop");
        let hop = if let Some(native) = (!matches!(receiver, ResolvedLeafOutbound::Direct { .. }))
            .then(|| self.native_tcp_candidate(ctx, receiver, components, RouteMode::Flow))
            .flatten()
        {
            native_hop(native.map_err(|mut failure| {
                failure.stage = "relay_prepare";
                failure
            })?)?
        } else {
            let claimed = self
                .claim_outbound_leaf(ctx.config(), receiver.clone())
                .map_err(|error| failure("relay_prepare", error))?;
            self.prepare_claimed_tcp_relay_hop(ctx, &claimed)
                .map_err(|error| failure("relay_prepare", error))?
        };
        let prefix = self.prepare_tcp_relay_prefix(
            ctx,
            donors,
            components,
            (&hop.tag, &hop.protocol, &hop.server, hop.port),
        )?;
        Ok((prefix, hop))
    }

    /// A receiver contributes endpoint facts; only donor nodes supply TCP roles.
    /// Its actual legacy ownership still determines whether it can retain native loans.
    pub(in crate::inventory) fn prepare_tcp_relay_prefix<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        leaves: &[ResolvedLeafOutbound<'a>],
        components: Option<&crate::component::catalog::ComponentPlan>,
        receiver: (&str, &str, &str, u16),
    ) -> Result<PreparedTcpRelayPrefix, TcpOutboundFailure> {
        if leaves.is_empty() {
            return Err(failure(
                "relay_prepare",
                EngineError::InvalidPlan {
                    message: "carrier prefix has no selected donor".into(),
                },
            ));
        }
        let first_leaf = leaves
            .first()
            .expect("validated relay has at least two nodes");
        let native_at = |index: usize, leaf: &ResolvedLeafOutbound<'a>| {
            if index != 0 && matches!(leaf, ResolvedLeafOutbound::Direct { .. }) {
                return None;
            }
            self.native_tcp_candidate(ctx, leaf, components, RouteMode::Flow)
        };
        let first = if let Some(native) = native_at(0, first_leaf) {
            native?
        } else {
            let claimed = self
                .claim_outbound_leaf(ctx.config(), first_leaf.clone())
                .map_err(|error| failure("outbound_leaf_runtime", error))?;
            self.prepare_claimed_tcp_candidate(ctx, &claimed, RouteMode::Flow)?
        };
        let mut chain = PreparedTcpRelayPrefix {
            first,
            relay_hops: Vec::with_capacity(leaves.len() - 1),
            final_tag: receiver.0.into(),
            final_protocol: receiver.1.into(),
            final_server: receiver.2.into(),
            final_port: receiver.3,
        };
        for (index, leaf) in leaves.iter().enumerate().skip(1) {
            let hop = if let Some(native) = native_at(index, leaf) {
                native_hop(native.map_err(|mut error| {
                    error.stage = "relay_prepare";
                    error
                })?)?
            } else {
                let claimed = self
                    .claim_outbound_leaf(ctx.config(), leaf.clone())
                    .map_err(|error| failure("relay_prepare", error))?;
                self.prepare_claimed_tcp_relay_hop(ctx, &claimed)
                    .map_err(|error| failure("relay_prepare", error))?
            };
            chain.relay_hops.push(hop);
        }
        Ok(chain)
    }
}

fn native_hop(candidate: PreparedTcpCandidate) -> Result<PreparedTcpRelayHop, TcpOutboundFailure> {
    let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
        unreachable!("native candidate selects an actual Stream slot")
    };
    let (server, port) = candidate.endpoint.ok_or_else(|| {
        failure(
            "relay_prepare",
            zero_core::Error::Unsupported(
                "endpoint-less Stream requires its standard relay executor",
            )
            .into(),
        )
    })?;
    Ok(PreparedTcpRelayHop {
        tag: candidate.tag.expect("native Stream has a configured tag"),
        protocol: candidate.protocol,
        server,
        port,
        execution: PreparedTcpRelayExecution::Native(slot),
    })
}

fn failure(stage: &'static str, error: EngineError) -> TcpOutboundFailure {
    TcpOutboundFailure {
        stage,
        error,
        upstream_endpoint: None,
        network: None,
    }
}
