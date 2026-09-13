use crate::inventory::{
    PreparedTcpCandidate, PreparedTcpRelayChain, PreparedTcpRelayHop, PreparedTcpRelayPrefix,
};

pub(super) fn relay_chain_identity(prepared: &PreparedTcpRelayChain, generation: u64) -> String {
    identity(&prepared.first, &prepared.relay_hops, None, generation)
}

pub(super) fn relay_prefix_identity(prepared: &PreparedTcpRelayPrefix, generation: u64) -> String {
    identity(
        &prepared.first,
        &prepared.relay_hops,
        Some((
            &prepared.final_tag,
            &prepared.final_protocol,
            &prepared.final_server,
            prepared.final_port,
        )),
        generation,
    )
}

fn identity(
    first: &PreparedTcpCandidate,
    relay_hops: &[PreparedTcpRelayHop],
    final_hop: Option<(&str, &str, &str, u16)>,
    generation: u64,
) -> String {
    let mut identity = format!("egress={generation}");
    if let Some(tag) = &first.tag {
        identity.push_str(&format!("|{}:{}", tag.len(), tag));
    }
    identity.push_str(&format!("|{}:{}", first.protocol.len(), first.protocol));
    if let Some((server, port)) = &first.endpoint {
        identity.push_str(&format!("|{}:{server}:{port}", server.len()));
    }
    for hop in relay_hops {
        push_hop(
            &mut identity,
            &hop.tag,
            &hop.protocol,
            &hop.server,
            hop.port,
        );
    }
    if let Some((tag, protocol, server, port)) = final_hop {
        push_hop(&mut identity, tag, protocol, server, port);
    }
    identity
}

fn push_hop(identity: &mut String, tag: &str, protocol: &str, server: &str, port: u16) {
    identity.push_str(&format!(
        "|{}:{}|{}:{}|{}:{}:{}",
        tag.len(),
        tag,
        protocol.len(),
        protocol,
        server.len(),
        server,
        port
    ));
}
