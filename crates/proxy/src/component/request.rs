//! Compatibility projection from the existing ingress owner and session facts.
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};
use zero_core::{
    component::*,
    request::{Destination, ProcessFacts, RequestFacts},
    Address, Session,
};

#[derive(Debug, Clone)]
pub(crate) struct IngressIdentity {
    pub(crate) tag: String,
    pub(crate) role: RoleInstanceId,
}

pub(crate) fn facts(session: &Session, ingress: &IngressIdentity) -> RequestFacts {
    let endpoint = |address: &Address, port| match address {
        Address::Ipv4(ip) => Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(*ip)), port)),
        Address::Ipv6(ip) => Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(*ip)), port)),
        Address::Domain(_) => None,
    };
    RequestFacts {
        session: SessionId(session.id),
        inbound: ingress.role.role,
        // Legacy Session has only the effective source. Do not invent the
        // original carrier address after a trusted PROXY prelude rewrites it.
        original_source: None,
        effective_source: session
            .source_ip
            .as_ref()
            .zip(session.source_port)
            .and_then(|(ip, port)| endpoint(ip, port)),
        local_endpoint: session
            .inbound_local
            .as_ref()
            .and_then(|(ip, port)| endpoint(ip, *port)),
        original_target: session
            .original_target
            .as_ref()
            .map(|address| Destination::new(address.clone(), session.port)),
        target: Some(Destination::new(session.target.clone(), session.port)),
        target_source: session.target_host_source,
        subject: session
            .auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_ref())
            .map(|key| AuthSubject {
                owner: ingress.role.component,
                principal: PrincipalId(Arc::from(key.as_str())),
            }),
        peer: session.inbound_peer_identity.clone().map(PeerId),
        protocol: session.protocol,
        sni: session.sni.clone(),
        process: (session.process_id.is_some()
            || session.process_name.is_some()
            || session.process_path.is_some())
        .then(|| ProcessFacts {
            id: session.process_id,
            name: session.process_name.clone(),
            path: session.process_path.clone(),
        }),
    }
}
