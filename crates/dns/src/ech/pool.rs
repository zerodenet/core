//! ECH is another question type in the original DoH resource namespace.
use super::*;
use crate::{
    backends::{DohDnsResolver, ResolverBackend},
    ResolverServer,
};
use std::{net::SocketAddr, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DohKey {
    secure: bool,
    host: String,
    port: u16,
    path: String,
    server_name: String,
    peers: Vec<SocketAddr>,
}
impl DohKey {
    pub(crate) fn same_endpoint(&self, other: &Self) -> bool {
        self.secure == other.secure
            && self.host == other.host
            && self.port == other.port
            && self.path == other.path
            && self.server_name == other.server_name
    }
    fn configured(server: &zero_config::DnsServerConfig) -> io::Result<Option<Self>> {
        let zero_config::DnsServerConfig::Doh {
            host,
            port,
            path,
            server_name,
            detour: None,
            ..
        } = server
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            secure: true,
            host: host.clone(),
            port: *port,
            path: path.clone(),
            server_name: server_name.as_ref().unwrap_or(host).clone(),
            peers: crate::backends::server_endpoints(server, *port)?,
        }))
    }
}
pub(super) fn backend(
    endpoint: &EchDnsEndpoint,
    peers: &[SocketAddr],
    configured: &[ResolverServer],
    owners: &crate::transport_owners::TransportOwners,
    egress: &zero_platform_tokio::EgressInterfaceControl,
) -> io::Result<Arc<ResolverBackend>> {
    let mut path = endpoint.url.path().to_owned();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = endpoint.url.query() {
        path.push('?');
        path.push_str(query);
    }
    let key = DohKey {
        secure: endpoint.url.scheme() == "https",
        host: endpoint.host().to_owned(),
        port: endpoint.port(),
        path,
        server_name: endpoint.host().to_owned(),
        peers: peers.to_vec(),
    };
    for server in configured {
        if DohKey::configured(&server.config)?.as_ref() == Some(&key)
            && server.backend.resource_admission_open()
        {
            // Preserve the actual configured clients/connection lock/owner,
            // including fixture trust; no ECH-specific pool is constructed.
            return Ok(server.backend.clone());
        }
    }
    owners.doh_endpoint(key.clone(), || {
        let ips = peers.iter().map(SocketAddr::ip).collect();
        let resolver = if key.secure {
            DohDnsResolver::new(
                key.host.clone(),
                key.port,
                key.path.clone(),
                ips,
                Some(key.server_name.clone()),
                egress.clone(),
            )?
        } else {
            DohDnsResolver::new_cleartext(
                key.host.clone(),
                key.port,
                key.path.clone(),
                ips,
                egress.clone(),
            )?
        };
        Ok(Arc::new(ResolverBackend::Doh(resolver)))
    })
}
