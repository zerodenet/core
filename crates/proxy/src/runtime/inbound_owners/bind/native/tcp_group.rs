//! Additional declared TCP originals share the actual component owner and Scope.
use super::*;
use zero_core::request::Destination;
pub(crate) struct MetadataViews {
    pub resources: Arc<crate::component::instance::HostResources>,
    pub tcp: Vec<(Destination, ControlledTcpListener)>,
}
pub(super) fn prepare(
    identity: ComponentInstanceId,
    declarations: &[ResourceDeclaration],
    attempts: NonZeroUsize,
    cancel: CancellationToken,
    registrar: &zero_platform_tokio::contract::scope::TaskRegistrar,
) -> Result<Vec<(Destination, Arc<grant::NativeTcpGrant>)>, ChannelError> {
    declarations
        .iter()
        .skip(1)
        .filter_map(|declaration| {
            let ResourceDeclaration::TcpListener { destination, .. } = declaration else {
                return None;
            };
            let address = match destination.address {
                zero_core::Address::Ipv4(ip) => {
                    Some(std::net::SocketAddr::new(ip.into(), destination.port))
                }
                zero_core::Address::Ipv6(ip) => {
                    Some(std::net::SocketAddr::new(ip.into(), destination.port))
                }
                zero_core::Address::Domain(_) => None,
            };
            Some(
                grant::NativeTcpGrant::new(
                    identity,
                    declaration,
                    address,
                    attempts,
                    cancel.clone(),
                    registrar,
                )
                .map(|grant| (destination.clone(), Arc::new(grant))),
            )
        })
        .collect()
}
pub(super) fn bind(
    resources: Arc<crate::component::instance::HostResources>,
    tcp: &[(Destination, Arc<grant::NativeTcpGrant>)],
) -> Result<MetadataViews, EngineError> {
    let mut views = Vec::with_capacity(tcp.len());
    for (destination, grant) in tcp {
        views.push((destination.clone(), grant.bind()?));
    }
    Ok(MetadataViews {
        resources,
        tcp: views,
    })
}
