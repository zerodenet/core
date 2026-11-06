//! Cold native declarations and the actual standard component recipe.
//! There is no legacy operation envelope or alternate binder.
use std::{net::SocketAddr, sync::Arc};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::ResourceDeclaration;

pub(crate) struct PreparedInboundBind {
    recipe: crate::component::ComponentRecipe,
    recovery: Option<Arc<dyn super::InboundRecovery>>,
    resources: Arc<[ResourceDeclaration]>,
    address: Option<SocketAddr>,
}
impl PreparedInboundBind {
    pub(crate) fn from_recipe(
        recipe: crate::component::ComponentRecipe,
    ) -> Result<Self, EngineError> {
        use zero_platform_tokio::contract::component::{ResourceOwner, RoleKind};
        let spec = recipe.spec();
        crate::component::instance::packet_datagram_read_spec(&spec.resources, usize::MAX)
            .map_err(std::io::Error::other)?;
        if spec.owner != ResourceOwner::Component
            || spec.roles != [RoleKind::Inbound]
            || !spec.dependencies.is_empty()
        {
            return Err(unsupported());
        }
        let metadata_destination;
        let destination = match spec.resources.as_slice() {
            #[cfg(feature = "raw-ip-runtime")]
            [
                ResourceDeclaration::DatagramListener {
                    destination,
                    accepted: None,
                    max_pending_sends,
                    ..
                },
                ResourceDeclaration::NativeIcmp { .. },
                ResourceDeclaration::PacketDatagramReads { datagram: 0, .. },
            ] if spec.task_limit.is_some() && max_pending_sends.get() == 1 => destination,
            [
                ResourceDeclaration::MetadataDatagram { source, .. },
                auxiliary @ ..,
            ] if spec.task_limit.is_some() && valid_metadata_aux(auxiliary) => {
                metadata_destination = zero_core::request::Destination::new(
                    match source.ip() {
                        std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                        std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                    },
                    source.port(),
                );
                &metadata_destination
            }
            [
                ResourceDeclaration::DatagramListener {
                    destination,
                    accepted: Some(limits),
                    ..
                },
                auxiliary @ ..,
            ] if limits.task_slots().is_some() && valid_associations(auxiliary) => destination,
            [
                ResourceDeclaration::TcpListener {
                    destination,
                    accepted: Some(limits),
                    ..
                },
                auxiliary @ ..,
            ] if limits.task_slots().is_some() => {
                match auxiliary {
                    [] => {}
                    auxiliary if valid_associations(auxiliary) => {}
                    _ if spec.task_limit.is_some() && valid_handoff(&spec.resources) => {}
                    [
                        ResourceDeclaration::DatagramListener {
                            destination: datagram,
                            accepted: Some(peers),
                            ..
                        },
                    ] if datagram == destination
                        && limits.max_resources_per_socket == 0
                        && peers.task_slots().is_some() => {}
                    [ResourceDeclaration::AcceptedDatagram { .. }]
                        if limits.max_resources_per_socket == 2
                            && limits.max_tasks_per_socket.get() >= 2 => {}
                    _ => return Err(unsupported()),
                }
                destination
            }
            _ => return Err(unsupported()),
        };
        let address = match &destination.address {
            zero_core::Address::Ipv4(ip) => Some(SocketAddr::new(
                std::net::IpAddr::V4((*ip).into()),
                destination.port,
            )),
            zero_core::Address::Ipv6(ip) => Some(SocketAddr::new(
                std::net::IpAddr::V6((*ip).into()),
                destination.port,
            )),
            zero_core::Address::Domain(host) if !host.is_empty() => None,
            _ => return Err(unsupported()),
        };
        let resources = Arc::from(spec.resources.as_slice());
        Ok(Self {
            recipe,
            resources,
            recovery: None,
            address,
        })
    }
    pub(crate) fn with_recovery(mut self, recovery: Arc<dyn super::InboundRecovery>) -> Self {
        self.recovery = Some(recovery);
        self
    }
    pub(crate) fn take_recovery(&mut self) -> Option<Arc<dyn super::InboundRecovery>> {
        self.recovery.take()
    }
    pub(crate) fn resources(&self) -> &[ResourceDeclaration] {
        &self.resources
    }
    pub(crate) fn address(&self) -> Option<SocketAddr> {
        self.address
    }
    pub(crate) fn host_tasks(&self) -> usize {
        let tcp = self
            .resources
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    ResourceDeclaration::TcpListener {
                        destination: zero_core::request::Destination {
                            address: zero_core::Address::Domain(_),
                            ..
                        },
                        ..
                    }
                )
            })
            .count();
        tcp + usize::from(
            self.address.is_none()
                && !matches!(
                    self.resources.first(),
                    Some(ResourceDeclaration::TcpListener { .. })
                ),
        )
    }
    pub(crate) fn accepted_tcp_slots(&self) -> Option<usize> {
        self.resources
            .iter()
            .try_fold(0usize, |sum, resource| match resource {
                ResourceDeclaration::TcpListener {
                    accepted: Some(limits),
                    ..
                } => sum.checked_add(limits.task_slots()?.get()),
                _ => Some(sum),
            })
    }
    pub(crate) fn internal_tasks(&self) -> usize {
        let recipe = &self.recipe;
        recipe
            .spec()
            .resources
            .iter()
            .map(|resource| match resource {
                ResourceDeclaration::DatagramListener {
                    accepted: Some(limits),
                    ..
                } => limits.task_slots().map_or(usize::MAX, |s| s.get()),
                ResourceDeclaration::DatagramAssociations {
                    max_associations,
                    max_tasks_per_association,
                    ..
                } => max_associations
                    .get()
                    .saturating_mul(max_tasks_per_association.get()),
                _ => 0,
            })
            .try_fold(recipe.spec().task_limit.map_or(0, |n| n.get()), |sum, n| {
                sum.checked_add(n)
            })
            .unwrap_or(usize::MAX)
    }
    pub(crate) fn into_recipe(self) -> crate::component::ComponentRecipe {
        self.recipe
    }
}
fn unsupported() -> EngineError {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "host listener lease currently requires one declared literal TCP or datagram listener",
    )
    .into()
}

fn valid_associations(resources: &[ResourceDeclaration]) -> bool {
    let mut associations = false;
    let mut targets = false;
    let mut tree = false;
    resources.iter().all(|resource| match resource {
        ResourceDeclaration::DatagramAssociations {
            max_associations,
            max_tasks_per_association,
            ..
        } if !associations => {
            associations = true;
            max_associations
                .get()
                .checked_mul(max_tasks_per_association.get())
                .is_some()
        }
        ResourceDeclaration::FileTree { root, .. } if !tree => {
            tree = true;
            !root.as_os_str().is_empty()
        }
        ResourceDeclaration::InboundTargets { endpoints, .. } if !targets => {
            targets = true;
            !endpoints.is_empty()
                && endpoints
                    .iter()
                    .enumerate()
                    .all(|(index, endpoint)| !endpoints[..index].contains(endpoint))
        }
        _ => false,
    })
}

fn valid_metadata_aux(resources: &[ResourceDeclaration]) -> bool {
    let mut icmp = false;
    let mut tcp = Vec::new();
    let rest: Vec<_> = resources
        .iter()
        .filter(|r| {
            if let ResourceDeclaration::TcpListener {
                destination,
                accepted: Some(limits),
                ..
            } = r
            {
                if tcp.len() >= 2 || tcp.contains(destination) || limits.task_slots().is_none() {
                    return true;
                }
                tcp.push(destination.clone());
                false
            } else if let ResourceDeclaration::NativeIcmp { max_sockets } = r {
                if icmp || max_sockets.get() != 1 {
                    return true;
                }
                icmp = true;
                false
            } else {
                true
            }
        })
        .cloned()
        .collect();
    valid_associations(&rest)
}

fn valid_handoff(resources: &[ResourceDeclaration]) -> bool {
    matches!(resources,[ResourceDeclaration::TcpListener {destination,accepted:Some(tcp),..},ResourceDeclaration::DatagramListener {destination:udp,accepted:Some(peers),..},ResourceDeclaration::ListenerHandoff {stream,datagram,..},ResourceDeclaration::Process {program,max_starts}]
        if destination==udp && (*stream||*datagram) && tcp.task_slots().is_some() && peers.task_slots().is_some() && !program.as_os_str().is_empty() && max_starts.get()==1)
}
