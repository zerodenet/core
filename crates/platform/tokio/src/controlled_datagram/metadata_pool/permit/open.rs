use super::*;
use crate::contract::network::UpstreamBinding;
use std::net::SocketAddr;
use zero_core::request::Destination;

/// Crate-private sealed resolver projection, never a new public opener SPI.
pub(crate) struct MetadataCandidate<'a> {
    pub instance: ComponentInstanceId,
    pub binding: &'a UpstreamBinding,
    pub endpoint: &'a Destination,
    pub peer: SocketAddr,
    pub dns_generation: Option<crate::contract::dns::DnsGeneration>,
    pub egress: &'a crate::EgressInterfaceControl,
    pub observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

impl MetadataPoolPermit {
    pub fn open(
        &self,
        query: &TaskJoinWitness,
        egress: &crate::EgressInterfaceControl,
        generation: zero_core::component::NetworkGeneration,
        end: tokio::time::Instant,
    ) -> Result<GrantedMetadataListener, ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata pool dropped"))?;
        let bound = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| error(Reason::InvalidConfig, "unregistered metadata pool"))?;
        let ResourceDeclaration::ControlledMetadataDatagram { endpoint, dial, .. } =
            &bound.declaration
        else {
            return Err(error(
                Reason::InvalidConfig,
                "dynamic metadata requires sealed host candidates",
            ));
        };
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: dial.clone(),
            network_generation: generation,
        };
        let endpoint_fact = Destination::new(
            match endpoint.ip() {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            },
            endpoint.port(),
        );
        self.open_candidate(
            query,
            MetadataCandidate {
                instance: self.0.instance,
                binding: &binding,
                endpoint: &endpoint_fact,
                peer: *endpoint,
                dns_generation: None,
                egress,
                observer: None,
            },
            Some(end),
            || Ok(()),
        )
    }

    pub(crate) fn open_candidate(
        &self,
        query: &TaskJoinWitness,
        target: MetadataCandidate<'_>,
        end: Option<tokio::time::Instant>,
        validate: impl Fn() -> Result<(), ChannelError>,
    ) -> Result<GrantedMetadataListener, ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata pool dropped"))?;
        let bound = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| error(Reason::InvalidConfig, "unregistered metadata pool"))?;
        bound.check(&owner, query)?;
        if !self
            .0
            .caller
            .get()
            .is_some_and(|caller| query.same_registration(caller))
            || target.instance != self.0.instance
            || !target.binding.prefix.is_empty()
        {
            return Err(error(
                Reason::StalePlan,
                "metadata candidate belongs to another owner or caller",
            ));
        }
        let permitted = match &bound.declaration {
            ResourceDeclaration::ControlledMetadataDatagram { endpoint, dial, .. } => {
                target.peer == *endpoint && target.binding.dial == *dial
            }
            ResourceDeclaration::ControlledMetadataCarrier {
                endpoint,
                dial,
                ports,
                ..
            } => {
                target.endpoint == endpoint
                    && target.binding.dial == *dial
                    && if ports.is_empty() {
                        target.peer.port() == endpoint.port
                    } else {
                        ports.contains(&target.peer.port())
                    }
            }
            _ => false,
        };
        if !permitted {
            return Err(error(
                Reason::StalePlan,
                "metadata candidate differs from its original declaration",
            ));
        }
        validate()?;
        if end.is_some_and(|end| tokio::time::Instant::now() >= end) {
            return Err(error(
                Reason::Timeout,
                "metadata operation deadline elapsed",
            ));
        }
        let selection = target
            .egress
            .select_for_peer_with_policy(target.peer, &target.binding.dial)
            .map_err(|source| {
                error(Reason::Io, "metadata egress selection failed").with_source(source)
            })?;
        if selection.generation() != target.binding.network_generation.0 {
            return Err(error(Reason::StalePlan, "metadata network changed"));
        }
        let mut native = self.0.native()?;
        if native.attempted
            || self.0.closed.get().is_some()
            || self
                .0
                .protocol_stop
                .get()
                .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(error(
                Reason::Revoked,
                "metadata original was already attempted",
            ));
        }
        native.attempted = true;
        let grant = native
            .owner
            .acquire(target.peer, &target.binding.dial, &selection)
            .map_err(|source| {
                error(Reason::Io, "metadata original acquisition failed").with_source(source)
            })?;
        let super::super::super::metadata::GrantedMetadataDatagram { rx, tx, control } = grant;
        native.control = Some(control);
        native
            .control
            .as_mut()
            .expect("installed original control")
            .set_observer(target.observer)
            .map_err(|source| {
                error(Reason::Io, "metadata observation failed").with_source(source)
            })?;
        let (issuer, claim) = native
            .control
            .as_mut()
            .expect("installed original control")
            .claim_views(bound.scope.clone())
            .map_err(|source| error(Reason::Io, "metadata claim failed").with_source(source))?;
        native.issuer = Some(issuer);
        let observation = native
            .control
            .as_mut()
            .expect("installed original control")
            .take_observation()
            .map_err(|source| {
                error(Reason::Io, "metadata observation failed").with_source(source)
            })?;
        native.endpoints = Some((
            tx.local_addr().map_err(|source| {
                error(Reason::Io, "metadata local address failed").with_source(source)
            })?,
            target.peer,
        ));
        native.binding = Some((
            target.endpoint.clone(),
            target.binding.clone(),
            target.dns_generation,
        ));
        bound.check(&owner, query)?;
        validate()?;
        if end.is_some_and(|end| tokio::time::Instant::now() >= end) {
            return Err(error(
                Reason::Timeout,
                "metadata operation deadline elapsed during acquisition",
            ));
        }
        if target.egress.generation() != target.binding.network_generation.0 {
            return Err(error(
                Reason::StalePlan,
                "metadata network changed during acquisition",
            ));
        }
        if self
            .0
            .protocol_stop
            .get()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(error(
                Reason::Revoked,
                "original protocol retired during acquisition",
            ));
        }
        native.transferred = true;
        owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .last_binding = native.binding.clone();
        owner.changed.notify_waiters();
        Ok(GrantedMetadataListener {
            scope: bound.scope.clone(),
            rx,
            tx,
            observation,
            users_claim: Some(claim),
        })
    }
}
