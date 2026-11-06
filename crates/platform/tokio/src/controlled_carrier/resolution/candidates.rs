use super::*;
pub(in crate::controlled_carrier) struct Source {
    pub grantee: ComponentInstanceId,
    pub related_parent: Option<super::super::CarrierAdoption>,
    pub binding: UpstreamBinding,
    pub endpoint: Destination,
    pub egress: Arc<EgressInterfaceControl>,
    pub observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl Source {
    pub(in crate::controlled_carrier) fn check(
        &self,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        context: &CarrierContext,
    ) -> Result<(), ChannelError> {
        if let Some(parent) = &self.related_parent {
            super::super::related::check_parent(parent, self.grantee, binding)?;
        }
        let reason = if binding != &self.binding
            || endpoint != &self.endpoint
            || !binding.prefix.is_empty()
            || self.egress.generation() != binding.network_generation.0
            || context.session.is_some()
        {
            Some(Reason::StalePlan)
        } else if context.cancel.is_cancelled() {
            Some(Reason::Cancelled)
        } else if context
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            Some(Reason::Timeout)
        } else {
            None
        };
        reason.map_or(Ok(()), |reason| {
            Err(error(
                self.grantee,
                Stage::Dial,
                reason,
                DeliveryState::NotSubmitted,
            ))
        })
    }
    pub(in crate::controlled_carrier) fn candidates(
        self,
        peers: Vec<SocketAddr>,
        context: CarrierContext,
    ) -> CarrierCandidates {
        CarrierCandidates {
            source: self,
            peers,
            context,
            resolver: None,
        }
    }
}
/// Opaque host result. Candidate addresses cannot replace the declared domain,
/// scope, hard dial policy, network generation or original physical owner.
pub struct CarrierCandidates {
    source: Source,
    peers: Vec<SocketAddr>,
    context: CarrierContext,
    pub(super) resolver: Option<(Arc<dyn DnsService>, DnsGeneration)>,
}
impl CarrierCandidates {
    /// Current host-selected address observation only. This cannot acquire
    /// native I/O, alter the declared domain or bypass the selected prefix.
    pub fn peer(&self, index: usize) -> Option<SocketAddr> {
        self.opener(index).ok()?;
        self.peers.get(index).copied()
    }
    /// Acquire ancillary originals through the same actual host-selected Node
    /// candidates. A port override is accepted only by the original explicit
    /// rotation declaration, never by changing the retained domain authority.
    pub fn open_metadata(
        &self,
        index: usize,
        port: Option<u16>,
        permit: &crate::controlled_datagram::metadata_pool::MetadataPoolPermit,
        query: &crate::contract::scope::TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> Result<
        (
            SocketAddr,
            crate::contract::component::GrantedMetadataListener,
        ),
        ChannelError,
    > {
        self.opener(index)?;
        let mut peer = self.peers[index];
        if let Some(port) = port {
            peer.set_port(port);
        }
        let grant = permit.open_candidate(
            query,
            crate::controlled_datagram::metadata_pool::MetadataCandidate {
                instance: self.source.grantee,
                binding: &self.source.binding,
                endpoint: &self.source.endpoint,
                peer,
                dns_generation: self.resolver.as_ref().map(|(_, generation)| *generation),
                egress: &self.source.egress,
                observer: self.source.observer.clone(),
            },
            end,
            || self.opener(index).map(|_| ()),
        )?;
        Ok((peer, grant))
    }
    fn check_grant(&self, grant: &CarrierGrant) -> Result<(), ChannelError> {
        if grant
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone())
            .is_some_and(|bound| !bound.background_custody_available())
        {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.peers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
    fn opener(&self, index: usize) -> Result<NativeCarrierOpener, ChannelError> {
        self.source
            .check(&self.source.binding, &self.source.endpoint, &self.context)?;
        if self
            .resolver
            .as_ref()
            .is_some_and(|(resolver, generation)| resolver.generation() != *generation)
        {
            return Err(error(
                self.source.grantee,
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let peer = self.peers.get(index).copied().ok_or_else(|| {
            error(
                self.source.grantee,
                Stage::Dial,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            )
        })?;
        let mut opener = NativeCarrierOpener::new(
            self.source.grantee,
            self.source.binding.clone(),
            peer,
            self.source.egress.clone(),
            self.source.observer.clone(),
        )?;
        opener.endpoint = self.source.endpoint.clone();
        opener.related_parent = self.source.related_parent.clone();
        opener.dns = self.resolver.clone();
        Ok(opener)
    }
    pub fn prepare_stream(
        &self,
        index: usize,
        grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        self.check_grant(&grant)?;
        self.opener(index)?.prepare_controlled_stream(
            &self.source.binding,
            self.source.endpoint.clone(),
            self.context.clone(),
            grant,
        )
    }
    pub fn prepare_datagram(
        &self,
        index: usize,
        grant: CarrierGrant,
    ) -> Result<CarrierDatagram, ChannelError> {
        self.check_grant(&grant)?;
        self.opener(index)?.prepare_controlled_datagram(
            &self.source.binding,
            self.source.endpoint.clone(),
            self.context.clone(),
            grant,
        )
    }
}
