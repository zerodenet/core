//! Same original grant and unique queues, before starting any worker.
use super::*;
impl PacketSegment {
    /// No worker starts here. The pool stores this owner before calling start.
    pub fn prepare(
        grant: GrantedMetadataListener,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        peer: SocketAddr,
        logical: SocketAddr,
    ) -> Result<Self, Box<RejectedPacketSegment>> {
        Self::prepare_inner(grant, queues, tasks, peer, logical, None)
    }
    #[cfg(feature = "split_http")]
    pub fn prepare_with_endpoint(
        grant: GrantedMetadataListener,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        peer: SocketAddr,
        logical: SocketAddr,
        limits: super::OwnedEndpointLimits,
    ) -> Result<Self, Box<RejectedPacketSegment>> {
        Self::prepare_inner(grant, queues, tasks, peer, logical, Some(limits))
    }
    fn prepare_inner(
        mut grant: GrantedMetadataListener,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        peer: SocketAddr,
        logical: SocketAddr,
        endpoint_limits: Option<super::OwnedEndpointLimits>,
    ) -> Result<Self, Box<RejectedPacketSegment>> {
        let fail = |error, grant, queues| {
            Box::new(RejectedPacketSegment {
                error,
                grant,
                queues,
            })
        };
        let capabilities = grant.tx.capabilities();
        if !grant.scope.matches(&tasks) || !grant.scope.registration_open() {
            return Err(fail(io::ErrorKind::PermissionDenied.into(), grant, queues));
        }
        let Some(claim) = grant.users_claim.take() else {
            return Err(fail(
                io::Error::other("original packet claim missing"),
                grant,
                queues,
            ));
        };
        if !claim.matches(&grant.rx, &grant.tx) {
            grant.users_claim = Some(claim);
            return Err(fail(
                io::Error::other("foreign packet views"),
                grant,
                queues,
            ));
        }
        if let Some(memory) = queues
            .0
            .as_ref()
            .and_then(|channels| channels.memory.as_ref())
        {
            // Real native GSO capability is known now. Reserve its complete
            // legal batch peak before any endpoint/packet driver can start.
            if let Err(source) = memory.shared.grow_native(capabilities.transmit_segments) {
                grant.users_claim = Some(claim);
                return Err(fail(source, grant, queues));
            }
        }
        let source = match super::HostedMetadataSocket::new(grant, &tasks) {
            Ok(source) => source,
            Err(rejected) => {
                let mut rejected = *rejected;
                rejected.grant.users_claim = Some(claim);
                return Err(fail(rejected.error, rejected.grant, queues));
            }
        };
        let prepared = match endpoint_limits {
            Some(limits) => claim
                .bind_packet_endpoint(tasks.clone(), limits.drivers, limits.users)
                .map(|((graph, receiver), (endpoint, endpoint_receiver))| {
                    (graph, receiver, Some((limits, endpoint, endpoint_receiver)))
                }),
            None => claim
                .bind_users(
                    tasks.clone(),
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(1).unwrap(),
                )
                .map(|(graph, receiver)| (graph, receiver, None)),
        };
        let (graph, receiver, _endpoint_users) = match prepared {
            Ok(value) => value,
            Err(claim) => {
                return Err(fail(
                    io::ErrorKind::PermissionDenied.into(),
                    source.into_unstarted(claim),
                    queues,
                ))
            }
        };
        if let Some(native) = queues
            .0
            .as_ref()
            .and_then(|channels| channels.native.as_ref())
        {
            let mut current = native.lock().unwrap();
            current.current = Some(capabilities);
            current.maximum_transmit_segments = current
                .maximum_transmit_segments
                .max(capabilities.transmit_segments);
        }
        Ok(Self {
            identity: queues.1.clone(),
            graph,
            receiver,
            #[cfg(feature = "split_http")]
            endpoint_users: _endpoint_users,
            source: Some(source),
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            received: None,
            // Scope cancellation fences business and registration, but this
            // same ancillary original must remain available for Quinn's final
            // accepted transmissions until the endpoint drivers have joined.
            // This token controls cleanup only; it grants no native authority.
            stop: CancellationToken::new(),
            final_retirement: Arc::new(AtomicBool::new(false)),
            peer,
            logical,
            tasks,
        })
    }
}
