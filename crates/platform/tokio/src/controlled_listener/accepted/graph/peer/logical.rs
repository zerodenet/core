//! Logical Stream/association graphs reuse their actual registered primary.
use super::*;
impl AcceptedPeerScope {
    /// A host-owned logical association uses the same retained resource ledger
    /// and exact Scope witness, without fabricating a native TCP/UDP admission.
    pub fn for_association(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        local: std::net::SocketAddr,
    ) -> Result<Self, ChannelError> {
        Self::for_logical_association(
            registrar,
            task,
            witness,
            cancel,
            max_tasks,
            max_resources,
            Some(local),
        )
    }
    /// A real logical channel need not have a native socket-local address.
    /// Actual task registration still authorizes the same single resource graph;
    /// absence never manufactures an endpoint or a TCP/UDP admission.
    pub fn for_logical_association(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        local: Option<std::net::SocketAddr>,
    ) -> Result<Self, ChannelError> {
        if !witness
            .scope_identity()
            .same_scope(&registrar.scope_identity())
            || !registrar.registration_open()
            || witness.task_id() != Some(task)
            || witness.is_joined()
            || cancel.is_cancelled()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "association differs from actual host registration",
            ));
        }
        let authority = Authority {
            scope: registrar.scope_identity(),
            registrar: registrar.clone(),
            max_tasks,
            max_resources,
        };
        let mut users = Users::new(authority, Weak::new(), Weak::new(), cancel, None, None);
        users.peer = Some(Arc::new(Ledger {
            origin: AcceptedOrigin::Association,
            reports: Mutex::default(),
            custody: AtomicUsize::new(0),
            failure: Mutex::new(None),
            user_failure: Mutex::new(None),
        }));
        if let Some(local) = local {
            let _ = users.local.set(local);
        }
        users.state().primary = true;
        users.state().tasks.insert(
            task,
            Node {
                witness: witness.clone(),
                primary: true,
                process: None,
                process_helper: false,
                received_failure: None,
                fault: None,
            },
        );
        let origin = Arc::new(OnceLock::new());
        let _ = origin.set((task, witness));
        Ok(Self {
            users: Arc::new(users),
            task: origin,
            admission: None,
            control: None,
        })
    }
    pub fn retain_carrier_origin(
        &self,
        original: crate::controlled_carrier::CarrierOrigin,
    ) -> Result<(), ChannelError> {
        let same_source = original.listener().is_some_and(|actual| {
            actual
                .owner()
                .scope()
                .same_scope(&self.users.authority.scope)
        }) || original.accepted().is_some_and(|actual| {
            actual
                .scope_identity()
                .same_scope(&self.users.authority.scope)
        });
        if original.is_retired() || !same_source {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "carrier origin differs from its actual Source Scope",
            ));
        }
        self.users.carrier_origin.set(original).map_err(|_| {
            failure(
                Stage::Prepare,
                Reason::StalePlan,
                "actual Source carrier already installed",
            )
        })
    }
    pub fn for_carrier(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        local: std::net::SocketAddr,
    ) -> Result<Self, ChannelError> {
        let mut graph = Self::for_association(
            registrar,
            task,
            witness,
            cancel,
            max_tasks,
            max_resources,
            local,
        )?;
        let ledger = Arc::get_mut(&mut graph.users)
            .expect("unpublished carrier graph")
            .peer
            .as_mut()
            .expect("actual carrier ledger");
        Arc::get_mut(ledger)
            .expect("unpublished carrier ledger")
            .origin = AcceptedOrigin::Carrier;
        Ok(graph)
    }
    /// Source-owned Stream borrowing over a genuine sealed original, including
    /// Packet/device roots that have no socket-local endpoint observation.
    pub fn for_stream_carrier(
        registrar: &WeakTaskRegistrar,
        task: TaskId,
        witness: TaskJoinWitness,
        cancel: CancellationToken,
        max_tasks: NonZeroUsize,
        max_resources: usize,
        original: &crate::controlled_carrier::CarrierOrigin,
    ) -> Result<Self, ChannelError> {
        let mut graph = Self::for_logical_association(
            registrar,
            task,
            witness,
            cancel,
            max_tasks,
            max_resources,
            original.endpoints().and_then(|facts| facts.local()),
        )?;
        let _ = graph.users.carrier_origin.set(original.clone());
        let ledger = Arc::get_mut(&mut graph.users)
            .expect("unpublished Stream carrier graph")
            .peer
            .as_mut()
            .expect("actual logical ledger");
        Arc::get_mut(ledger)
            .expect("unpublished carrier ledger")
            .origin = AcceptedOrigin::Carrier;
        Ok(graph)
    }
}
