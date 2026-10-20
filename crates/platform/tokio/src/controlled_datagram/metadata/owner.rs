use super::*;
impl MetadataOwner {
    /// Identity validation only: all views must refer to the same original in
    /// this persistent owner. Equal reservation numbers from another owner do
    /// not match. This does not assert retirement, native close or task exit.
    pub fn owns_grant(&self, grant: &GrantedMetadataDatagram) -> bool {
        self.views
            .get(&grant.control.reservation())
            .is_some_and(|shared| {
                Arc::ptr_eq(shared, &grant.control.shared)
                    && Arc::ptr_eq(shared, &grant.tx.shared)
                    && Arc::ptr_eq(shared, &grant.rx.shared)
            })
    }
    /// Cold construction only. Even capability probes have an owner now.
    pub fn new(limits: MetadataLimits) -> std_io::Result<Self> {
        if limits.concurrent_send_calls.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "UDP send admission exceeds semaphore capacity",
            ));
        }
        limits
            .write_pollers
            .get()
            .checked_add(limits.concurrent_send_calls.get())
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                std_io::Error::new(
                    std_io::ErrorKind::InvalidInput,
                    "UDP guarded operation capacity overflows",
                )
            })?;
        Ok(Self {
            main: ControlledDatagramOwner::new(limits.main_sockets),
            probes: probes::Probes::new(
                limits.probe_sockets,
                limits.probe_attempts_per_initialization,
            ),
            limits,
            views: BTreeMap::new(),
        })
    }
    pub fn bind(
        &mut self,
        source: SocketAddr,
    ) -> Result<GrantedMetadataDatagram, DatagramAcquisitionError> {
        self.reap();
        // A prior unresolved probe cannot be bypassed by creating a new main.
        self.probes
            .check_unknown()
            .map_err(|source| {
                self.main.acquisition_error(None, AcquisitionOutcome::KnownNoNative, source)
            })?;
        self.probes.begin();
        let socket = self.main.bind_raw_listener(source)?;
        let setup = {
            let native = socket.entry.lock();
            let NativeHandleState::Raw(original) = &*native else {
                unreachable!("installed raw main original")
            };
            quinn_udp::UdpSocketState::new_with_owned_probes(original.into(), &mut self.probes)
        };
        let state = match setup {
            Ok(state) => state,
            Err(error) => {
                let source = Arc::new(error);
                *socket
                    .entry
                    .acquisition_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(source.clone());
                let outcome = match socket.close() {
                    Ok(receipt) => AcquisitionOutcome::Closed(receipt),
                    Err(_) => AcquisitionOutcome::Unknown(socket.reservation()),
                };
                self.main.reap_confirmed();
                return Err(self.main.acquisition_error(
                    Some(socket.reservation()),
                    outcome,
                    source,
                ));
            }
        };
        let socket = self.main.register_native(socket)?;
        let pollers = poller::Broker::new(self.limits.write_pollers);
        // This can only fail if a freshly initialized checked namespace cannot
        // allocate its first ID; no user callback runs between these operations.
        let registration = pollers.receiver().expect("fresh checked poller identity");
        let shared = Arc::new(Shared {
            socket,
            state,
            pollers,
            sends: tokio::sync::Semaphore::new(self.limits.concurrent_send_calls.get()),
            send_limit: self.limits.concurrent_send_calls,
        });
        self.views
            .insert(shared.socket.reservation(), shared.clone());
        Ok(GrantedMetadataDatagram {
            rx: MetadataDatagramRx {
                shared: shared.clone(),
                registration,
            },
            tx: MetadataDatagramTx {
                shared: shared.clone(),
            },
            control: MetadataDatagramControl {
                shared,
                observation_transferred: false,
            },
        })
    }
    fn reap(&mut self) {
        self.main.reap_confirmed();
        self.views.retain(|id, _| self.main.retains(*id));
    }
    pub fn occupied_main(&mut self) -> usize {
        self.reap();
        self.main.occupied()
    }
    /// Native failures from the most recent initialization, including option
    /// errors whose real closed probe allowed the pinned capability=1 result.
    pub fn probe_diagnostics(&self) -> &[Arc<std_io::Error>] {
        &self.probes.diagnostics
    }
    pub fn occupied_probes(&mut self) -> usize {
        self.probes.owner.occupied()
    }
    /// Aggregate confirmed counts are committed before any new acquisition.
    /// Main and probe namespaces remain distinct in these actual owner reports.
    pub fn take_report(&mut self) -> MetadataOwnerReport {
        self.reap();
        MetadataOwnerReport {
            main: self.main.take_report(),
            probes: self.probes.owner.take_report(),
        }
    }
    /// The host first stops actual consumers. No task join is asserted here.
    pub fn close_all(&mut self) -> MetadataOwnerReport {
        for shared in self.views.values() {
            shared.socket.entry.retired.cancel();
            shared.pollers.signal();
        }
        let main = self.main.close_all();
        let probes = self.probes.owner.close_all();
        self.reap();
        MetadataOwnerReport { main, probes }
    }
}
