//! Original Scope registration, native loans and leases are separate gates.
use super::*;
use crate::OwnedDnsQueryTasks;
impl DriverOwner {
    pub(in super::super) fn native_at_capacity(&self) -> bool {
        self.reap();
        let state = self.state.lock().expect("DoH driver owner poisoned");
        state.native.len() >= self.limit
    }
    pub(in super::super) fn original_changes(&self) -> &tokio::sync::Notify {
        self.changed.as_ref()
    }
    pub(crate) fn preserves_host_scope(
        &self,
        preserves: &dyn Fn(
            zero_core::component::ComponentInstanceId,
            &zero_platform_tokio::contract::scope::TaskScopeIdentity,
        ) -> bool,
    ) -> bool {
        let host_bound = {
            let state = self.state.lock().expect("DoH driver owner poisoned");
            match state.mode {
                PoolMode::Unused => return true,
                PoolMode::Standalone => return false,
                PoolMode::Host(_) => true,
            }
        };
        let original = self
            .provider
            .lock()
            .expect("DoH original provider poisoned")
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        host_bound
            && original
                .and_then(|provider| provider.host_scope())
                .is_some_and(|(instance, scope)| preserves(instance, &scope))
    }

    pub(in super::super) fn select_host(
        &self,
        provider: Arc<dyn crate::native::NativeDnsProvider>,
    ) -> io::Result<Arc<dyn crate::native::NativeDnsProvider>> {
        let tasks = provider.tasks();
        tasks.ensure_open()?;
        self.ensure_open()?;
        let canonical = {
            let mut original = self
                .provider
                .lock()
                .expect("DoH original provider poisoned");
            let mut state = self.state.lock().expect("DoH driver owner poisoned");
            match &state.mode {
                PoolMode::Unused => {
                    *original = Some(Arc::downgrade(&provider));
                    state.mode = PoolMode::Host(tasks);
                    return Ok(provider);
                }
                PoolMode::Host(current) if current.same_owner(&tasks) => return Ok(provider),
                PoolMode::Host(_) => original
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotConnected,
                            "DoH canonical provider retired",
                        )
                    })?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "DoH pool has another original owner",
                    ));
                }
            }
        };
        // Source/Scope observations can reap or release their own owners;
        // never perform them under this pool's state/connection locks.
        canonical.tasks().ensure_open()?;
        if !shared_sources(canonical.as_ref(), provider.as_ref()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DoH pool has another actual host/source owner",
            ));
        }
        self.ensure_open()?;
        // Cached clients retain their canonical provider, original LoanOwner
        // and exact driver. Pending setup is serialized by the original
        // connect_lock. No borrower signs a new Scope onto that driver.
        Ok(canonical)
    }
    pub(in super::super) fn spawn_native(
        &self,
        tasks: &OwnedDnsQueryTasks,
        client: Arc<ClientState>,
        network: u64,
        job: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        let _callbacks = tasks.defer_callbacks();
        self.reap();
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        if state.closing
            || self.cancel.is_cancelled()
            || !matches!(&state.mode, PoolMode::Host(current) if current.same_owner(tasks))
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH original host admission retired",
            ));
        }
        if state.native.len() >= self.limit {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "DoH native endpoint driver capacity exhausted",
            ));
        }
        state.native.push(client.clone());
        #[cfg(feature = "test-support")]
        let job = {
            let panic = client.panic_original_driver.clone();
            async move {
                tokio::select! { biased;
                    _ = panic.cancelled() => panic!("actual original DoH driver failure"),
                    _ = job => {},
                }
            }
        };
        match tasks.spawn_driver(job, client.cancel.clone(), network) {
            Ok(witness) => {
                client
                    .native
                    .get()
                    .expect("retained original before spawn")
                    .task
                    .set(witness)
                    .expect("original task bound once");
                drop(state);
                self.changed.notify_waiters();
                Ok(())
            }
            Err(source) => {
                state.native.retain(|entry| !Arc::ptr_eq(entry, &client));
                Err(source)
            }
        }
    }
    pub(super) async fn drain_native(&self, end: Instant) -> io::Result<()> {
        self.reap();
        let entries = {
            let state = self.state.lock().expect("DoH driver owner poisoned");
            state
                .native
                .iter()
                .filter(|client| client.cancel.is_cancelled())
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut failure = None;
        for client in entries {
            let idle = async {
                loop {
                    let changed = self.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if client.leases.load(std::sync::atomic::Ordering::Acquire) == 0 {
                        break;
                    }
                    changed.await;
                }
            };
            let settled = async {
                tokio::time::timeout_at(end, idle)
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                client
                    .native
                    .get()
                    .expect("native original retained")
                    .settle(end)
                    .await
            }
            .await;
            if let Err(source) = settled {
                failure.get_or_insert(source);
            }
        }
        self.reap();
        failure.map_or(Ok(()), Err)
    }
}

fn shared_sources(
    first: &dyn crate::native::NativeDnsProvider,
    second: &dyn crate::native::NativeDnsProvider,
) -> bool {
    let Some((first_node, first_query)) = first.source_roots() else {
        return false;
    };
    let Some((second_node, second_query)) = second.source_roots() else {
        return false;
    };
    let first_tasks = first.tasks();
    let second_tasks = second.tasks();
    Arc::ptr_eq(&first_node, &second_node)
        && Arc::ptr_eq(&first_query, &second_query)
        && (first_node.owns_tasks(&first_tasks) || first_query.owns_tasks(&first_tasks))
        && (first_node.owns_tasks(&second_tasks) || first_query.owns_tasks(&second_tasks))
        && first
            .budget()
            .zip(second.budget())
            .is_some_and(|(first, second)| Arc::ptr_eq(&first, &second))
}

#[cfg(feature = "test-support")]
impl DriverOwner {
    pub(crate) fn panic_original_driver(
        &self,
    ) -> io::Result<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        let actual = {
            let state = self.state.lock().expect("DoH driver owner poisoned");
            state.native.iter().find_map(|client| {
                let task = client.native.get()?.task.get()?;
                (!task.is_joined() && !client.cancel.is_cancelled())
                    .then(|| (client.clone(), task.clone()))
            })
        }
        .ok_or_else(|| io::Error::other("no live original DoH driver"))?;
        actual.0.panic_original_driver.cancel();
        Ok(actual.1)
    }
}
