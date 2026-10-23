//! Start borrows lifecycle control; the installed host join owner remains independent.
use super::*;

impl ComponentSlot {
    pub(crate) async fn start(&self, deadline: Instant) -> Result<(), ChannelError> {
        if let Some(failure) = self.task_fault() {
            return Err(failure.error());
        }
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "component was stopped"));
        }
        if Instant::now() >= deadline {
            return Err(self.failure(Reason::Timeout, "component start deadline elapsed"));
        }
        self.check_host_installation()?;
        if let Some(result) = self.live.get() {
            match result {
                Ok(live) if live.started.get().is_none() => {}
                _ => {
                    if matches!(result, Ok(live) if matches!(live.started.get(), Some(Ok(())))) {
                        self.scan_tasks(deadline).await?;
                    }
                    return started(self.id, result);
                }
            }
        }
        // An exhausted waiter cannot fault the initializer or acquire its I/O.
        let _starting = tokio::time::timeout_at(deadline, self.start_gate.lock())
            .await
            .map_err(|_| self.failure(Reason::Timeout, "component start deadline elapsed"))?;
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "component was stopped"));
        }
        if Instant::now() >= deadline {
            return Err(self.failure(Reason::Timeout, "component start deadline elapsed"));
        }
        self.check_host_installation()?;
        let live = self
            .live
            .get_or_init(|| async {
                // No await or resource acquisition inside this initializer. The
                // persistent slot owns Live before any start future can suspend.
                let runtime = tokio::runtime::Handle::current();
                let tasks = TaskScope::suspended_hosted(runtime, self.recipe.spec().task_limit);
                let resources = HostResources::new_for_instance(
                    self.instance_id(),
                    &self.recipe.spec().resources,
                    self.cancel.clone(),
                    tasks.registrar().scope_identity(),
                )
                .map_err(Arc::new)?;
                let context = BuildContext {
                    component: self.id,
                    generation: self.generation,
                    dependencies: self.dependencies.clone(),
                    roles: ComponentRoleIds {
                        outbound: self
                            .recipe
                            .spec()
                            .roles
                            .iter()
                            .any(|role| {
                                matches!(role,
                                zero_platform_tokio::contract::component::RoleKind::Stream
                                | zero_platform_tokio::contract::component::RoleKind::Datagram)
                            })
                            .then_some(OutboundId(self.id.0)),
                        ..Default::default()
                    },
                    resources: resources.clone(),
                    tasks: tasks.registrar(),
                    cancel: self.cancel.child_token(),
                    background_carrier: None,
                };
                let instance = self.recipe.build(context).map_err(Arc::new)?;
                Ok(Live {
                    resources,
                    stream: instance
                        .roles
                        .stream
                        .clone()
                        .map(|inner| match self.admission.get() {
                            Some(host) => Arc::new(crate::component::admission::FencedStream {
                                inner,
                                host: host.clone(),
                                instance: self.instance_id(),
                                owner: self.admission_owner.clone(),
                            })
                                as Arc<dyn DynStreamOutbound>,
                            None => inner,
                        }),
                    datagram: instance.roles.datagram.clone().map(|inner| {
                        match self.admission.get() {
                            Some(host) => Arc::new(crate::component::admission::FencedDatagram {
                                inner,
                                host: host.clone(),
                                instance: self.instance_id(),
                                owner: self.admission_owner.clone(),
                            })
                                as Arc<dyn DynDatagramOutbound>,
                            None => inner,
                        }
                    }),
                    started: OnceLock::new(),
                    owner: Mutex::new(Owner {
                        instance,
                        pending_close: None,
                        protocol_closed: false,
                    }),
                    tasks: TaskMonitor::new(tasks),
                })
            })
            .await;
        if let Ok(live) = live {
            self.wake_supervision();
            if live.started.get().is_none() {
                let mut owner = live.owner.lock().await;
                let mut cancellation = StartCancellation {
                    slot: self,
                    armed: true,
                };
                let started = match live.resources.activate(deadline) {
                    Ok(_access) => {
                        // The installed join owner is independent of the borrowed
                        // lifecycle. Never hold it across protocol start await.
                        let registration = live.tasks.enable_registration();
                        match registration {
                            Ok(()) => tokio::select! {
                            biased;
                            failure = live.tasks.failed() => Err(failure.error()),
                            _ = self.cancel.cancelled() => Err(self.failure(Reason::Revoked, "component was stopped")),
                            result = tokio::time::timeout_at(deadline, owner.instance.lifecycle.start(deadline)) => result.unwrap_or_else(|_| Err(self.failure(Reason::Timeout, "component start deadline elapsed"))),
                                },
                            Err(source) => Err(self
                                .failure(Reason::Revoked, "component task registration is closed")
                                .with_source(source)),
                        }
                    }
                    Err(error) => Err(error),
                };
                let started = match started {
                    Ok(()) if self.recipe.spec().task_limit.is_some() => {
                        live.tasks.scan(deadline, || self.wake_supervision()).await
                    }
                    other => other,
                };
                let started = match live.tasks.fault() {
                    Some(failure) => Err(failure.error()),
                    None => started,
                };
                if started.is_err() {
                    owner.instance.lifecycle.revoke(CloseReason::Shutdown);
                }
                live.started
                    .set(started.map_err(Arc::new))
                    .expect("component start is serialized");
                cancellation.armed = false;
            }
        }
        if let Some(failure) = self.task_fault() {
            Err(failure.error())
        } else {
            started(self.id, live)
        }
    }
    fn check_host_installation(&self) -> Result<(), ChannelError> {
        // Existing zero-resource role access may build a cold handle, whose
        // actual invocation is still fenced. A task/native owner may start
        // only after the host installs that exact generation/token.
        if self.recipe.spec().task_limit.is_some() || !self.recipe.spec().resources.is_empty() {
            if let Some(host) = self.admission.get() {
                host.check_installed(self.instance_id(), &self.admission_owner)?;
            }
        }
        Ok(())
    }
}
