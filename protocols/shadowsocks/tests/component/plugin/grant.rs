use super::*;
#[derive(Default)]
pub(super) struct Grant {
    pub(super) starts: AtomicUsize,
    #[cfg(any(target_os = "linux", windows))]
    retirement: std::sync::OnceLock<Arc<retirement::Retirement>>,
    #[cfg(any(target_os = "linux", windows))]
    pub(super) generation_offset: AtomicUsize,
    #[cfg(any(target_os = "linux", windows))]
    pub(super) actor:
        std::sync::Mutex<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>>,
    #[cfg(any(target_os = "linux", windows))]
    pub(super) originals:
        std::sync::Mutex<Vec<Arc<zero_platform_tokio::owned_process::OwnedProcess>>>,
    #[cfg(any(target_os = "linux", windows))]
    pub(super) endpoint:
        std::sync::Mutex<Option<Arc<zero_platform_tokio::owned_process::ProcessEndpointHandoff>>>,
}
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
#[cfg(any(target_os = "linux", windows))]
mod retirement;
#[cfg(any(target_os = "linux", windows))]
impl Grant {
    pub(super) fn for_scope(scope: &TaskScope, generation: u64) -> Self {
        let grant = Self::default();
        grant
            .generation_offset
            .store((generation - 1) as usize, Ordering::SeqCst);
        assert!(grant
            .retirement
            .set(Arc::new(retirement::Retirement::new(scope, generation)))
            .is_ok());
        grant
    }
    pub(super) fn confirm_closed(&self, scope: &TaskScope) {
        let observer = self.retirement.get().unwrap();
        assert!(observer.scope.matches(&scope.registrar()));
        assert!(
            !observer.scope.resource_open(),
            "actual provider admission must be closed"
        );
        assert!(self
            .originals
            .lock()
            .unwrap()
            .iter()
            .all(|original| original
                .closed()
                .is_some_and(|closed| closed.matches(original))));
        assert!(self
            .endpoint
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|endpoint| endpoint
                .closed()
                .is_some_and(|closed| closed.matches(endpoint))));
        observer.publish();
    }
}
impl ResourceAccess for Grant {
    #[cfg(any(target_os = "linux", windows))]
    fn process_endpoint_retirement(
        &self,
        ordinal: usize,
    ) -> Option<Arc<dyn zero_platform_tokio::owned_process::ProcessEndpointRetirement>> {
        if ordinal != 1 {
            return None;
        }
        Some(self.retirement.get()?.clone())
    }
    #[cfg(any(target_os = "linux", windows))]
    fn prepare_owned_process(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, BuildError> {
        if self.retirement.get().is_some_and(|observer| {
            !observer.scope.matches(tasks) || !observer.scope.resource_open()
        }) {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::Revoked,
                "actual Process declaration admission is closed",
            ));
        }
        if request.program != std::path::Path::new("python3") {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "undeclared test executable",
            ));
        }
        self.starts
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                failure(
                    ComponentId(770),
                    Stage::Prepare,
                    Reason::Capacity,
                    "single actual Process original allowance exhausted",
                )
            })?;
        let original = zero_platform_tokio::owned_process::OwnedProcess::prepare(request, tasks)
            .map_err(|source| {
                failure(
                    ComponentId(770),
                    Stage::Prepare,
                    Reason::Io,
                    "cold original failed",
                )
                .with_source(source)
            })?;
        self.originals.lock().unwrap().push(original.clone());
        Ok(original)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn reserve_process_endpoint(
        &self,
        ordinal: usize,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
        actor: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<Arc<zero_platform_tokio::owned_process::ProcessEndpointHandoff>, BuildError> {
        if ordinal != 1 || self.endpoint.lock().unwrap().is_some() {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "foreign or duplicate endpoint ordinal",
            ));
        }
        let endpoint = zero_platform_tokio::owned_process::ProcessEndpointHandoff::new(
            zero_core::component::ComponentInstanceId {
                component: ComponentId(770),
                generation: zero_core::component::ResourceGeneration(
                    1 + self.generation_offset.load(Ordering::SeqCst) as u64,
                ),
            },
            0,
            zero_platform_tokio::owned_process::ProcessEndpointSpec {
                stream: true,
                datagram: false,
                ipv6: false,
                network_generation: zero_core::component::NetworkGeneration(0),
            },
            tasks,
            CancellationToken::new(),
        )?;
        *self.endpoint.lock().unwrap() = Some(endpoint.clone());
        let original = self.prepare_owned_process(
            zero_platform_tokio::contract::component::ProcessRequest::new("python3"),
            tasks,
        )?;
        *self.actor.lock().unwrap() = Some(actor.clone());
        original.bind_actor(actor.clone()).map_err(|source| {
            failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "actual actor bind failed",
            )
            .with_source(source)
        })?;
        original.attach_endpoint(&endpoint).map_err(|source| {
            failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "actual endpoint bind failed",
            )
            .with_source(source)
        })?;
        endpoint.acquire(tasks, actor)?;
        Ok(endpoint)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn prepare_owned_process_with_endpoint(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
        endpoint: &Arc<zero_platform_tokio::owned_process::ProcessEndpointHandoff>,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, BuildError> {
        if !self
            .endpoint
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, endpoint))
        {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::StalePlan,
                "foreign actual handoff",
            ));
        }
        let original = self.originals.lock().unwrap()[0].clone();
        if !self
            .actor
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|actor| !actor.is_joined() && actor.scope_identity().matches(tasks))
        {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::StalePlan,
                "foreign actual scope",
            ));
        }
        original
            .install_endpoint_request(request, endpoint)
            .map_err(|source| {
                failure(
                    ComponentId(770),
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    "request differs from cold job",
                )
                .with_source(source)
            })?;
        Ok(original)
    }
    fn spawn_process(
        &self,
        request: zero_platform_tokio::contract::component::ProcessRequest,
    ) -> Result<tokio::process::Child, BuildError> {
        if request.program != std::path::Path::new("python3") {
            return Err(failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::InvalidConfig,
                "undeclared test executable",
            ));
        }
        self.starts
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                failure(
                    ComponentId(770),
                    Stage::Prepare,
                    Reason::Capacity,
                    "single actual Child allowance exhausted",
                )
            })?;
        request.command().spawn().map_err(|source| {
            failure(
                ComponentId(770),
                Stage::Prepare,
                Reason::Io,
                "actual child launch failed",
            )
            .with_source(source)
        })
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
