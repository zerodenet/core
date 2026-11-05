//! System queries use the same coordinator and actual host or library owner.
use super::*;
impl DnsSystem {
    fn system_resolver(&self, expected: u64) -> io::Result<TokioSystemResolver> {
        let inner = self.inner.read().expect("dns system lock poisoned");
        if self.config_generation.load(Ordering::Acquire) == expected {
            if let DnsSystemInner::System(backend) = &*inner {
                match backend.as_ref() {
                    ResolverBackend::System(resolver) => return Ok(resolver.clone()),
                    #[cfg(any(feature = "udp", feature = "doh", feature = "dot", feature = "doq"))]
                    _ => {}
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "DNS configuration changed before system lookup",
        ))
    }
    pub(super) async fn resolve_system_coordinated(
        &self,
        domain: &str,
        role: DnsQueryRole,
        config_generation: u64,
    ) -> io::Result<Vec<IpAddress>> {
        if self.owned_root.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned DNS UDP root does not authorize OS resolver work",
            ));
        }
        let sys_resolver = self.system_resolver(config_generation)?;
        let generation = self.egress_interface.generation();
        let mut scope = QueryScope::new(role, generation);
        scope.config_generation = config_generation;
        let key = QueryKey::new(domain, 0, scope);
        let domain = domain.to_owned();
        let node = matches!(role, DnsQueryRole::Node | DnsQueryRole::Bootstrap)
            .then_some(self.node_queries.as_ref())
            .flatten();
        let native = node
            .map(|node| node.source.clone())
            .or_else(|| self.native_source.clone())
            .or_else(|| self.query_coordinator.native_source().cloned());
        let coordinator = node
            .map(|node| &node.coordinator)
            .unwrap_or(&self.query_coordinator);
        // RFC 6761 answers require neither an OS lookup nor a native task.
        // Keep the same local lifecycle and configuration-generation fences.
        if message::normalize_domain(&domain).is_ok_and(|name| localhost::is_localhost(&name)) {
            coordinator.ensure_local_open()?;
            return Ok(localhost::addresses(Default::default()));
        }
        let work = coordinator.resolve(key, async move {
            match native {
                Some(source) => {
                    source
                        .lookup_system(
                            domain,
                            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        )
                        .await?
                        .result
                }
                None => sys_resolver.resolve(&domain).await,
            }
        });
        match node {
            Some(node) => node.source.tasks()?.bootstrap(work).await?,
            None => work.await,
        }
    }

    pub(super) async fn resolve_system_type_coordinated(
        &self,
        domain: &str,
        query_type: u16,
        role: DnsQueryRole,
        config_generation: u64,
    ) -> io::Result<Vec<IpAddress>> {
        self.resolve_system_type_with_family_coordinated(
            domain,
            query_type,
            role,
            AddressFamily::Auto,
            config_generation,
        )
        .await
    }

    pub(super) async fn resolve_system_type_with_family_coordinated(
        &self,
        domain: &str,
        query_type: u16,
        role: DnsQueryRole,
        family: AddressFamily,
        config_generation: u64,
    ) -> io::Result<Vec<IpAddress>> {
        if self.owned_root.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned DNS UDP root does not authorize OS resolver work",
            ));
        }
        let resolver = self.system_resolver(config_generation)?;
        let generation = self.egress_interface.generation();
        let mut scope = QueryScope::new(role, generation);
        scope.config_generation = config_generation;
        scope.family = family;
        let key = QueryKey::new(domain, query_type, scope);
        let domain = domain.to_owned();
        let node = matches!(role, DnsQueryRole::Node | DnsQueryRole::Bootstrap)
            .then_some(self.node_queries.as_ref())
            .flatten();
        let native = node
            .map(|node| node.source.clone())
            .or_else(|| self.native_source.clone())
            .or_else(|| self.query_coordinator.native_source().cloned());
        let coordinator = node
            .map(|node| &node.coordinator)
            .unwrap_or(&self.query_coordinator);
        if message::normalize_domain(&domain).is_ok_and(|name| localhost::is_localhost(&name)) {
            coordinator.ensure_local_open()?;
            let addresses = crate::family::filter_addresses(
                &domain,
                localhost::addresses(Default::default()),
                family,
            )?;
            return Ok(addresses
                .into_iter()
                .filter(|address| {
                    matches!(
                        (query_type, address),
                        (message::TYPE_A, IpAddress::V4(_))
                            | (message::TYPE_AAAA, IpAddress::V6(_))
                    )
                })
                .collect());
        }
        let work = coordinator.resolve(key, async move {
            match native {
                Some(source) => {
                    let lookup = source
                        .lookup_system(
                            domain.clone(),
                            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        )
                        .await?;
                    let addresses =
                        crate::family::filter_addresses(&domain, lookup.result?, family)?;
                    Ok(addresses
                        .into_iter()
                        .filter(|address| {
                            matches!(
                                (query_type, address),
                                (message::TYPE_A, IpAddress::V4(_))
                                    | (message::TYPE_AAAA, IpAddress::V6(_))
                            )
                        })
                        .collect())
                }
                None => resolver.resolve_type(&domain, query_type, family).await,
            }
        });
        match node {
            Some(node) => node.source.tasks()?.bootstrap(work).await?,
            None => work.await,
        }
    }
}

impl DnsSystem {
    pub(super) async fn resolve_native_system(
        &self,
        request: zero_platform_tokio::contract::dns::DnsRequest,
        context: zero_platform_tokio::contract::dns::DnsQueryContext,
        source: Arc<native::NativeDnsSource>,
        coordinator: QueryCoordinator<Vec<IpAddress>>,
    ) -> Result<
        zero_platform_tokio::contract::dns::DnsResolution,
        zero_platform_tokio::contract::ChannelError,
    > {
        use zero_platform_tokio::contract::{dns::DnsService, ChannelError, Impact, Reason, Stage};
        let error = |source: io::Error| {
            ChannelError::new(
                Stage::Dial,
                match source.kind() {
                    io::ErrorKind::TimedOut => Reason::Timeout,
                    io::ErrorKind::Interrupted => Reason::Cancelled,
                    io::ErrorKind::Unsupported => Reason::Unsupported,
                    _ => Reason::Io,
                },
                Impact::Session,
            )
            .with_source(source)
        };
        let generation = self.generation();
        if context
            .expected_generation
            .is_some_and(|expected| expected != generation)
        {
            return Err(ChannelError::new(
                Stage::Dial,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        let mut network = self.egress_interface.subscribe_generation();
        if *network.borrow() != generation.network.0 {
            return Err(ChannelError::new(
                Stage::Dial,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        let domain = message::normalize_domain(&request.name).map_err(error)?;
        let mut scope = QueryScope::new(request.role, generation.network.0);
        scope.config_generation = generation.resolver;
        scope.family = request.family;
        let key = QueryKey::new(&domain, 0, scope);
        let end = context
            .deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
        let result = coordinator.resolve(key, async move {
            let lookup = source.lookup_system(domain.clone(), end).await?;
            crate::family::filter_addresses(&domain, lookup.result?, request.family)
        });
        let addresses = tokio::select! {biased;
            _=context.cancel.cancelled()=>return Err(error(io::Error::new(io::ErrorKind::Interrupted,"system DNS caller cancelled"))),
            _=network.changed()=>return Err(ChannelError::new(Stage::Dial, Reason::StalePlan, Impact::Session)),
            result=tokio::time::timeout_at(end,result)=>result.map_err(|source|error(io::Error::new(io::ErrorKind::TimedOut,source)))?.map_err(error)?,
        };
        if context.cancel.is_cancelled() || self.generation() != generation {
            return Err(ChannelError::new(
                Stage::Dial,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        Ok(zero_platform_tokio::contract::dns::DnsResolution {
            addresses,
            generation,
        })
    }
}
