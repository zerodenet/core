//! The same browser provider keyed by its accepted settings, in its Host Scope.
//! Its registry weak reference cannot acquire IO, ACK a task or prove closure.
mod active;
use super::*;
pub(in crate::transport) use active::Active;
use zero_core::{component::ComponentInstanceId, request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    scope::TaskScopeIdentity,
    ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::PacketMetadataArc;
use zero_transport::browser_dialer::ScopedBrowserDialer;

/// The original raw settings/entry, never a cold native listener or task owner.
pub struct ProviderOptions {
    access: Access,
    address: Destination,
    identity: Arc<[u8]>,
    tasks: std::num::NonZeroUsize,
}
pub struct ProviderFactory;
struct Owner {
    provider: ScopedBrowserDialer,
    active: PacketMetadataArc<Active>,
    access: Access,
}
impl ProviderOptions {
    pub(in crate::transport) fn new(access: Access) -> Result<Self, ChannelError> {
        let socket: std::net::SocketAddr = access
            .0
            .settings
            .listen
            .parse()
            .map_err(|source| error(Reason::InvalidConfig).with_source(source))?;
        let address = Destination::new(
            match socket.ip() {
                std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
            },
            socket.port(),
        );
        let _ = ScopedBrowserDialer::declaration(address.clone(), &options(&access.0.settings))?;
        let tasks = std::num::NonZeroUsize::new(
            (access.0.settings.idle_capacity as usize)
                .checked_add(9)
                .ok_or_else(|| error(Reason::Capacity))?,
        )
        .unwrap();
        // Stream directly into the existing digest; no unbounded debug String.
        struct Digest(blake3::Hasher);
        impl std::fmt::Write for Digest {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0.update(text.as_bytes());
                Ok(())
            }
        }
        use std::fmt::Write;
        let mut digest = Digest(blake3::Hasher::new());
        write!(
            &mut digest,
            "actual.browser.provider.v1:{:?}",
            access.0.settings
        )
        .unwrap();
        access.retain_recipe()?;
        Ok(Self {
            access,
            address,
            tasks,
            identity: digest.0.finalize().as_bytes().as_slice().into(),
        })
    }
    pub(crate) fn dialer(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<BrowserDialer, ChannelError> {
        self.active(instance)?.dialer()
    }
    /// Expected instance comes from this role's actual declared dependency.
    /// This only selects the original provider value, not a caller identity.
    fn active(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<PacketMetadataArc<Active>, ChannelError> {
        let weak = {
            self.access
                .0
                .owned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        };
        let active = weak
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| error(Reason::Revoked))?;
        if active.instance != instance {
            return Err(error(Reason::StalePlan));
        }
        Ok(active)
    }
}
impl Drop for ProviderOptions {
    fn drop(&mut self) {
        // Only accepted data is released here. The real Host Owner still owns
        // any listener, Source originals, users and Scope registrations.
        let previous = self.access.0.recipes.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous != 0,
            "one guard per actual accepted provider recipe"
        );
    }
}
fn options(settings: &zero_traits::BrowserDialerSettings) -> BrowserDialerOptions {
    BrowserDialerOptions {
        idle_capacity: settings.idle_capacity as usize,
        task_timeout: Duration::from_millis(settings.task_timeout_ms),
        max_task_bytes: settings.max_task_bytes as usize,
        max_payload_bytes: settings.max_payload_bytes as usize,
    }
}
impl ComponentFactory for ProviderFactory {
    type Options = ProviderOptions;
    fn describe(&self, options: &ProviderOptions) -> ComponentSpec {
        let declaration = ScopedBrowserDialer::declaration(
            options.address.clone(),
            &self::options(&options.access.0.settings),
        )
        .expect("actual browser settings validated before recipe");
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            resources: vec![declaration],
            static_identity: options.identity.clone(),
            task_limit: Some(options.tasks),
        }
    }
    fn build(
        &self,
        options: Arc<ProviderOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let provider = ScopedBrowserDialer::prepare(
            options.address.clone(),
            self::options(&options.access.0.settings),
            context.resources.clone(),
            &context.tasks,
            context.cancel.clone(),
        )?;
        let memory = context.resources.packet_driver_metadata()?;
        let header = memory
            .reserve(
                PacketMetadataArc::<Active>::layout()
                    .map_err(|source| error(Reason::Capacity).with_source(source))?,
            )
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        let active = PacketMetadataArc::from_prepaid(
            Active::new(
                context.instance(),
                context.tasks.scope_identity(),
                provider.dialer(),
                context.cancel.child_token(),
            ),
            header,
        )
        .map_err(|(source, _, _)| error(Reason::Capacity).with_source(source))?;
        // Store only this same provider's weak metadata. Replaced metadata and
        // possible last-header notifications drop after the registry lock.
        options.access.install_owned(&active)?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                provider,
                active,
                access: options.access.clone(),
            }),
            roles: ComponentRoles::default(),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        self.provider.start(deadline).await?;
        self.active.ready.store(true, Ordering::Release);
        tracing::info!(page_url = self.provider.page_url(), "Browser Dialer ready");
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.active.cancel.cancel();
        self.provider.revoke(reason);
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let report = self.provider.shutdown(deadline).await?;
        if report.is_complete() {
            self.release_registry();
        }
        Ok(report)
    }
}
impl Owner {
    fn release_registry(&self) {
        let own = PacketMetadataArc::downgrade(&self.active);
        let removed = {
            let mut current = self
                .access
                .0
                .owned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if current.as_ref().is_some_and(|current| {
                zero_traits::packet_metadata::PacketMetadataWeak::ptr_eq(current, &own)
            }) {
                self.access.0.hosted.store(false, Ordering::Release);
                current.take()
            } else {
                None
            }
        };
        // Metadata only. This does not return a Native reservation or ACK a
        // Scope task, and no foreign wake/drop executes under the registry.
        drop(removed);
        drop(own);
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}

impl Access {
    fn retain_recipe(&self) -> Result<(), ChannelError> {
        let registry = self.1.upgrade().ok_or_else(|| error(Reason::Revoked))?;
        let entries = registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.0.retired.load(Ordering::Acquire)
            || !entries
                .get(&self.0.settings)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.0))
        {
            return Err(error(Reason::Revoked));
        }
        self.0
            .recipes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| error(Reason::Capacity))?;
        Ok(())
    }
    fn install_owned(&self, active: &PacketMetadataArc<Active>) -> Result<(), ChannelError> {
        let registry = self.1.upgrade().ok_or_else(|| error(Reason::Revoked))?;
        // Serialize against the same library bind operation before publishing
        // the Host provider. No second native pool is created on contention or
        // when a genuine earlier library owner still retains its listener.
        let server = self
            .0
            .server
            .try_lock()
            .map_err(|_| error(Reason::Capacity))?;
        if server.is_some() {
            return Err(error(Reason::Capacity));
        }
        let retired = {
            let entries = registry
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !entries
                .get(&self.0.settings)
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.0))
            {
                return Err(error(Reason::Revoked));
            }
            if self.0.retired.load(Ordering::Acquire) || self.0.hosted.load(Ordering::Acquire) {
                return Err(error(Reason::Revoked));
            }
            let mut current = self
                .0
                .owned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let retired = current.replace(PacketMetadataArc::downgrade(active));
            self.0.hosted.store(true, Ordering::Release);
            retired
        };
        // Exact closed weak custody can refund/wake after its last header.
        // Neither registry lock encloses that original destructor.
        drop(server);
        drop(retired);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/transport/browser_dialer/recipe.rs"]
mod tests;
