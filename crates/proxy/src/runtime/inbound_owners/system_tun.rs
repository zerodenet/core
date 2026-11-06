//! System TUN uses the actual inbound registry and its canonical host allocator.
//! It is not a synthetic configured listener or a second task/native owner.
use super::*;
use crate::component::{instance::HostResources, ComponentRecipe};
use zero_core::component::{ComponentRoleIds, InboundId};
use zero_platform_tokio::contract::{
    component::{BuildContext, ResourceAccess, ResourceDeclaration, RoleKind},
    inbound::DynInboundSource,
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle},
    ChannelError, CloseReason,
};

// Configuration tags cannot name this private reservation namespace. Routing
// and emitted facts continue using the caller's real TUN tag.
pub(crate) const SYSTEM_TUN_OWNER: &str = "\0system-tun";
pub(crate) struct SystemTunLease {
    owner: Arc<InboundOwner>,
    installed: InboundInstallContext,
    recipe: ComponentRecipe,
    armed: bool,
    observers: Arc<super::observers::ObserverParking>,
}
impl InboundOwnerRegistry {
    /// Called under the existing TUN operation/configuration transaction lock.
    /// Installation is durable before any device factory can acquire an IO.
    pub(crate) fn reserve_system_tun(
        &self,
        protocols: &ProtocolInventory,
        recipe: ComponentRecipe,
        shutdown: watch::Sender<bool>,
    ) -> Result<SystemTunLease, EngineError> {
        let spec = recipe.spec();
        if spec.roles != [RoleKind::Inbound]
            || !matches!(
                spec.resources.as_slice(),
                [
                    ResourceDeclaration::ControlledPacketDevice { .. },
                    ResourceDeclaration::NativeIcmp { .. }
                ]
            )
        {
            return Err(std::io::Error::other("invalid concrete System TUN source recipe").into());
        }
        let limit = spec
            .task_limit
            .map_or(0, NonZeroUsize::get)
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| std::io::Error::other("System TUN task ceiling overflow"))?;
        let mut active = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.closing
            || active.contains_key(SYSTEM_TUN_OWNER)
            || reserved.entries.contains_key(SYSTEM_TUN_OWNER)
        {
            return Err(std::io::Error::other(
                "previous System TUN owner still requires confirmed retirement",
            )
            .into());
        }
        let mut owner = InboundOwner::new(protocols.allocate_resource_identity()?, limit, shutdown);
        owner.shared_scope_children = true;
        let owner = Arc::new(owner);
        owner.begin_listener_installation();
        active.insert(SYSTEM_TUN_OWNER.into(), owner.clone());
        drop(reserved);
        drop(active);
        self.mark_listener_generation();
        Ok(SystemTunLease {
            installed: InboundInstallContext {
                entry: owner.clone(),
                protocols: protocols.clone(),
            },
            owner,
            recipe,
            armed: true,
            observers: self.observers.clone(),
        })
    }
}
impl SystemTunLease {
    /// Install the real routes/helper cleanup owner before main snapshots its
    /// resources. This delegates to this same Source; no new task or provider.
    pub(crate) fn register_after_scope(
        &self,
        resource: Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
    ) -> Result<zero_core::component::ComponentInstanceId, EngineError> {
        self.installed.register_after_scope(resource)
    }
    pub(crate) fn owner(&self) -> Arc<InboundOwner> {
        self.owner.clone()
    }
    /// Start concrete TUN preparation in this same already retained Scope.
    /// The caller installs its original resource before registering any work.
    pub(crate) fn preparation_tasks(
        &self,
    ) -> Result<zero_platform_tokio::contract::scope::TaskRegistrar, EngineError> {
        self.owner.preparation_tasks()
    }

    /// All child resources enter the same retained owner before spawn_driver
    /// snapshots it. Native identity is read only after the real start acquires.
    pub(crate) fn start(
        mut self,
        runtime: crate::runtime::route_runtime::InboundListenerRuntime,
        shutdown: watch::Receiver<bool>,
        ingress: crate::inbound::tun::TunIngressOptions,
    ) -> Result<Arc<InboundOwner>, EngineError> {
        let identity = self.owner.identity();
        let tasks = self.owner.accepted_registrar().clone();
        let cancel = self.owner.task_access().cancellation();
        let resources = HostResources::new_for_instance(
            identity,
            &self.recipe.spec().resources,
            cancel.clone(),
            tasks.scope_identity(),
        )
        .map_err(std::io::Error::other)?;
        // The concrete provider stays outside the main body. Its native close
        // must follow this same Scope's consumed wire and main task exits.
        self.owner
            .install_after_scope_resource(identity, erase_lifecycle(Native(resources.clone())))?;
        let icmp = resources
            .icmp_probe_grant()
            .ok_or_else(|| std::io::Error::other("actual System TUN ICMP provider missing"))?;
        self.owner
            .icmp_probe
            .set(icmp)
            .map_err(|_| std::io::Error::other("System TUN ICMP provider already installed"))?;
        let prepared =
            crate::runtime::inbound_operation::raw_ip::PacketSourceContext::prepare_device_source(
                &self.installed,
            )?;
        let access: Arc<dyn ResourceAccess> = resources.clone();
        *self.owner.inbound_resources.lock().unwrap() = Some(Arc::downgrade(&access));
        let mut instance = self
            .recipe
            .build(BuildContext {
                component: identity.component,
                generation: identity.generation,
                dependencies: Arc::from([]),
                roles: ComponentRoleIds {
                    inbound: Some(InboundId(identity.component.0)),
                    ..Default::default()
                },
                resources: access,
                tasks,
                cancel,
                background_carrier: None,
            })
            .map_err(std::io::Error::other)?;
        let source: Box<dyn DynInboundSource> =
            instance.roles.inbound.take().ok_or_else(|| {
                std::io::Error::other("System TUN factory omitted its actual source")
            })?;
        self.owner.install_after_scope_resource(
            identity,
            erase_lifecycle(SourceLifecycle {
                native: resources.clone(),
                protocol: instance.lifecycle,
            }),
        )?;
        let owner = Arc::downgrade(&self.owner);
        let runtime = runtime.with_source_owner(&self.owner);
        self.owner.spawn_driver(async move {
            let original = resources.original_packet_owner(0).ok_or_else(|| {
                std::io::Error::other("System TUN start omitted its acquired original")
            })?;
            let context = prepared.complete_tun(&original, ingress)?;
            {
                let owner = owner.upgrade().ok_or_else(|| {
                    std::io::Error::other("actual System TUN owner retired before source install")
                })?;
                *owner.packet_source.lock().unwrap() = Some(context);
            }
            crate::runtime::inbound_operation::source::run(
                source,
                runtime,
                shutdown,
                NonZeroUsize::new(1).unwrap(),
                "tun",
            )
            .await
        })?;
        self.observers.enqueue_system_tun(self.owner.clone())?;
        self.owner.seal_listener_installation();
        self.armed = false;
        Ok(self.owner.clone())
    }
}
impl Drop for SystemTunLease {
    fn drop(&mut self) {
        if self.armed {
            self.owner.task_access().request_stop();
            self.owner.seal_listener_installation();
        }
    }
}
struct Native(Arc<HostResources>);
impl ResourceLifecycle for Native {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.shutdown_io(deadline).await
    }
}
struct SourceLifecycle {
    native: Arc<HostResources>,
    protocol: Box<dyn DynResourceLifecycle>,
}
impl ResourceLifecycle for SourceLifecycle {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        let _access = self.native.activate(deadline)?;
        self.protocol.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.protocol.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.protocol.shutdown(deadline).await
    }
}
