//! The standard System TUN source transfers the same concrete native IO once.
//! The installed host retains native close; this source retains actual frames.
#[cfg(test)]
mod budget;
mod channel;
mod driver;
mod owner;
mod protocol;
use std::{
    net::IpAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{mpsc, Notify};
use zero_core::{
    component::*,
    packet::{IpPacket, PacketFacts},
};
use zero_platform_tokio::contract::{
    component::*,
    inbound::*,
    lifecycle::*,
    packet::*,
    scope::{TaskJoinWitness, TaskScopeIdentity},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
use zero_platform_tokio::controlled_carrier::PacketProviderRegistration;

pub(crate) struct SystemTunSourceOptions {
    spec: ComponentSpec,
    resource_name: Arc<str>,
    addresses: Vec<(IpAddr, IpAddr)>,
    mtu: u16,
    state: Arc<SystemTunSourceState>,
    native_io: Arc<dyn zero_traits::IoObserver>,
}
#[derive(Default)]
pub(crate) struct SystemTunSourceState {
    acquired: OnceLock<Acquired>,
    driver: OnceLock<TaskJoinWitness>,
    scope: OnceLock<TaskScopeIdentity>,
    changed: Notify,
}
struct Acquired {
    interface: Arc<str>,
    identity: PacketDeviceIdentity,
    original: PacketProviderRegistration,
}
impl SystemTunSourceState {
    /// The real native acquisition stays with the same host provider. Waiting
    /// does not manufacture readiness, a wire join, or a native-close receipt.
    pub(crate) async fn wait_acquired(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Arc<str>, ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(acquired) = self.acquired.get() {
                return Ok(acquired.interface.clone());
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Revoked)),
                _ = changed => {},
            }
        }
    }
    pub(crate) fn interface(&self) -> Option<Arc<str>> {
        Some(self.acquired.get()?.interface.clone())
    }
    pub(crate) fn identity(&self) -> Option<PacketDeviceIdentity> {
        Some(self.acquired.get()?.identity.clone())
    }
    pub(crate) fn original(&self) -> Option<PacketProviderRegistration> {
        Some(self.acquired.get()?.original.clone())
    }
    pub(crate) fn driver(&self) -> Option<TaskJoinWitness> {
        self.driver.get().cloned()
    }
}
impl SystemTunSourceOptions {
    pub(crate) fn new(
        resource_name: Arc<str>,
        binding: PacketDeviceBinding,
        addresses: Vec<(IpAddr, IpAddr)>,
        mtu: u16,
        tag: String,
        scope_task_limit: NonZeroUsize,
        native_io: Arc<dyn zero_traits::IoObserver>,
    ) -> Result<Arc<Self>, ChannelError> {
        if resource_name.is_empty()
            || addresses.is_empty()
            || mtu == 0
            || addresses
                .iter()
                .any(|(address, mask)| address.is_ipv4() != mask.is_ipv4())
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        use sha2::{Digest, Sha256};
        let mut identity = Sha256::new();
        identity.update(b"zero.system-tun.source.v1");
        identity.update(format!(
            "{resource_name:?}{binding:?}{addresses:?}{mtu:?}{tag:?}{scope_task_limit:?}"
        ));
        Ok(Arc::new(Self {
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: vec![RoleKind::Inbound],
                dependencies: vec![],
                static_identity: identity.finalize().to_vec().into(),
                // The registry adds the same original main task. All wire,
                // child and route workers share this exact remaining ceiling.
                task_limit: NonZeroUsize::new(scope_task_limit.get() - 1),
                resources: vec![
                    ResourceDeclaration::ControlledPacketDevice {
                        name: resource_name.clone(),
                        binding,
                    },
                    ResourceDeclaration::NativeIcmp {
                        max_sockets: NonZeroUsize::new(64).unwrap(),
                    },
                ],
            },
            resource_name,
            addresses,
            mtu,
            state: Arc::default(),
            native_io,
        }))
    }
    pub(crate) fn ingress_options(
        &self,
        dns_hijack: bool,
        dns_hijacked_queries: Arc<std::sync::atomic::AtomicU64>,
    ) -> super::TunIngressOptions {
        super::TunIngressOptions {
            addresses: self.addresses.clone(),
            mtu: self.mtu,
            dns_hijack,
            dns_hijacked_queries,
            native_io: self.native_io.clone(),
        }
    }
    pub(crate) fn state(&self) -> Arc<SystemTunSourceState> {
        self.state.clone()
    }
}
pub(crate) struct SystemTunSourceFactory;
struct Shared {
    pause: Arc<PacketSourcePause>,
    checkpoint: Mutex<Option<PacketSourceCheckpoint>>,
    state: Mutex<State>,
    incoming: Arc<Mutex<Option<IpPacket>>>,
    outgoing: Arc<Mutex<Option<IpPacket>>>,
    pending_tx: Arc<Mutex<Option<IpPacket>>>,
    changed: Notify,
    cancel: CancellationToken,
    metadata: Arc<SystemTunSourceState>,
    addresses: Vec<(IpAddr, IpAddr)>,
    mtu: u16,
}
#[derive(Default)]
struct State {
    io: Option<GrantedPacketDevice>,
    start: Option<Start>,
    source_taken: bool,
}
struct Start {
    facts: PacketFacts,
    observer: Arc<dyn PacketObserver>,
    incoming: mpsc::Sender<IpPacket>,
    outgoing: mpsc::Receiver<IpPacket>,
}
struct Source {
    shared: Arc<Shared>,
    instance: ComponentInstanceId,
    inbound: InboundId,
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
impl ComponentFactory for SystemTunSourceFactory {
    type Options = SystemTunSourceOptions;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let inbound = context
            .roles
            .inbound
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let scope = context.tasks.scope_identity();
        if options.state.scope.set(scope.clone()).is_err() {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let pause = context
            .resources
            .packet_source_pause()
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        if pause.instance() != instance || !pause.scope().same_scope(&scope) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let shared = Arc::new(Shared {
            pause,
            checkpoint: Mutex::new(None),
            state: Mutex::new(State::default()),
            incoming: Arc::default(),
            outgoing: Arc::default(),
            pending_tx: Arc::default(),
            changed: Notify::new(),
            cancel: context.cancel.child_token(),
            metadata: options.state.clone(),
            addresses: options.addresses.clone(),
            mtu: options.mtu,
        });
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner::Owner {
                shared: shared.clone(),
                resources: context.resources,
                tasks: context.tasks,
                name: options.resource_name.clone(),
            }),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(Source {
                    shared,
                    instance,
                    inbound,
                })),
                ..Default::default()
            },
        })
    }
}
