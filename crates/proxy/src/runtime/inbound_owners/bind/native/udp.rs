//! The runtime phase guard around the single canonical native datagram owner.
use super::*;
use crate::component::instance::HostDatagramListenerOwner;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use zero_platform_tokio::contract::{
    CloseReason, Impact, Reason, Stage,
    component::GrantedDatagramListener,
    lifecycle::{CloseReport, ResourceLifecycle},
};

pub(in crate::runtime::inbound_owners) struct NativeDatagramGrant {
    identity: ComponentInstanceId,
    originals: Option<Arc<zero_runtime::HostOriginalBudgets>>,
    #[cfg(feature = "raw-ip-runtime")]
    packet_reads: Option<Arc<crate::component::instance::HostPacketReadReservation>>,
    addresses: std::sync::OnceLock<Arc<[std::net::SocketAddr]>>,
    declaration: ResourceDeclaration,
    attempts: NonZeroUsize,
    resolution: std::sync::OnceLock<(
        zero_core::component::TaskId,
        zero_platform_tokio::contract::scope::TaskJoinWitness,
    )>,
    resolved: std::sync::OnceLock<Result<(), crate::component::instance::SharedError>>,
    native: Mutex<Option<HostDatagramListenerOwner>>,
    cancel: CancellationToken,
    bound: AtomicBool,
    original: std::sync::OnceLock<
        zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
    >,
    scope: Option<zero_platform_tokio::contract::scope::TaskRegistrar>,
}
impl NativeDatagramGrant {
    pub(super) fn new(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: Option<std::net::SocketAddr>,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::DatagramListener {
            max_bind_attempts, ..
        } = declaration
        else {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        };
        if attempts > *max_bind_attempts {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        // As with the resolving TCP grant, one hostname bind may try the
        // original lookup's bounded candidates. Literal startup stays one try.
        let attempts = if address.is_none() {
            *max_bind_attempts
        } else {
            attempts
        };
        let addresses = std::sync::OnceLock::new();
        if let Some(address) = address {
            let _ = addresses.set(Arc::from([address]));
        }
        Ok(Self {
            identity,
            originals: None,
            #[cfg(feature = "raw-ip-runtime")]
            packet_reads: None,
            addresses,
            declaration: declaration.clone(),
            attempts,
            resolution: std::sync::OnceLock::new(),
            resolved: std::sync::OnceLock::new(),
            native: Mutex::new(if address.is_some() {
                Some(HostDatagramListenerOwner::new(
                    identity.component,
                    declaration,
                    attempts,
                )?)
            } else {
                None
            }),
            cancel,
            bound: AtomicBool::new(false),
            original: std::sync::OnceLock::new(),
            scope: None,
        })
    }
    pub(super) fn with_original_budgets(
        mut self,
        originals: Arc<zero_runtime::HostOriginalBudgets>,
    ) -> Self {
        self.originals = Some(originals);
        self
    }
    pub(super) fn new_scoped(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: Option<std::net::SocketAddr>,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
        scope: zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<Self, ChannelError> {
        let mut original = Self::new(identity, declaration, address, attempts, cancel)?;
        original.scope = Some(scope);
        Ok(original)
    }
    pub(in crate::runtime::inbound_owners) fn register_driver(
        &self,
        original: &zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity,
        witness: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        ChannelError,
    > {
        if self.cancel.is_cancelled()
            || !self
                .scope
                .as_ref()
                .is_some_and(|tasks| witness.scope_identity().matches(tasks))
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let native = self.native.lock().expect("native datagram owner poisoned");
        let actual = native
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component))?
            .register_driver(original, witness)?;
        let retained = self.original.get_or_init(|| actual.clone());
        if !retained.same_original(&actual) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        Ok(actual)
    }
    pub(in crate::runtime::inbound_owners) fn original_origin(
        &self,
    ) -> Option<zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin> {
        self.original
            .get()
            .filter(|original| !original.is_retired())
            .cloned()
    }
    pub(super) fn bind(&self) -> Result<GrantedDatagramListener, EngineError> {
        self.bind_after(|| {})
    }
    fn bind_after(&self, acquired: impl FnOnce()) -> Result<GrantedDatagramListener, EngineError> {
        if self.cancel.is_cancelled() {
            return Err(crate::runtime::inbound_owners::preparing::stopped_preparation());
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = &self.packet_reads {
            reads
                .prepare_before_listener_bind(
                    self.scope
                        .as_ref()
                        .ok_or_else(|| {
                            ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
                        })
                        .map_err(std::io::Error::other)?,
                )
                .map_err(std::io::Error::other)?;
        }
        let mut native = self.native.lock().expect("native datagram owner poisoned");
        if self.cancel.is_cancelled() {
            return Err(crate::runtime::inbound_owners::preparing::stopped_preparation());
        }
        // The unique native ledger was registered before this synchronous call;
        // no await or post-bind cancellation can lose the original socket.
        let owner = native
            .as_mut()
            .ok_or_else(|| std::io::Error::other("original UDP resolution is not installed"))?;
        if let (Some(originals), Some(tasks)) = (&self.originals, &self.scope) {
            owner
                .bind_original_budgets(originals.clone(), tasks.downgrade())
                .map_err(std::io::Error::other)?;
        }
        let addresses = self
            .addresses
            .get()
            .ok_or_else(|| std::io::Error::other("UDP addresses are not resolved"))?;
        let mut last = None;
        let mut view = None;
        for address in addresses.iter().take(self.attempts.get()) {
            match owner.bind_listener(*address) {
                Ok(bound) => {
                    view = Some(bound);
                    break;
                }
                Err(error) => {
                    if !error.retry_permitted() {
                        return Err(std::io::Error::other(error).into());
                    }
                    last = Some(error);
                }
            }
        }
        let view =
            view.ok_or_else(|| std::io::Error::other(last.expect("nonempty bounded addresses")))?;
        if let Some(tasks) = &self.scope {
            owner
                .register_provider(self.identity, tasks)
                .map_err(std::io::Error::other)?;
        }
        self.bound.store(true, Ordering::Release);
        acquired();
        Ok(view)
    }
}

pub(super) struct DatagramResource(pub(super) Arc<NativeDatagramGrant>);
impl ResourceLifecycle for DatagramResource {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.0.cancel.is_cancelled() || !self.0.bound.load(Ordering::Acquire) {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
            error.component = Some(self.0.identity.component);
            return Err(error);
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.cancel.cancel();
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = &self.0.packet_reads {
            reads.revoke();
        }
        // Every native operation is synchronous. This same short lock makes
        // revoke linearize after an in-flight bind, including another thread.
        if let Some(owner) = self
            .0
            .native
            .lock()
            .expect("native datagram owner poisoned")
            .as_mut()
        {
            owner.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = &self.0.packet_reads {
            reads.revoke();
        }
        let mut native = self
            .0
            .native
            .lock()
            .expect("native datagram owner poisoned");
        if let Some(native) = native.as_mut() {
            native.revoke();
            native.shutdown(deadline)
        } else {
            Ok(CloseReport::default())
        }
    }
}

mod packet_reads;
mod resolution;

#[cfg(test)]
#[path = "udp/tests.rs"]
mod tests;
