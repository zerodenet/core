//! One actual reservation's setup/driver and native Source children.
//! The controller holds no HostResources, strong Scope or generation backedge.
use super::*;
use std::sync::{Mutex, OnceLock};
use zero_platform_tokio::{
    contract::{
        lifecycle::DynChannelControl,
        scope::{TaskJoinWitness, TaskJoined, TaskScopeIdentity, WeakTaskRegistrar},
    },
    controlled_carrier::{CarrierProtocolRetirement, ProtocolOriginalChildren},
};
pub(super) enum Ready {
    Stream(zero_platform_tokio::controlled_carrier::loan::StreamLoan),
    Http(super::http::Prepared),
    Failed(zero_platform_tokio::contract::RetainedError),
}
pub(super) struct Entry {
    pub ordinal: u64,
    pub diagnostics: super::diagnostics::Diagnostics,
    pub stop: CancellationToken,
    pub scope: TaskScopeIdentity,
    pub tasks: WeakTaskRegistrar,
    pub children: ProtocolOriginalChildren,
    pub retirement: OnceLock<CarrierProtocolRetirement>,
    pub source: OnceLock<zero_platform_tokio::AcceptedTcpCarrierOrigin>,
    // Pure handoff status only. The actual IO is held by the original
    // one-shot receiver/caller, never Root Entry -> own-slot CarrierIo.
    delivery: std::sync::atomic::AtomicU8,
    result: zero_traits::packet_metadata::PacketMetadataWeak<super::delivery::Owner>,
    pub actor: Mutex<Option<Option<TaskJoinWitness>>>,
    pub driver: Mutex<Option<Option<TaskJoinWitness>>>,
    received_actor: OnceLock<TaskJoined>,
    received_driver: OnceLock<TaskJoined>,

    pub changed: tokio::sync::Notify,
}
pub(super) struct Control(pub PacketMetadataArc<Entry>);
impl Entry {
    pub fn new(
        ordinal: u64,
        diagnostics: super::diagnostics::Diagnostics,
        tasks: WeakTaskRegistrar,
        stop: CancellationToken,
        children: ProtocolOriginalChildren,
        http: bool,
        result: zero_traits::packet_metadata::PacketMetadataWeak<super::delivery::Owner>,
    ) -> Self {
        Self {
            ordinal,
            diagnostics,
            scope: tasks.scope_identity(),
            tasks,
            stop,
            children,
            retirement: OnceLock::new(),
            source: OnceLock::new(),
            delivery: std::sync::atomic::AtomicU8::new(0),
            result,
            actor: Mutex::new(None),
            driver: Mutex::new((!http).then_some(None)),
            received_actor: OnceLock::new(),
            received_driver: OnceLock::new(),

            changed: tokio::sync::Notify::new(),
        }
    }
    pub fn receive_delivery(&self) -> Result<(), ChannelError> {
        self.delivery
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| error(Stage::Prepare, Reason::Revoked))
    }
    pub fn abandon_delivery(&self) {
        let _ = self.delivery.compare_exchange(
            0,
            2,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        self.changed.notify_waiters();
    }
    pub fn set_actor(&self, actor: Option<TaskJoinWitness>) {
        let replaced = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(actor);
        assert!(replaced.is_none(), "one actual setup registration outcome");
        drop(replaced);
        self.changed.notify_waiters();
    }
    pub fn set_driver(&self, actual: TaskJoinWitness) {
        let old = self
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(Some(actual));
        assert!(old.is_none(), "one exact registered physical driver");
        drop(old);
        self.changed.notify_waiters();
    }
    pub fn skip_driver(&self) {
        let mut driver = self
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if driver.is_none() {
            *driver = Some(None);
        }
        drop(driver);
        self.changed.notify_waiters();
    }
    pub async fn receive_actor(&self) -> Result<Option<TaskJoined>, ChannelError> {
        self.receive_registration(&self.actor, &self.received_actor)
            .await
    }
    async fn receive_registration(
        &self,
        registration: &Mutex<Option<Option<TaskJoinWitness>>>,
        received: &OnceLock<TaskJoined>,
    ) -> Result<Option<TaskJoined>, ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let current = registration
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            match current {
                Some(Some(witness)) => {
                    let joined = witness.wait().await;
                    if !witness.matches_joined(&joined) {
                        return Err(error(Stage::Close, Reason::StalePlan));
                    }
                    // Root holds this same Entry before first IO. Strongly
                    // persist the original result before local failure ACK.
                    let actual = received.get_or_init(|| joined.clone());
                    if actual.id() != joined.id() {
                        return Err(error(Stage::Close, Reason::StalePlan));
                    }
                    if let Err(source) = joined.result() {
                        if std::ptr::eq(registration, &self.actor) {
                            self.diagnostics.terminal(source);
                        } else {
                            self.diagnostics.tcp_driver(source);
                        }
                        witness
                            .acknowledge_hosted_failure(&joined)
                            .map_err(|source| {
                                error(Stage::Close, Reason::Internal).with_source(source)
                            })?;
                    }
                    return Ok(Some(joined));
                }
                Some(None) => return Ok(None),
                None => {
                    // Only the actual sealed Scope census resolves a synchronous
                    // uncertain registration. Expired weak metadata is unknown.
                    if let Some(scope) = self.tasks.retirement_observation() {
                        tokio::select! {
                            _=scope.wait_drained()=>return Ok(None),
                            _=&mut changed=>{},
                        }
                    } else {
                        changed.await;
                    }
                }
            }
        }
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
        self.0.changed.notify_waiters();
    }
    fn wait_closed(
        &mut self,
        _: tokio::time::Instant,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.stop.cancel();
            self.0.abandon_delivery();
            if let Some(result) = self.0.result.upgrade() {
                result.discard::<Ready>();
            }
            self.0.receive_actor().await?;
            if self.0.delivery.load(std::sync::atomic::Ordering::Acquire) == 2 {
                self.0.skip_driver();
            }
            self.0
                .receive_registration(&self.0.driver, &self.0.received_driver)
                .await?;
            if self.0.delivery.load(std::sync::atomic::Ordering::Acquire) == 2 {
                self.0.diagnostics.undelivered();
            }
            // Both actual exits and their original error Arcs are now held by
            // this preinstalled finite capsule before any local ACK.
            if let Some(source) = self.0.diagnostics.pending_failure() {
                return Err(ChannelError::retained(source));
            }
            // This is the actual operation; the observer deadline never owns
            // it. No adoption.retire or self-Scope join is called here.
            if let Some(source) = self.0.source.get() {
                source.request_retirement();
                if let Err(source) = source.wait_settled_original().await {
                    self.0.diagnostics.physical(source);
                }
            }
            if let Err(source) = self.0.children.wait_settled_original().await {
                self.0.diagnostics.physical(source);
            }
            if let Some(source) = self.0.diagnostics.pending_failure() {
                return Err(ChannelError::retained(source));
            }
            Ok(CloseReport::default())
        })
    }
}
