//! The original TEMP reconnect future has one actual Host Scope registration.
//! Its physical ancestor remains Unknown; this records only software custody.
use super::*;
use crate::component::background::packet_driver::PacketPathExecutionSource;
use std::sync::{Mutex, OnceLock};
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, WeakTaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};
mod io;
mod worker;
#[derive(Clone)]
enum Actor {
    Vacant,
    Registering,
    Registered(TaskJoinWitness),
}
struct State {
    path: tokio::sync::watch::Sender<Arc<dyn PacketPathCarrier>>,
    pending: Mutex<Option<Arc<dyn PacketPathCarrier>>>,
    closing: tokio::sync::watch::Sender<bool>,
    actor: Mutex<Actor>,
    execution: PacketPathExecutionSource,
    operation: Arc<dyn PreparedUdpPacketPathOperation>,
    tasks: WeakTaskRegistrar,
    wire: TaskJoinWitness,
    request_failure: OnceLock<Arc<ChannelError>>,
}
pub(crate) struct RegisteredProxiedRawIpWireCarrier {
    state: Arc<State>,
    sent_once: AtomicBool,
}
fn failed(source: ChannelError) -> EngineError {
    std::io::Error::other(source).into()
}
fn broken(message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Close, Reason::Protocol, Impact::Component)
        .with_source(std::io::Error::other(message))
}
impl RegisteredProxiedRawIpWireCarrier {
    pub(crate) async fn start(
        operation: Arc<dyn PreparedUdpPacketPathOperation>,
        execution: PacketPathExecutionSource,
        tasks: WeakTaskRegistrar,
    ) -> Result<Self, EngineError> {
        let wire = execution.caller().clone();
        if wire.is_joined() || !wire.scope_identity().same_scope(&tasks.scope_identity()) {
            return Err(failed(broken(
                "outer path belongs to another original Scope",
            )));
        }
        let initial: Arc<dyn PacketPathCarrier> = execution.build_carrier(&operation).await?;
        let (path, _) = tokio::sync::watch::channel(initial.clone());
        let (closing, _) = tokio::sync::watch::channel(false);
        let carrier = Self {
            state: Arc::new(State {
                path,
                pending: Mutex::new(None),
                closing,
                actor: Mutex::new(Actor::Vacant),
                execution,
                operation,
                tasks,
                wire,
                request_failure: OnceLock::new(),
            }),
            sent_once: AtomicBool::new(false),
        };
        // This exact owner now strongly retains the complete Path and its
        // original views/control, before sealed receiver confirmation escapes.
        initial
            .confirm_driver_handoff(&carrier.state.wire)
            .map_err(failed)?;
        Ok(carrier)
    }
}
impl State {
    fn stop(&self) {
        self.closing.send_replace(true);
        let current = self.path.borrow().clone();
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // External cancel callbacks run after both original owner locks end.
        if let Err(source) = current.request_driver_retirement(&self.wire) {
            self.request_failure.get_or_init(|| Arc::new(source));
        }
        if let Some(pending) = pending {
            if let Err(source) = pending.request_driver_retirement(&self.wire) {
                self.request_failure.get_or_init(|| Arc::new(source));
            }
        }
    }
    fn actor(&self) -> Actor {
        self.actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn background_drained(&self) -> bool {
        match self.actor() {
            Actor::Vacant => true,
            Actor::Registering => false,
            Actor::Registered(w) => w
                .try_joined()
                .is_some_and(|j| w.matches_joined(&j) && j.registration_released()),
        }
    }
    fn rebuild_if_current(
        self: &Arc<Self>,
        failed_path: Arc<dyn PacketPathCarrier>,
    ) -> Result<(), EngineError> {
        if *self.closing.borrow() || !Arc::ptr_eq(&failed_path, &self.path.borrow()) {
            return Ok(());
        }
        let previous = {
            let mut actor = self
                .actor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let available = match &*actor {
                Actor::Vacant => true,
                Actor::Registering => false,
                Actor::Registered(w) => w
                    .try_joined()
                    .is_some_and(|j| w.matches_joined(&j) && j.registration_released()),
            };
            if !available {
                return Ok(());
            }
            std::mem::replace(&mut *actor, Actor::Registering)
        };
        drop(previous);
        let state = self.clone();
        let (release, gate) = tokio::sync::oneshot::channel();
        let registered = self
            .execution
            .spawn_reconnect(&self.tasks, move |execution| async move {
                // The same actual reconnect witness is also retained by this
                // concrete carrier before the original body can perform any IO.
                if gate.await.is_err() {
                    return Ok(());
                }
                state.rebuild(failed_path, execution).await
            });
        match registered {
            Ok((_, witness)) => {
                *self
                    .actor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Actor::Registered(witness);
                let _ = release.send(());
                Ok(())
            }
            Err(source) => {
                *self
                    .actor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Actor::Vacant;
                Err(failed(source))
            }
        }
    }
}
impl Drop for RegisteredProxiedRawIpWireCarrier {
    fn drop(&mut self) {
        self.state.stop();
    }
}
