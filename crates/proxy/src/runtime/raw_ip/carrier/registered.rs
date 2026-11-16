//! Original wire and reconnect attempts share their real Source cleanup book.
//! Shared donors retain their own native/Scope receipts in the Host graph.
use super::*;
use crate::component::background::packet_driver::PacketPathExecutionSource;
use std::sync::{Mutex, OnceLock};
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, WeakTaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};
mod io;
mod retirement;
mod worker;
#[derive(Clone)]
enum Actor {
    Vacant,
    Registering,
    Registered(TaskJoinWitness),
}
struct WirePath {
    carrier: Arc<dyn PacketPathCarrier>,
    endpoint: SocketAddr,
}
struct State {
    path: tokio::sync::watch::Sender<Option<Arc<WirePath>>>,
    initialization: tokio::sync::Mutex<()>,
    pending: Mutex<Option<Arc<WirePath>>>,
    desired: Mutex<SocketAddr>,
    closing: tokio::sync::watch::Sender<bool>,
    actor: Mutex<Actor>,
    execution: PacketPathExecutionSource,
    recipe: Arc<RolePacketPath>,
    tasks: WeakTaskRegistrar,
    wire: TaskJoinWitness,
    request_failure: OnceLock<Arc<ChannelError>>,
    closed: AtomicBool,
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
    pub(crate) fn request_stop(&self) {
        self.state.stop();
    }

    pub(crate) fn retirement_complete(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    pub(crate) fn matches_driver(&self, driver: &TaskJoinWitness) -> bool {
        self.state.wire.same_registration(driver)
    }

    /// Install this owner in the actual Source before calling initialize.
    /// Preparation acquires no carrier and starts no background operation.
    pub(crate) fn prepare(
        recipe: Arc<RolePacketPath>,
        execution: PacketPathExecutionSource,
        tasks: WeakTaskRegistrar,
        endpoint: SocketAddr,
    ) -> Result<Self, EngineError> {
        let wire = execution.caller().clone();
        if wire.is_joined() || !wire.scope_identity().same_scope(&tasks.scope_identity()) {
            return Err(failed(broken(
                "outer path belongs to another original Scope",
            )));
        }
        let (path, _) = tokio::sync::watch::channel(None);
        let (closing, _) = tokio::sync::watch::channel(false);
        let carrier = Self {
            state: Arc::new(State {
                path,
                initialization: tokio::sync::Mutex::new(()),
                pending: Mutex::new(None),
                desired: Mutex::new(endpoint),
                closing,
                actor: Mutex::new(Actor::Vacant),
                execution,
                recipe,
                tasks,
                wire,
                request_failure: OnceLock::new(),
                closed: AtomicBool::new(false),
            }),
            sent_once: AtomicBool::new(false),
        };
        Ok(carrier)
    }

    pub(crate) async fn initialize(&self) -> Result<(), EngineError> {
        let state = &self.state;
        let _initialization = state.initialization.lock().await;
        if state.stopped() {
            return Err(background_stopped().into());
        }
        if state.path.borrow().is_some() {
            return Ok(());
        }
        // An interrupted initializer can leave an acquired candidate or only
        // an original attempt in the Source book. Neither may be overwritten.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let pending = state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let report = match pending {
            Some(ref pending) => state.retire_path(pending, deadline).await,
            None => state.execution.retire_carriers(deadline).await,
        }
        .map_err(failed)?;
        if !report.is_complete() {
            return Err(failed(broken(
                "initial outer carrier retirement is pending",
            )));
        }
        let previous = state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(previous);
        if state.stopped() {
            return Err(background_stopped().into());
        }
        let endpoint = *state
            .desired
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let initial = Arc::new(WirePath {
            carrier: state
                .execution
                .build_carrier(&state.recipe, endpoint)
                .await?,
            endpoint,
        });
        *state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(initial.clone());
        // The Source retained this owner before preparation suspended, and
        // this owner retains its exact returned views before confirming them.
        initial
            .carrier
            .confirm_driver_handoff(&state.wire)
            .map_err(failed)?;
        if state.stopped() {
            state.stop();
            return Err(background_stopped().into());
        }
        state.path.send_replace(Some(initial));
        let installed = state
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(installed);
        Ok(())
    }
}
impl State {
    fn is_current(&self, path: &Arc<WirePath>) -> bool {
        self.path
            .borrow()
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(path, current))
    }
    fn stop(&self) {
        self.closing.send_replace(true);
        self.execution.request_carrier_retirement();
        let current = self.path.borrow().clone();
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // External cancel callbacks run after both original owner locks end.
        if let Some(current) = current {
            if let Err(source) = current.carrier.request_driver_retirement(&self.wire) {
                self.request_failure.get_or_init(|| Arc::new(source));
            }
        }
        if let Some(pending) = pending {
            if let Err(source) = pending.carrier.request_driver_retirement(&self.wire) {
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
    fn rebuild_if_current(self: &Arc<Self>, failed_path: Arc<WirePath>) -> Result<(), EngineError> {
        if *self.closing.borrow() || self.execution.is_cancelled() || !self.is_current(&failed_path)
        {
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
                // Wake a reader that observed Registering before the actual
                // witness was published; this is notification, never readiness
                // or a new path/native receipt.
                self.path.send_modify(|_| {});
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
