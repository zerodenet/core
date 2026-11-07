//! The original reconnect future runs once in its original Host Scope.
//! This is a concrete task-registration boundary, not authority from a TaskId.
use super::*;
use crate::contract::lifecycle::{ComponentError, TaskClass};
use std::future::Future;
use zero_core::component::TaskId;
#[cfg(feature = "native-packet-origin")]
mod metadata;
pub(super) struct Reconnect {
    witness: OnceLock<TaskJoinWitness>,
    failures: ActorFailuresRef,
    observing_failure: Mutex<()>,
    failure_delivery: OnceLock<Arc<faults::Delivery>>,
    accepting: std::sync::atomic::AtomicBool,
    forwarded: Arc<source_failures::Forwarded>,
}
impl Reconnect {
    pub(super) fn fault_report(&self) -> Option<ChannelError> {
        self.failure_delivery.get().and_then(|same| same.report())
    }
    fn delivery(&self) -> &Arc<faults::Delivery> {
        self.failure_delivery.get_or_init(|| {
            let delivery = faults::Delivery::new();
            delivery.register_actor(self.failures.clone());
            delivery
        })
    }
    pub(super) fn registration_released(&self) -> bool {
        self.witness.get().is_some_and(|actual| {
            actual.try_joined().is_some_and(|joined| {
                actual.matches_joined(&joined)
                    && joined.registration_released()
                    && (joined.result().is_ok() || joined.failure_accepted())
            })
        })
    }
    pub(super) fn fault_received(&self) -> bool {
        self.failure_delivery
            .get()
            .is_none_or(|actual| actual.acknowledged())
    }
    pub(super) fn healthy_or_released(&self) -> bool {
        if self.failures.failed() {
            return false;
        }
        self.witness
            .get()
            .is_some_and(|actual| match actual.try_joined() {
                None => {
                    self.accepting.load(std::sync::atomic::Ordering::Acquire)
                        && actual.scope_identity().resource_open()
                }
                Some(joined) => {
                    actual.matches_joined(&joined)
                        && joined.result().is_ok()
                        && joined.registration_released()
                }
            })
    }
}
impl PacketDriverCalls {
    fn original_actor(&self, actual: &TaskJoinWitness) -> bool {
        if let Some(logical) = self.original.logical_packet() {
            logical
                .drivers()
                .iter()
                .any(|wire| wire.same_registration(actual))
        } else {
            self.original
                .driver()
                .is_some_and(|wire| wire.same_registration(actual))
        }
    }
    pub(super) fn accepts_actor(&self, actual: &TaskJoinWitness) -> bool {
        self.original_actor(actual)
            || self
                .reconnects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|actor| {
                    actor
                        .witness
                        .get()
                        .is_some_and(|w| w.same_registration(actual))
                })
    }
    /// Register the existing per-peer N=1 reconnect future before its first IO.
    /// The caller retains its existing N=1 admission; this creates no second
    /// retry loop, JoinHandle owner, backoff policy or background cleanup worker.
    pub fn spawn_reconnect<F, Fut>(
        self: &Arc<Self>,
        tasks: &WeakTaskRegistrar,
        wire: &TaskJoinWitness,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ComponentError>> + Send + 'static,
    {
        if self.retiring.load(std::sync::atomic::Ordering::Acquire)
            || !self.original_actor(wire)
            || self.original.is_retired()
            || wire.is_joined()
            || !wire.scope_identity().same_scope(&tasks.scope_identity())
            || !wire.scope_identity().resource_open()
        {
            return Err(failure(Reason::Revoked));
        }
        if let Some(source) = self.receive_actor_failures() {
            return Err(source);
        }
        self.collect_retired();
        #[cfg(feature = "native-packet-origin")]
        let mut metadata = metadata::Prepared::reserve(self)?;
        #[cfg(feature = "native-packet-origin")]
        let _notifications = metadata.memory.defer_notifications();
        // The same calls lock serializes epoch change and both actual producer
        // admissions. No actor can register into an already handed-off epoch.
        let mut retired_epoch = None;
        let calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delivery = self.advance_fault_epoch(&calls, &mut retired_epoch)?;
        #[cfg(feature = "native-packet-origin")]
        let actor = metadata.install(self)?;
        #[cfg(not(feature = "native-packet-origin"))]
        let actor = Arc::new(Reconnect {
            witness: OnceLock::new(),
            failures: Arc::new(faults::ActorFailures::default()),
            observing_failure: Mutex::new(()),
            failure_delivery: OnceLock::new(),
            accepting: true.into(),
            forwarded: self.source_failures.prepare(),
        });
        // Extract a single already-joined/ACKed original at a time. The
        // actual task/error state is never dropped under the actors mutex.
        for _ in 0..self.max_calls.get() {
            let retired = {
                let mut actors = self
                    .reconnects
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                actors
                    .iter()
                    .position(|actual| {
                        !actual.failures.failed()
                            && actual.witness.get().is_some_and(|w| {
                                w.try_joined().is_some_and(|j| {
                                    w.matches_joined(&j)
                                        && j.result().is_ok()
                                        && j.registration_released()
                                })
                            })
                    })
                    .map(|at| actors.remove(at))
            };
            let Some(retired) = retired else { break };
            if let Some(old) = retired.failure_delivery.get() {
                old.remove_empty_actor(&retired.failures);
            }
            self.source_failures.remove_empty(&retired.forwarded);
            drop(retired);
        }
        {
            let mut actors = self
                .reconnects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.retiring.load(std::sync::atomic::Ordering::Acquire) || delivery.failed() {
                return Err(failure(Reason::Revoked));
            }
            if actors.len() >= self.max_calls.get() {
                return Err(failure(Reason::Capacity));
            }
            #[cfg(feature = "native-packet-origin")]
            actors
                .try_push(actor.clone(), &metadata.memory, self.max_calls.get())
                .map_err(|(source, _)| memory::memory_error(source))?;
            #[cfg(not(feature = "native-packet-origin"))]
            actors.push(actor.clone());
        }
        drop(calls);
        let (admit, admitted) = tokio::sync::oneshot::channel();
        let completion = Returning(actor.clone());
        let registered = tasks.spawn_tracked(TaskClass::Auxiliary, async move {
            let _completion = completion;
            let Ok(witness) = admitted.await else {
                return Ok(());
            };
            make(witness).await
        });
        match registered {
            Ok((id, witness)) => {
                actor
                    .witness
                    .set(witness.clone())
                    .unwrap_or_else(|_| unreachable!("one original reconnect registration"));
                // The original actor and full witness are retained before the
                // gate can poll the real reconnect body or invoke an IO waker.
                let _ = admit.send(witness.clone());
                Ok((id, witness))
            }
            Err(source) => {
                let removed = {
                    let mut actors = self
                        .reconnects
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    actors
                        .iter()
                        .position(|actual| ReconnectRef::ptr_eq(actual, &actor))
                        .map(|index| actors.remove(index))
                };
                if let Some(old) = actor.failure_delivery.get() {
                    old.remove_empty_actor(&actor.failures);
                }
                self.source_failures.remove_empty(&actor.forwarded);
                drop(removed);
                let reason = match source {
                    crate::contract::scope::SpawnError::Capacity
                    | crate::contract::scope::SpawnError::IdentityExhausted => Reason::Capacity,
                    _ => Reason::Revoked,
                };
                Err(failure(reason).with_source(source))
            }
        }
    }
}

impl PacketDriverCalls {
    pub(super) fn receive_actor_failures(&self) -> Option<ChannelError> {
        let mut first_unreceived = None;
        let mut first_retained = None;
        for index in 0..self.max_calls.get() {
            let actor = self
                .reconnects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .cloned();
            let Some(actor) = actor else { break };
            let Ok(_observing) = actor.observing_failure.try_lock() else {
                continue;
            };
            let Some(witness) = actor.witness.get() else {
                continue;
            };
            let Some(joined) = witness.try_joined() else {
                continue;
            };
            let Err(source) = joined.result() else {
                continue;
            };
            // Exact Scope result Arc is in the stable bounded metadata before
            // hosted failure acceptance. Host registration ACK is independent.
            actor
                .delivery()
                .retain_actor(&actor.failures.result, source);
            if !joined.failure_accepted() && !actor.failures.acceptance.failed() {
                if let Err(source) = witness.acknowledge_hosted_failure(&joined) {
                    actor.delivery().retain_actor(
                        &actor.failures.acceptance,
                        Arc::new(failure(Reason::Internal).with_source(source)),
                    );
                }
            }
            if let Some(report) = actor.fault_report() {
                let unreceived = !actor.fault_received();
                let report = self.source_failures.receive(&actor.forwarded, report);
                if unreceived && first_unreceived.is_none() {
                    first_unreceived = Some(report);
                } else if first_retained.is_none() {
                    first_retained = Some(report);
                }
            }
        }
        // A previously accepted actor fault cannot starve a different actor's
        // actual late failure/receipt. Both remain strongly retained and fatal
        // for admission; this changes delivery ordering only, never ACK proof.
        first_unreceived.or(first_retained)
    }
}

/// Body completion only seals admission during the actual join publication
/// window. It is never a joined/ACK/native/IO-release observation.
struct Returning(ReconnectRef);
impl Drop for Returning {
    fn drop(&mut self) {
        self.0
            .accepting
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
