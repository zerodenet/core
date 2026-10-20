use super::*;
use crate::contract::lifecycle::ComponentError;
use std::future::Future;

/// Body output slot of the same actually registered task, not a task/exit ACK.
/// Recording output cannot create or replace a native joined receipt.
pub struct DriverBodySlot<O: DriverKind, B: Send + Sync + 'static>(Arc<Record<O, B>>);
impl<O: DriverKind, B: Send + Sync + 'static> DriverBodySlot<O, B> {
    pub fn origin(&self) -> O {
        self.0.origin
    }
    pub fn record(&self, body: B) -> Arc<B> {
        let body = Arc::new(body);
        let mut stored = self.0.body.lock().unwrap();
        assert!(stored.is_none(), "an actual driver records its body once");
        *stored = Some(body.clone());
        body
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    /// Reserve the existing finite ledger before calling the real registrar.
    /// The supplied body is still the caller's original driver future. Neither
    /// this method nor its output slot fabricates a native TaskId or join.
    pub fn spawn<F>(
        &self,
        origin: O,
        class: TaskClass,
        body: impl FnOnce(DriverBodySlot<O, B>) -> F,
    ) -> Result<TaskId, Arc<DriverRegistrationFailure<O>>>
    where
        F: Future<Output = Result<(), ComponentError>> + Send + 'static,
    {
        let rejection = {
            let mut state = self.shared.state.lock().unwrap();
            if state.retired {
                Some(DriverRegistrationReason::Retired)
            } else if state.records.len() >= self.shared.limit.get() - state.pending {
                Some(DriverRegistrationReason::LocalCapacity)
            } else {
                state.pending += 1;
                None
            }
        };
        if let Some(reason) = rejection {
            self.shared.reject(Some(origin), reason);
            return Err(self.shared.state.lock().unwrap().rejected.clone().unwrap());
        }
        let record = Arc::new(Record {
            origin,
            registration: Mutex::new(None),
            body: Mutex::new(None),
            received: Mutex::new(None),
            delivered: AtomicBool::new(false),
        });
        // If an unknown panic interrupts before a real registration returns,
        // pending remains charged: there is no invented ID, exit or refund.
        let callbacks = self.shared.registrar.defer_callbacks();
        let registration = self
            .shared
            .registrar
            .spawn_tracked(class, body(DriverBodySlot(record.clone())));
        let result = match registration {
            Ok((id, witness)) => {
                *record.registration.lock().unwrap() = Some((id, witness));
                let mut state = self.shared.state.lock().unwrap();
                state.pending -= 1;
                assert!(state.records.insert(id, record).is_none());
                Ok(id)
            }
            Err(source) => {
                let mut state = self.shared.state.lock().unwrap();
                state.pending -= 1;
                state.retired = true;
                let failure = state.rejected.get_or_insert_with(|| {
                    Arc::new(DriverRegistrationFailure {
                        origin: Some(origin),
                        reason: DriverRegistrationReason::Scope(source),
                    })
                });
                Err(failure.clone())
            }
        };
        // The exact actual ID/witness (or actual refusal) is durable before any
        // hosted callback, arbitrary wake or panic can re-enter this ledger.
        drop(callbacks);
        self.shared.changed.notify_waiters();
        result
    }
}
