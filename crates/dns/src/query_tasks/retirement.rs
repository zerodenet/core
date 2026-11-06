//! Durable observations of the same registrations in the existing task budget.
//! No task, native authority or separate cleanup executor is stored here.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
pub(crate) struct Retirement {
    pub(super) registration: Arc<OnceLock<Option<QueryRegistration>>>,
    pub(super) capacity: Arc<Mutex<Option<OwnedSemaphorePermit>>>,
    ancestors: Vec<TaskJoinWitness>,
    finalizer: Option<std::sync::Weak<dyn crate::native::NativeDnsProvider>>,
    native_returned: AtomicBool,
    cleanup_failure: OnceLock<Arc<io::Error>>,
}
impl Retirement {
    pub(super) fn new(
        kind: TaskKind,
        ancestors: Vec<TaskJoinWitness>,
        capacity: OwnedSemaphorePermit,
        finalizer: Option<std::sync::Weak<dyn crate::native::NativeDnsProvider>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registration: Arc::new(OnceLock::new()),
            capacity: Arc::new(Mutex::new(Some(capacity))),
            ancestors,
            finalizer,
            native_returned: AtomicBool::new(kind == TaskKind::Blocking),
            cleanup_failure: OnceLock::new(),
        })
    }
    pub(super) fn retain_cleanup_failure(&self, source: io::Error) -> Arc<io::Error> {
        self.cleanup_failure
            .get_or_init(|| Arc::new(source))
            .clone()
    }
    pub(crate) fn cleanup_failure(&self) -> Option<Arc<io::Error>> {
        self.cleanup_failure.get().cloned()
    }
    pub(super) fn returned_native(&self) {
        self.native_returned.store(true, Ordering::Release);
    }
    fn native_complete(&self) -> bool {
        if self.native_returned.load(Ordering::Acquire) {
            return true;
        }
        let Some(Some(original)) = self.registration.get() else {
            return self.registration.get().is_some();
        };
        if !original.witness.is_joined() {
            return false;
        }
        if self
            .finalizer
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|provider| provider.query_native_retirement_complete(&original.witness))
        {
            self.returned_native();
            return true;
        }
        false
    }
    pub(super) fn release_joined(&self) -> bool {
        let Some(registration) = self.registration.get() else {
            return false;
        };
        let joined = match registration {
            None => None,
            Some(original) => match original.witness.try_joined() {
                Some(joined) => Some(joined),
                None => return false,
            },
        };
        let returned = self
            .capacity
            .lock()
            .expect("DNS task capacity poisoned")
            .take();
        drop(returned);
        // Standalone tasks keep their explicit lifecycle. Hosted failure
        // metadata survives original Record reaping until actual native cleanup
        // and failure reception; expired Weak/owner Drop never proves closure.
        joined.as_ref().is_none_or(|joined| {
            self.finalizer.is_none()
                || self.native_complete()
                    && (joined.result().is_ok() || joined.registration_released())
        })
    }
}
impl OwnedDnsQueryTasks {
    pub(crate) fn native_retirement_complete(
        &self,
        root: &Arc<Retirement>,
        query: &TaskJoinWitness,
    ) -> bool {
        if self
            .0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .identity_fault
        {
            return false;
        }
        if !root
            .registration
            .get()
            .and_then(Option::as_ref)
            .is_some_and(|original| original.witness.same_registration(query))
        {
            return false;
        }
        let mut originals = self
            .0
            .budget
            .retained
            .lock()
            .expect("DNS budget poisoned")
            .clone();
        if !originals.iter().any(|actual| Arc::ptr_eq(actual, root)) {
            originals.push(root.clone());
        }
        originals
            .iter()
            .filter(|original| {
                Arc::ptr_eq(original, root)
                    || original
                        .ancestors
                        .iter()
                        .any(|parent| parent.same_registration(query))
            })
            .all(|original| {
                original.registration.get().is_some_and(|actual| {
                    actual
                        .as_ref()
                        .is_none_or(|actual| actual.witness.is_joined())
                }) && original.native_complete()
            })
    }
    /// Observe only the same provider's actual native-ledger evidence. This
    /// does not cancel, join, acknowledge or close anything.
    pub fn observe_native_retirements(&self) {
        let originals = self
            .0
            .budget
            .retained
            .lock()
            .expect("DNS budget poisoned")
            .clone();
        for original in originals {
            let _ = original.native_complete();
        }
    }
}

pub(super) struct Preparing {
    original: Arc<Retirement>,
    cold: bool,
}
impl Retirement {
    pub(super) fn preparing(self: &Arc<Self>) -> Preparing {
        Preparing {
            original: self.clone(),
            cold: true,
        }
    }
}
impl Preparing {
    pub(super) fn submitting(mut self) {
        self.cold = false;
    }
}
impl Drop for Preparing {
    fn drop(&mut self) {
        // Before the actual registrar call: there is neither task nor IO to
        // join. Once submitted, a panic/unknown can never install this fact.
        if self.cold {
            let _ = self.original.registration.set(None);
        }
    }
}

impl std::fmt::Debug for Retirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsOriginalRetirement")
            .field("registered", &self.registration.get().is_some())
            .field(
                "native_returned",
                &self.native_returned.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}
