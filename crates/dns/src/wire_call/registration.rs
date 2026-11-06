//! Cold admission belongs to the one selected leader, never a caller's Drop.
use super::*;
use crate::native::NativeDnsProvider;
/// The original query registration is retained independently of Flight's result.
/// Before spawn completes Pending remains pending, even if its caller drops.
#[derive(Default)]
pub(crate) struct Registration {
    pub(super) admission: OnceLock<Admission>,
    pub(super) changed: Notify,
}
impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsFlightRegistration")
            .field("admitted", &self.admission.get().is_some())
            .finish_non_exhaustive()
    }
}
pub(super) enum Admission {
    NoWorker,
    Hosted {
        query: TaskJoinWitness,
        provider: Arc<dyn NativeDnsProvider>,
    },
    Unowned,
}
impl Registration {
    pub(crate) fn hosted(&self, query: TaskJoinWitness, tasks: &crate::OwnedDnsQueryTasks) {
        let admission = match tasks.cleanup_provider(&query) {
            Some(provider) => Admission::Hosted { query, provider },
            None => Admission::Unowned,
        };
        assert!(
            self.admission.set(admission).is_ok(),
            "one original Query registration"
        );
        self.changed.notify_waiters();
    }
    pub(crate) fn no_worker(&self) {
        assert!(
            self.admission.set(Admission::NoWorker).is_ok(),
            "one failed Query admission"
        );
        self.changed.notify_waiters();
    }
    pub(crate) fn unowned(&self) {
        assert!(
            self.admission.set(Admission::Unowned).is_ok(),
            "one explicit library worker"
        );
        self.changed.notify_waiters();
    }
}

pub(crate) struct Preparing {
    original: Arc<Registration>,
    cold: bool,
}
impl Registration {
    pub(crate) fn preparing(self: &Arc<Self>) -> Preparing {
        Preparing {
            original: self.clone(),
            cold: true,
        }
    }
}
impl Preparing {
    pub(crate) fn submitting(mut self) {
        self.cold = false;
    }
}
impl Drop for Preparing {
    fn drop(&mut self) {
        // The only leader was dropped before entering synchronous registration.
        // No worker/start gate has been submitted and no follower may submit it.
        if self.cold {
            self.original.no_worker();
        }
    }
}
