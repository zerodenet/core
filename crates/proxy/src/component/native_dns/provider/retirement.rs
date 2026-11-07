//! Positive caller-native facts are separate from retained typed task failures.
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
impl custody::NativeUse {
    fn native_retirement_complete(&self) -> bool {
        let logical_complete = {
            let Ok(logical) = self.logical.try_lock() else {
                return false;
            };
            logical
                .as_ref()
                .is_none_or(|owner| owner.closed().is_some_and(|closed| closed.matches(owner)))
        };
        logical_complete
            && self
                .original
                .closed()
                .is_some_and(|closed| closed.matches(&self.original))
            && self
                .original
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.original))
    }
}
impl Provider {
    pub(super) fn caller_native_retirement_complete(&self, caller: &TaskJoinWitness) -> bool {
        if !caller
            .scope_identity()
            .same_scope(&self.registrar.scope_identity())
        {
            return false;
        }
        let users = self
            .users
            .lock()
            .expect("DNS users poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        let pending = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        if !users
            .iter()
            .chain(&pending)
            .all(|entry| entry.native_retirement_complete())
        {
            return false;
        }
        #[cfg(feature = "dns")]
        {
            let quic = self
                .quic_users
                .lock()
                .expect("DNS QUIC users poisoned")
                .iter()
                .filter(|entry| entry.task.same_registration(caller))
                .cloned()
                .collect::<Vec<_>>();
            if !quic.iter().all(|entry| entry.native_retirement_complete()) {
                return false;
            }
        }
        true
    }
}

/// Append only real failed registrations to the lifecycle's original report.
/// The archive keeps secondary typed cleanup sources without inventing a task.
#[derive(Debug)]
struct FaultReport {
    original: Arc<ChannelError>,
    _archive: Arc<zero_dns::OwnedDnsQueryFaults>,
}
impl std::fmt::Display for FaultReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.original.as_ref(), f)
    }
}
impl std::error::Error for FaultReport {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original.as_ref())
    }
}
pub(in crate::component::native_dns) fn report_faults(
    faults: &Arc<zero_dns::OwnedDnsQueryFaults>,
    reported: &mut std::collections::BTreeSet<zero_platform_tokio::contract::scope::TaskId>,
    pending: &mut CloseReport,
) {
    for fault in faults.faults() {
        if !reported.insert(fault.joined.id()) {
            continue;
        }
        let original = &fault.error;
        let mut error = ChannelError::new(original.stage, original.reason, original.impact);
        error.component = original.component;
        error.session = original.session;
        error.delivery = original.delivery;
        error.retry = original.retry;
        pending
            .failed_tasks
            .push(zero_platform_tokio::contract::lifecycle::TaskFailure {
                id: fault.joined.id(),
                class: fault.joined.class(),
                error: error.with_source(FaultReport {
                    original: original.clone(),
                    _archive: faults.clone(),
                }),
            });
    }
}
/// Compare actual typed source identities, never messages or error kinds.
pub(in crate::component::native_dns) fn reported_source(
    error: &BuildError,
    faults: &zero_dns::OwnedDnsQueryFaults,
) -> bool {
    fn same(a: &(dyn std::error::Error + 'static), b: &(dyn std::error::Error + 'static)) -> bool {
        if std::ptr::eq(a, b) {
            return true;
        }
        if let (Some(a), Some(b)) = (
            a.downcast_ref::<Arc<ChannelError>>(),
            b.downcast_ref::<Arc<ChannelError>>(),
        ) {
            return Arc::ptr_eq(a, b);
        }
        if let (Some(a), Some(b)) = (
            a.downcast_ref::<Arc<std::io::Error>>(),
            b.downcast_ref::<Arc<std::io::Error>>(),
        ) {
            return Arc::ptr_eq(a, b);
        }
        false
    }
    fn common(
        a: &(dyn std::error::Error + 'static),
        b: &(dyn std::error::Error + 'static),
    ) -> bool {
        let mut left = Some(a);
        while let Some(a) = left {
            let mut right = Some(b);
            while let Some(b) = right {
                if same(a, b) {
                    return true;
                }
                right = b.source();
            }
            left = a.source();
        }
        false
    }
    faults.faults().iter().any(|fault| {
        common(error, fault.error.as_ref())
            || fault
                .cleanup_failure
                .as_ref()
                .is_some_and(|source| common(error, source.as_ref()))
    })
}
impl Provider {
    pub(in crate::component::native_dns) fn native_retirement_complete(&self) -> bool {
        let users = self.users.lock().expect("DNS users poisoned").clone();
        let pending = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .clone();
        if !users
            .iter()
            .chain(&pending)
            .all(|entry| entry.native_retirement_complete())
        {
            return false;
        }
        #[cfg(feature = "dns")]
        {
            let quic = self
                .quic_users
                .lock()
                .expect("DNS QUIC users poisoned")
                .clone();
            if !quic.iter().all(|entry| entry.native_retirement_complete()) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
struct RetirementFailures(Vec<BuildError>);
impl std::fmt::Display for RetirementFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} original DNS retirement failures", self.0.len())
    }
}
impl std::error::Error for RetirementFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source as _)
    }
}
pub(in crate::component::native_dns) fn unreported_failures(
    mut failures: Vec<BuildError>,
) -> BuildError {
    if failures.len() == 1 {
        return failures.pop().expect("one original failure");
    }
    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
        .with_source(RetirementFailures(failures))
}

#[derive(Debug)]
struct PendingDeadline(tokio::time::error::Elapsed);
impl std::fmt::Display for PendingDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}
impl std::error::Error for PendingDeadline {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
pub(in crate::component::native_dns) fn pending_deadline(
    source: tokio::time::error::Elapsed,
) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, PendingDeadline(source))
}
pub(in crate::component::native_dns) fn pending_observation(source: &std::io::Error) -> bool {
    source.get_ref().is_some_and(|actual| {
        actual.is::<zero_dns::DnsRetirementPending>() || actual.is::<PendingDeadline>()
    })
}

#[cfg(test)]
#[path = "retirement/tests.rs"]
mod tests;
