//! The actual Host's durable declaration close proof, without Scope ownership.
use super::*;
use zero_platform_tokio::contract::{component::ResourceAccess, scope::TaskJoinWitness};
#[derive(Clone, Default)]
pub(in crate::transport::plugin::outbound) struct Retirement {
    #[cfg(any(target_os = "linux", windows))]
    original: Option<Arc<dyn zero_platform_tokio::owned_process::ProcessEndpointRetirement>>,
}
impl std::fmt::Debug for Retirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActualProcessRetirement")
            .finish_non_exhaustive()
    }
}
impl Retirement {
    pub(super) fn declared(
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        ordinal: Option<usize>,
        resources: &dyn ResourceAccess,
    ) -> io::Result<Self> {
        #[cfg(any(target_os = "linux", windows))]
        {
            let original=ordinal.map(|ordinal|resources.process_endpoint_retirement(ordinal).ok_or_else(||conflict("actual Process endpoint has no durable original retirement observer"))).transpose()?;
            if original.as_ref().is_some_and(|original| {
                original.instance() != instance
                    || original.process_ordinal() != 0
                    || !original.scope_identity().matches(tasks)
            }) {
                return Err(conflict("actual Process retirement observer differs from this declared instance and Scope"));
            }
            Ok(Self { original })
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = (instance, tasks, resources);
            if ordinal.is_some() {
                return Err(conflict("native Process endpoint provider is unavailable"));
            }
            Ok(Self {})
        }
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        #[cfg(any(target_os = "linux", windows))]
        {
            match (&self.original, &other.original) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = other;
            true
        }
    }
    pub(super) fn closed(&self, actor: Option<&TaskJoinWitness>) -> Option<bool> {
        #[cfg(any(target_os = "linux", windows))]
        {
            self.original.as_ref().map(|original| {
                original.originals_closed()
                    && actor.is_none_or(|actor| {
                        original
                            .scope_identity()
                            .same_scope(&actor.scope_identity())
                    })
            })
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            let _ = actor;
            None
        }
    }
}

impl PluginPlan {
    pub(super) fn confirmed_retired(&self) -> bool {
        if !self.cancel.is_cancelled() {
            return false;
        }
        let actor = self.actor.lock().unwrap().clone();
        if actor.as_ref().is_some_and(|actor| {
            !actor.try_joined().is_some_and(|joined| {
                actor.matches_joined(&joined) && joined.registration_released()
            })
        }) {
            return false;
        }
        let retirement = self.retirement.lock().unwrap().clone();
        if let Some(closed) = retirement.closed(actor.as_ref()) {
            return closed;
        }
        #[cfg(any(target_os = "linux", windows))]
        {
            if let Some(opening) = self.pending_endpoint.lock().unwrap().as_ref() {
                return opening
                    .endpoint
                    .closed()
                    .is_some_and(|closed| closed.matches(&opening.endpoint))
                    && opening.process.as_ref().is_some_and(|process| {
                        process
                            .closed()
                            .is_some_and(|closed| closed.matches(process))
                    });
            }
            let Ok(running) = self.running.try_lock() else {
                return false;
            };
            return match running.as_ref() {
                Some(lease) => {
                    lease.child.lock().unwrap().owned().is_some_and(|process| {
                        process
                            .closed()
                            .is_some_and(|closed| closed.matches(&process))
                    }) && lease.process_endpoint.as_ref().is_none_or(|endpoint| {
                        endpoint
                            .closed()
                            .is_some_and(|closed| closed.matches(endpoint))
                    })
                }
                None => actor.is_none(),
            };
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        self.untouched()
    }
}
