//! Immediate handoff to the actual Scope's existing lifecycle receiver.
use super::*;
use zero_platform_tokio::contract::{
    component::ResourceAccess, lifecycle::ResourceFailure, scope::TaskScopeIdentity,
};
#[derive(Clone)]
pub(in crate::component::native_dns::provider) struct Receiver {
    resources: std::sync::Weak<dyn ResourceAccess>,
    scope: TaskScopeIdentity,
}
impl Receiver {
    pub(super) fn receive_report(&self, report: &mut CloseReport) -> std::io::Result<()> {
        let resources = self.resources.upgrade().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "original DNS fault receiver retired",
            )
        })?;
        resources
            .receive_protocol_report(&self.scope, report)
            .map_err(std::io::Error::other)?;
        Ok(())
    }
    pub(super) fn retain_error(&self, source: std::io::Error) -> std::io::Result<std::io::Error> {
        if retirement::pending_observation(&source) {
            return Ok(source);
        }
        let kind = source.kind();
        let original = Arc::new(error(source));
        let result = self
            .resources
            .upgrade()
            .ok_or_else(|| {
                crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Revoked,
                    "original DNS fault receiver retired",
                )
            })
            .and_then(|resources| resources.receive_protocol_failure(&self.scope, &original));
        result.map_err(|receiver| {
            std::io::Error::other(
                BuildError::retained(original.clone()).with_source(Unreceived {
                    original: original.clone(),
                    _receiver: Arc::new(receiver),
                }),
            )
        })?;
        Ok(std::io::Error::new(kind, original))
    }
}

struct Unreceived {
    original: Arc<BuildError>,
    _receiver: Arc<BuildError>,
}
impl std::fmt::Debug for Unreceived {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnreceivedDnsCleanup")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for Unreceived {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual DNS cleanup fault has no matching Scope receiver")
    }
}
impl std::error::Error for Unreceived {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original.as_ref())
    }
}
impl Provider {
    pub(in crate::component::native_dns) fn receive_cleanup_failure(
        &self,
        source: BuildError,
    ) -> Result<Arc<ResourceFailure>, BuildError> {
        let original = Arc::new(source);
        self.resources
            .receive_protocol_failure(&self.registrar.scope_identity(), &original)
            .map_err(|receiver| {
                BuildError::retained(original.clone()).with_source(Unreceived {
                    original,
                    _receiver: Arc::new(receiver),
                })
            })
    }
    /// Return the actual input error, not the canonical RF/source. Feeding a
    /// canonical receiver back through a query TaskFailure would form a cycle.
    pub(super) fn retain_io_cleanup_failure(
        &self,
        source: std::io::Error,
    ) -> std::io::Result<std::io::Error> {
        self.cleanup_receiver().retain_error(source)
    }
    pub(super) fn cleanup_receiver(&self) -> Receiver {
        Receiver {
            resources: Arc::downgrade(&self.resources),
            scope: self.registrar.scope_identity(),
        }
    }
}
