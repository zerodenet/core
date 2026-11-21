//! Opaque identity from the actual installed accepted handler, never a witness setter.
use super::*;
impl TcpRuntimeServices {
    /// Restore the exact original UDP flow invocation. This copies no plan or
    /// configuration, and proves no new admission or native permission.
    pub(crate) fn resume_business_identity(
        mut self,
        session: &zero_core::Session,
        original: &crate::component::direct_bridge::Invocation,
    ) -> Result<Self, zero_engine::EngineError> {
        let zero_platform_tokio::contract::context::RequestIdentity::Business(facts) =
            &original.identity
        else {
            return Err(
                std::io::Error::other("retained UDP invocation is not a business request").into(),
            );
        };
        if facts.session.0 != session.id {
            return Err(std::io::Error::other(
                "retained UDP invocation belongs to another session",
            )
            .into());
        }
        original
            .admitted
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::other("retained UDP invocation has no original admission")
            })?
            .check(self.engine(), facts.session)?;
        if let Some(current) = self
            .execution
            .invocation
            .as_ref()
            .filter(|call| call.admitted.is_some())
        {
            if !matches!(&current.identity, zero_platform_tokio::contract::context::RequestIdentity::Business(current) if std::sync::Arc::ptr_eq(current, facts))
            {
                return Err(std::io::Error::other(
                    "UDP continuation cannot replace another admitted invocation",
                )
                .into());
            }
        }
        self.execution.invocation = Some(original.clone());
        Ok(self)
    }
    pub(crate) fn with_accepted_caller(
        mut self,
        caller: Option<zero_platform_tokio::AcceptedCaller>,
    ) -> Result<Self, zero_engine::EngineError> {
        let Some(caller) = caller else {
            return Ok(self);
        };
        caller.check_open().map_err(std::io::Error::other)?;
        let actual = caller.task_join_witness().ok_or_else(|| {
            std::io::Error::other("accepted caller's actual scope registration is unavailable")
        })?;
        let invocation = self
            .execution
            .invocation
            .as_mut()
            .filter(|invocation| invocation.admitted.is_some())
            .ok_or_else(|| {
                std::io::Error::other(
                    "accepted caller requires its original admitted business demand",
                )
            })?;
        #[cfg(feature = "raw-ip-runtime")]
        if invocation.caller_native.is_some() {
            return Err(std::io::Error::other(
                "one admitted demand cannot replace its actual raw caller with an accepted caller",
            )
            .into());
        }
        if invocation.caller_accepted.as_ref().is_some_and(|previous| {
            previous
                .task_join_witness()
                .is_none_or(|previous| !previous.same_registration(&actual))
        }) {
            return Err(std::io::Error::other(
                "admitted demand already belongs to a different actual accepted task",
            )
            .into());
        }
        invocation.caller_accepted = Some(caller);
        Ok(self)
    }
}
