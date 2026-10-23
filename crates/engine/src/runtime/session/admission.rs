//! One authoritative policy/hook/device/quota and registry admission path.
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

impl Engine {
    /// Existing callers keep allocating here. A supplied Session.id is never
    /// accepted as registration authority by this compatibility entry point.
    pub fn prepare_session(
        &self,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<(), EngineError> {
        self.prepare_session_admitted(session, inbound_tag)
            .map(|_| ())
    }

    /// Same policy and registry operation, returning sealed successful authority.
    pub fn prepare_session_admitted(
        &self,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<crate::AdmittedSession, EngineError> {
        session.id = self.allocate_session_id()?.0;
        self.admit_session(session, inbound_tag)
    }

    pub fn check_admitted_session(
        &self,
        admitted: &crate::AdmittedSession,
        id: zero_core::component::SessionId,
    ) -> Result<(), EngineError> {
        admitted.check_issuer(&self.next_session_id, id)
    }

    /// Projection of an already admitted demand, including its cancellation.
    /// This checks sealed origin/ID only and grants no new live acquisition.
    pub fn check_admitted_session_origin(
        &self,
        admitted: &crate::AdmittedSession,
        id: zero_core::component::SessionId,
    ) -> Result<(), EngineError> {
        admitted.check_origin(&self.next_session_id, id)
    }

    pub(super) fn admit_session(
        &self,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<crate::AdmittedSession, EngineError> {
        session.inbound_tag = Some(inbound_tag.to_owned());
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mode = self.mode.lock().unwrap_or_else(|error| error.into_inner());
        self.principal_policies.validate(session.auth.as_ref())?;
        let hook = self
            .flow_hook
            .read()
            .expect("flow hook lock poisoned")
            .clone();
        if let Some(hook) = hook {
            let context = FlowContext::from_session(session, mode.kind(), now_ms);
            if let Err(reason) = hook.on_flow_start(&context) {
                tracing::warn!(flow_id = session.id, reason = %reason.message, "flow blocked by hook");
                return Err(EngineError::AdmissionDenied {
                    reason: reason.message,
                });
            }
        }
        let device_registration =
            self.acquire_principal_device(session.auth.as_ref(), session.source_ip.as_ref())?;
        let quota_registration = self.principal_quotas.acquire(session.auth.as_ref())?;
        let inserted = self.session_registry.insert(
            session,
            mode.kind(),
            device_registration,
            quota_registration,
        );
        self.session_registry.bind_traffic(
            session.id,
            false,
            self.traffic.meter(&zero_api::TrafficScope::Inbound {
                tag: inbound_tag.into(),
            }),
        );
        self.session_registry.bind_endpoint_traffic(
            session.id,
            false,
            self.endpoint_role_meter(inbound_tag, false),
        );
        if let Some(peer) = &session.inbound_peer_identity {
            self.bind_session_peer(session.id, inbound_tag, false, peer);
        }
        self.stats.record_start();
        self.event_log
            .push_flow_started(&inserted.active, inserted.principal_observation.as_ref());
        Ok(inserted.admission.receipt(
            zero_core::component::SessionId(session.id),
            &self.next_session_id,
        ))
    }
}
