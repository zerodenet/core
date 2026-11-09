//! One pure wire policy in the existing accepted configuration operation.
use super::*;
use crate::runtime::Proxy;
use std::sync::{OnceLock, Weak};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLimitCandidate};
pub(in crate::runtime) type WireCandidate = PacketMetadataArc<WireTransition>;
pub(in crate::runtime) struct WireTransition {
    policy: PacketMetadataLimitCandidate,
    expected: OnceLock<Weak<zero_engine::EngineRuntimeSnapshot>>,
}
impl std::fmt::Debug for WireTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsWireTransition")
            .field("metadata_committed", &self.policy.metadata_committed())
            .finish_non_exhaustive()
    }
}
impl WireTransition {
    pub(in crate::runtime) fn bind_snapshot(
        &self,
        snapshot: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Result<(), String> {
        if self.policy.target()
            != zero_runtime::HostOriginalBudgets::dns_wire_limits(&snapshot.config().runtime)
                .map_err(|error| error.to_string())?
        {
            return Err("DNS wire candidate differs from accepted target".into());
        }
        if let Err(snapshot) = self.expected.set(Arc::downgrade(snapshot)) {
            if !self
                .expected
                .get()
                .is_some_and(|same| Weak::ptr_eq(same, &snapshot))
            {
                return Err("DNS wire candidate already bound to another actual snapshot".into());
            }
        }
        Ok(())
    }
    pub(in crate::runtime) fn bind_plan(
        &self,
        snapshot: &Arc<zero_engine::EngineRuntimeSnapshot>,
        plan: &Arc<crate::component::catalog::ComponentPlan>,
    ) -> Result<(), String> {
        if !self
            .expected
            .get()
            .is_some_and(|same| Weak::ptr_eq(same, &Arc::downgrade(snapshot)))
        {
            return Err("DNS wire allocation plan lacks its exact accepted snapshot".into());
        }
        plan.bind_dns_wire_candidate(self.policy.issuer())
            .map_err(|error| error.to_string())
    }
    pub(in crate::runtime) fn rollback(&self) {
        self.policy.rollback();
    }
    pub(in crate::runtime) fn policy(&self) -> &PacketMetadataLimitCandidate {
        &self.policy
    }
}
impl ProxyHandle {
    pub(super) fn prepare_wire_transition(
        &self,
        target: &RuntimeConfig,
    ) -> Result<Option<WireCandidate>, String> {
        let originals = self.proxy.protocols.runtime_host().original_budgets();
        let limits = zero_runtime::HostOriginalBudgets::dns_wire_limits(&target.runtime)
            .map_err(|error| error.to_string())?;
        if originals.dns_wire.snapshot().limit == limits {
            return Ok(None);
        }
        let memory = originals.packet.driver_metadata_view();
        let header = memory
            .reserve(
                zero_traits::packet_metadata::PacketMetadataCounter::candidate_layout()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        let policy =
            originals
                .dns_wire
                .prepare_limit(limits, header)
                .map_err(|(error, header)| {
                    drop(header);
                    error.to_string()
                })?;
        let header = memory
            .reserve(
                PacketMetadataArc::<WireTransition>::layout().map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        PacketMetadataArc::from_prepaid(
            WireTransition {
                policy,
                expected: OnceLock::new(),
            },
            header,
        )
        .map(Some)
        .map_err(|(error, value, header)| {
            drop(value);
            drop(header);
            error.to_string()
        })
    }
}
impl Proxy {
    pub(in crate::runtime) fn pending_wire_transition(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Option<WireCandidate> {
        self.host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .filter(|pending| Arc::ptr_eq(&pending.expected, expected))
            .and_then(|pending| pending.wire.clone())
    }
    pub(in crate::runtime) fn rollback_pending_dns_wire(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) {
        let wire = self.pending_wire_transition(expected);
        if let Some(wire) = wire {
            wire.rollback();
        }
    }
}
