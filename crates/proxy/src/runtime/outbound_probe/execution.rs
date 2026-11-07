//! Accepted diagnostic work on the actual host's original Direct Scope.
use super::*;
use crate::transport::extract_tcp_stream;
use std::time::{Duration, Instant};
use tokio::time::timeout_at;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;
use zero_traits::AsyncSocket;
impl OutboundProbeRuntime {
    pub(super) async fn run_accepted_probe(
        &self,
        target_id: TargetId,
        request: OutboundProbeRequest,
        intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
        credit: zero_runtime::ProbeAdmission,
        original: Arc<ProbeOriginal>,
    ) -> Result<u64, OutboundProbeError> {
        if original.cancelled.load(Ordering::Acquire) {
            return Err(cancelled_probe());
        }
        let Some((resolved, _plan)) = self.resolve_target_id(target_id) else {
            return Err(OutboundProbeError::new(
                "target_resolution_failed",
                "failed to resolve outbound probe target",
            ));
        };
        if matches!(resolved, ResolvedOutbound::Relay { .. }) {
            return Err(OutboundProbeError::new(
                "unsupported_target",
                "relay chain cannot be used as an outbound latency probe target",
            ));
        }
        let runtime = self.clone();
        let host = self.services.protocols().runtime_host();
        let config = self.services.snapshot().config().clone();
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.probe_deadline().timeout_ms);
        let purpose = if intent == crate::runtime::tcp_dispatch::TcpDispatchIntent::PolicyProbe {
            zero_core::request::ServicePurpose::HealthProbe
        } else {
            zero_core::request::ServicePurpose::DiagnosticProbe
        };
        host.run_probe(
            &config,
            None,
            credit,
            purpose,
            deadline,
            move |call| async move {
                original.install(call.clone());
                // The original task must not retain the publication that now
                // observes that same task's Entry across its actual IO await.
                drop(original);
                call.check_open().map_err(|source| {
                    OutboundProbeError::from_engine(std::io::Error::other(source).into())
                })?;
                runtime
                    .probe_resolved_outbound(resolved, &request, intent, call)
                    .await
            },
        )
        .await
        .map_err(OutboundProbeError::from_probe_failure)?
    }

    async fn probe_resolved_outbound(
        &self,
        resolved: ResolvedOutbound<'static>,
        request: &OutboundProbeRequest,
        intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
        call: zero_runtime::ProbeCall,
    ) -> Result<u64, OutboundProbeError> {
        if matches!(resolved, ResolvedOutbound::Relay { .. }) {
            return Err(OutboundProbeError::new(
                "unsupported_target",
                "relay chain cannot be used as an outbound latency probe target",
            ));
        }

        let timeout_ms = self.probe_deadline().timeout_ms;
        match timeout_at(call.deadline(), async {
            let started_at = Instant::now();
            let session = Session::new(
                0,
                Address::Domain(request.host.clone()),
                request.port,
                Network::Tcp,
                ProtocolType::UNKNOWN,
            );
            let outbound = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
                self.services.clone().with_service_invocation(
                    crate::component::direct_bridge::ServiceInvocation {
                        deadline: call.deadline(),
                        caller: Some(call),
                        purpose: if intent
                            == crate::runtime::tcp_dispatch::TcpDispatchIntent::PolicyProbe
                        {
                            zero_core::request::ServicePurpose::HealthProbe
                        } else {
                            zero_core::request::ServicePurpose::DiagnosticProbe
                        },
                    },
                ),
                &session,
                resolved,
                zero_engine::RouteMode::Auto,
                intent,
            )
            .await
            .map_err(|failure| failure.error)?;
            let result = extract_tcp_stream(outbound)?;
            let mut socket = result.upstream;
            socket
                .write_all(request.request.as_bytes())
                .await
                .map_err(EngineError::from)?;

            let mut response = [0_u8; 1];
            let read = socket
                .read(&mut response)
                .await
                .map_err(EngineError::from)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "outbound probe target closed without an HTTP response",
                )
                .into());
            }
            if intent == crate::runtime::tcp_dispatch::TcpDispatchIntent::PolicyProbe
                && !result.is_direct
            {
                self.services.engine().record_outbound_success_in_snapshot(
                    self.services.snapshot(),
                    &result.outbound_tag,
                );
            }
            Ok(started_at.elapsed().as_millis() as u64)
        })
        .await
        {
            Ok(result) => result.map_err(OutboundProbeError::from_engine),
            Err(_) => Err(OutboundProbeError::new(
                "probe_timeout",
                format!("outbound latency probe timed out after {timeout_ms} ms"),
            )),
        }
    }
}
