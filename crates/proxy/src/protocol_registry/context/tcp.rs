mod execution;
use super::{OutboundAdapterContext, UpstreamConnectServices};
use crate::inventory::ProtocolInventory;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;
pub(crate) use execution::TcpExecutionServices;
use std::sync::Arc;
use zero_dns::DnsSystem;
use zero_engine::{Engine, EngineRuntimeSnapshot};

#[derive(Clone)]
pub(crate) struct TcpRuntimeServices {
    execution: TcpExecutionServices,
    protocols: ProtocolInventory,
}
impl std::ops::Deref for TcpRuntimeServices {
    type Target = TcpExecutionServices;
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}
impl TcpRuntimeServices {
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn with_io_observer(
        mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        self.execution.upstream = self.execution.upstream.clone().with_observer(observer);
        self
    }

    pub(crate) fn execution(&self) -> TcpExecutionServices {
        self.execution.clone()
    }
    pub(crate) fn new(
        engine: Engine,
        snapshot: Arc<EngineRuntimeSnapshot>,
        resolver: Arc<DnsSystem>,
        protocols: ProtocolInventory,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
        principal_rate_limits: PrincipalRateLimitRegistry,
        retired_datagrams: Arc<crate::component::datagram_retirement::RetiredDatagrams>,
    ) -> Self {
        let components = protocols.prepare_components(snapshot.config().clone());
        Self {
            execution: TcpExecutionServices {
                engine,
                snapshot,
                current_source: std::sync::Weak::new(),
                upstream: UpstreamConnectServices::new(
                    resolver,
                    protocols.direct_connector(),
                    egress_interface,
                ),
                principal_rate_limits,
                retired_datagrams,
                components,
                invocation: None,
                ingress: None,
            },
            protocols,
        }
    }

    pub(crate) fn with_current_service_source(
        mut self,
        source: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
    ) -> Self {
        self.execution.current_source = source;
        self
    }

    pub(crate) fn with_current_snapshot(&self) -> Self {
        let mut services = self.clone();
        services.execution.snapshot = services.engine.runtime_snapshot();
        services.execution.components = services
            .protocols
            .prepare_components(services.execution.snapshot.config().clone());
        services
    }

    pub(crate) fn with_service_invocation(
        mut self,
        invocation: crate::component::direct_bridge::ServiceInvocation,
    ) -> Self {
        self.execution.invocation = Some(invocation.into());
        self
    }

    pub(crate) fn for_inbound(mut self, tag: String) -> Self {
        if self
            .execution
            .ingress
            .as_ref()
            .is_some_and(|identity| identity.tag == tag)
        {
            return self;
        }
        match self.protocols.ingress_identity(tag) {
            Ok(identity) => self.execution.ingress = Some(identity),
            Err(error) => {
                self.execution.components = Err(error);
                self.execution.ingress = None;
            }
        }
        self
    }

    pub(crate) fn with_business_identity(
        mut self,
        session: &zero_core::Session,
    ) -> Result<Self, zero_engine::EngineError> {
        if let Err(error) = &self.execution.components {
            return Err(zero_engine::EngineError::Io(std::io::Error::other(
                crate::component::instance::retained(error.clone()),
            )));
        }
        if self.execution.invocation.is_some() || session.id == 0 {
            return Ok(self);
        }
        if let Some(ingress) = &self.execution.ingress {
            self.execution.invocation = Some(crate::component::direct_bridge::Invocation {
                #[cfg(feature = "raw-ip-runtime")]
                caller_native: None,
                admitted: None,
                identity: zero_platform_tokio::contract::context::RequestIdentity::Business(
                    Arc::new(crate::component::request::facts(session, ingress)),
                ),
                // The existing business caller has per-operation timers, with
                // no absolute total-connect timeout to silently shorten.
                deadline: None,
            });
        }
        Ok(self)
    }

    /// Only the real ingress admission return may bind this invocation. A
    /// compatibility business projection remains without controlled authority.
    pub(crate) fn with_admitted_business_identity(
        self,
        session: &zero_core::Session,
        demand: crate::component::business_demand::AdmittedTcpDemand,
    ) -> Result<Self, zero_engine::EngineError> {
        demand.check(self.engine(), zero_core::component::SessionId(session.id))?;
        let mut services = self.with_business_identity(session)?;
        let invocation = services.execution.invocation.as_mut().ok_or_else(|| {
            zero_engine::EngineError::AdmissionDenied {
                reason: "admitted TCP demand requires its real ingress identity".into(),
            }
        })?;
        if !matches!(&invocation.identity,
            zero_platform_tokio::contract::context::RequestIdentity::Business(facts)
                if facts.session.0 == session.id)
        {
            return Err(zero_engine::EngineError::AdmissionDenied {
                reason: "admitted TCP demand cannot replace another request identity".into(),
            });
        }
        invocation.admitted = Some(demand);
        Ok(services)
    }

    /// Carries the exact existing caller obligation. This grants no native I/O,
    /// selected component membership, registration adoption, or Closed receipt.
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn with_caller_native(
        mut self,
        registration: Option<
            crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration,
        >,
    ) -> Result<Self, zero_engine::EngineError> {
        if let Some(registration) = registration {
            registration.check_open().map_err(std::io::Error::other)?;
            let invocation = self
                .execution
                .invocation
                .as_mut()
                .filter(|invocation| invocation.admitted.is_some())
                .ok_or_else(|| {
                    std::io::Error::other(
                        "native caller association requires the original admitted demand",
                    )
                })?;
            if invocation
                .caller_native
                .as_ref()
                .is_some_and(|previous| !previous.same_slot(&registration))
            {
                return Err(std::io::Error::other(
                    "admitted invocation already belongs to another actual native caller",
                )
                .into());
            }
            invocation.caller_native = Some(registration);
        }
        Ok(self)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn protocols(&self) -> &ProtocolInventory {
        &self.protocols
    }

    pub(crate) fn prepare_tcp_outbound<'a>(
        &'a self,
        resolved: &'a zero_engine::ResolvedOutbound<'a>,
        mode: zero_engine::RouteMode,
    ) -> Result<crate::inventory::PreparedTcpOutbound, crate::transport::TcpOutboundFailure> {
        self.protocols.prepare_tcp_outbound(
            OutboundAdapterContext::new(self.config()),
            resolved,
            mode,
            self.execution
                .invocation
                .as_ref()
                .and(self.execution.components.as_deref().ok()),
        )
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> Result<crate::transport::RelayCarrier, crate::transport::TcpOutboundFailure> {
        crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_carrier(
            self.execution(),
            prepared,
        )
        .await
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_lazy_tcp_relay_carrier<'a>(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a> {
        crate::runtime::tcp_dispatch::relay::prepare_lazy_tcp_relay_carrier(
            self.execution(),
            prepared,
        )
    }
}
