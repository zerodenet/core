//! Active Rvs is an ordinary standard Source borrowing its actual Stream donor.
//! No transport runtime, pool, socket constructor or task owner lives here.
mod loans;
use super::{WorkerAttempt, WorkerDemand};
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{AcceptContext, HandshakeContext, InboundConnectionFacts},
    inbound::{erase_inbound, AcceptedRequest, InboundHandshake, InboundSource, PendingInbound},
    lifecycle::{erase_lifecycle, BuildError},
    network::{CarrierContext, UpstreamTransport},
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
pub struct Options {
    demand: Arc<WorkerDemand>,
    endpoint: Destination,
    identity: Arc<[u8]>,
}
impl Options {
    pub fn from_options(
        input: crate::transport::VlessOutboundOptionsRef<'_>,
        endpoint: Destination,
    ) -> Result<Self, zero_core::Error> {
        let demand = WorkerDemand::from_options(input)?;
        let mut identity = b"vless.active-rvs.standard-source.v1".to_vec();
        identity.extend_from_slice(&demand.uuid);
        identity.extend_from_slice(demand.flow.unwrap_or("").as_bytes());
        identity.extend_from_slice(&demand.backlog.frames().to_le_bytes());
        identity.extend_from_slice(&demand.backlog.bytes().to_le_bytes());
        identity.extend_from_slice(format!("{endpoint:?}").as_bytes());
        Ok(Self {
            demand: Arc::new(demand),
            endpoint,
            identity: blake3::hash(&identity).as_bytes().to_vec().into(),
        })
    }
}
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let inbound = context
            .roles
            .inbound
            .ok_or_else(|| failure(Reason::StalePlan))?;
        let inbound = zero_core::component::RoleInstanceId {
            component: context.component,
            generation: context.generation,
            role: zero_core::component::RoleId::Inbound(inbound),
        };
        if context.dependencies.len() != 1 {
            return Err(failure(Reason::StalePlan));
        }
        let upstream = context
            .background_carrier
            .ok_or_else(|| failure(Reason::Unsupported))?;
        let binding = upstream.binding();
        if binding.prefix.len() != 1
            || binding.prefix[0].component != context.dependencies[0].component
            || binding.prefix[0].generation != context.dependencies[0].generation
            || binding.prefix[0].role
                != zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(
                    context.dependencies[0].component.0,
                ))
        {
            return Err(failure(Reason::StalePlan));
        }
        let loans = Arc::new(loans::Loans::default());
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(loans::Owner {
                loans: loans.clone(),
                cancel: context.cancel.clone(),
            }),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(Source {
                    options,
                    upstream,
                    loans,
                    cancel: context.cancel,
                    inbound,
                    pending: None,
                })),
                ..Default::default()
            },
        })
    }
}
struct Source {
    options: Arc<Options>,
    upstream: UpstreamTransport,
    loans: Arc<loans::Loans>,
    cancel: CancellationToken,
    inbound: zero_core::component::RoleInstanceId,
    // Keep the exact in-progress demand/acquisition when an accept waiter ends.
    pending: Option<BoxFuture<'static, Result<Handshake, ChannelError>>>,
}
impl InboundSource for Source {
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        self.loans.reap().map_err(|source| {
            ChannelError::new(Stage::Close, source.reason, source.impact).with_source(source)
        })?;
        if self.cancel.is_cancelled() {
            return Ok(None);
        }
        if self.pending.is_none() {
            let options = self.options.clone();
            let upstream = self.upstream.clone();
            let loans = self.loans.clone();
            let cancel = self.cancel.clone();
            let inbound = self.inbound;
            self.pending = Some(Box::pin(async move {
                let attempt = options.demand.next().await;
                let loan = upstream
                    .borrow_stream(
                        options.endpoint.clone(),
                        CarrierContext {
                            session: None,
                            deadline: Some(
                                tokio::time::Instant::now() + std::time::Duration::from_secs(15),
                            ),
                            cancel,
                        },
                    )
                    .await?;
                // The actual unique controller is installed before any Rvs I/O,
                // endpoint check or subsequent suspension can occur.
                let original = loan.owner.original().clone();
                loans.install(loan.owner);
                let endpoints = original.endpoints();
                let facts = InboundConnectionFacts {
                    original_source: endpoints.as_ref().map(|facts| facts.peer()),
                    effective_source: endpoints.as_ref().map(|facts| facts.peer()),
                    local_endpoint: endpoints.as_ref().and_then(|facts| facts.local()),
                    sni: None,
                };
                Ok(Handshake {
                    attempt,
                    io: Box::pin(loan.io),
                    facts,
                    inbound,
                    original,
                })
            }));
        }
        let result = tokio::select! { biased;
            _=self.cancel.cancelled()=>return Ok(None),
            _=context.cancel.cancelled()=>return Err(failure(Reason::Cancelled)),
            result=zero_platform_tokio::contract::context::with_deadline(context.deadline,self.pending.as_mut().unwrap())=>result.map_err(|source|failure(Reason::Timeout).with_source(source))?,
        };
        self.pending.take();
        result.map(|pending| {
            let facts = pending.facts.clone();
            let original = pending.original.clone();
            Some(
                PendingInbound::new(pending)
                    .with_carrier_facts(facts)
                    .with_stream_carrier(original),
            )
        })
    }
}
struct Handshake {
    original: zero_platform_tokio::controlled_carrier::CarrierOrigin,
    attempt: WorkerAttempt,
    io: BoxStream,
    facts: InboundConnectionFacts,
    inbound: zero_core::component::RoleInstanceId,
}
impl InboundHandshake for Handshake {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        if context.inbound != self.inbound || context.accepted.is_none() {
            return Err(failure(Reason::StalePlan));
        }
        self.attempt.into_requests(self.io, &context, self.facts)
    }
}
fn failure(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}
