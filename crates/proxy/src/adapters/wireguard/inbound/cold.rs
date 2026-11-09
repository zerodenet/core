//! Cold listener selection. Installation alone claims the original executor.
use super::device::ListenerLease;
use super::*;

#[derive(Clone)]
pub(in crate::adapters::wireguard) enum Binding {
    Standalone,
    Ready(Arc<LinkedEndpoint>),
    Pending(Arc<super::super::lifecycle::PendingLinkedEndpoint>),
}
impl Binding {
    pub(super) fn ready(&self) -> Result<Option<Arc<LinkedEndpoint>>, EngineError> {
        match self {
            Self::Standalone => Ok(None),
            Self::Ready(linked) => Ok(Some(linked.clone())),
            Self::Pending(pending) => pending
                .ready()
                .map(Some)
                .ok_or_else(|| invalid("WireGuard linked dependency has not completed BuildNew")),
        }
    }
}

pub(crate) struct Options {
    pub(in crate::adapters::wireguard) inbound: InboundConfig,
    pub(in crate::adapters::wireguard) live: Arc<LiveInboundDevice>,
    pub(in crate::adapters::wireguard) binding: Binding,
    spec: zero_platform_tokio::contract::component::ComponentSpec,
    outer_network: Option<std::sync::Weak<crate::component::background::BackgroundNetwork>>,
    pub(in crate::adapters::wireguard) recovery: Arc<dyn crate::protocol_registry::InboundRecovery>,
}
impl Options {
    pub(in crate::adapters::wireguard) fn new_with_limits(
        inbound: &InboundConfig,
        live: Arc<LiveInboundDevice>,
        binding: Binding,
        pending: super::super::PendingEndpointMap,
        limits: crate::runtime::inbound_operation::raw_ip::RawIpSourceLimits,
    ) -> Result<Self, EngineError> {
        let recovery = Arc::new(super::recovery::Recovery::new(
            inbound,
            live.clone(),
            binding.clone(),
            pending,
            limits,
        )?);
        // Validate cold protocol input without mutating the current crypto owner.
        prepare_device(inbound)?;
        // The same actual cold graph supplies its immutable formatter family
        // and exact byte workspaces before any Source listener bind attempts.
        let supplemental = match &binding {
            Binding::Standalone => Default::default(),
            Binding::Pending(actual) => actual.source_supplemental_memory()?,
            Binding::Ready(actual) => match actual.owned_resources() {
                Some(resources) => resources.source_supplemental_memory()?,
                None => Default::default(), // existing explicit pre-canonical owner
            },
        };
        let outer_tasks = match &binding {
            Binding::Standalone => 0,
            Binding::Pending(actual) => actual.source_outer_tasks()?,
            Binding::Ready(actual) => match actual.owned_resources() {
                Some(actual) => actual.source_outer_tasks()?,
                None => 0,
            },
        };
        let outer_network = match &binding {
            Binding::Standalone => None,
            Binding::Pending(actual) => actual.source_outer_network()?,
            Binding::Ready(actual) => match actual.owned_resources() {
                Some(actual) => actual.source_outer_network()?,
                None => None, // original explicit pre-canonical library owner
            },
        };
        let mut spec = super::super::controlled_bind::spec_with_limits(inbound, limits)?;
        if outer_tasks != 0 {
            // These are the old original reader and original N=1 reconnect
            // futures per configured endpoint. No additional worker or marker
            // is created; both now consume their one actual Source registrar.
            let actual = spec
                .task_limit
                .map(|limit| limit.get())
                .unwrap_or(0)
                .checked_add(outer_tasks)
                .and_then(std::num::NonZeroUsize::new)
                .ok_or_else(|| invalid("actual Source task declaration overflow"))?;
            spec.task_limit = Some(actual);
        }

        let Some(
            zero_platform_tokio::contract::component::ResourceDeclaration::PacketDatagramReads {
                supplemental: actual,
                ..
            },
        ) = spec.resources.last_mut()
        else {
            return Err(invalid(
                "actual Packet Source has no read-memory declaration",
            ));
        };
        *actual = supplemental;
        Ok(Self {
            spec,
            outer_network,
            inbound: inbound.clone(),
            live,
            binding,
            recovery,
        })
    }
}
pub(crate) struct Factory;
impl zero_platform_tokio::contract::component::ComponentFactory for Factory {
    type Options = Options;
    fn describe(
        &self,
        options: &Options,
    ) -> zero_platform_tokio::contract::component::ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: zero_platform_tokio::contract::component::BuildContext,
    ) -> Result<
        zero_platform_tokio::contract::component::ComponentInstance,
        zero_platform_tokio::contract::lifecycle::BuildError,
    > {
        use zero_platform_tokio::contract::{
            component::*, inbound::erase_inbound, lifecycle::erase_lifecycle,
        };
        let construct = || -> Result<_, EngineError> {
            let linked = options.binding.ready()?;
            if let Some(linked) = &linked {
                if !Arc::ptr_eq(&linked.live, &options.live)
                    || linked
                        .state
                        .lock()
                        .unwrap()
                        .devices
                        .iter()
                        .any(|d| !d.is_usable())
                {
                    return Err(invalid("linked dependency changed or retired"));
                }
            }
            let lease = options.live.claim_listener()?;
            let endpoint = linked
                .as_ref()
                .map(|linked| linked.take_endpoint())
                .transpose()?;
            let identity = inbound_protocol_identity(&options.inbound.protocol)
                .ok_or_else(|| invalid("cannot identify inbound"))?;
            if !options.live.matches_identity(identity) {
                options
                    .live
                    .replace(prepare_device(&options.inbound)?, identity);
            }
            let destination = super::super::controlled_bind::destination(&options.inbound)?;
            let (owner, source) = crate::runtime::inbound_operation::raw_ip::source::source(
                Box::new(WireguardInboundDevice(options.live.clone())),
                endpoint,
                destination,
                context,
                options.outer_network.clone(),
            )
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            Ok(ComponentInstance {
                lifecycle: erase_lifecycle(Owner {
                    inner: owner,
                    lease: Some(lease),
                }),
                roles: ComponentRoles {
                    inbound: Some(erase_inbound(source)),
                    ..Default::default()
                },
            })
        };
        construct().map_err(|source| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(source)
        })
    }
}
struct Owner {
    inner: crate::runtime::inbound_operation::raw_ip::source::Owner,
    lease: Option<ListenerLease>,
}
impl zero_platform_tokio::contract::lifecycle::ResourceLifecycle for Owner {
    async fn start(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.inner.start(deadline).await
    }
    fn revoke(&self, reason: zero_platform_tokio::contract::CloseReason) {
        self.inner.revoke(reason);
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<
        zero_platform_tokio::contract::lifecycle::CloseReport,
        zero_platform_tokio::contract::ChannelError,
    > {
        let report = self.inner.shutdown(deadline).await?;
        if report.is_complete() {
            self.lease.take();
        }
        Ok(report)
    }
}
