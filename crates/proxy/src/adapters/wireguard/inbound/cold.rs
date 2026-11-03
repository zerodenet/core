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
    pub(in crate::adapters::wireguard) recovery: Arc<dyn crate::protocol_registry::InboundRecovery>,
}
impl Options {
    pub(in crate::adapters::wireguard) fn new(
        inbound: &InboundConfig,
        live: Arc<LiveInboundDevice>,
        binding: Binding,
        pending: super::super::PendingEndpointMap,
    ) -> Result<Self, EngineError> {
        let recovery = Arc::new(super::recovery::Recovery::new(
            inbound,
            live.clone(),
            binding.clone(),
            pending,
        )?);
        // Validate cold protocol input without mutating the current crypto owner.
        prepare_device(inbound)?;
        Ok(Self {
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
        super::super::controlled_bind::spec(&options.inbound)
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
            );
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
