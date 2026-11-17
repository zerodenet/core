//! Endpoint-free standard roles use the same real accepted reverse workers.
use super::*;
use std::sync::Weak;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::*,
    context::OpenContext,
    datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle},
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_transport::pooled_driver::datagram;
mod datagram_role;
mod stream;
#[derive(Clone)]
pub struct Options {
    registry: crate::reverse::PortalRegistry,
    tag: String,
    identity: Arc<[u8]>,
    max_sources: std::num::NonZeroUsize,
}
impl Options {
    pub fn with_source_identity(mut self, identity: [u8; 32]) -> Self {
        let mut actual = self.identity.to_vec();
        actual.extend_from_slice(&identity);
        self.identity = actual.into();
        self
    }
    pub fn with_source_limit(mut self, limit: std::num::NonZeroUsize) -> Self {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(&limit.get().to_le_bytes());
        self.identity = identity.into();
        self.max_sources = limit;
        self
    }

    pub fn new(registry: crate::reverse::PortalRegistry, tag: &str) -> Self {
        let mut identity = b"vless.reverse.portal.standard.v1".to_vec();
        identity.extend_from_slice(&(tag.len() as u64).to_be_bytes());
        identity.extend_from_slice(tag.as_bytes());
        Self {
            registry,
            tag: tag.into(),
            identity: identity.into(),
            max_sources: std::num::NonZeroUsize::new(8192).unwrap(),
        }
    }
}
pub struct Factory;
struct Outbound {
    portal: Weak<State>,
    admission: Arc<SessionAdmission>,
}
struct Owner {
    portal: Portal,
    admission: SessionOwner,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::AcceptedCarrierSources {
                max_sources: options.max_sources,
            }],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, ChannelError> {
        let portal = options
            .registry
            .install_component(&options.tag, context.instance())?;
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let provider = Arc::new(
            zero_platform_tokio::controlled_carrier::AcceptedCarrierProvider::new(
                context.instance(),
                options.max_sources,
            ),
        );
        portal
            .0
            .provider
            .set(provider.clone())
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        let outbound = || Outbound {
            portal: Arc::downgrade(&portal.0),
            admission: admission.clone(),
        };
        let roles = ComponentRoles {
            accepted_carriers: Some(provider),
            stream: Some(erase_stream(outbound())),
            datagram: Some(zero_platform_tokio::contract::datagram::erase_datagram(
                outbound(),
            )),
            ..Default::default()
        };
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                portal,
                admission: owner,
            }),
            roles,
        })
    }
}
impl Outbound {
    fn portal(&self, context: &OpenContext) -> Result<Portal, ChannelError> {
        self.admission.check(context)?;
        if !context.upstream.binding().prefix.is_empty() {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        self.portal
            .upgrade()
            .filter(|state| !state.retired.load(Ordering::Acquire))
            .map(Portal)
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
    fn origin(context: &OpenContext, network: u8) -> crate::mux::origin::Origin {
        context
            .business_facts()
            .map(|facts| crate::mux::origin::Origin::from_facts(facts, network))
            .unwrap_or_default()
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), ChannelError> {
        self.admission.start(end).await?;
        self.portal.0.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.admission.revoke(reason);
        if let Some(provider) = self.portal.0.provider.get() {
            provider.request_retirement();
        }
        self.portal.retire();
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        loop {
            let workers = self.portal.0.workers.lock().unwrap().clone();
            for worker in &workers {
                if !worker.close(end).await? {
                    break;
                }
            }
            self.portal
                .0
                .workers
                .lock()
                .unwrap()
                .retain(|worker| !worker.custody.confirmed());
            if self.portal.0.workers.lock().unwrap().is_empty()
                && self
                    .portal
                    .0
                    .provider
                    .get()
                    .is_some_and(|provider| provider.returned().is_some())
            {
                return Ok(CloseReport::default());
            }
            if tokio::time::Instant::now() >= end {
                return Ok(CloseReport {
                    unfinished: vec![
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "Rvs original accepted workers and transferred views".into(),
                        ),
                    ],
                    ..Default::default()
                });
            }
            tokio::time::sleep_until(
                end.min(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
            )
            .await;
        }
    }
}
pub(crate) fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
