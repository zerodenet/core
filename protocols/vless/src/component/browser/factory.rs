//! Thin VLESS component wrapper around `zero_transport::browser_dialer::component::Owner`.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::admission::{SessionAdmission, SessionOwner};
use zero_transport::{
    browser_dialer::component::{Handle, Owner as BrowserOwner},
    runner::LifecycleChain,
};

pub struct Factory;

pub(in crate::component) struct Shared {
    pub options: Arc<Options>,
    pub handle: Handle,
}

pub(super) struct Outbound {
    pub shared: PacketMetadataArc<Shared>,
    pub admission: Arc<SessionAdmission>,
}

struct InstallCarrier {
    carrier: crate::component::quic::TransportCarrier,
    shared: PacketMetadataArc<Shared>,
}

impl ResourceLifecycle for InstallCarrier {
    async fn start(&mut self, _deadline: Instant) -> Result<(), BuildError> {
        self.carrier.install_browser(&self.shared)
    }
    fn revoke(&self, _reason: CloseReason) {}
    async fn shutdown(&mut self, _deadline: Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}

impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: if options.carrier.is_some() {
                vec![]
            } else {
                std::iter::once(RoleKind::Stream)
                    .chain(
                        (!crate::flow::is_vision_flow(options.protocol.flow))
                            .then_some(RoleKind::Datagram),
                    )
                    .collect()
            },
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        };
        options.browser.declare(&mut spec);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (browser_owner, handle) = BrowserOwner::build(options.browser.clone(), &context)?;
        let header = handle
            .memory()
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let shared = PacketMetadataArc::from_prepaid(Shared { options, handle }, header)
            .map_err(|(source, _, _)| capacity(source))?;
        let install = shared
            .options
            .carrier
            .clone()
            .map(|carrier| InstallCarrier {
                carrier,
                shared: shared.clone(),
            });
        let lifecycle = LifecycleChain::new()
            .push_typed(browser_owner)
            .push_typed(session)
            .push_typed_opt(install)
            .into_boxed();
        Ok(ComponentInstance {
            lifecycle,
            roles: if shared.options.carrier.is_some() {
                ComponentRoles::default()
            } else {
                ComponentRoles {
                    stream: Some(zero_platform_tokio::contract::stream::erase_stream(
                        Outbound {
                            shared: shared.clone(),
                            admission: admission.clone(),
                        },
                    )),
                    datagram: (!crate::flow::is_vision_flow(shared.options.protocol.flow)).then(
                        || {
                            zero_platform_tokio::contract::datagram::erase_datagram(Outbound {
                                shared,
                                admission,
                            })
                        },
                    ),
                    ..Default::default()
                }
            },
        })
    }
}
