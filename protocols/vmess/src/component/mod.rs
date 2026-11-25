//! Protocol-owned ordinary VMess Stream/Datagram roles over a constrained real carrier.
mod borrowed;
mod carrier;
mod datagram;
pub mod grpc;
pub mod inbound;
pub(crate) mod material;
pub mod mux;
mod options;
mod owner;
mod submit;
mod transport;
pub use options::Options;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceDeclaration, ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError},
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::{
    outbound_stack::{
        connect_layered_transport_stack, connect_relay_transport_stack, StreamTransportStack,
    },
    profile::{OwnedH2Profile, OwnedHttpUpgradeProfile},
    runner::{GuardedCall, LifecycleChain},
};

pub struct Factory;
#[derive(Clone)]
struct Outbound {
    browser: Option<zero_transport::browser_dialer::component::Handle>,
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    prepared_tls: owner::Prepared,
    ech_resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: options
                .material
                .as_ref()
                .and_then(|material| material.resource())
                .map(|file| vec![file])
                .unwrap_or_else(|| {
                    options
                        .ca_file
                        .as_ref()
                        .map(|file| {
                            vec![ResourceDeclaration::File {
                                path: file.path.clone(),
                                max_bytes: file.max_bytes,
                            }]
                        })
                        .unwrap_or_default()
                }),
            task_limit: None,
        };
        if let Some(browser) = &options.browser {
            browser.declare(&mut spec);
        }
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, None)
    }
}
impl Factory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    fn build_owned(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let prepared_browser = options
            .browser
            .as_ref()
            .map(|options| {
                zero_transport::browser_dialer::component::Owner::build(options.clone(), &context)
            })
            .transpose()?;
        let (browser_owner, browser) = match prepared_browser {
            Some((owner, handle)) => (Some(owner), Some(handle)),
            None => (None, None),
        };
        let instance = context.instance();
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let prepared_tls = Arc::new(Mutex::new(None));
        let inner_lifecycle = if options.ca_file.is_some() || options.material.is_some() {
            erase_lifecycle(owner::CaOwner::new(
                owner,
                admission.clone(),
                options.clone(),
                prepared_tls.clone(),
                context.resources,
                instance,
                context.cancel.clone(),
            ))
        } else {
            erase_lifecycle(owner)
        };
        let lifecycle = LifecycleChain::new()
            .push_typed_opt(browser_owner)
            .push(inner_lifecycle)
            .into_boxed();
        let outbound = Outbound {
            browser,
            options,
            admission,
            prepared_tls,
            ech_resolver: resolver,
        };
        Ok(ComponentInstance {
            lifecycle,
            roles: ComponentRoles {
                stream: Some(erase_stream(outbound.clone())),
                datagram: Some(zero_platform_tokio::contract::datagram::erase_datagram(
                    outbound,
                )),
                ..Default::default()
            },
        })
    }
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let session = self.admission.check(&context)?;
        let io = self.open_transport(&request.destination, &context).await?;
        submit::submit(
            &self.options,
            &self.admission,
            request,
            context,
            session,
            io,
        )
        .await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        let browser = self.browser.as_ref().ok_or_else(|| {
            self.admission.error(
                None,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            )
        })?;
        let call = GuardedCall::enter(&self.admission, &context, Some(&self.options.dial))?;
        crate::shared::validate_request_target(&request.destination.address).map_err(|source| {
            call.error(
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        })?;
        let session = call.session();
        if !context.upstream.binding().prefix.is_empty() {
            return self.borrow_prefix(request, context, session).await;
        }
        let (io, control, original) = browser.open_parts(&context).await?;
        let connected = submit::submit(
            &self.options,
            &self.admission,
            request,
            context,
            session,
            io,
        )
        .await?;
        Ok(Connected {
            io: zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
                connected.io,
                control,
                original,
            ),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}

impl Outbound {
    async fn open_transport(
        &self,
        destination: &zero_core::request::Destination,
        context: &OpenContext,
    ) -> Result<zero_transport::TcpRelayStream, ChannelError> {
        let call = GuardedCall::enter(&self.admission, context, Some(&self.options.dial))?;
        crate::shared::validate_request_target(&destination.address).map_err(|source| {
            call.error(
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
        })?;
        if context.upstream.binding().prefix.is_empty() {
            if let Some(browser) = &self.browser {
                return browser.open(context).await;
            }
        }
        let io = call.open_stream(self.options.endpoint.clone()).await?;
        self.wrap_transport(io, context, call.session()).await
    }
}
