//! Protocol-owned ordinary Trojan Stream roles over a constrained real carrier.
mod carrier;
mod connect;
mod datagram;
pub mod inbound;
pub mod mux;
mod options;
mod transport;
pub use options::Options;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError},
    network::CarrierContext,
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::{
    outbound_stack::{connect_relay_transport_stack, StreamTransportStack},
    profile::{OwnedH2Profile, OwnedHttpUpgradeProfile},
};

pub struct Factory;
struct Outbound {
    browser: Option<zero_transport::browser_dialer::component::Handle>,
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
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
        let prepared_browser = options
            .browser
            .as_ref()
            .map(|options| {
                zero_transport::browser_dialer::component::Owner::build(options.clone(), &context)
            })
            .transpose()?;
        let browser = prepared_browser.as_ref().map(|(_, handle)| handle.clone());
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        Ok(ComponentInstance {
            lifecycle: match prepared_browser {
                Some((browser, _)) => zero_transport::browser_dialer::component::Combined::wrap(
                    browser,
                    erase_lifecycle(owner),
                ),
                None => erase_lifecycle(owner),
            },
            roles: ComponentRoles {
                datagram: Some(zero_platform_tokio::contract::datagram::erase_datagram(
                    Outbound {
                        browser: browser.clone(),
                        options: options.clone(),
                        admission: admission.clone(),
                    },
                )),
                stream: Some(erase_stream(Outbound {
                    browser,
                    options,
                    admission,
                })),
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
        let (session, bytes) = self.prepare_stream(&request, &context)?;
        let io = self.open_transport(&context, session).await?;
        self.submit_stream(io, bytes, context, session).await
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
        let (session, bytes) = self.prepare_stream(&request, &context)?;
        let (io, control, original) = browser.open_parts(&context).await?;
        let connected = self.submit_stream(io, bytes, context, session).await?;
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
