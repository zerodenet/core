//! Protocol-owned ordinary VMess Stream roles over a constrained real carrier.
mod carrier;
pub mod grpc;
pub mod inbound;
mod options;
mod owner;
mod submit;
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
    network::CarrierContext,
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
};

pub struct Factory;
struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    prepared_tls: Arc<Mutex<Option<zero_transport::tls::PreparedClient>>>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: options
                .ca_file
                .as_ref()
                .map(|file| {
                    vec![ResourceDeclaration::File {
                        path: file.path.clone(),
                        max_bytes: file.max_bytes,
                    }]
                })
                .unwrap_or_default(),
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let prepared_tls = Arc::new(Mutex::new(None));
        let lifecycle = if options.ca_file.is_some() {
            erase_lifecycle(owner::CaOwner::new(
                owner,
                admission.clone(),
                options.clone(),
                prepared_tls.clone(),
                context.resources,
            ))
        } else {
            erase_lifecycle(owner)
        };
        Ok(ComponentInstance {
            lifecycle,
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options,
                    admission,
                    prepared_tls,
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
        let session = self.admission.check(&context)?;
        if context.upstream.binding().dial != self.options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        crate::shared::validate_request_target(&request.destination.address).map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let prepared_tls = self
            .prepared_tls
            .lock()
            .expect("prepared VMess TLS state")
            .clone();
        if self.options.ca_file.is_some() && prepared_tls.is_none() {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let opened = context
            .upstream
            .open_stream(self.options.endpoint.clone(), carrier);
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Dial,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,opened) => result.map_err(|source| self.admission.error(session,Stage::Dial,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error| self.admission.attach(error,session))?;
        let transport = async {
            let io = zero_transport::TcpRelayStream::new(carrier::Carrier::new(io));
            let stack = StreamTransportStack {
                tls: self.options.tls.as_ref(),
                ws: self.options.ws.as_ref(),
                grpc: self.options.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
                http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                source_dir: None,
            };
            if let Some(prepared) = prepared_tls {
                let io = prepared.connect(io, &self.options.server).await?;
                connect_layered_transport_stack(
                    io,
                    stack,
                    &self.options.server,
                    self.options.endpoint.port,
                    "vmess: ws and grpc are mutually exclusive",
                )
                .await
            } else {
                connect_relay_transport_stack(
                    io,
                    stack,
                    &self.options.server,
                    self.options.endpoint.port,
                    "vmess: ws and grpc are mutually exclusive",
                )
                .await
            }
        };
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,transport) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            let mut error = self.admission.error(session, Stage::Handshake, Reason::Io, DeliveryState::Unknown);
            // No VMess request was written. Only an empty prefix proves that
            // another proxy has not already submitted its own CONNECT.
            if context.upstream.binding().prefix.is_empty() {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            error.with_source(source)
        })?;
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
}
