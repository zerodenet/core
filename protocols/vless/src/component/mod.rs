//! Protocol-owned ordinary VLESS Stream roles over a constrained real carrier.
mod carrier;
pub(crate) mod navigation;
mod options;
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
    scope::TaskRegistrar,
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
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    tasks: TaskRegistrar,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            // Even failed REALITY proxy authentication can retain the actual
            // verified decoy carrier; this requires a real host task owner.
            task_limit: options
                .reality
                .as_ref()
                .and_then(|_| std::num::NonZeroUsize::new(64)),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options,
                    admission,
                    tasks: context.tasks,
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
        if request.destination.port == 0 {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let bytes = crate::outbound::build_tcp_request_to(
            &request.destination.address,
            request.destination.port,
            &self.options.uuid,
            self.options.flow,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
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
            if let Some(reality) = &self.options.reality {
                let stream = reality
                    .connect_with_navigation(io, |stream, host, plan| {
                        navigation::start(
                            stream,
                            host,
                            plan,
                            self.tasks.clone(),
                            self.admission.clone(),
                        )
                    })
                    .await
                    .map_err(zero_transport::RuntimeError::Io)?;
                let control = zero_traits::AsyncSocket::transport_bypass_control(&stream);
                Ok(zero_transport::TcpRelayStream::with_transport_bypass_control(stream, control))
            } else {
                connect_relay_transport_stack(
                    io,
                    StreamTransportStack {
                        tls: self.options.tls.as_ref(),
                        ws: self.options.ws.as_ref(),
                        grpc: self.options.grpc.as_ref(),
                        h2: None::<&OwnedH2Profile>,
                        http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                        source_dir: None,
                    },
                    &self.options.server,
                    self.options.endpoint.port,
                    "vless: ws and grpc are mutually exclusive",
                )
                .await
            }
        };
        let mut io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,transport) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            let mut error = self.admission.error(session, Stage::Handshake, Reason::Io, DeliveryState::Unknown);
            // No VLESS request was written. Only an empty prefix proves that
            // another proxy has not already submitted its own CONNECT.
            if context.upstream.binding().prefix.is_empty() {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            error.with_source(source)
        })?;
        let control = zero_traits::AsyncSocket::transport_bypass_control(&io);
        if crate::flow::is_vision_flow(self.options.flow) && control.is_none() {
            let mut error = self.admission.error(
                session,
                Stage::Handshake,
                Reason::Unsupported,
                DeliveryState::Unknown,
            );
            if context.upstream.binding().prefix.is_empty() {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            return Err(error.with_source(zero_core::Error::Unsupported(
                "Vision requires TLS 1.3, REALITY or VLESS Encryption",
            )));
        }
        let submit = async {
            io.write_all(&bytes).await?;
            io.flush().await
        };
        tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,submit) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            // WebSocket early data can defer the upgrade until write/flush.
            // Only successful flush proves that the complete carrier is ready.
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, bytes.len() as u64);
        }
        // The response is validated when the caller reads, so peers that wait
        // for application data do not deadlock against an eager response read.
        let stream = crate::deferred_response::DeferredVlessResponseStream::new(io)
            .with_connect_observer(session, context.observer);
        let io: BoxStream = if crate::flow::is_vision_flow(self.options.flow) {
            Box::pin(crate::vision::VisionStream::with_testseed(
                stream,
                self.options.uuid,
                control,
                self.options.testseed,
            ))
        } else {
            Box::pin(stream)
        };
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
