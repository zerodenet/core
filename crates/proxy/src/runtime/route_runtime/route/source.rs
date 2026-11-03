//! Standard request delivery. A single short-lived identity owner retains the
//! real Engine ticket across handshake; protocols receive no registry or kernel.
use super::model::InboundRouteRuntime;
use std::sync::{Arc, Mutex};
#[cfg(all(
    feature = "udp-response-runtime",
    feature = "upstream-association-runtime"
))]
mod datagram;
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) use datagram::continuity::Host as DatagramContinuityHost;
#[cfg(feature = "managed-datagram-runtime")]
pub(super) use datagram::PeerExecution;
mod connection;
mod fallback;
mod tasks;
use zero_core::{component::SessionId, SessionAuth};
use zero_engine::{Engine, EngineError, SessionIdentity};
use zero_platform_tokio::contract::{
    context::{AcceptContext, HandshakeContext, SessionIdentitySource},
    inbound::{AcceptedRequest, PendingInbound, RequestMultiplexer, RequestSourceEnd},
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
struct HandshakeIdentity {
    engine: Engine,
    minted: Mutex<Option<(SessionIdentity, Option<SessionAuth>)>>,
}
impl SessionIdentitySource for HandshakeIdentity {
    fn allocate(&self) -> Result<SessionId, ChannelError> {
        self.allocate_authenticated(None)
    }
    fn allocate_authenticated(
        &self,
        authentication: Option<SessionAuth>,
    ) -> Result<SessionId, ChannelError> {
        let mut minted = self
            .minted
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if minted.is_some() {
            return Err(ChannelError::new(
                Stage::Handshake,
                Reason::Internal,
                Impact::Session,
            ));
        }
        let identity = self.engine.mint_session_identity().map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Internal, Impact::Session)
                .with_source(source)
        })?;
        let id = identity.id();
        *minted = Some((identity, authentication));
        Ok(id)
    }
}
impl InboundRouteRuntime {
    pub(in crate::runtime) fn inbound_instance(
        &self,
    ) -> Result<zero_core::component::RoleInstanceId, EngineError> {
        self.tcp_runtime
            .runtime_services()
            .execution()
            .ingress
            .map(|i| i.role)
            .ok_or_else(|| std::io::Error::other("missing actual ingress role").into())
    }

    pub(in crate::runtime) fn connection_facts(
        &self,
    ) -> zero_platform_tokio::contract::context::InboundConnectionFacts {
        self.connection.clone().unwrap_or_else(|| {
            zero_platform_tokio::contract::context::InboundConnectionFacts {
                original_source: self.source_addr(),
                effective_source: self.source_addr(),
                local_endpoint: self.tcp_runtime.local_addr(),
                sni: None,
            }
        })
    }

    pub(crate) fn run_requests(
        self,
        mut source: Box<dyn RequestMultiplexer>,
    ) -> BoxFuture<'static, Result<(), EngineError>> {
        Box::pin(async move {
            let cancel = CancellationToken::new();
            // Cancellation requests teardown; only the original Scope/provider
            // can confirm closure. This guard never manufactures a receipt.
            let _cancel_on_drop = cancel.clone().drop_guard();
            let device = match self.acquire_principal_device(source.auth()) {
                Ok(device) => device,
                Err(error) => {
                    source.close("device_limit");
                    return Err(error);
                }
            };
            let (principal_cancel, mut principal_stopped) =
                tokio::sync::mpsc::unbounded_channel::<String>();
            let principal = source
                .auth()
                .and_then(|auth| auth.principal_key.as_deref())
                .map(|key| {
                    self.register_principal_cancellation(key, move |reason| {
                        let _ = principal_cancel.send(reason);
                    })
                });
            let _registrations = (device, principal);
            let carrier_cancel = source.cancellation();
            let limit = source.max_in_flight().get();
            if limit == 1 {
                // Sequential message framing stays in the actual original
                // accepted task; keep-alive does not create per-message tasks.
                loop {
                    let pending = tokio::select! {
                        _ = carrier_stopped(&carrier_cancel) => { source.close("carrier_closed"); cancel.cancel(); break; }
                        next = source.next(accept_context(&cancel)) => next.map_err(std::io::Error::other)?,
                        Some(reason) = principal_stopped.recv() => { source.close(&reason); cancel.cancel(); break; }
                    };
                    let Some(pending) = pending else {
                        break;
                    };
                    tokio::select! {
                        result = self.clone().complete_request(pending, cancel.clone()) => tasks::report(result),
                        _ = carrier_stopped(&carrier_cancel) => { source.close("carrier_closed"); cancel.cancel(); break; }
                        Some(reason) = principal_stopped.recv() => { source.close(&reason); cancel.cancel(); break; }
                    }
                }
                return Ok(());
            }
            let mut tasks = tasks::Requests::new(self.accepted_tasks());
            let mut ended = false;
            while !ended || tasks.len() != 0 {
                tokio::select! {
                    _ = carrier_stopped(&carrier_cancel), if !ended => { source.close("carrier_closed"); cancel.cancel(); ended = true; }
                    Some(reason) = principal_stopped.recv(), if !ended => {
                        source.close(&reason); cancel.cancel(); ended = true;
                    }
                    next = source.next(accept_context(&cancel)), if !ended && tasks.len() < limit => {
                        match next.map_err(std::io::Error::other)? {
                            Some(pending) => tasks.spawn(self.clone(), pending, cancel.clone()).map_err(std::io::Error::other)?,
                            None => {
                                ended = true;
                                if source.end_disposition() == RequestSourceEnd::CarrierClosed { cancel.cancel(); }
                            }
                        }
                    }
                    _ = tasks.join_next(), if tasks.len() != 0 => {}
                }
            }
            Ok(())
        })
    }
    pub(in crate::runtime) async fn complete_request(
        self,
        pending: PendingInbound,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let services = self.tcp_runtime.runtime_services();
        let inbound = services
            .execution()
            .ingress
            .ok_or_else(|| std::io::Error::other("inbound source has no actual host role"))?
            .role;
        let identities = Arc::new(HandshakeIdentity {
            engine: services.engine().clone(),
            minted: Mutex::new(None),
        });
        let connection = Arc::new(std::sync::OnceLock::new());
        let context = HandshakeContext {
            connection: Some(connection.clone()),
            accepted: self
                .accepted_tasks()
                .map(|tasks| tasks.caller())
                .transpose()
                .map_err(std::io::Error::other)?,
            inbound,
            identities: identities.clone(),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            cancel: cancel.clone(),
        };
        let accepted = match pending.complete(context).await {
            Ok(accepted) => accepted,
            Err(error)
                if error.reason == Reason::Revoked
                    && identities
                        .minted
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .is_none() =>
            {
                return Ok(())
            }
            Err(error) => return Err(std::io::Error::other(error).into()),
        };
        let runtime = match connection.get() {
            Some(facts) => self.with_prepared_connection(facts.clone())?,
            None => self,
        };
        let identity = identities
            .minted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        match accepted {
            AcceptedRequest::Stream(request) => {
                let (identity, authentication) = identity.ok_or_else(|| {
                    std::io::Error::other("business stream did not mint its actual identity")
                })?;
                runtime
                    .tcp_runtime
                    .serve_accepted_stream(request, authentication, identity)
                    .await
            }
            AcceptedRequest::Message(request) => {
                let (identity, authentication) = identity.ok_or_else(|| {
                    std::io::Error::other("business message did not mint its actual identity")
                })?;
                runtime
                    .tcp_runtime
                    .serve_accepted_message(request, authentication, identity)
                    .await
            }
            #[cfg(all(
                feature = "udp-response-runtime",
                feature = "upstream-association-runtime"
            ))]
            AcceptedRequest::Datagram(request) => {
                let (identity, authentication) = identity.ok_or_else(|| {
                    std::io::Error::other("business Datagram did not mint its actual identity")
                })?;
                datagram::run(runtime, request, identity, authentication, cancel).await
            }
            AcceptedRequest::Control(control) => {
                if identity.is_some() {
                    return Err(std::io::Error::other(
                        "control must not invent a business identity",
                    )
                    .into());
                }
                crate::runtime::inbound_operation::control::run(runtime, control, Some(cancel))
                    .await
            }
            AcceptedRequest::Fallback(request) => {
                if identity.is_some() {
                    return Err(std::io::Error::other(
                        "fallback must not mint a business identity",
                    )
                    .into());
                }
                fallback::run(runtime, request, cancel).await
            }
            AcceptedRequest::Multiplex(source) => {
                if identity.is_some() {
                    return Err(std::io::Error::other(
                        "request source must not invent a business identity",
                    )
                    .into());
                }
                runtime.run_requests(source).await
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "standard inbound role executor is not installed for this request",
            )
            .into()),
        }
    }
}

fn accept_context(cancel: &CancellationToken) -> AcceptContext {
    AcceptContext {
        deadline: None,
        cancel: cancel.clone(),
    }
}

async fn carrier_stopped(cancel: &Option<CancellationToken>) {
    match cancel {
        Some(cancel) => cancel.cancelled().await,
        None => std::future::pending().await,
    }
}
