//! One standard Datagram executor. No protocol matching or alternate UDP pipe.
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) mod continuity;
mod owner;
mod relay;
mod response;
use super::*;
use tokio::sync::Mutex as AsyncMutex;
use zero_platform_tokio::contract::{
    inbound::{AcceptedDatagram, ReplyOutcome},
    lifecycle::{erase_lifecycle, TaskClass},
    CloseReason,
};

pub(super) async fn run(
    runtime: InboundRouteRuntime,
    request: Box<dyn AcceptedDatagram>,
    identity: SessionIdentity,
    auth: Option<SessionAuth>,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let facts = request.facts();
    let services = runtime.tcp_runtime.runtime_services();
    let execution = services.execution();
    let ingress = execution
        .ingress
        .as_ref()
        .ok_or_else(|| std::io::Error::other("Datagram requires its actual inbound role"))?;
    let subject = auth
        .as_ref()
        .and_then(|auth| auth.principal_key.as_ref())
        .map(|key| zero_core::component::AuthSubject {
            owner: ingress.role.component,
            principal: zero_core::component::PrincipalId(Arc::from(key.as_str())),
        });
    if facts.session != identity.id()
        || facts.inbound != ingress.role.role
        || facts.effective_source != runtime.source_addr()
        || facts.local_endpoint != runtime.tcp_runtime.local_addr()
        || facts.subject != subject
    {
        return Err(std::io::Error::other(
            "Datagram facts differ from actual host identity/authentication",
        )
        .into());
    }
    #[cfg(feature = "managed-datagram-runtime")]
    if let Some(continuity) = request.continuity() {
        return continuity::run(runtime, request, continuity, auth, cancel).await;
    }
    #[cfg(feature = "managed-datagram-runtime")]
    if let Some(peer) = runtime.peer_datagram.clone() {
        let udp = runtime
            .udp_runtime()
            .with_datagram_returns(peer.returns)
            .with_peer_resources(peer.resources);
        let result = drive(runtime, request, auth, peer.cancel, peer.state.clone(), udp).await;
        if result.is_err() {
            peer.state.lock().await.dropped = true;
        }
        return result;
    }
    let tasks = runtime
        .accepted_tasks()
        .ok_or_else(|| std::io::Error::other("Datagram has no actual accepted task owner"))?;
    let caller = tasks.caller().map_err(std::io::Error::other)?;
    let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
    let retired = services.execution().retired_datagrams();
    let state = Arc::new(AsyncMutex::new(owner::State::default()));
    let stop = cancel.child_token();
    let original = Arc::new(
        caller
            .retain_logical_resource(erase_lifecycle(owner::Owner {
                state: state.clone(),
                cancel: stop.clone(),
                returns: returns.clone(),
                retired,
                #[cfg(feature = "managed-datagram-runtime")]
                peer_retirement: None,
            }))
            .map_err(std::io::Error::other)?,
    );
    let retained = original.clone();
    let (result, receive) = tokio::sync::oneshot::channel();
    // One declared native driver in the original accepted graph and Scope.
    // Primary negotiation stays Auxiliary; packets never spawn per-flow tasks.
    tasks
        .spawn(TaskClass::Critical, move |driver| {
            let binding = retained.bind_driver(&driver);
            async move {
                binding?;
                let udp = runtime
                    .udp_runtime()
                    .with_datagram_returns(returns)
                    .with_accepted_caller(driver.caller()?);
                let operation = drive(runtime, request, auth, stop.clone(), state.clone(), udp);
                let completion = tokio::select! { biased;
                    _ = stop.cancelled() => Ok(()),
                    result = operation => result,
                };
                if completion.is_err() {
                    state
                        .try_lock()
                        .expect("driver released original dispatch")
                        .dropped = true;
                }
                let _ = result.send(completion);
                // Ordinary client failures are delivered to the original Auxiliary
                // parent. A native driver panic still reaches the real Critical Scope.
                Ok(())
            }
        })
        .map_err(std::io::Error::other)?;
    let completion = receive.await.map_err(std::io::Error::other)?;
    if !original
        .close(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
        .await
        .map_err(std::io::Error::other)?
    {
        return Err(
            std::io::Error::other("logical Datagram original cleanup remains pending").into(),
        );
    }
    completion
}

async fn drive(
    runtime: InboundRouteRuntime,
    request: Box<dyn AcceptedDatagram>,
    auth: Option<SessionAuth>,
    stop: CancellationToken,
    state: Arc<AsyncMutex<owner::State>>,
    udp: crate::runtime::udp_ingress::UdpIngressRuntime,
) -> Result<(), EngineError> {
    let Some(session) = request
        .respond(ReplyOutcome::LocalChannelReady)
        .await
        .map_err(std::io::Error::other)?
    else {
        return Err(EngineError::Io(std::io::Error::other(
            "successful Datagram reply omitted its channel",
        )));
    };
    let mut state = state
        .try_lock()
        .expect("single original accepted Datagram driver");
    let zero_platform_tokio::contract::inbound::InboundDatagramSession { io, negotiation } =
        session;
    let zero_platform_tokio::contract::datagram::DatagramSession {
        tx,
        rx,
        control,
        limits,
    } = io;
    state.control = Some(control); // Persist before dispatcher preparation/await.
    let dispatch = udp.new_dispatch(runtime.inbound_tag()).await?;
    state.dispatch = Some(dispatch);
    let owner::State {
        dispatch, control, ..
    } = &mut *state;
    relay::run(
        &udp,
        runtime.inbound_tag(),
        dispatch.as_mut().unwrap(),
        control.as_mut().unwrap().as_mut(),
        tx,
        rx,
        limits,
        negotiation,
        auth.as_ref(),
        &stop,
    )
    .await
    .map(|_| ())
}
#[cfg(feature = "managed-datagram-runtime")]
#[derive(Clone)]
pub(in crate::runtime) struct PeerExecution {
    resources: crate::runtime::inbound_owners::PeerResourceContext,
    state: Arc<AsyncMutex<owner::State>>,
    returns: Arc<crate::component::datagram_retirement::DatagramReturns>,
    cancel: CancellationToken,
}
#[cfg(feature = "managed-datagram-runtime")]
impl InboundRouteRuntime {
    pub(in crate::runtime) async fn prepare_datagram_peer(
        mut self,
        context: crate::runtime::inbound_owners::PeerResourceContext,
        retirement: zero_platform_tokio::contract::inbound::udp::DatagramPeerRetirement,
        cancel: CancellationToken,
    ) -> Result<Self, EngineError> {
        let state = Arc::new(AsyncMutex::new(owner::State::default()));
        let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
        let original = owner::Owner {
            state: state.clone(),
            returns: returns.clone(),
            cancel: cancel.clone(),
            retired: self
                .tcp_runtime
                .runtime_services()
                .execution()
                .retired_datagrams(),
            peer_retirement: Some(retirement),
        };
        context
            .claim(
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                |_| Ok((erase_lifecycle(original), ())),
            )
            .await
            .map_err(std::io::Error::other)?;
        self.peer_datagram = Some(PeerExecution {
            resources: context,
            state,
            returns,
            cancel,
        });
        Ok(self)
    }
}
