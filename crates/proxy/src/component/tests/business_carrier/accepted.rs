//! A real original accepted FD, its installed handler task and the B projection.
#![cfg(feature = "vmess")]
use crate::{
    component::{business_demand::AdmittedTcpDemand, direct_bridge::Invocation},
    runtime::tcp_ingress::TcpIngressRuntime,
    Proxy,
};
use std::{num::NonZeroUsize, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    time::Instant,
};
use zero_core::{
    component::SessionId, request::Destination, Address, Network, ProtocolType, Session,
    SessionAuth,
};
use zero_platform_tokio::{
    contract::{
        component::{AcceptedTcpLimits, ResourceDeclaration},
        lifecycle::TaskClass,
        scope::{TaskJoinWitness, TaskScope},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    AcceptedTcpCaller, AcceptedTcpClaim, ControlledTcpListenerOwner,
};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}
pub(in crate::component) struct Caller {
    scope: TaskScope,
    listener: Option<zero_platform_tokio::ControlledTcpListener>,
    owner: ControlledTcpListenerOwner,
    claim: AcceptedTcpClaim,
    peer: TcpStream,
    source: AcceptedTcpCaller,
    witness: TaskJoinWitness,
    complete: Option<oneshot::Sender<()>>,
    pub(in crate::component) session: Session,
    pub(in crate::component) invocation: Invocation,
}
impl Caller {
    pub(in crate::component) async fn new(proxy: &Proxy) -> Self {
        Self::new_for_session(
            proxy,
            Session::new(
                0,
                Address::Domain("native.example".into()),
                443,
                Network::Tcp,
                ProtocolType::new("verified"),
            ),
        )
        .await
    }
    pub(in crate::component) async fn new_for_session(proxy: &Proxy, session: Session) -> Self {
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(one()));
        let declaration = ResourceDeclaration::TcpListener {
            destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            max_bind_attempts: one(),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one(),
                max_tasks_per_socket: one(),
            }),
        };
        let mut owner = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one(),
            CancellationToken::new(),
            one(),
            one(),
            &scope.registrar(),
        )
        .unwrap();
        let mut listener = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr().unwrap();
        scope.enable_registration().unwrap();
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), listener.accept_owned());
        let mut peer = peer.unwrap();
        let (accepted, remote) = accepted.unwrap();
        assert_eq!(peer.local_addr().unwrap(), remote);
        let claim = accepted.claim();
        let (complete, completed) = oneshot::channel();
        let (send, receive) = oneshot::channel();
        let engine = proxy.engine().clone();
        let services = proxy.tcp_runtime_services();
        let (_, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                move |mut socket, tasks| async move {
                    let caller = tasks.caller()?;
                    let remote = socket.peer_addr().map_err(failure)?;
                    let local = socket.local_addr().map_err(failure)?;
                    let mut hello = [0; 2];
                    socket.read_exact(&mut hello).await.map_err(failure)?;
                    assert_eq!(&hello, b"go");
                    let runtime = TcpIngressRuntime::new(services, "in".into(), Some(remote))
                        .with_local_addr(Some(local))
                        .with_accepted_caller(caller.clone());
                    let mut session = session;
                    session.source_ip = Some(match remote.ip() {
                        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                    });
                    session.source_port = Some(remote.port());
                    session.inbound_local = Some((
                        match local.ip() {
                            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                        },
                        local.port(),
                    ));
                    let mut auth = SessionAuth::new("verified");
                    auth.principal_key = Some("business-grpc".into());
                    auth.quota_remaining_bytes = Some(4096);
                    auth.device_limit = Some(1);
                    auth.up_bps = Some(4096);
                    auth.down_bps = Some(8192);
                    auth.policy_revision = Some(7);
                    session.apply_auth(auth);
                    let receipt = engine
                        .prepare_session_admitted(&mut session, "in")
                        .map_err(failure)?;
                    let demand = AdmittedTcpDemand::new(&engine, SessionId(session.id), receipt)
                        .map_err(failure)?;
                    let invocation = runtime
                        .runtime_services()
                        .with_admitted_business_identity(&session, demand)
                        .map_err(failure)?
                        .with_accepted_caller(runtime.accepted_caller())
                        .map_err(failure)?
                        .execution()
                        .invocation
                        .unwrap();
                    let actual = invocation
                        .caller_accepted
                        .as_ref()
                        .unwrap()
                        .task_join_witness()
                        .unwrap();
                    assert!(actual.same_registration(&caller.task_join_witness().unwrap()));
                    send.send((session, invocation, caller))
                        .unwrap_or_else(|_| panic!("actual accepted caller receiver"));
                    completed.await.map_err(failure)?;
                    drop(socket);
                    Ok(())
                },
            )
            .unwrap();
        peer.write_all(b"go").await.unwrap();
        let (session, invocation, source) = receive.await.unwrap();
        assert!(source.check_open().is_ok());
        assert!(!witness.is_joined());
        Self {
            scope,
            listener: Some(listener),
            owner,
            claim,
            peer,
            source,
            witness,
            complete: Some(complete),
            session,
            invocation,
        }
    }
    pub(in crate::component) fn witness(&self) -> TaskJoinWitness {
        self.witness.clone()
    }
    pub(in crate::component) fn verify_carrier_completion(&self) {
        assert!(self.witness.is_joined());
        assert!(self.owner.accepted_evidence().unwrap().provider_confirmed());
        assert_eq!(
            self.owner
                .accepted_evidence()
                .unwrap()
                .confirmed_native_closes,
            1
        );
    }
    pub(in crate::component) async fn join(&mut self) {
        self.complete.take().unwrap().send(()).unwrap();
        let exit = self.scope.next_exit().await.unwrap();
        let delivered = self
            .owner
            .accepted_task_receiver()
            .unwrap()
            .observe_joined(exit.id)
            .unwrap();
        assert_eq!(delivered.id(), exit.id);
        assert!(delivered.result().is_ok());
        let actual = self.witness.try_joined().unwrap();
        assert_eq!(exit.id, actual.id());
        assert!(actual.result().is_ok());
        assert!(exit.result.is_ok());
        assert!(self.source.check_open().is_err());
        assert_eq!(
            self.peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "actual join must not masquerade as native FD closure"
        );
        assert!(self
            .owner
            .close_accepted_graph(&self.claim, end())
            .await
            .unwrap()
            .unwrap()
            .matches(&self.claim));
        assert_eq!(self.peer.read(&mut [0; 1]).await.unwrap(), 0);
        drop(self.listener.take());
        assert!(self.owner.wait_closed(end()).await.unwrap().is_complete());
        assert!(self.owner.accepted_evidence().unwrap().provider_confirmed());
        assert!(self.scope.shutdown(end()).await.is_complete());
    }
}

#[tokio::test]
async fn actual_accepted_source_needs_the_original_admission_and_cannot_replace_another_actual_caller(
) {
    tokio::time::timeout(Duration::from_secs(6), async {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let mut first = Caller::new(&proxy).await;
        let mut other = Caller::new(&proxy).await;
        assert!(!first
            .source
            .scope_identity()
            .same_scope(&other.source.scope_identity()));
        let projection = proxy
            .tcp_runtime_services()
            .for_inbound("in".into())
            .with_business_identity(&first.session)
            .unwrap();
        assert!(
            projection
                .with_accepted_caller(Some(first.source.clone()))
                .is_err(),
            "opaque caller alone cannot replace an actual Engine admission"
        );
        let actual = proxy
            .tcp_runtime_services()
            .for_inbound("in".into())
            .with_admitted_business_identity(
                &first.session,
                first.invocation.admitted.clone().unwrap(),
            )
            .unwrap()
            .with_accepted_caller(Some(first.source.clone()))
            .unwrap();
        assert!(actual
            .clone()
            .with_accepted_caller(Some(first.source.clone()))
            .is_ok());
        assert!(
            actual
                .with_accepted_caller(Some(other.source.clone()))
                .is_err(),
            "an original demand cannot borrow a foreign actual handler"
        );
        let source = first.source.clone();
        first.join().await;
        other.join().await;
        assert!(source.check_open().is_err());
        let original = first.witness.try_joined().unwrap();
        let foreign = other.witness.try_joined().unwrap();
        assert_eq!(original.id(), first.witness.try_joined().unwrap().id());
        assert_eq!(foreign.id(), other.witness.try_joined().unwrap().id());
        assert_eq!(original.class(), TaskClass::Auxiliary);
        assert_eq!(foreign.class(), TaskClass::Auxiliary);
        assert!(original.result().is_ok());
        assert!(foreign.result().is_ok());
        assert!(!first.witness.same_registration(&other.witness));
        proxy
            .engine()
            .track_session(first.session.id)
            .finish(zero_engine::SessionOutcome::DirectRelayed)
            .unwrap();
        proxy
            .engine()
            .track_session(other.session.id)
            .finish(zero_engine::SessionOutcome::DirectRelayed)
            .unwrap();
    })
    .await
    .unwrap();
}
