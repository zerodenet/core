//! The existing raw-IP caller ledger, actual host Scope and B ingress projection.
#![cfg(all(feature = "raw-ip-runtime", feature = "vmess"))]
use crate::{
    component::{business_demand::AdmittedTcpDemand, direct_bridge::Invocation},
    runtime::{
        inbound_operation::raw_ip::native_obligations::{
            ChildKind, NativeObligations, NativeRegistration,
        },
        inbound_owners::InboundOwnerRegistry,
    },
    Proxy,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session, SessionAuth};
use zero_platform_tokio::contract::scope::TaskJoinWitness;

pub(in crate::component) struct Caller {
    registry: Arc<InboundOwnerRegistry>,
    registration: NativeRegistration,
    complete: Option<oneshot::Sender<()>>,
    parent: Option<oneshot::Sender<()>>,
    pub(in crate::component) session: Session,
    pub(in crate::component) invocation: Invocation,
}
impl Caller {
    pub(in crate::component) async fn new(proxy: &Proxy) -> Self {
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry
            .install("actual-carrier-caller", &proxy.protocols, 1, stop)
            .unwrap();
        let group = installed
            .child_tasks(NonZeroUsize::new(1).unwrap())
            .unwrap();
        let host = installed.tasks();
        let owner = installed.owner();
        let native = NativeObligations::new(1, 1, 1);
        installed.register(native.resource()).unwrap();
        let mut permit = native.reserve(ChildKind::Tcp).unwrap();
        let registration = permit.registration();
        let (parent, parent_done) = oneshot::channel();
        owner
            .spawn_driver(async move {
                parent_done.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap();
        let (complete, completed) = oneshot::channel();
        let (send, receive) = oneshot::channel();
        let engine = proxy.engine().clone();
        let services = proxy.tcp_runtime_services().for_inbound("in".into());
        let same = registration.clone();
        group
            .spawn_bound(&host, move |binding| async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                permit.bind_caller(binding).map_err(std::io::Error::other)?;
                permit.enter().map_err(std::io::Error::other)?;
                let mut session = Session::new(
                    0,
                    Address::Domain("native.example".into()),
                    443,
                    Network::Tcp,
                    ProtocolType::new("verified"),
                );
                session.source_ip = Some(Address::Ipv4([192, 0, 2, 12]));
                session.source_port = Some(48012);
                let mut auth = SessionAuth::new("verified");
                auth.principal_key = Some("business-grpc".into());
                auth.quota_remaining_bytes = Some(4096);
                auth.device_limit = Some(1);
                auth.up_bps = Some(4096);
                auth.down_bps = Some(8192);
                auth.policy_revision = Some(7);
                session.apply_auth(auth);
                let receipt = engine.prepare_session_admitted(&mut session, "in")?;
                let demand = AdmittedTcpDemand::new(&engine, SessionId(session.id), receipt)?;
                let invocation = services
                    .with_admitted_business_identity(&session, demand)?
                    .with_caller_native(Some(same))?
                    .execution()
                    .invocation
                    .unwrap();
                send.send((session, invocation))
                    .unwrap_or_else(|_| panic!("actual caller invocation receiver"));
                completed.await.map_err(std::io::Error::other)?;
                Ok(())
            })
            .unwrap();
        let (session, invocation) = receive.await.unwrap();
        assert!(invocation
            .caller_native
            .as_ref()
            .unwrap()
            .same_slot(&registration));
        assert!(registration.check_open().is_ok());
        Self {
            registry,
            registration,
            complete: Some(complete),
            parent: Some(parent),
            session,
            invocation,
        }
    }
    pub(in crate::component) fn witness(&self) -> TaskJoinWitness {
        self.registration.caller_witness().unwrap()
    }
    pub(in crate::component) async fn join(&mut self) {
        self.complete.take().unwrap().send(()).unwrap();
        self.parent.take().unwrap().send(()).unwrap();
        // The real Scope joins even though the separate raw native obligation
        // remains deliberately unaccounted until its original owner consumes C.
        assert!(self
            .registry
            .confirm_shutdown(
                "actual-carrier-caller",
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err());
        let actual = self.registration.caller_joined().unwrap();
        let witnessed = self.witness().try_joined().unwrap();
        assert_eq!(actual.task(), witnessed.id());
        assert_eq!(actual.class(), witnessed.class());
        assert!(actual.failure().is_none());
        assert!(witnessed.result().is_ok());
    }
}
