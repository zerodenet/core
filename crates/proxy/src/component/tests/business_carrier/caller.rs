//! The existing raw-IP caller ledger, actual host Scope and B ingress projection.
#![cfg(all(feature = "raw-ip-runtime", feature = "vmess"))]
use crate::{
    component::{business_demand::AdmittedBusinessDemand, direct_bridge::Invocation},
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
    jobs: super::jobs::Sender,
    parent: Option<oneshot::Sender<()>>,
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
        let (complete, mut completed) = oneshot::channel();
        let (jobs, mut pending_jobs) = super::jobs::channel();
        let (send, receive) = oneshot::channel();
        let engine = proxy.engine().clone();
        let services = proxy.tcp_runtime_services().for_inbound("in".into());
        let same = registration.clone();
        group
            .spawn_bound(&host, move |binding| async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                permit.bind_caller(binding).map_err(std::io::Error::other)?;
                permit.enter().map_err(std::io::Error::other)?;
                let mut session = session;
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
                let demand = AdmittedBusinessDemand::new(&engine, SessionId(session.id), receipt)?;
                let invocation = services
                    .with_admitted_business_identity(&session, demand)?
                    .with_caller_native(Some(same))?
                    .execution()
                    .invocation
                    .unwrap();
                send.send((session, invocation.clone()))
                    .unwrap_or_else(|_| panic!("actual caller invocation receiver"));
                loop {
                    tokio::select! {
                        done = &mut completed => { done.map_err(std::io::Error::other)?; break; }
                        job = pending_jobs.recv() => {
                            let job = job.expect("original caller command sender");
                            job(invocation.clone()).await;
                        }
                    }
                }
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
            jobs,
            parent: Some(parent),
            session,
            invocation,
        }
    }
    pub(in crate::component) async fn run<F, Fut, T>(&self, job: F) -> T
    where
        F: FnOnce(Invocation) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.jobs.run(job).await
    }
    pub(in crate::component) fn witness(&self) -> TaskJoinWitness {
        self.registration.caller_witness().unwrap()
    }
    pub(in crate::component) fn verify_carrier_completion(&self) {
        let receipt = self
            .registration
            .carrier_completion()
            .expect("original raw caller must observe its actual native and users closure");
        assert!(receipt.matches(&self.registration));
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
        assert!(
            self.registration.carrier_completion().is_none(),
            "caller exit and transport exit alone cannot mint native closure"
        );
    }
}

#[tokio::test]
async fn raw_udp_admission_keeps_the_actual_registered_caller_and_source_metadata() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut caller = Caller::new(&proxy).await;
    let witness = caller.witness();
    let actual = proxy.clone();
    let session = caller
        .run(move |invocation| async move {
            let native = invocation.caller_native.unwrap();
            let runtime =
                crate::runtime::udp_ingress::UdpIngressRuntime::new(actual.tcp_runtime_services())
                    .with_native(Some(native.clone()));
            let mut session = Session::new(
                0,
                Address::Ipv4([203, 0, 113, 9]),
                53,
                Network::Udp,
                ProtocolType::new("actual-raw-udp"),
            );
            let admission = runtime
                .prepare_udp_session(&mut session, "in")
                .await
                .unwrap();
            let runtime = runtime.with_admission(&session, admission).unwrap();
            let execution = runtime
                .runtime_services()
                .datagram_execution("in", &session)
                .unwrap();
            let retained = execution
                .invocation
                .as_ref()
                .unwrap()
                .caller_native
                .as_ref()
                .unwrap();
            assert!(retained.same_slot(&native));
            let (memory, original) =
                crate::component::stream_bridge::actual_protocol_caller(&execution)
                    .unwrap()
                    .unwrap();
            assert!(original.same_registration(&witness));
            assert!(memory.same_root(&native.protocol_metadata().unwrap()));
            session
        })
        .await;
    proxy
        .engine()
        .track_session(session.id)
        .finish(zero_engine::SessionOutcome::Cancelled)
        .unwrap();
    proxy
        .engine()
        .track_session(caller.session.id)
        .finish(zero_engine::SessionOutcome::Cancelled)
        .unwrap();
    caller.join().await;
}
