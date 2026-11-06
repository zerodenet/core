use super::*;
use crate::{
    contract::{
        component::{ControlledStreamDemandClass, ProcessRequest, ResourceDeclaration},
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::{TaskClass, TaskScope},
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
    owned_process::{ProcessBudget, ProcessBudgetLimits},
};
use std::{num::NonZeroUsize, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};
use zero_core::{
    Address,
    component::{ComponentId, NetworkGeneration, ResourceGeneration},
    request::Destination,
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn instance(n: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(n),
        generation: ResourceGeneration(1),
    }
}
fn endpoint() -> Destination {
    Destination::new(Address::Domain("actual.plugin.remote.invalid".into()), 443)
}
struct Running {
    scope: TaskScope,
    process: Arc<OwnedProcess>,
    handoff: Arc<ProcessEndpointHandoff>,
    witness: TaskJoinWitness,
    budget: Arc<ProcessBudget>,
    stop: oneshot::Sender<()>,
    egress: Arc<crate::EgressInterfaceControl>,
}
impl Running {
    async fn start() -> Self {
        let scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let spec = ProcessEndpointSpec {
            stream: true,
            datagram: true,
            ipv6: false,
            network_generation: NetworkGeneration(egress.generation()),
        };
        let handoff = ProcessEndpointHandoff::new(
            instance(719),
            0,
            spec,
            &scope.registrar(),
            CancellationToken::new(),
        )
        .unwrap();
        let process =
            OwnedProcess::prepare(ProcessRequest::new("python3"), &scope.registrar()).unwrap();
        let budget = ProcessBudget::new(ProcessBudgetLimits {
            max_jobs: 1,
            max_jobs_per_instance: 1,
        })
        .unwrap();
        budget.reserve(instance(719), &process).unwrap();
        let (go, begin) = oneshot::channel();
        let (ready, waiting) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        let actual = process.clone();
        let original = handoff.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                let witness = begin.await.unwrap();
                actual.start(witness).unwrap();
                actual.wait_started(end()).await.unwrap();
                let deadline = end();
                loop {
                    match original
                        .open_probe(&actual.actor().unwrap(), deadline)
                        .await
                    {
                        Ok(mut probe) => {
                            assert_eq!(probe.peer_addr().unwrap(), original.address().unwrap());
                            assert_ne!(probe.local_addr().unwrap(), probe.peer_addr().unwrap());
                            let closed = probe.close(end()).await.unwrap().unwrap();
                            assert!(closed.matches(&probe) && closed.had_native_acquisition());
                            assert!(
                                !actual.actor().unwrap().is_joined(),
                                "probe does not self-wait its original actor"
                            );
                            break;
                        }
                        Err(error) => {
                            assert!(Instant::now() < deadline, "{error}");
                            assert!(original.close_probe(end()).await.unwrap().is_some());
                            tokio::task::yield_now().await;
                        }
                    }
                }
                ready.send(()).unwrap();
                stopped.await.unwrap();
                actual.request_stop();
                actual.wait_tree_ended(end()).await.unwrap();
                Ok(())
            })
            .unwrap();
        process.bind_actor(witness.clone()).unwrap();
        process.attach_endpoint(&handoff).unwrap();
        let address = handoff.acquire(&scope.registrar(), &witness).unwrap();
        assert_eq!(budget.snapshot().charged_jobs, 1);
        let reservations = handoff.close_reservations(end()).await.unwrap();
        assert!(reservations.matches(&handoff));
        assert_eq!(
            (reservations.tcp_originals, reservations.udp_originals),
            (1, 1)
        );
        let mut request = ProcessRequest::new("python3");
        request.arguments = vec![
            "-c".into(),
            include_str!("echo.py").into(),
            address.ip().to_string().into(),
            address.port().to_string().into(),
        ];
        process.install_endpoint_request(request, &handoff).unwrap();
        go.send(witness.clone()).unwrap();
        tokio::time::timeout_at(end(), waiting)
            .await
            .unwrap()
            .unwrap();
        Self {
            scope,
            process,
            handoff,
            witness,
            budget,
            stop,
            egress,
        }
    }
    async fn finish(mut self) {
        self.stop.send(()).unwrap();
        let (native, tasks) =
            tokio::join!(self.process.wait_closed(end()), self.scope.shutdown(end()));
        assert!(tasks.is_complete(), "{tasks:?}");
        assert!(self.witness.is_joined());
        assert!(native.unwrap().unwrap().matches(&self.process));
        assert!(self.handoff.closed().unwrap().matches(&self.handoff));
        assert_eq!(self.budget.snapshot().charged_jobs, 0);
    }
}
mod pending;
mod users;
#[tokio::test]
async fn process_endpoint_probe_uses_actual_native_and_never_waits_its_own_actor_join() {
    Running::start().await.finish().await;
}
