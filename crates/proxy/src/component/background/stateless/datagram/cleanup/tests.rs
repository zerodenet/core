//! Real originals: a ready native report cannot disappear with a sibling wait.
use super::*;
use std::{future::Future, task::Poll};
use tokio::io::AsyncReadExt;
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_platform_tokio::{
    contract::{lifecycle::ChannelControl, network::UpstreamOpener, scope::TaskScope},
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
};
struct LoanControl(LoanOwner);
impl DynChannelControl for LoanControl {
    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.try_close()
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.close(end))
    }
}
#[tokio::test]
async fn ready_original_report_survives_cancelled_close_while_second_real_tcp_view_is_pending() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let end = Instant::now() + std::time::Duration::from_secs(3);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), peer.port());
        let instance = ComponentInstanceId {
            component: ComponentId(1851),
            generation: ResourceGeneration(1),
        };
        let cancel = Arc::new(CancellationToken::new());
        let n = NonZeroUsize::new(2).unwrap();
        let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
        scope.enable_registration().unwrap();
        let mut owner = CarrierOwner::new(instance, n, &cancel);
        let declaration = ResourceDeclaration::ControlledCarrierPool {
            endpoint: endpoint.clone(),
            dial: Default::default(),
            datagram: false,
            max_sockets: n,
        };
        let _binding = owner
            .registration()
            .bind_declared(&declaration, &scope.registrar(), &cancel)
            .unwrap();
        owner.start(end).await.unwrap();
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let mut loans = Vec::new();
        let mut originals = Vec::new();
        let mut stops = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            let mut grant = owner.grants().reserve().unwrap();
            let view = grant.claim_transferred_view().unwrap();
            let original = grant.adoption();
            let stop = grant.cancellation();
            let native =
                NativeCarrierOpener::new(instance, binding.clone(), peer, egress.clone(), None)
                    .unwrap();
            let call = CarrierContext {
                session: None,
                deadline: Some(end),
                cancel: CancellationToken::new(),
            };
            let io = native
                .prepare_controlled_stream(&binding, endpoint.clone(), call, grant)
                .unwrap()
                .open_with_deadline(Some(end))
                .await
                .unwrap()
                .into_parts()
                .0;
            loans.push(io.into_stream_loan(view).unwrap());
            originals.push(original);
            stops.push(stop);
            peers.push(listener.accept().await.unwrap().0);
        }
        let second = loans.pop().unwrap();
        let first = loans.pop().unwrap();
        drop(first.io);
        let mut held = second.io;
        let mut bytes = [0; 1];
        {
            let read = held.read_exact(&mut bytes);
            tokio::pin!(read);
            futures_util::future::poll_fn(|cx| {
                assert!(
                    read.as_mut().poll(cx).is_pending(),
                    "actual TCP read is pending on the original second socket"
                );
                Poll::Ready(())
            })
            .await;
        }
        let transfer = Arc::new(Transfer {
            driver_input: Mutex::new(None),
            grant: Mutex::new(None),
            view: Mutex::new(None),
            caller: None,
            preparation: None,
            pending: Mutex::new(Vec::new()),
            preparing: Mutex::new(None),
            cleanup: Default::default(),
            cleanup_network: None,
            retained_prefix: Default::default(),
            owner: tokio::sync::Mutex::new(Some(second.owner)),
            stop: stops.pop().unwrap(),
            adoption: originals[1].clone(),
            published: true.into(),
            opening: false.into(),
        });
        let mut control = super::super::Control {
            inner: Box::new(LoanControl(first.owner)),
            transfer,
            cleanup: Mutex::default(),
        };
        let mut closing = Box::pin(control.wait_closed(end));
        futures_util::future::poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            originals[0].closed().is_some_and(
                |closed| closed.matches(&originals[0]) && closed.had_native_acquisition()
            )
        );
        assert!(
            originals[1].closed().is_none(),
            "second held actual view is not a close receipt"
        );
        drop(closing);
        let counts = {
            let state = control.cleanup.lock().unwrap();
            assert!(state.parts[0].complete && !state.parts[1].complete);
            let report = &state.parts[0].report;
            (
                report.closed_io.len(),
                report.released_resources.len(),
                report.exited_tasks.len(),
                report.failed_tasks.len(),
            )
        };
        assert!(
            counts.0 > 0,
            "first real native close report is retained in the original controller"
        );
        drop(held);
        let report = control.wait_closed(end).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(
            report.closed_io.len(),
            counts.0 + 1,
            "first accepted batch was not lost or repeated"
        );
        assert_eq!(report.released_resources.len(), counts.1 + 1);
        assert_eq!(report.exited_tasks.len(), counts.2);
        assert_eq!(report.failed_tasks.len(), counts.3);
        for original in &originals {
            assert!(original.closed().unwrap().matches(original));
            assert!(original.users_closed().unwrap().matches(original));
        }
        assert_eq!(owner.retained(), 0);
        assert!(owner.shutdown(end).await.unwrap().is_complete());
        assert!(scope.shutdown(end).await.is_complete());
        drop(peers);
    })
    .await
    .unwrap();
}

mod faults;
