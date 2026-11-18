//! Original TCP IO outlives logical revocation only for its installed protocol cleanup.
use super::*;
use crate::contract::{
    component::ResourceDeclaration,
    lifecycle::{CloseReport, DynChannelControl},
    scope::TaskJoinWitness,
    BoxFuture, ChannelError,
};
use std::{future::Future, sync::OnceLock, task::Poll};

struct Protocol {
    stop: CancellationToken,
    driver: Arc<OnceLock<TaskJoinWitness>>,
}
impl DynChannelControl for Protocol {
    fn cancel(&self, _: crate::contract::CloseReason) {
        self.stop.cancel();
    }
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async {
            let witness = self
                .driver
                .get()
                .expect("original driver registered before retirement");
            let joined = witness.wait().await;
            assert!(witness.matches_joined(&joined));
            joined.result().map_err(ChannelError::retained)?;
            Ok(CloseReport::default()) // Protocol only; no fabricated native receipt.
        })
    }
}

#[tokio::test]
async fn original_protocol_tcp_flushes_its_tail_after_host_and_scope_stop_before_native_close() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
        let tasks = scope.registrar();
        let host = Arc::new(CancellationToken::new());
        let totals = CarrierOriginalTotals::new(NonZeroUsize::MIN);
        let registration = fixture.owner.registration();
        registration.bind_declared_with_totals(
            &ResourceDeclaration::ControlledCarrierPool {
                endpoint: fixture.destination.clone(), dial: fixture.transport.binding().dial.clone(),
                datagram: false, max_sockets: NonZeroUsize::MIN,
            }, &tasks, &host, &totals,
        ).unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let stop = CancellationToken::new();
        let driver = Arc::new(OnceLock::new());
        let (mut grant, _retirement) = fixture.grants.reserve_stream_protocol_original(
            &tasks, Box::new(Protocol { stop: stop.clone(), driver: driver.clone() }).into(),
        ).unwrap();
        let view = grant.claim_driver_view().unwrap();
        let carrier = fixture.transport.prepare_controlled_stream(fixture.destination.clone(), context(), grant).unwrap().open(deadline()).await.unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (io, original) = carrier.into_parts();
        let input = io.into_driver().try_map(|io| Ok::<_, ChannelError>((io, view))).unwrap();
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (task_id, witness) = original.spawn_draining_protocol_driver(&tasks, TaskClass::Auxiliary, input, move |(mut io, view)| async move {
            let _view = view;
            {
                let mut byte = [0];
                let read = io.read(&mut byte);
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending(), "actual native TCP read must suspend");
                    Poll::Ready(())
                }).await;
                ready.send(()).unwrap();
                tokio::select! {biased; _=stop.cancelled()=>{}, result=&mut read=>panic!("original read ended before protocol stop: {result:?}")}
            }
            io.write_all(b"actual HTTP tail\r\n0\r\n\r\n").await.map_err(|source| error(instance(), Stage::Send, Reason::Io, DeliveryState::Unknown).with_source(source))?;
            io.shutdown().await.map_err(|source| error(instance(), Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source))?;
            Ok(())
        }).unwrap();
        driver.set(witness.clone()).unwrap();
        pending.await.unwrap();
        host.cancel();
        scope.begin_shutdown();
        fixture.owner.revoke();
        assert!(!witness.is_joined());
        assert!(original.closed().is_none());
        let (report, ()) = tokio::join!(fixture.owner.shutdown(deadline()), async {
            let exit = scope.next_exit().await.unwrap();
            assert_eq!(exit.id, task_id);
        });
        assert!(report.unwrap().is_complete());
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"actual HTTP tail\r\n0\r\n\r\n");
        let native = original.closed().unwrap();
        assert!(native.matches(&original) && native.had_native_acquisition());
        assert_eq!(native.driver_exit().unwrap().id(), task_id);
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(original.users_closed().unwrap().matches(&original));
    }).await.unwrap();
}
