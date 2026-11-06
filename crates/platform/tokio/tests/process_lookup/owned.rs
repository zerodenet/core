//! Explicit standalone library Scope/root. No Host, Source or native socket
//! grant is fabricated for these existing OS socket-owner observations.
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration, TaskId};
use zero_platform_tokio::{
    contract::{
        scope::{TaskClass, TaskJoinWitness, TaskScope},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::CarrierOriginalTotals,
    LocalProcessInfo, LocalProcessLookup, LocalProcessTransport,
};
pub(super) struct LookupCaller {
    scope: TaskScope,
    caller: TaskJoinWitness,
    id: TaskId,
    writer: tokio::io::DuplexStream,
    root: Arc<CarrierOriginalTotals>,
}
impl LookupCaller {
    pub(super) async fn new() -> Self {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        scope.enable_registration().unwrap();
        let (mut reader, writer) = tokio::io::duplex(1);
        let (entered, pending) = tokio::sync::oneshot::channel();
        let (id, caller) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                let read = reader.read_u8();
                tokio::pin!(read);
                let mut entered = Some(entered);
                poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(entered) = entered.take() {
                            entered.send(()).unwrap();
                        }
                    }
                    result
                })
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                        .with_source(source)
                })?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap(); // Notification follows a real IO Pending poll.
        Self {
            scope,
            caller,
            id,
            writer,
            root: CarrierOriginalTotals::new(NonZeroUsize::MIN),
        }
    }
    pub(super) async fn lookup(
        &mut self,
        source: std::net::SocketAddr,
        transport: LocalProcessTransport,
    ) -> Option<LocalProcessInfo> {
        let registrar = self.scope.registrar();
        let original = LocalProcessLookup::prepare(
            self.caller.clone(),
            source,
            transport,
            registrar.cancellation(),
            NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        original
            .bind_original_totals(
                ComponentInstanceId {
                    component: ComponentId(914),
                    generation: ResourceGeneration(1),
                },
                &registrar.downgrade(),
                &self.root,
            )
            .unwrap();
        assert_eq!(
            self.root.snapshot().charged,
            0,
            "preparation has no actual query worker or native acquisition"
        );
        original.register(&registrar.downgrade()).unwrap().start();
        let worker = original.witness().unwrap();
        assert!(worker
            .scope_identity()
            .same_scope(&self.caller.scope_identity()));
        let result = original.result(&CancellationToken::new()).await.unwrap();
        assert!(
            original.collect_native_after_join().is_err(),
            "metadata output is not its actual Scope join"
        );
        let receipt = self.scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), worker.task_id().unwrap());
        let joined = worker.wait().await;
        assert!(worker.matches_joined(&joined));
        joined.result().unwrap();
        original.collect_native_after_join().unwrap();
        assert!(!joined.registration_released());
        assert!(
            !original.retired(),
            "native cleanup and real helper join do not ACK that registration"
        );
        assert_eq!(self.root.snapshot().charged, 1);
        self.scope.acknowledge(&receipt).unwrap();
        original
            .wait_retired(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(original.retired());
        assert_eq!(self.root.snapshot().charged, 0);
        assert!(
            !self.caller.is_joined(),
            "only the same helper retired; the real calling Scope remains live"
        );
        result
    }
    pub(super) async fn finish(mut self) {
        self.writer.write_u8(1).await.unwrap();
        let receipt = self.scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), self.id);
        assert!(self.caller.matches_joined(&self.caller.wait().await));
        receipt.result().as_ref().unwrap();
        self.scope.acknowledge(&receipt).unwrap();
        self.scope.begin_shutdown();
        assert!(self.scope.is_drained());
        assert_eq!(self.root.snapshot().charged, 0);
    }
}
