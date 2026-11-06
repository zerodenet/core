//! Real EOF/cancelled socket errors survive cancellation and late fault delivery.
use super::*;
use std::{io, sync::OnceLock};
struct FaultControl {
    owner: LoanOwner,
    failure: Arc<OnceLock<Arc<io::Error>>>,
    emitted: bool,
    pending: Option<CloseReport>,
}
#[derive(Debug)]
struct IoCause(Arc<io::Error>);
impl std::fmt::Display for IoCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for IoCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl FaultControl {
    fn receive(&mut self, report: CloseReport) -> Result<CloseReport, ChannelError> {
        if !report.is_complete() {
            return Ok(report);
        }
        self.pending = Some(report);
        self.finish()
    }
    fn finish(&mut self) -> Result<CloseReport, ChannelError> {
        if !self.emitted {
            if let Some(source) = self.failure.get() {
                self.emitted = true;
                return Err(ChannelError::new(
                    zero_platform_tokio::contract::Stage::Close,
                    Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(IoCause(source.clone())));
            }
        }
        Ok(self
            .pending
            .take()
            .expect("only an actual complete native report is handed back"))
    }
}
impl DynChannelControl for FaultControl {
    fn cancel(&self, reason: CloseReason) {
        self.owner.cancel(reason);
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        if self.pending.is_some() {
            return self.finish();
        }
        let report = self.owner.try_close()?;
        self.receive(report)
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            if self.pending.is_some() {
                return self.finish();
            }
            let report = self.owner.close(end).await?;
            self.receive(report)
        })
    }
}
#[tokio::test]
async fn actual_socket_faults_share_one_durable_received_row_and_late_secondary_without_losing_native_reports(
) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let end = Instant::now() + std::time::Duration::from_secs(3);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), peer.port());
        let instance = ComponentInstanceId {
            component: ComponentId(1852),
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
        let _bound = owner
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
        let mut loans = vec![];
        let mut originals = vec![];
        let mut stops = vec![];
        let mut peers = vec![];
        for _ in 0..2 {
            let mut grant = owner.grants().reserve().unwrap();
            let view = grant.claim_transferred_view().unwrap();
            let adoption = grant.adoption();
            stops.push(grant.cancellation());
            let native =
                NativeCarrierOpener::new(instance, binding.clone(), peer, egress.clone(), None)
                    .unwrap();
            let io = native
                .prepare_controlled_stream(
                    &binding,
                    endpoint.clone(),
                    CarrierContext {
                        session: None,
                        deadline: Some(end),
                        cancel: CancellationToken::new(),
                    },
                    grant,
                )
                .unwrap()
                .open_with_deadline(Some(end))
                .await
                .unwrap()
                .into_parts()
                .0;
            loans.push(io.into_stream_loan(view).unwrap());
            originals.push(adoption);
            peers.push(listener.accept().await.unwrap().0);
        }
        let second = loans.pop().unwrap();
        let mut first = loans.pop().unwrap();
        use tokio::io::AsyncWriteExt;
        peers[0].shutdown().await.unwrap();
        let mut byte = [0];
        let eof = first.io.read_exact(&mut byte).await.unwrap_err();
        assert_eq!(eof.kind(), io::ErrorKind::UnexpectedEof);
        let failures = [Arc::new(OnceLock::new()), Arc::new(OnceLock::new())];
        failures[0].set(Arc::new(eof)).unwrap();
        let first_origin = first.owner.original().clone();
        let second_origin = second.owner.original().clone();
        let first = zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
            Box::pin(first.io),
            Box::new(FaultControl {
                owner: first.owner,
                failure: failures[0].clone(),
                emitted: false,
                pending: None,
            }),
            first_origin,
        );
        let second = zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
            Box::pin(second.io),
            Box::new(FaultControl {
                owner: second.owner,
                failure: failures[1].clone(),
                emitted: false,
                pending: None,
            }),
            second_origin,
        );
        drop(first.io);
        let mut held = second.io;
        {
            let read = held.read_exact(&mut byte);
            tokio::pin!(read);
            futures_util::future::poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let transfer = Arc::new(Transfer {
            grant: Mutex::new(None),
            view: Mutex::new(None),
            caller: None,
            preparation: None,
            pending: Mutex::new(Vec::new()),
            preparing: Mutex::new(None),
            cleanup: Default::default(),
            owner: tokio::sync::Mutex::new(Some(second.owner)),
            stop: stops.pop().unwrap(),
            adoption: originals[1].clone(),
            published: true.into(),
            opening: false.into(),
        });
        let mut control = super::super::super::Control {
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
        drop(closing);
        // The first error and actual native close were durable before the other
        // real socket returned. No successful report or task result is invented.
        assert!(originals[0].closed().unwrap().matches(&originals[0]));
        assert!(originals[1].closed().is_none());
        let error = Arc::new(control.try_close().unwrap_err());
        let receipt = error.cleanup_failure_receipt().unwrap().clone();
        assert!(!receipt.is_acknowledged());
        let row = control
            .cleanup
            .lock()
            .unwrap()
            .faults
            .as_ref()
            .unwrap()
            .clone();
        assert!(row.sources[0].get().is_some());
        assert!(row.sources[1].get().is_none());
        assert!(receipt.acknowledge_retained(&error));
        // The actual pending LoanIo now reports its canonical cancellation.
        let interrupted = held.read_exact(&mut byte).await.unwrap_err();
        assert_eq!(interrupted.kind(), io::ErrorKind::BrokenPipe);
        failures[1].set(Arc::new(interrupted)).unwrap();
        drop(held);
        let pending = control.wait_closed(end).await.unwrap();
        assert!(!pending.is_complete());
        assert!(Arc::ptr_eq(
            &row,
            control.cleanup.lock().unwrap().faults.as_ref().unwrap()
        ));
        assert!(
            row.sources[1].get().is_some(),
            "the receiver's already retained row gained its actual late cause"
        );
        assert!(receipt.same_receipt(control.cleanup.lock().unwrap().receipt.as_ref().unwrap()));
        let report = control.wait_closed(end).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 2);
        assert_eq!(report.released_resources.len(), 2);
        for original in &originals {
            assert!(original.closed().unwrap().matches(original));
            assert!(original.users_closed().unwrap().matches(original));
        }
        assert_eq!(owner.retained(), 0);
        assert!(owner.shutdown(end).await.unwrap().is_complete());
        assert!(scope.shutdown(end).await.is_complete());
        drop(control);
        assert!(error.cleanup_failure_receipt().unwrap().is_acknowledged());
        assert!(row.sources.iter().all(|source| source.get().is_some()));
        drop(peers);
    })
    .await
    .unwrap();
}
