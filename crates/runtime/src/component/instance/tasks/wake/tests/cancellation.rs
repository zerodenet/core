use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};

struct CancelPanic {
    monitor: Weak<TaskMonitor>,
    pending: AtomicBool,
}
impl Wake for CancelPanic {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // Real reentry first: panic must never poison the enclosing owner lock.
        let _actual = self.monitor.upgrade().unwrap().registrar_for_test();
        if self.pending.swap(false, Ordering::SeqCst) {
            panic!("actual cancellation callback panic after parent unlock");
        }
    }
}

#[test]
fn cancellation_callback_panic_retains_unjoined_task_until_actual_io_exit_and_fault_delivery() {
    bounded(|| {
        runtime().block_on(async {
            let monitor = monitor(NonZeroUsize::new(1));
            let registrar = monitor.registrar_for_test();
            let (mut io, mut peer) = tokio::io::duplex(1);
            let (entered, observed) = oneshot::channel();
            let (id, witness) = registrar
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    let reading = io.read_u8();
                    tokio::pin!(reading);
                    let mut entered = Some(entered);
                    std::future::poll_fn(|cx| {
                        let result = reading.as_mut().poll(cx);
                        if result.is_pending() {
                            if let Some(entered) = entered.take() {
                                entered.send(()).unwrap();
                            }
                        }
                        result
                    })
                    .await
                    .unwrap();
                    Err(
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(std::io::Error::other(
                                "actual worker fault after retained I/O",
                            )),
                    )
                })
                .unwrap();
            observed.await.unwrap(); // Actual registered read was polled Pending.
            let cancellation = registrar.cancellation();
            let mut cancelling = Box::pin(cancellation.cancelled());
            let waker = Waker::from(Arc::new(CancelPanic {
                monitor: Arc::downgrade(&monitor),
                pending: AtomicBool::new(true),
            }));
            assert!(cancelling
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            let failure =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| monitor.begin_shutdown()));
            assert!(failure.is_err());
            assert!(monitor.inner.try_lock().is_ok());
            assert!(cancellation.is_cancelled());
            assert!(witness.try_joined().is_none());
            assert_eq!(
                registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
                Err(SpawnError::Closing)
            );
            let mut pending = CloseReport::default();
            monitor
                .collect(tokio::time::Instant::now(), |report| pending.append(report))
                .await;
            assert!(pending.unfinished.contains(&Unfinished::Task(id)));
            assert!(pending.exited_tasks.is_empty());
            assert!(pending.failed_tasks.is_empty());
            assert!(!monitor.lock().scope.is_joined());
            // Repeating cancellation cannot stand in for this actual I/O exit.
            peer.write_u8(7).await.unwrap();
            let target = Arc::new(WaitTarget::default());
            let joined = std::future::poll_fn(|cx| {
                target.register(cx);
                monitor.poll(&target, instance(), 1);
                match witness.try_joined() {
                    Some(joined) => Poll::Ready(joined),
                    None => Poll::Pending,
                }
            })
            .await;
            assert_eq!(joined.id(), id);
            assert_eq!(joined.class(), TaskClass::Auxiliary);
            let original = joined.result().unwrap_err();
            let source = crate::component::tests::source::<std::io::Error>(original.as_ref());
            let mut final_report = CloseReport::default();
            {
                let collecting = monitor.collect(
                    tokio::time::Instant::now() + Duration::from_secs(1),
                    |report| final_report.append(report),
                );
                tokio::pin!(collecting);
                std::future::poll_fn(|cx| {
                    target.register(cx);
                    monitor.poll(&target, instance(), 1);
                    collecting.as_mut().poll(cx)
                })
                .await;
            }
            assert!(final_report.is_complete());
            assert_eq!(final_report.exited_tasks, [id]);
            assert_eq!(final_report.failed_tasks.len(), 1);
            let fault = &final_report.failed_tasks[0];
            assert_eq!(fault.id, id);
            assert_eq!(fault.class, TaskClass::Auxiliary);
            assert!(std::ptr::eq(
                source,
                crate::component::tests::source::<std::io::Error>(&fault.error)
            ));
            assert!(monitor.lock().scope.is_drained());
            assert!(
                monitor.fault().is_none(),
                "Auxiliary is not promoted to Critical"
            );
        });
    });
}
