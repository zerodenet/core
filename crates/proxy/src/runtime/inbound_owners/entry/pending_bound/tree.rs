//! The bounded tree is the actual pre-bound shape, not an I/O history.
use super::*;
use zero_platform_tokio::{contract::lifecycle::ChannelControl, ControlledTcpListenerControl};

pub(in crate::runtime::inbound_owners::entry) struct PendingNativeBound {
    tree: AsyncMutex<Node>,
    report: Mutex<CloseReport>,
    errors: Mutex<Vec<SharedError>>,
    closed: AtomicBool,
}
struct Node {
    kind: Kind,
    failure: Option<SharedError>,
}
enum Kind {
    Raw(Option<zero_platform_tokio::TokioListener>),
    Tcp(ControlledTcpListenerControl),
    Closed,
}
impl Node {
    fn raw(bound: zero_platform_tokio::TokioListener) -> Self {
        Self {
            kind: Kind::Raw(Some(bound)),
            failure: None,
        }
    }
    fn prepare(&mut self, cancel: CancellationToken) {
        let Kind::Raw(bound) = &mut self.kind else {
            return;
        };
        // Called synchronously under the persistent owner's pending-slot lock.
        let listener = bound.take().expect("one original bound object");
        match listener.try_into_controlled(cancel) {
            Ok((view, control)) => {
                self.kind = Kind::Tcp(control);
                drop(view);
            }
            Err(listener) => self.kind = Kind::Raw(Some(listener)),
        }
    }

    fn close<'a>(
        &'a mut self,
        report: &'a Mutex<CloseReport>,
        errors: &'a Mutex<Vec<SharedError>>,
        deadline: Instant,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<bool, SharedError>> + Send + 'a>> {
        Box::pin(async move {
            match &mut self.kind {
                Kind::Tcp(control) => match control.wait_closed(deadline).await {
                    Ok(mut closed) => {
                        let complete = closed.is_complete();
                        // Current unfinished ownership is the retained node,
                        // not a history of expired deadline descriptions.
                        closed.unfinished.clear();
                        report
                            .lock()
                            .expect("pending native close report poisoned")
                            .append(closed);
                        if complete {
                            self.kind = Kind::Closed;
                        }
                        Ok(complete)
                    }
                    Err(error) => {
                        if self.failure.is_none() {
                            let error = Arc::new(error);
                            errors
                                .lock()
                                .expect("pending native errors poisoned")
                                .push(error.clone());
                            self.failure = Some(error);
                        }
                        Err(self.failure.as_ref().unwrap().clone())
                    }
                },
                Kind::Closed => Ok(true),
                Kind::Raw(bound) => {
                    assert!(bound.is_some());
                    Ok(false)
                }
            }
        })
    }
}
impl PendingNativeBound {
    pub(super) fn new(bound: zero_platform_tokio::TokioListener) -> Self {
        Self {
            tree: AsyncMutex::new(Node::raw(bound)),
            report: Mutex::new(CloseReport::default()),
            errors: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        }
    }
    pub(super) fn prepare(&self, cancel: CancellationToken) {
        self.tree
            .try_lock()
            .expect("new pending listener is not polling")
            .prepare(cancel);
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> Result<bool, SharedError> {
        let result = self
            .tree
            .lock()
            .await
            .close(&self.report, &self.errors, deadline)
            .await;
        if matches!(result, Ok(true)) {
            self.closed.store(true, Ordering::Release);
        }
        result
    }
    pub(in crate::runtime::inbound_owners::entry) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(in crate::runtime::inbound_owners::entry) fn take_report(&self) -> CloseReport {
        std::mem::take(
            &mut *self
                .report
                .lock()
                .expect("pending native close report poisoned"),
        )
    }
    pub(in crate::runtime::inbound_owners::entry) fn failures(&self) -> Box<[SharedError]> {
        self.errors
            .lock()
            .expect("pending native errors poisoned")
            .clone()
            .into_boxed_slice()
    }
    pub(in crate::runtime::inbound_owners::entry) fn first_failure(&self) -> Option<SharedError> {
        self.errors
            .lock()
            .expect("pending native errors poisoned")
            .first()
            .cloned()
    }
}
impl InboundOwner {
    pub(in crate::runtime::inbound_owners::entry) async fn close_pending_bound(
        &self,
        deadline: Instant,
    ) -> Result<(), std::io::Error> {
        let pending = self
            .pending_native_bound
            .lock()
            .expect("pending native listener poisoned")
            .clone();
        let Some(pending) = pending else {
            return Ok(());
        };
        let result = pending.shutdown(deadline).await;
        self.report
            .lock()
            .expect("inbound close report poisoned")
            .append(pending.take_report());
        self.retain_pending_native_faults(&pending);
        match result {
            Ok(true) => Ok(()),
            Ok(false) => self
                .pending_native_result()
                .map_err(|error| match error {
                    EngineError::Io(error) => error,
                    error => std::io::Error::other(error),
                })
                .and_then(|()| {
                    Err(std::io::Error::other(
                        "original pending native listener still lacks its close receipt",
                    ))
                }),
            Err(source) => Err(std::io::Error::other(retained(source))),
        }
    }
    pub(in crate::runtime::inbound_owners::entry) fn retain_pending_native_faults(
        &self,
        pending: &PendingNativeBound,
    ) {
        if let Some(source) = pending.first_failure() {
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert(source.into());
        }
    }
}
