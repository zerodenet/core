//! The bounded tree is the actual pre-bound shape, not an I/O history.
use super::*;
use crate::protocol_registry::BoundInbound;
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
    Raw(Option<BoundInbound>),
    Tcp(ControlledTcpListenerControl),
    #[cfg(feature = "managed-datagram-runtime")]
    Datagram(std::sync::Arc<tokio::net::UdpSocket>),
    #[cfg(any(
        feature = "inbound-listener-group-runtime",
        feature = "managed-datagram-runtime"
    ))]
    Group(Vec<Node>),
    Closed,
}
impl Node {
    fn raw(bound: BoundInbound) -> Self {
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
        match bound.take().expect("one original bound object") {
            BoundInbound::Tcp(listener) => {
                match listener.try_into_controlled(cancel) {
                    Ok((view, control)) => {
                        self.kind = Kind::Tcp(control);
                        drop(view); // only retires admission; control still owns the fd
                    }
                    // Another actual owner has its control. Retain this same
                    // view, never manufacture a second control or a close ACK.
                    Err(listener) => self.kind = Kind::Raw(Some(BoundInbound::Tcp(listener))),
                }
            }
            #[cfg(feature = "inbound-listener-group-runtime")]
            BoundInbound::Group(listeners) => {
                self.kind = Kind::Group(listeners.into_iter().map(Node::raw).collect());
                if let Kind::Group(children) = &mut self.kind {
                    for child in children {
                        child.prepare(cancel.clone());
                    }
                }
            }
            #[cfg(feature = "managed-datagram-runtime")]
            BoundInbound::TcpAndDatagram(tcp, datagram) => {
                self.kind = Kind::Group(vec![
                    Node::raw(BoundInbound::Tcp(tcp)),
                    Node {
                        kind: Kind::Datagram(datagram),
                        failure: None,
                    },
                ]);
                if let Kind::Group(children) = &mut self.kind {
                    children[0].prepare(cancel);
                }
            }
            #[cfg(feature = "datagram-route-runtime")]
            BoundInbound::Datagram(socket) => {
                self.kind = Kind::Raw(Some(BoundInbound::Datagram(socket)));
            }
            #[cfg(feature = "datagram-route-runtime")]
            BoundInbound::ControlledDatagram(view) => {
                // The lease registry retains the sole control. A transferred
                // view is never enough to issue an independent close receipt.
                self.kind = Kind::Raw(Some(BoundInbound::ControlledDatagram(view)));
            }
            #[cfg(feature = "transport_quic")]
            BoundInbound::Quic(listener) => {
                self.kind = Kind::Raw(Some(BoundInbound::Quic(listener)));
            }
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
                #[cfg(any(
                    feature = "inbound-listener-group-runtime",
                    feature = "managed-datagram-runtime"
                ))]
                Kind::Group(children) => {
                    let mut complete = true;
                    let mut failure = None;
                    for child in children {
                        match child.close(report, errors, deadline).await {
                            Ok(closed) => complete &= closed,
                            Err(error) => {
                                complete = false;
                                failure.get_or_insert(error);
                            }
                        }
                        // Every completed child and source is durable before
                        // another leaf can suspend or the caller can cancel.
                    }
                    failure.map_or(Ok(complete), Err)
                }
                Kind::Closed => Ok(true),
                Kind::Raw(bound) => {
                    assert!(bound.is_some());
                    Ok(false)
                }
                #[cfg(feature = "managed-datagram-runtime")]
                Kind::Datagram(socket) => {
                    let _original = socket;
                    Ok(false) // still the actual Arc; no UDP native receipt
                }
            }
        })
    }
}
impl PendingNativeBound {
    pub(super) fn new(bound: BoundInbound) -> Self {
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
                .get_or_insert(source);
        }
    }
}
