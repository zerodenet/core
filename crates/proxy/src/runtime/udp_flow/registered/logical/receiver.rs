//! The actual standard Datagram RX, polled by its original association caller.
use super::{datagram::DatagramConnection, *};
use crate::protocol_registry::TcpExecutionServices;
use std::{
    future::Future,
    pin::Pin,
    sync::{Mutex, Weak},
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{
    component::{PacketDriverMetadataView, PacketMetadataVec},
    scope::TaskJoinWitness,
    ChannelError, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
pub(crate) type Reply = (Address, u16, zero_traits::PacketBuffer, Option<u64>);
type Read = Pin<Box<dyn Future<Output = Result<Reply, EngineError>> + Send>>;
pub(crate) struct NativeReceiver {
    pub(crate) original: TaskJoinWitness,
    pub(crate) metadata: PacketDriverMetadataView,
    pub(crate) maximum: usize,
    #[cfg(test)]
    pending: AtomicBool,
    session: u64,
    io: Mutex<Option<Arc<DatagramConnection>>>,
    work: Mutex<Option<Read>>,
    closed: Arc<AtomicBool>,
    stopped: AtomicBool,
    complete: AtomicBool,
    panicked: AtomicBool,
    wake: Mutex<Option<Weak<crate::runtime::udp_flow::response_join::set::wake::Relay>>>,
    // The actual Box is destroyed before this reusable exact allocation credit.
    _body_credit: PacketMetadataCredit,
}
async fn read(io: Arc<DatagramConnection>, session: u64) -> Result<Reply, EngineError> {
    let (target, port, payload) = io.receive_packet().await?;
    Ok((target, port, payload, Some(session)))
}

fn failure(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Receive, reason, Impact::Session)
        .with_source(std::io::Error::other(message))
}
impl NativeReceiver {
    pub(super) fn prepare(
        execution: &TcpExecutionServices,
        io: Arc<DatagramConnection>,
        closed: Arc<AtomicBool>,
        session: u64,
    ) -> Result<PacketMetadataArc<Self>, EngineError> {
        let (metadata, original) =
            crate::component::stream_bridge::actual_protocol_caller(execution)
                .map_err(std::io::Error::other)?
                .ok_or_else(|| {
                    std::io::Error::other("Datagram RX requires its actual registered caller")
                })?;
        let body = read(io.clone(), session);
        let bytes = std::mem::size_of_val(&body);
        let body_layout = PacketMetadataLayout::new(usize::from(bytes != 0), bytes);
        let layout = PacketMetadataArc::<Self>::layout()
            .and_then(|layout| layout.checked_add(body_layout))
            .map_err(std::io::Error::other)?;
        let mut credit = metadata.reserve(layout).map_err(std::io::Error::other)?;
        let body_credit = credit.split(body_layout).map_err(std::io::Error::other)?;
        PacketMetadataArc::from_prepaid(
            Self {
                original,
                #[cfg(test)]
                pending: false.into(),
                metadata,
                maximum: execution.retired_datagrams().capacity(),
                session,
                io: Mutex::new(Some(io)),
                work: Mutex::new(Some(Box::pin(body))),
                closed,
                stopped: false.into(),
                complete: false.into(),
                panicked: false.into(),
                wake: Mutex::new(None),
                _body_credit: body_credit,
            },
            credit,
        )
        .map_err(|(source, _, _)| std::io::Error::other(source).into())
    }
    pub(in crate::runtime::udp_flow) fn bind(
        &self,
        wake: &Arc<crate::runtime::udp_flow::response_join::set::wake::Relay>,
    ) {
        *self.wake.lock().unwrap() = Some(Arc::downgrade(wake));
    }
    pub(crate) fn changed(&self) {
        let wake = self.wake.lock().unwrap().clone();
        if let Some(wake) = wake.and_then(|wake| wake.upgrade()) {
            wake.signal();
        }
    }
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.changed();
    }
    pub(crate) fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    fn return_view(&self) {
        let work = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let io = self
            .io
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(work);
        drop(io);
        self.complete.store(true, Ordering::Release);
    }
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<Reply, Arc<EngineError>>> {
        if self.complete() || self.panicked.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if self.original.native_task_id() != tokio::task::try_id() {
            return Poll::Ready(Err(Arc::new(
                std::io::Error::other(failure(
                    Reason::StalePlan,
                    "Datagram RX moved away from its actual caller",
                ))
                .into(),
            )));
        }
        if self.stopped.load(Ordering::Acquire) {
            self.return_view();
            return Poll::Pending;
        }
        if self.closed.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        let work = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let work = work.unwrap_or_else(|| {
            let io = self
                .io
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .expect("same live native RX")
                .clone();
            Box::pin(read(io, self.session))
        });
        let mut polling = Polling {
            receiver: self,
            work: Some(work),
            finished: false,
        };
        match polling.work.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                #[cfg(test)]
                self.pending.store(true, Ordering::Release);
                Poll::Pending
            }
            Poll::Ready(result) => {
                #[cfg(test)]
                self.pending.store(false, Ordering::Release);
                polling.finished = true;
                drop(polling.work.take());
                if result.is_err() {
                    self.closed.store(true, Ordering::Release);
                }
                Poll::Ready(result.map_err(Arc::new))
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn session_for_test(&self) -> u64 {
        self.session
    }
    #[cfg(test)]
    pub(crate) fn pending_for_test(&self) -> bool {
        self.pending.load(Ordering::Acquire) && !self.complete()
    }
    pub(crate) fn return_after_lease(&self) {
        // Called by the same sole dispatcher borrower for a failed first-open,
        // or by ResponseSetOwner after acquiring that returned exclusive lease.
        // No read is still being polled. End this exact
        // operation's custody; the original Scope and protocol/native controller
        // remain independent gates in their existing providers. Waiting for our
        // own caller join here would deadlock its actual cleanup continuation.
        self.stop();
        self.return_view();
    }
}
struct Polling<'a> {
    receiver: &'a NativeReceiver,
    work: Option<Read>,
    finished: bool,
}
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if std::thread::panicking() {
            self.receiver.panicked.store(true, Ordering::Release);
        }
        let previous = self
            .receiver
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(self.work.take().expect("same native receive operation"));
        assert!(previous.is_none(), "one original association RX poller");
    }
}
#[derive(Default)]
pub(crate) struct Receivers {
    rows: Mutex<PacketMetadataVec<PacketMetadataArc<NativeReceiver>>>,
    cursor: std::sync::atomic::AtomicUsize,
}
impl Receivers {
    pub(crate) fn insert(
        &self,
        receiver: &PacketMetadataArc<NativeReceiver>,
    ) -> Result<(), EngineError> {
        let _notifications = receiver.metadata.notifications();
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        rows.retain(|entry| !entry.complete());
        if rows.iter().any(|entry| std::ptr::eq(&**entry, &**receiver)) {
            return Ok(());
        }
        if receiver.original.is_joined()
            || receiver.original.native_task_id() != tokio::task::try_id()
            || rows
                .iter()
                .any(|entry| !entry.original.same_registration(&receiver.original))
        {
            return Err(std::io::Error::other(failure(
                Reason::StalePlan,
                "Datagram response group belongs to another original caller",
            ))
            .into());
        }
        rows.try_push_with_metadata(receiver.clone(), &receiver.metadata, receiver.maximum)
            .map_err(|(source, _)| std::io::Error::other(source).into())
    }
    fn at(&self, index: usize) -> Option<PacketMetadataArc<NativeReceiver>> {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(index)
            .cloned()
    }
    pub(crate) fn len(&self) -> usize {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|entry| !entry.complete())
            .count()
    }
    pub(crate) fn stop(&self) {
        let mut index = 0;
        while let Some(entry) = self.at(index) {
            entry.stop();
            index += 1;
        }
    }
    pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<Reply, Arc<EngineError>>> {
        let count = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        if count == 0 {
            return Poll::Pending;
        }
        let start = self.cursor.load(Ordering::Relaxed) % count;
        for step in 0..count {
            let index = (start + step) % count;
            if let Some(entry) = self.at(index) {
                if let Poll::Ready(reply) = entry.poll(cx) {
                    self.cursor.store((index + 1) % count, Ordering::Relaxed);
                    return Poll::Ready(reply);
                }
            }
        }
        Poll::Pending
    }
    #[cfg(test)]
    pub(crate) fn snapshot_for_test(&self) -> Vec<PacketMetadataArc<NativeReceiver>> {
        self.rows.lock().unwrap().iter().cloned().collect()
    }
    pub(crate) fn return_after_lease(&self) {
        let mut index = 0;
        while let Some(entry) = self.at(index) {
            entry.return_after_lease();
            index += 1;
        }
    }
}
