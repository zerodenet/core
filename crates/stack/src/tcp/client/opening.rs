//! One original TCP opening and its independent, retained logical controller.
//! Native endpoint ownership and real worker joins remain with the stack host.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicUsize, Mutex as StdMutex, Weak},
};
use tokio::sync::Notify;

struct Original {
    key: ConnKey,
    id: u64,
    control: Arc<TcpSendControl>,
}
struct Shared {
    connections: Weak<Mutex<HashMap<ConnKey, Conn>>>,
    original: StdMutex<Option<Original>>,
    stopped: AtomicBool,
    opening: AtomicUsize,
    settled: Notify,
}
/// A cold opening does not acquire a connection tuple, spawn or submit SYN.
/// Its controller can be installed by the caller before the first I/O poll.
pub struct ClientTcpOpening {
    connecting: Pin<Box<dyn Future<Output = Result<UserTcpStream, ClientTcpStackError>> + Send>>,
    control: ClientTcpSocketControl,
}
#[derive(Clone)]
pub struct ClientTcpSocketControl(Arc<Shared>);
/// Exact logical table removal and stopped I/O only. This does not attest
/// native endpoint closure, queued packet drain or a retransmission-task join.
pub struct ClientTcpClosed(Arc<Shared>);
struct Opening {
    control: ClientTcpSocketControl,
    connected: bool,
}
impl ClientTcpStack {
    pub fn prepare_connect(
        self: &Arc<Self>,
        local: IpAddr,
        remote: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> ClientTcpOpening {
        let control = ClientTcpSocketControl(Arc::new(Shared {
            connections: Arc::downgrade(&self.connections),
            original: StdMutex::new(None),
            stopped: AtomicBool::new(false),
            opening: AtomicUsize::new(0),
            settled: Notify::new(),
        }));
        let stack = self.clone();
        let original = control.clone();
        ClientTcpOpening {
            control,
            connecting: Box::pin(async move {
                let mut attempt = original.enter()?;
                let stream = stack
                    .connect_original(local, remote, observer, Some(&original))
                    .await?;
                if original.is_stopped() {
                    return Err(ClientTcpStackError::OutboundClosed);
                }
                attempt.connected = true;
                Ok(stream)
            }),
        }
    }
}
impl ClientTcpOpening {
    pub fn control(&self) -> ClientTcpSocketControl {
        self.control.clone()
    }
    pub async fn connect(self) -> Result<UserTcpStream, ClientTcpStackError> {
        self.connecting.await
    }
}
impl ClientTcpSocketControl {
    fn enter(&self) -> Result<Opening, ClientTcpStackError> {
        if self.is_stopped() {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        self.0.opening.fetch_add(1, Ordering::AcqRel);
        let attempt = Opening {
            control: self.clone(),
            connected: false,
        };
        if self.is_stopped() {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        Ok(attempt)
    }
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.load(Ordering::Acquire)
    }
    pub(super) async fn cancelled(&self) {
        loop {
            let changed = self.0.settled.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_stopped() {
                return;
            }
            changed.await;
        }
    }
    pub fn cancel(&self) {
        self.0.stopped.store(true, Ordering::Release);
        let control = self
            .0
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|original| original.control.clone());
        if let Some(control) = control {
            control.observe_transport_closed();
        }
        self.0.settled.notify_waiters();
    }
    /// Called while the original connection-table lock is held, before the
    /// matching entry escapes and before worker registration/SYN submission.
    pub(super) fn install(
        &self,
        key: ConnKey,
        id: u64,
        control: Arc<TcpSendControl>,
    ) -> Result<(), ClientTcpStackError> {
        let mut original = self
            .0
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_stopped() || original.is_some() {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        *original = Some(Original { key, id, control });
        Ok(())
    }
    pub async fn close(&self) -> ClientTcpClosed {
        self.cancel();
        loop {
            let settled = self.0.settled.notified();
            tokio::pin!(settled);
            settled.as_mut().enable();
            if self.0.opening.load(Ordering::Acquire) == 0 {
                break;
            }
            settled.await;
        }
        let original = self
            .0
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|original| (original.key, original.id));
        if let (Some(connections), Some((key, id))) = (self.0.connections.upgrade(), original) {
            let mut connections = connections.lock().await;
            if connections
                .get(&key)
                .is_some_and(|connection| connection.id == id)
            {
                // This explicit original removal also drops its connect waiter;
                // it never removes a later tuple incarnation.
                connections.remove(&key);
            }
        }
        ClientTcpClosed(self.0.clone())
    }
}
impl ClientTcpClosed {
    pub fn matches(&self, control: &ClientTcpSocketControl) -> bool {
        Arc::ptr_eq(&self.0, &control.0)
    }
}
impl Drop for Opening {
    fn drop(&mut self) {
        if !self.connected {
            self.control.cancel();
        }
        self.control.0.opening.fetch_sub(1, Ordering::AcqRel);
        self.control.0.settled.notify_waiters();
    }
}
