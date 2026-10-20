use super::*;
use crate::controlled_datagram::ControlledDatagramIdentity;
use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Clone)]
pub struct RootDatagramUse {
    request: RootDatagramRequest,
    query: TaskJoinWitness,
    identity: ControlledDatagramIdentity,
    returned: Arc<AtomicBool>,
}
impl RootDatagramUse {
    pub(super) fn new(
        request: RootDatagramRequest,
        query: TaskJoinWitness,
        identity: ControlledDatagramIdentity,
    ) -> Self {
        Self {
            request,
            query,
            identity,
            returned: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn request(&self) -> &RootDatagramRequest {
        &self.request
    }
    pub fn query(&self) -> &TaskJoinWitness {
        &self.query
    }
    pub fn view_returned(&self) -> bool {
        self.returned.load(Ordering::Acquire)
    }
    pub fn same_use(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.returned, &other.returned)
    }
    pub fn matches_control(&self, control: &ControlledDatagramControl) -> bool {
        self.identity.matches_control(control)
    }
    pub(super) fn matches_request(&self, request: &RootDatagramRequest) -> bool {
        self.request.authority.matches_request(request)
            && self.request.access.ptr_eq(&request.access)
            && self.request.instance == request.instance
            && self.request.ordinal == request.ordinal
            && self.request.peer == request.peer
            && self.request.generation == request.generation
    }
}
/// Sole IO view; no native/control/raw-socket escape hatch. Drop marks only
/// return of this unique view. It is neither native close nor query join.
pub struct RootDatagramIo {
    socket: Option<ControlledDatagramSocket>,
    usage: RootDatagramUse,
}
impl RootDatagramIo {
    pub(super) fn new(socket: ControlledDatagramSocket, usage: RootDatagramUse) -> Self {
        Self {
            socket: Some(socket),
            usage,
        }
    }
    pub(super) fn usage(&self) -> &RootDatagramUse {
        &self.usage
    }
    pub(super) fn into_returned(mut self) -> RootDatagramUse {
        drop(self.socket.take());
        self.usage.returned.store(true, Ordering::Release);
        self.usage.clone()
    }
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket
            .as_ref()
            .expect("unique live root IO")
            .local_addr()
    }
    pub async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> std::io::Result<usize> {
        if peer != self.usage.request.peer {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "root UDP peer differs from exact declaration",
            ));
        }
        self.socket
            .as_ref()
            .expect("unique live root IO")
            .send_to_addr(bytes, peer)
            .await
    }
    pub async fn recv_from_addr(&self, bytes: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.socket
            .as_ref()
            .expect("unique live root IO")
            .recv_from_addr(bytes)
            .await
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std::io::Result<()> {
        self.socket
            .as_mut()
            .expect("unique live root IO")
            .set_observer(observer)
    }
}
impl Drop for RootDatagramIo {
    fn drop(&mut self) {
        drop(self.socket.take());
        self.usage.returned.store(true, Ordering::Release);
    }
}
