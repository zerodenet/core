//! Views of the canonical host's original, never another native owner.
use super::*;
use zero_platform_tokio::contract::{
    component::GrantedMetadataListener,
    scope::{TaskRegistrar, TaskScopeIdentity},
};
use zero_platform_tokio::controlled_datagram::metadata::MetadataDatagramObservation;

pub struct HostedMetadataSocket {
    observation: MetadataDatagramObservation,
    socket: Arc<MetadataSocket>,
    scope: TaskScopeIdentity,
    pub(in crate::quic::owned) callbacks: Arc<callbacks::Broker>,
}
pub struct RejectedHostedMetadataSocket {
    pub error: io::Error,
    pub grant: GrantedMetadataListener,
}
impl fmt::Debug for RejectedHostedMetadataSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RejectedHostedMetadataSocket")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl HostedMetadataSocket {
    pub fn new(
        grant: GrantedMetadataListener,
        registrar: &TaskRegistrar,
    ) -> Result<Self, Box<RejectedHostedMetadataSocket>> {
        let checked = if !grant.scope.matches(registrar) || !grant.scope.registration_open() {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "metadata grant belongs to another or closed host scope",
            ))
        } else if !grant.observation.matches(&grant.rx, &grant.tx) {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata views belong to different originals",
            ))
        } else {
            callbacks::Broker::new(grant.tx.max_concurrent_send_calls().get())
        };
        let callbacks = match checked {
            Ok(callbacks) => callbacks,
            Err(error) => return Err(Box::new(RejectedHostedMetadataSocket { error, grant })),
        };
        let GrantedMetadataListener {
            scope,
            rx,
            tx,
            observation,
        } = grant;
        Ok(Self {
            observation,
            socket: Arc::new(MetadataSocket {
                receiver: Mutex::new(Some(rx)),
                sender: tx,
                drops: Mutex::new(NativeDrops::default()),
                callbacks: callbacks.clone(),
            }),
            scope,
            callbacks,
        })
    }
    pub(in crate::quic::owned) fn matches_claim(
        &self,
        claim: &zero_platform_tokio::controlled_datagram::metadata::MetadataViewClaim,
    ) -> bool {
        self.socket
            .receiver
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|rx| claim.matches(rx, &self.socket.sender))
    }
    pub(in crate::quic::owned) fn matches(&self, registrar: &TaskRegistrar) -> bool {
        self.scope.matches(registrar) && self.scope.registration_open()
    }
    pub(in crate::quic::owned) fn socket(&self) -> Arc<dyn quinn::AsyncUdpSocket> {
        self.socket.clone()
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        if self.callbacks.failed() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "metadata observation has faulted",
            ));
        }
        self.observation.set_observer(observer.map(|observer| {
            Arc::new(callbacks::Observer::new(observer, self.callbacks.clone()))
                as Arc<dyn zero_traits::IoObserver>
        }))
    }
}
