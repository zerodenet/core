//! Narrow access to an already installed active Source Scope. This constructs
//! no Scope, listener, main witness, native resource, packet or process grant.
use super::*;
impl InboundTaskAccess {
    pub(in crate::runtime) fn for_service_scope(
        identity: ComponentInstanceId,
        registrar: TaskRegistrar,
        cancel: CancellationToken,
        groups: &Arc<Mutex<Vec<Arc<super::super::InboundPeerTasks>>>>,
    ) -> Self {
        Self {
            carrier_host: Arc::default(),
            accepted_drained: Arc::new(AtomicBool::new(true)),
            acquisition: Arc::default(),
            peer_groups: Arc::downgrade(groups),
            identity,
            registrar,
            cancel,
            shutdown: None,
            #[cfg(feature = "raw-ip-runtime")]
            main_registration: std::sync::Weak::new(),
            #[cfg(feature = "raw-ip-runtime")]
            packet_pause: Arc::default(),
            #[cfg(feature = "raw-ip-runtime")]
            packet_source: std::sync::Weak::new(),
        }
    }
}
