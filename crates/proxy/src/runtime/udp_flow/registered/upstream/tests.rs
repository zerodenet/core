use std::sync::{Arc, Mutex};
use std::time::Duration;

use zero_core::Address;
use zero_engine::EngineError;

use super::{
    TrackedUpstreamAssociationState, UpstreamAssociationCloseReason, UpstreamAssociationRuntime,
    UpstreamAssociationTarget, UpstreamAssociationTransport,
};

#[test]
fn tracked_upstream_state_matches_target_and_association() {
    let mut state = TrackedUpstreamAssociationState::<String, &'static str>::new();
    let first = "alpha".to_owned();
    let second = "beta".to_owned();

    assert!(!state.matches_target(&first));
    assert_eq!(state.target(), None);
    assert_eq!(state.association(), None);

    assert!(state.insert(first.clone(), "first-association").is_none());
    assert!(state.matches_target(&first));
    assert_eq!(state.target().map(|value| value.as_str()), Some("alpha"));
    assert_eq!(state.association(), Some(&"first-association"));

    let replaced = state
        .insert(second.clone(), "second-association")
        .expect("replace tracked association");
    assert_eq!(replaced.target(), &first);
    assert_eq!(replaced.association(), &"first-association");
    assert!(state.matches_target(&second));
    assert_eq!(state.target().map(|value| value.as_str()), Some("beta"));
    assert_eq!(state.association(), Some(&"second-association"));
}

#[test]
fn tracked_upstream_state_take_preserves_target() {
    let mut state = TrackedUpstreamAssociationState::<String, u8>::new();
    let target = "alpha".to_owned();

    let _ = state.insert(target.clone(), 7);
    let tracked = state.take().expect("tracked association");

    assert_eq!(tracked.target(), &target);
    assert_eq!(tracked.association(), &7);
    assert_eq!(state.target(), None);
    assert_eq!(state.association(), None);
    assert!(!state.matches_target(&target));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DummyTarget {
    outbound_tag: String,
    server: String,
    port: u16,
}

impl DummyTarget {
    fn new(outbound_tag: &str, server: &str, port: u16) -> Self {
        Self {
            outbound_tag: outbound_tag.to_owned(),
            server: server.to_owned(),
            port,
        }
    }
}

impl UpstreamAssociationTarget for DummyTarget {
    fn outbound_tag(&self) -> &str {
        &self.outbound_tag
    }

    fn log_parts(&self) -> (&str, &str, u16) {
        (&self.outbound_tag, &self.server, self.port)
    }
}

#[derive(Debug, Clone)]
struct DummyAssociation {
    close_log: Arc<Mutex<Vec<UpstreamAssociationCloseReason>>>,
}

impl DummyAssociation {
    fn new(close_log: Arc<Mutex<Vec<UpstreamAssociationCloseReason>>>) -> Self {
        Self { close_log }
    }
}

#[async_trait::async_trait]
impl UpstreamAssociationTransport<DummyTarget> for DummyAssociation {
    async fn establish(
        _execution: crate::protocol_registry::TcpExecutionServices,
        _observer: crate::protocol_registry::UdpAssociationObserver,
        _target: DummyTarget,
    ) -> Result<Self, EngineError> {
        panic!("dummy establish should not be called in unit tests")
    }

    async fn send_packet(
        &self,
        _target: &Address,
        _port: u16,
        _payload: &[u8],
    ) -> Result<usize, EngineError> {
        panic!("dummy send_packet should not be called in unit tests")
    }

    async fn recv_response_parts(
        &self,
        _buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), EngineError> {
        panic!("dummy recv_response_parts should not be called in unit tests")
    }

    fn close(self, reason: UpstreamAssociationCloseReason) {
        self.close_log.lock().expect("close log").push(reason);
    }
}

#[test]
fn upstream_association_runtime_reports_outbound_tag_and_closes_idle_association() {
    let close_log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = UpstreamAssociationRuntime::<DummyTarget, DummyAssociation>::default();
    let target = DummyTarget::new("alpha", "server-a", 1080);

    runtime.touch_idle(Duration::from_secs(1));
    let _ = runtime.upstream.insert(
        target.clone(),
        DummyAssociation::new(Arc::clone(&close_log)),
    );

    assert_eq!(runtime.upstream_outbound_tag(), Some("alpha"));
    assert!(runtime.idle_deadline().is_some());

    let closed = runtime.close_idle().expect("closed target");
    assert_eq!(closed, target);
    assert_eq!(runtime.upstream_outbound_tag(), None);
    assert_eq!(runtime.idle_deadline(), None);
    assert_eq!(
        *close_log.lock().expect("close log"),
        vec![UpstreamAssociationCloseReason::IdleTimeout]
    );
}

#[test]
fn upstream_association_runtime_close_all_uses_closed_reason() {
    let close_log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = UpstreamAssociationRuntime::<DummyTarget, DummyAssociation>::default();

    runtime.touch_idle(Duration::from_secs(1));
    let _ = runtime.upstream.insert(
        DummyTarget::new("beta", "server-b", 2080),
        DummyAssociation::new(Arc::clone(&close_log)),
    );

    runtime.close_all_upstreams();

    assert_eq!(runtime.upstream_outbound_tag(), None);
    assert_eq!(runtime.idle_deadline(), None);
    assert_eq!(
        *close_log.lock().expect("close log"),
        vec![UpstreamAssociationCloseReason::Closed]
    );
}
