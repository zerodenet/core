use super::*;
use crate::BlockReason;
use std::sync::{atomic::AtomicUsize, Mutex};

struct Hook {
    calls: Arc<AtomicUsize>,
    deny: Arc<AtomicBool>,
    ids: Arc<Mutex<Vec<u64>>>,
}
impl FlowHook for Hook {
    fn on_flow_start(&self, context: &FlowContext) -> Result<(), BlockReason> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.ids.lock().unwrap().push(context.flow_id);
        if self.deny.load(Ordering::Relaxed) {
            Err(BlockReason::new("fixture", "actual hook rejection"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn hook_rejection_consumes_the_ticket_once_before_registration_and_retry_needs_a_fresh_identity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let deny = Arc::new(AtomicBool::new(true));
    let ids = Arc::new(Mutex::new(vec![]));
    let engine = new_engine().with_flow_hook(Hook {
        calls: calls.clone(),
        deny: deny.clone(),
        ids: ids.clone(),
    });
    let identity = engine.mint_session_identity().unwrap();
    let mut business = session(identity.id());
    let foreign = new_engine().mint_session_identity().unwrap();
    refused(
        engine.prepare_session_with_identity(&foreign, &mut business, "foreign"),
        "different Engine",
    );
    let mut mismatched = session(SessionId(identity.id().0 + 17));
    refused(
        engine.prepare_session_with_identity(&identity, &mut mismatched, "mismatched"),
        "does not match",
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "identity rejection precedes actual policy hooks"
    );
    refused(
        engine.prepare_session_with_identity(&identity, &mut business, "in"),
        "actual hook rejection",
    );
    no_business(&engine);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(ids.lock().unwrap().as_slice(), [identity.id().0]);
    deny.store(false, Ordering::Relaxed);
    refused(
        engine.prepare_session_with_identity(&identity, &mut business, "in"),
        "already used",
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "replayed ticket cannot invoke the hook again"
    );
    let fresh = engine.mint_session_identity().unwrap();
    let mut business = session(fresh.id());
    engine
        .prepare_session_with_identity(&fresh, &mut business, "in")
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    engine
        .track_session(business.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}

fn principal(id: SessionId, source: u8) -> Session {
    let mut business = session(id);
    business.source_ip = Some(Address::Ipv4([192, 0, 2, source]));
    let mut auth = SessionAuth::new("verified-fixture");
    auth.principal_key = Some("account:identity".into());
    auth.policy_revision = Some(1);
    business.apply_auth(auth);
    business
}

#[test]
fn native_admission_uses_the_same_device_registry_as_the_legacy_wrapper() {
    let engine = new_engine();
    let mut legacy = principal(SessionId(0), 1);
    legacy.auth.as_mut().unwrap().device_limit = Some(1);
    engine.prepare_session(&mut legacy, "legacy").unwrap();
    let identity = engine.mint_session_identity().unwrap();
    let mut other = principal(identity.id(), 2);
    other.auth.as_mut().unwrap().device_limit = Some(1);
    refused(
        engine.prepare_session_with_identity(&identity, &mut other, "native"),
        "device limit",
    );
    assert_eq!(engine.active_sessions().len(), 1);
    engine
        .track_session(legacy.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
    refused(
        engine.prepare_session_with_identity(&identity, &mut other, "native"),
        "already used",
    );
    let fresh = engine.mint_session_identity().unwrap();
    other.id = fresh.id().0;
    engine
        .prepare_session_with_identity(&fresh, &mut other, "native")
        .unwrap();
    engine
        .track_session(other.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}

#[test]
fn native_and_legacy_sessions_share_the_actual_quota_and_principal_cancellation() {
    let engine = new_engine();
    let mut legacy = principal(SessionId(0), 1);
    legacy.auth.as_mut().unwrap().quota_remaining_bytes = Some(10);
    engine.prepare_session(&mut legacy, "legacy").unwrap();
    let identity = engine.mint_session_identity().unwrap();
    let mut native = principal(identity.id(), 2);
    native.auth.as_mut().unwrap().quota_remaining_bytes = Some(10);
    engine
        .prepare_session_with_identity(&identity, &mut native, "native")
        .unwrap();
    let mut old = engine.track_session(legacy.id);
    let mut new = engine.track_session(native.id);
    engine.record_session_inbound_rx(native.id, 6);
    assert!(!old.is_cancelled());
    assert!(!new.is_cancelled());
    engine.record_session_inbound_tx(legacy.id, 4);
    assert!(old.is_cancelled());
    assert!(new.is_cancelled());
    assert_eq!(
        new.cancellation_reason().as_deref(),
        Some("quota_exhausted")
    );
    old.finish(SessionOutcome::Cancelled).unwrap();
    new.finish(SessionOutcome::Cancelled).unwrap();
    let rejected = engine.mint_session_identity().unwrap();
    native.id = rejected.id().0;
    refused(
        engine.prepare_session_with_identity(&rejected, &mut native, "native"),
        "exhausted its traffic quota",
    );
    assert!(engine.active_sessions().is_empty());
    assert_eq!(events(&engine, event_type::FLOW_STARTED).len(), 2);
}

#[test]
fn native_admission_preserves_the_authoritative_policy_revision_gate() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":1080},
        "protocol":{"type":"vless","users":[{"id":"11111111-1111-1111-1111-111111111111",
        "principal_key":"account:identity","policy_revision":2}]}}],
        "route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    let engine = Engine::new(config).unwrap();
    let stale = engine.mint_session_identity().unwrap();
    let mut business = principal(stale.id(), 1);
    refused(
        engine.prepare_session_with_identity(&stale, &mut business, "in"),
        "stale policy revision",
    );
    no_business(&engine);
    business.auth.as_mut().unwrap().policy_revision = Some(2);
    refused(
        engine.prepare_session_with_identity(&stale, &mut business, "in"),
        "already used",
    );
    let current = engine.mint_session_identity().unwrap();
    business.id = current.id().0;
    engine
        .prepare_session_with_identity(&current, &mut business, "in")
        .unwrap();
    engine
        .track_session(business.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}
