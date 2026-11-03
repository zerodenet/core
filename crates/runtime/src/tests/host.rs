use super::*;

// A publication owner need not implement Debug or depend on engine snapshots.
struct PrivateAck(&'static str);

#[test]
fn generic_host_retains_private_publication_without_exposing_it_in_debug() {
    let host = RuntimeHost::<PrivateAck>::new(ComponentRegistry::default());
    *host.reload_ack().lock().unwrap() = Some(PrivateAck("private-candidate"));
    let formatted = format!("{host:?}");
    assert!(formatted.contains("RuntimeHost"));
    assert!(!formatted.contains("private-candidate"));
    assert_eq!(
        host.reload_ack().lock().unwrap().as_ref().unwrap().0,
        "private-candidate"
    );
}
