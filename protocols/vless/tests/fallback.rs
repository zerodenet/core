#![cfg(feature = "runtime")]
use vless::fallback::FallbackPolicy;
use zero_traits::{FallbackEndpoint, FallbackRule};
fn rule(name: &str, alpn: &str, path: &str, port: u16) -> FallbackRule {
    FallbackRule {
        name: name.into(),
        alpn: alpn.into(),
        path: path.into(),
        endpoint: FallbackEndpoint::Tcp {
            server: "localhost".into(),
            port,
        },
        proxy_protocol: 0,
    }
}
fn selected(policy: &FallbackPolicy, name: &str, alpn: &str, path: &str) -> u16 {
    let first = format!("GET {path} HTTP/1.1\r\nHost: local\r\n\r\n");
    match &policy
        .select(name, alpn, first.as_bytes())
        .unwrap()
        .endpoint
    {
        FallbackEndpoint::Tcp { port, .. } => *port,
        _ => unreachable!(),
    }
}
#[test]
fn fallback_inherits_defaults_with_name_then_alpn_then_path_precedence() {
    let policy = FallbackPolicy::new(vec![
        rule("", "", "", 1),
        rule("", "h2", "", 2),
        rule("", "", "/shared", 3),
        rule("example.test", "", "", 4),
        rule("example.test", "http/1.1", "/app", 5),
        rule("www.example.test", "", "", 6),
    ]);
    assert_eq!(selected(&policy, "unknown", "h2", "/"), 2);
    assert_eq!(selected(&policy, "example.test", "h2", "/"), 4);
    assert_eq!(
        selected(&policy, "example.test", "http/1.1", "/shared?q=1"),
        4
    );
    assert_eq!(selected(&policy, "example.test", "h2", "/shared?q=1"), 3);
    assert_eq!(selected(&policy, "EXAMPLE.TEST", "http/1.1", "/app"), 5);
    assert_eq!(selected(&policy, "www.example.test", "", "/"), 6);
    assert_eq!(selected(&policy, "sub.www.example.test", "", "/"), 6);
    assert_eq!(selected(&policy, "example.test", "unknown", "/missing"), 4);
}
#[test]
fn fallback_missing_defaults_fail_and_duplicate_rules_use_last_value() {
    let policy = FallbackPolicy::new(vec![rule("a", "h2", "/a", 1), rule("a", "h2", "/a", 2)]);
    assert_eq!(selected(&policy, "a", "h2", "/a"), 2);
    assert!(policy
        .select("b", "h2", b"GET /a HTTP/1.1\r\n\r\n")
        .is_none());
    assert!(policy
        .select("a", "h2", b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .is_none());
    assert!(policy
        .select("a", "http/1.1", b"GET /a HTTP/1.1\r\n\r\n")
        .is_none());
}
