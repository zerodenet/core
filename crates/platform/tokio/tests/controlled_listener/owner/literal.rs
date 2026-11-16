use super::*;
#[test]
fn cold_literal_refinement_retains_ipv6_scope_and_flow_without_changing_declared_ip_or_port() {
    let expected: SocketAddr = "[fe80::1]:19191".parse().unwrap();
    let mut native = owner(expected, 1);
    let scoped = SocketAddr::V6(std::net::SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        19191,
        17,
        9,
    ));
    native.refine_literal_endpoint(scoped).unwrap();
    let failure = match native.bind_listener(expected) {
        Err(source) => source,
        Ok(_) => panic!("unscoped endpoint escaped its actual declaration"),
    };
    assert_eq!(
        failure.reason,
        Reason::InvalidConfig,
        "the full scope/flow endpoint is retained before native IO"
    );
    assert!(native
        .refine_literal_endpoint("[fe80::2]:19191".parse().unwrap())
        .is_err());
    assert!(native
        .refine_literal_endpoint("[fe80::1]:19192".parse().unwrap())
        .is_err());
    assert_eq!(native.remaining_bind_attempts(), 1);
    assert_eq!(native.confirmed_close_count(), 0);
}
#[tokio::test]
async fn original_literal_native_binding_cannot_be_refined_after_acquisition_or_confirmed_close() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut native = owner(address, 1);
    native.refine_literal_endpoint(address).unwrap();
    let view = native.bind_listener(address).unwrap();
    assert!(native.refine_literal_endpoint(address).is_err());
    drop(view);
    let report = native.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(!report.closed_io.is_empty());
    assert!(native.refine_literal_endpoint(address).is_err());
}
