use crate::support::listener::EchoListener;
use std::net::TcpListener;

#[test]
fn selected_echo_endpoint_remains_owned_until_and_after_worker_handoff() {
    let echo = EchoListener::bind_avoiding(&[]);
    let port = echo.port();
    let competing = TcpListener::bind(("127.0.0.1", port));
    assert!(
        competing.is_err(),
        "selected echo endpoint {port} was released before handoff"
    );
    let listener = echo.into_listener();
    assert_eq!(listener.local_addr().unwrap().port(), port);
    assert!(TcpListener::bind(("127.0.0.1", port)).is_err());
}
