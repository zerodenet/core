use super::*;

#[tokio::test]
async fn host_validation_uses_reference_case_and_port_rules_before_http_dispatch() {
    for (host, expected, status) in [
        ("ExAmPlE.test", "example.test", 200),
        ("EXAMPLE.test:443", "example.test", 200),
        ("[2001:db8::1]:8443", "2001:db8::1", 200),
        ("example.test:443", "example.test:443", 404),
        ("2001:db8::1", "2001:db8::1", 404),
        ("other.test", "example.test", 404),
    ] {
        let request = http::Request::builder()
            .method("OPTIONS")
            .uri("/tunnel/")
            .header("host", host)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            dispatch_status(request, expected).await,
            status,
            "host={host}, expected={expected}"
        );
    }
}

#[tokio::test]
async fn http2_authority_takes_precedence_over_host_header() {
    let request = http::Request::builder()
        .version(http::Version::HTTP_2)
        .method("OPTIONS")
        .uri("https://EXAMPLE.test:443/tunnel/")
        .header("host", "other.test")
        .body(Body::empty())
        .unwrap();
    assert_eq!(dispatch_status(request, "example.test").await, 200);
}

async fn dispatch_status(request: http::Request<Body>, host: &str) -> u16 {
    let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
        host: Some(host.into()),
        path: "/tunnel/".into(),
        mode: "auto".into(),
        options: Default::default(),
    });
    let (sender, _receiver) = mpsc::channel(1);
    handle(
        request,
        profile,
        Sessions::default(),
        sender,
        Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
    .as_u16()
}
