use super::*;
use std::{error::Error, time::Duration};
use tokio::{net::TcpListener, time::Instant};
use zero_core::{component::NetworkGeneration, Address, Network, ProtocolType};
use zero_platform_tokio::contract::CancellationToken;

#[tokio::test]
async fn native_direct_dial_keeps_os_error_and_legacy_classification() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let services = proxy.tcp_runtime_services();
    let session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        port,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    let opener = DirectOpener::new(services.upstream(), session);
    let error = opener
        .open_stream(
            &UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(opener.generation()),
            },
            Destination::new(Address::Ipv4([127, 0, 0, 1]), port),
            CarrierContext {
                session: None,
                deadline: Instant::now() + Duration::from_secs(2),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .err()
        .unwrap();
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::ConnectionRefused);
    assert!(source.raw_os_error().is_some());
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    let evidence = opener.take_evidence().unwrap().err().unwrap();
    assert!(matches!(evidence.legacy_error, zero_core::Error::Io(_)));
    assert_eq!(evidence.stage, "connect_direct");
}
