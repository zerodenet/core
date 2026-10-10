use super::support::{target, Fixture};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
use zero_core::{Network, ProtocolType, Session};
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;

async fn connect_and_echo(revoke: u8) {
    let mut fixture = Fixture::new(1420);
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let services = proxy.tcp_runtime_services().execution();
    let network = proxy.egress_interface_control();
    let session = Session::new(
        1,
        super::support::address(target().ip()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    let operation = fixture.adapter.prepare_tcp();
    let (server_tx, mut replies) = mpsc::channel(128);
    let (server, _) = UserNetworkStack::new(server_tx, 1420).into_parts();
    let server = Arc::clone(&server);
    let device = fixture.device.clone();
    let generation = fixture.generation.clone();
    let mut packets = std::mem::replace(&mut fixture.packets, mpsc::channel(1).1);
    let echo_stack = server.clone();
    let echo = tokio::spawn(async move {
        let (mut stream, _, _) = echo_stack.accept().await.unwrap();
        let mut payload = [0; 12];
        stream.read_exact(&mut payload).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        std::future::pending::<()>().await;
    });
    let pump = tokio::spawn(async move {
        loop {
            tokio::select! {
                outgoing = packets.recv() => {
                    let Some(outgoing) = outgoing else { break; };
                    if packet::parse_tcp(&outgoing.packet).is_some_and(|tcp| tcp.syn) {
                        if revoke == 1 { generation.store(1, Ordering::Release); }
                        if revoke == 2 { network.invalidate_network(); }
                    }
                    server.feed(&outgoing.packet).await;
                }
                incoming = replies.recv() => {
                    let Some(incoming) = incoming else { break; };
                    let _ = device.deliver_decrypted_owned(incoming.into(), false).await;
                }
            }
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        operation.execute(services, &session),
    )
    .await
    .unwrap();
    if revoke != 0 {
        assert!(matches!(result, Err(error) if error.stage == "raw_ip_device"));
    } else {
        let established = result.unwrap_or_else(|e| panic!("{}: {}", e.stage, e.error));
        let mut stream = crate::transport::extract_tcp_stream(established)
            .unwrap()
            .upstream;
        stream.write_all(b"hello packet").await.unwrap();
        let mut received = [0; 12];
        tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&received, b"hello packet");
        drop(stream);
        assert!(!fixture.device.is_closed());
    }
    pump.abort();
    echo.abort();
}

#[tokio::test]
async fn pure_packet_endpoint_executes_generic_tcp_converter() {
    connect_and_echo(0).await;
}
#[tokio::test]
async fn generic_tcp_converter_rejects_generation_change_during_handshake() {
    connect_and_echo(1).await;
}

#[tokio::test]
async fn generic_tcp_converter_rejects_network_change_before_device_replacement() {
    connect_and_echo(2).await;
}
