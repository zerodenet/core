//! Whole ordinary registry/actual Host/real browser WS and exact Source return.
mod fixture;
mod mux;
mod native;
#[cfg(all(feature = "shadowsocks", unix))]
mod prefix;
use fixture::Running;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest, error::ProtocolError, Error as WebSocketError, Message,
    },
};
use zero_platform_tokio::controlled_carrier::{
    CarrierOrigin, CarrierOriginObservation, CarrierOwnerRegistration,
};
static SERIAL: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
const ID: &str = "11111111-2222-3333-4444-555555555555";
// This is an actual accepted configuration address, not a claimed free-port
// reservation. The Source's real Host bind is the only bind; a collision must
// return its real error. No helper probes, drops or rebinds a reserved socket.
const BROWSER: &str = "127.0.0.1:16892";
type Agent = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;
async fn browser_url() -> String {
    let mut page = TcpStream::connect(BROWSER).await.unwrap();
    page.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = Vec::new();
    page.read_to_end(&mut body).await.unwrap();
    drop(page);
    let page = String::from_utf8(body).unwrap();
    let marker = "/websocket?token=";
    let start = page.find(marker).unwrap();
    let end = page[start..].find('"').unwrap() + start;
    format!("ws://{BROWSER}{}", &page[start..end])
}
async fn register(url: &str) -> Agent {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", format!("http://{BROWSER}").parse().unwrap());
    connect_async(request).await.unwrap().0
}
async fn browser() -> Agent {
    register(&browser_url().await).await
}
async fn relay_browser(
    mut browser: Agent,
    roots: rustls::RootCertStore,
) -> Result<(), WebSocketError> {
    let task = browser.next().await.unwrap().unwrap().into_text().unwrap();
    let task: serde_json::Value = serde_json::from_str(&task).unwrap();
    assert_eq!(task["method"], "WS");
    assert!(task["url"].as_str().unwrap().ends_with("/original"));
    let url = task["url"].as_str().unwrap();
    let endpoint: http_uri::Uri = url.parse().unwrap();
    let raw = TcpStream::connect((
        endpoint.host().unwrap(),
        endpoint
            .port_u16()
            .unwrap_or(if endpoint.scheme_str() == Some("wss") {
                443
            } else {
                80
            }),
    ))
    .await
    .unwrap();
    let io: zero_platform_tokio::contract::stream::BoxStream = if endpoint.scheme_str()
        == Some("wss")
    {
        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        Box::pin(
            tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
                .connect(
                    rustls::pki_types::ServerName::try_from(endpoint.host().unwrap().to_owned())
                        .unwrap(),
                    raw,
                )
                .await
                .unwrap(),
        )
    } else {
        Box::pin(raw)
    };
    let (mut remote, _) = tokio_tungstenite::client_async(url, io).await.unwrap();
    browser.send(Message::Text("ok".into())).await.unwrap();
    let result = loop {
        tokio::select! {
            frame = browser.next() => match frame {
                Some(Ok(Message::Binary(bytes))) => remote.send(Message::Binary(bytes)).await.unwrap(),
                Some(Ok(Message::Close(_))) | None => break Ok(()),
                Some(Ok(_)) => {},
                Some(Err(error)) => break Err(error),
            },
            frame = remote.next() => match frame {
                Some(Ok(Message::Binary(bytes))) => {
                    if let Err(error) = browser.send(Message::Binary(bytes)).await {
                        break Err(error);
                    }
                }
                Some(Ok(Message::Close(_))) | None => break Ok(()),
                Some(Ok(_)) => {},
                Some(Err(error)) => panic!("actual remote WS failed: {error}"),
            },
        }
    };
    let _ = browser.close(None).await;
    let _ = remote.close(None).await;
    result
}

fn assert_retired_browser_agent(result: Result<(), WebSocketError>, source: &CarrierOrigin) {
    assert!(source.accepted().unwrap().is_settled());
    let closed = source.closed().unwrap();
    assert!(closed.matches(source) && closed.had_native_acquisition());
    // The Browser Source may revoke its TCP original before the WebSocket Close exchange.
    match result {
        Ok(()) | Err(WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake)) => {}
        Err(WebSocketError::Io(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) => {}
        Err(error) => panic!("actual browser control failed: {error}"),
    }
}
struct Actual {
    owner: CarrierOwnerRegistration,
    role: CarrierOriginObservation,
    source: CarrierOrigin,
}
fn capture(client: &Running) -> Actual {
    let services = client.proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let node = plan.configured("node").unwrap();
    assert_eq!(
        node.recipe.spec().task_limit.unwrap().get(),
        2,
        "original N1 setup plus data-driver ceiling, not a third task"
    );
    let owner = node.original_carrier_owner(0).unwrap();
    assert_eq!(owner.limit().get(), 1);
    let observed = owner.observe().unwrap();
    assert_eq!(observed.retained(), 1);
    let role = &observed.slots()[0];
    assert!(
        role.native().is_none() && role.driver().is_none(),
        "WS transfers the actual Source IO without a fabricated cached driver"
    );
    let setup = role.auxiliary().unwrap();
    let joined = setup.try_joined().unwrap();
    assert!(setup.matches_joined(&joined) && joined.registration_released());
    assert!(setup
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let source = role.borrowed_origin().unwrap().clone();
    let accepted = source.accepted().unwrap();
    assert!(!accepted.is_settled());
    let provider = plan.component(accepted.instance().component).unwrap();
    assert_eq!(provider.instance_id(), accepted.instance());
    assert!(node
        .dependency_instances()
        .contains(&provider.instance_id()));
    assert!(provider.recipe.spec().roles.is_empty());
    let receiver = provider.original_accepted_receiver(0).unwrap();
    assert!(accepted.matches_receiver(&receiver));
    assert!(receiver
        .task_failures()
        .unwrap()
        .scope_identity()
        .same_scope(accepted.scope_identity()));
    assert!(!accepted
        .scope_identity()
        .same_scope(&setup.scope_identity()));
    assert!(source.closed().is_none());
    Actual {
        owner,
        role: role.original().clone(),
        source,
    }
}
async fn returned(actual: &Actual) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if actual.role.closed().is_some()
                && actual.source.accepted().unwrap().is_settled()
                && actual
                    .owner
                    .observe()
                    .is_some_and(|owner| owner.retained() == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let role = actual.role.closed().unwrap();
    assert!(actual.role.matches_closed(&role));
    assert!(!role.had_native_acquisition());
    let source = actual.source.closed().unwrap();
    assert!(source.matches(&actual.source));
    assert!(source.had_native_acquisition());
}
async fn wire(client: &Running, sequence: u8, roots: rustls::RootCertStore) -> Actual {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = target.local_addr().unwrap();
    let payload = vec![sequence; 1025];
    let expected = payload.clone();
    let (uploaded, ready) = tokio::sync::oneshot::channel();
    let (release, waiting) = tokio::sync::oneshot::channel();
    let target = tokio::spawn(async move {
        let (mut io, _) = target.accept().await.unwrap();
        let mut body = vec![0; expected.len()];
        io.read_exact(&mut body).await.unwrap();
        assert_eq!(body, expected);
        uploaded.send(()).unwrap();
        waiting.await.unwrap();
        io.write_all(&body).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let browser = tokio::spawn(relay_browser(browser().await, roots));
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&destination.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0);
    io.write_all(&payload).await.unwrap();
    io.flush().await.unwrap();
    ready.await.unwrap();
    let actual = capture(client);
    release.send(()).unwrap();
    let mut reverse = vec![0; payload.len()];
    io.read_exact(&mut reverse).await.unwrap();
    assert_eq!(reverse, payload);
    io.shutdown().await.unwrap();
    drop(io);
    target.await.unwrap();
    let agent = browser.await.unwrap();
    returned(&actual).await;
    assert_retired_browser_agent(agent, &actual.source);
    actual
}

#[tokio::test]
async fn vmess_and_trojan_browser_sources_forward_tcp_udp_and_retire_actual_originals() {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(60),async {
        for kind in ["vmess","trojan"] {
            let certificate=rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
            let directory=tempfile::tempdir().unwrap();let cert=directory.path().join("cert.pem");let key=directory.path().join("key.pem");
            std::fs::write(&cert,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
            let mut protocol=serde_json::json!({"type":kind,"ws":{"path":"/original"}});
            if kind=="trojan" {protocol["password"]=serde_json::json!(ID);protocol["tls"]=serde_json::json!({"cert_path":cert,"key_path":key});}
            else {protocol["users"]=serde_json::json!([{"id":ID}]);}
            let remote=fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}),"remote").await;
            let mut outbound=serde_json::json!({"type":kind,"server":"127.0.0.1","port":remote.address.port(),"ws":{"path":"/original?ed=2048","browser_dialer":{"listen":BROWSER,"idle_capacity":1}}});
            outbound[if kind=="trojan" {"password"}else{"id"}]=serde_json::json!(ID);
            let mut alias=serde_json::json!({"type":if kind=="trojan" {"vmess"}else{"trojan"},"server":"127.0.0.1","port":remote.address.port(),"ws":{"path":"/original?ed=2048","browser_dialer":{"listen":BROWSER,"idle_capacity":1}}});
            alias[if kind=="trojan" {"id"}else{"password"}]=serde_json::json!(ID);
            let client=fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":outbound.clone()},{"tag":"other","protocol":alias}],"route":{"final":{"type":"route","outbound":"node"}}}),"entry").await;
            let plan=client.proxy.tcp_runtime_services().components.clone().unwrap();
            let node=plan.configured("node").unwrap();let other=plan.configured("other").unwrap();
            assert_eq!(node.dependency_instances(),other.dependency_instances(),"cross-protocol aliases share the exact original provider generation");
            let mut trust=rustls::RootCertStore::empty();trust.add(certificate.cert.der().clone()).unwrap();
            let first=wire(&client,0x39,trust.clone()).await;let second=wire(&client,0x71,trust.clone()).await;
            assert!(first.owner.same_owner(&second.owner));assert!(!first.source.same_original(&second.source));assert_eq!(first.source.accepted().unwrap().instance(),second.source.accepted().unwrap().instance());
            datagram(&client,trust.clone()).await;
            let roots=client.proxy.protocols.runtime_host().original_budgets();client.close().await;
            assert!(second.owner.closed().unwrap().matches(&second.owner));assert_eq!(roots.carrier.snapshot().charged,0);
            let last=fixture::start(serde_json::json!({"inbounds":[{"tag":"last","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}),"last").await;
            let relay=fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":outbound},{"tag":"last","protocol":{"type":"socks5","server":"127.0.0.1","port":last.address.port()}}],"outbound_groups":[{"tag":"chain","type":"relay","proxies":["node","last"]}],"route":{"final":{"type":"route","outbound":"chain"}}}),"entry").await;
            let borrowed=wire(&relay,0xa3,trust).await;
            let roots=relay.proxy.protocols.runtime_host().original_budgets();relay.close().await;
            assert!(borrowed.owner.closed().unwrap().matches(&borrowed.owner));assert_eq!(roots.carrier.snapshot().charged,0);
            last.close().await;
            remote.close().await;
        }
    }).await.unwrap();
}
async fn datagram(client: &Running, trust: rustls::RootCertStore) {
    let agent = tokio::spawn(relay_browser(browser().await, trust));
    let mut actual = None;
    datagram_exchange_observed(client, || actual = Some(capture(client))).await;
    let actual = actual.unwrap();
    let agent = agent.await.unwrap();
    returned(&actual).await;
    assert_retired_browser_agent(agent, &actual.source);
}
async fn datagram_exchange(client: &Running) {
    datagram_exchange_observed(client, || {}).await;
}
async fn datagram_exchange_observed(client: &Running, observe: impl FnOnce()) {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let echo = tokio::spawn(async move {
        let mut bytes = [0; 128];
        let (size, peer) = target.recv_from(&mut bytes).await.unwrap();
        target.send_to(&bytes[..size], peer).await.unwrap();
    });
    let mut control = TcpStream::connect(client.address).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut packet = vec![0, 0, 0, 1, 127, 0, 0, 1];
    packet.extend(port.to_be_bytes());
    packet.extend(b"browser-datagram");
    peer.send_to(
        &packet,
        (
            std::net::Ipv4Addr::LOCALHOST,
            u16::from_be_bytes([reply[8], reply[9]]),
        ),
    )
    .await
    .unwrap();
    let mut response = [0; 128];
    let size = peer.recv(&mut response).await.unwrap();
    assert_eq!(&response[..size], packet);
    observe();
    drop(control);
    echo.await.unwrap();
}
