use crate::transport::{
    VlessOutboundBuildOptionsRef, VlessOutboundLeaf, VlessOutboundOptionsRef, VlessTransportRuntime,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::{
    BrowserDialerSettings, ClientTlsProfile, GrpcTransportProfile, H2TransportProfile,
    HttpUpgradeTransportProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct WsProfile(BrowserDialerSettings);

impl WebSocketTransportProfile for WsProfile {
    fn browser_dialer(&self) -> Option<BrowserDialerSettings> {
        Some(self.0.clone())
    }

    fn path(&self) -> &str {
        "/browser"
    }

    fn header_pairs(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

struct UnusedProfile;

impl ClientTlsProfile for UnusedProfile {
    fn server_name(&self) -> Option<&str> {
        None
    }
    fn disable_sni(&self) -> bool {
        false
    }
    fn ca_cert_path(&self) -> Option<&str> {
        None
    }
    fn insecure(&self) -> bool {
        false
    }
    fn alpn(&self) -> &[String] {
        &[]
    }
    fn client_fingerprint(&self) -> Option<&str> {
        None
    }
}

impl GrpcTransportProfile for UnusedProfile {
    fn service_names(&self) -> &[String] {
        &[]
    }
}

impl H2TransportProfile for UnusedProfile {
    fn host(&self) -> Option<&str> {
        None
    }
    fn path(&self) -> &str {
        "/"
    }
}

impl HttpUpgradeTransportProfile for UnusedProfile {
    fn host(&self) -> Option<&str> {
        None
    }
    fn path(&self) -> &str {
        "/"
    }
}

impl SplitHttpTransportProfile for UnusedProfile {
    fn host(&self) -> Option<&str> {
        None
    }
    fn path(&self) -> &str {
        "/"
    }
    fn mode(&self) -> &str {
        "packet-up"
    }
}

fn leaf(runtime: &VlessTransportRuntime, ws: &WsProfile) -> VlessOutboundLeaf {
    let options: VlessOutboundBuildOptionsRef<
        '_,
        UnusedProfile,
        WsProfile,
        UnusedProfile,
        UnusedProfile,
        UnusedProfile,
        UnusedProfile,
    > = VlessOutboundBuildOptionsRef {
        final_mask: None,
        mkcp: None,
        hysteria: None,
        download: None,
        tag: "browser",
        server: "relay.example",
        port: 80,
        protocol: VlessOutboundOptionsRef {
            encryption: None,
            id: USER_ID,
            flow: None,
            testpre: 0,
            testseed: &[],
            mux_concurrency: None,
            xudp_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
            reality: None,
            quic: None,
        },
        tls: None,
        ws: Some(ws),
        grpc: None,
        h2: None,
        http_upgrade: None,
        split_http: None,
    };
    VlessOutboundLeaf::from_options_refs(None, options, runtime).unwrap()
}

async fn browser(
    page_url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let page = url::Url::parse(page_url).unwrap();
        let mut connection =
            tokio::net::TcpStream::connect((page.host_str().unwrap(), page.port().unwrap()))
                .await
                .unwrap();
        connection
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        connection.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let marker = "/websocket?token=";
        let start = response.find(marker).unwrap();
        let end = response[start..].find('"').unwrap() + start;
        let mut websocket = page.join(&response[start..end]).unwrap();
        websocket.set_scheme("ws").unwrap();
        let mut request = websocket.as_str().into_client_request().unwrap();
        request.headers_mut().insert(
            "origin",
            format!(
                "http://{}:{}",
                page.host_str().unwrap(),
                page.port().unwrap()
            )
            .parse()
            .unwrap(),
        );
        connect_async(request).await.unwrap().0
    })
    .await
    .expect("browser page fetch and registration complete within the test deadline")
}

fn session() -> Session {
    Session::new(
        1,
        Address::Domain("target.example".into()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    )
}

fn forbidden_native_dial(
    calls: Arc<AtomicUsize>,
) -> impl Clone
       + Fn(
    &str,
    u16,
) -> std::future::Ready<
    Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError>,
> {
    move |_, _| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(std::io::Error::other(
            "Browser Dialer must not open a native carrier",
        )
        .into()))
    }
}

#[tokio::test]
async fn prepared_leaf_release_reuses_listener_and_runtime_drop_retires_stream() {
    let settings = BrowserDialerSettings {
        listen: "127.0.0.1:0".into(),
        idle_capacity: 8,
        task_timeout_ms: 2_000,
        max_task_bytes: 64 * 1024,
        max_payload_bytes: 1024 * 1024,
    };
    let runtime = VlessTransportRuntime::default();
    let ws = WsProfile(settings.clone());
    let first_leaf = leaf(&runtime, &ws);
    let first_access = runtime.browser_dialer(settings.clone());
    let page_url = first_access.page_url().await.unwrap();
    drop(first_access);

    let mut agent = browser(&page_url).await;
    let peer = tokio::spawn(async move {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let assigned = agent.next().await.unwrap().unwrap();
            let task: serde_json::Value =
                serde_json::from_str(assigned.to_text().unwrap()).unwrap();
            assert_eq!(task["method"], "WS");
            assert_eq!(task["url"], "ws://relay.example/browser");
            agent.send(Message::Text("ok".into())).await.unwrap();
            assert!(!agent.next().await.unwrap().unwrap().into_data().is_empty());
            agent.send(Message::Binary(vec![0, 0])).await.unwrap();
            assert_eq!(agent.next().await.unwrap().unwrap().into_data(), b"hello");
            agent
                .send(Message::Binary(b"world".to_vec()))
                .await
                .unwrap();
        })
        .await
        .expect("first browser task handshake and relay complete within the test deadline");
    });
    let native_dials = Arc::new(AtomicUsize::new(0));
    let opened = tokio::time::timeout(
        TEST_TIMEOUT,
        first_leaf.open_tcp_stream(
            &session(),
            forbidden_native_dial(native_dials.clone()),
            zero_transport::OutboundDatagramSocketFactory::new(Default::default()),
            Arc::new(zero_transport::tls::ech::UnavailableEchConfigResolver),
        ),
    )
    .await
    .expect("first VLESS Browser Dialer handshake completes within the test deadline")
    .unwrap();
    drop(first_leaf);
    let (mut stream, _, _) = opened.into_parts();
    let mut reply = [0; 5];
    tokio::time::timeout(TEST_TIMEOUT, async {
        stream.write_all(b"hello").await.unwrap();
        stream.flush().await.unwrap();
        stream.read_exact(&mut reply).await.unwrap();
    })
    .await
    .expect("first Browser Dialer payload round trip completes within the test deadline");
    assert_eq!(&reply, b"world");
    peer.await.unwrap();
    drop(stream);
    assert_eq!(native_dials.load(Ordering::SeqCst), 0);

    let second_access = runtime.browser_dialer(settings.clone());
    assert_eq!(second_access.page_url().await.unwrap(), page_url);
    drop(second_access);
    let second_leaf = leaf(&runtime, &ws);
    let mut agent = browser(&page_url).await;
    let closed = tokio::spawn(async move {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let _ = agent.next().await.unwrap().unwrap();
            agent.send(Message::Text("ok".into())).await.unwrap();
            let _ = agent.next().await.unwrap().unwrap();
            agent.send(Message::Binary(vec![0, 0])).await.unwrap();
            agent.next().await
        })
        .await
        .expect("retired Browser Dialer channel closes within the test deadline")
    });
    let opened = tokio::time::timeout(
        TEST_TIMEOUT,
        second_leaf.open_tcp_stream(
            &session(),
            forbidden_native_dial(native_dials.clone()),
            zero_transport::OutboundDatagramSocketFactory::new(Default::default()),
            Arc::new(zero_transport::tls::ech::UnavailableEchConfigResolver),
        ),
    )
    .await
    .expect("second VLESS Browser Dialer handshake completes within the test deadline")
    .unwrap();
    drop(second_leaf);
    let (mut stream, _, _) = opened.into_parts();
    drop(runtime);
    let error = tokio::time::timeout(TEST_TIMEOUT, stream.read_u8())
        .await
        .expect("runtime retirement interrupts active Browser Dialer I/O")
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    // Cancellation invalidates I/O immediately. Releasing the invalidated
    // stream then closes the owned control WebSocket for the browser peer.
    drop(stream);
    let result = closed.await.unwrap();
    assert!(result.is_none() || result.unwrap().is_err());
}
