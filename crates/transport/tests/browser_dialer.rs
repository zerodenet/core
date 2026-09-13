#![cfg(feature = "ws")]

use futures_util::{SinkExt, StreamExt};
use http::Method;
use std::{io, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use zero_transport::browser_dialer::{BrowserDialerOptions, BrowserDialerServer, BrowserStream};

async fn server() -> BrowserDialerServer {
    BrowserDialerServer::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        BrowserDialerOptions {
            task_timeout: std::time::Duration::from_secs(2),
            ..BrowserDialerOptions::default()
        },
    )
    .await
    .unwrap()
}

async fn browser(
    server: &BrowserDialerServer,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let page = url::Url::parse(server.page_url()).unwrap();
    let mut websocket = registration_url(server).await;
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
}

async fn registration_url(server: &BrowserDialerServer) -> url::Url {
    let mut connection = tokio::net::TcpStream::connect(server.local_addr())
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
    url::Url::parse(server.page_url())
        .unwrap()
        .join(&response[start..end])
        .unwrap()
}

async fn task(
    browser: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    let message = browser.next().await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn browser_dialer_requires_page_token_and_exact_origin() {
    let server = server().await;
    assert!(url::Url::parse(server.page_url())
        .unwrap()
        .query()
        .is_none());
    let mut unauthorized = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    unauthorized
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    unauthorized.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("Referrer-Policy: no-referrer"));

    let mut missing_token = url::Url::parse(server.page_url()).unwrap();
    missing_token.set_scheme("ws").unwrap();
    missing_token.set_path("/websocket");
    let mut missing_token = missing_token.as_str().into_client_request().unwrap();
    missing_token.headers_mut().insert(
        "origin",
        format!("http://{}", server.local_addr()).parse().unwrap(),
    );
    assert!(connect_async(missing_token)
        .await
        .unwrap_err()
        .to_string()
        .contains("403"));

    let mut registration = registration_url(&server).await;
    registration.set_scheme("ws").unwrap();
    let mut ws = registration.as_str().into_client_request().unwrap();
    ws.headers_mut()
        .insert("origin", "http://attacker.invalid".parse().unwrap());
    let error = connect_async(ws).await.unwrap_err();
    assert!(error.to_string().contains("403"));

    let _authenticated = browser(&server).await;
}

#[tokio::test]
async fn browser_page_limits_idle_connections_to_configured_capacity() {
    let server = BrowserDialerServer::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        BrowserDialerOptions {
            idle_capacity: 3,
            ..BrowserDialerOptions::default()
        },
    )
    .await
    .unwrap();
    let mut connection = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    connection
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    connection.read_to_string(&mut response).await.unwrap();

    assert!(response.contains("const idleTarget = 3;"));
    assert!(!response.contains("__IDLE_TARGET__"));
}

#[tokio::test]
async fn browser_websocket_relays_bytes_and_failure_is_reported() {
    let server = server().await;
    let dialer = server.dialer();
    let mut agent = browser(&server).await;
    let relay = tokio::spawn(async move {
        let assigned = task(&mut agent).await;
        assert_eq!(assigned["method"], "WS");
        assert_eq!(assigned["url"], "wss://relay.example/ws");
        assert_eq!(assigned["extra"]["protocol"], "ZWFybHk");
        agent.send(Message::Text("ok".into())).await.unwrap();
        assert_eq!(agent.next().await.unwrap().unwrap().into_data(), b"hello");
        agent
            .send(Message::Binary(b"world".to_vec()))
            .await
            .unwrap();
    });
    let mut stream: BrowserStream = dialer
        .dial_websocket("wss://relay.example/ws", Some(b"early"), 0)
        .await
        .unwrap();
    let mut reply = [0; 5];
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        stream.write_all(b"hello").await.unwrap();
        stream.flush().await.unwrap();
        stream.read_exact(&mut reply).await.unwrap();
    })
    .await
    .expect("browser WebSocket payload round trip completes");
    assert_eq!(&reply, b"world");
    relay.await.unwrap();

    let mut agent = browser(&server).await;
    let failure = tokio::spawn(async move {
        let _ = task(&mut agent).await;
        agent.send(Message::Text("fail".into())).await.unwrap();
    });
    let error = dialer
        .dial_websocket("ws://relay.example/ws", None, 0)
        .await
        .err()
        .expect("browser failure must fail the dial");
    assert!(error.to_string().contains("failed: fail"));
    failure.await.unwrap();
}

#[tokio::test]
async fn browser_get_drop_cancels_channel_and_packet_ack_is_not_hidden() {
    let server = server().await;
    let dialer = server.dialer();
    let mut get_agent = browser(&server).await;
    let cancelled = tokio::spawn(async move {
        let assigned = task(&mut get_agent).await;
        assert_eq!(assigned["method"], "GET");
        get_agent.send(Message::Text("ok".into())).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), get_agent.next())
            .await
            .expect("dropping the stream closes the assigned channel");
        assert!(result.is_none() || result.unwrap().is_err());
    });
    let stream = dialer
        .dial_get("https://relay.example/down", &http::HeaderMap::new())
        .await
        .unwrap();
    drop(stream);
    cancelled.await.unwrap();

    let mut packet_agent = browser(&server).await;
    let packet = tokio::spawn(async move {
        let assigned = task(&mut packet_agent).await;
        assert_eq!(assigned["method"], "POST");
        packet_agent.send(Message::Text("ok".into())).await.unwrap();
        assert_eq!(
            packet_agent.next().await.unwrap().unwrap().into_data(),
            b"payload"
        );
        packet_agent
            .send(Message::Text("denied".into()))
            .await
            .unwrap();
    });
    let error = dialer
        .send_packet(
            &Method::POST,
            "https://relay.example/up",
            &http::HeaderMap::new(),
            b"payload",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("denied"));
    packet.await.unwrap();
}
