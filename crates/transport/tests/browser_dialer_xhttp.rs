#![cfg(feature = "split_http")]

use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use zero_transport::{
    browser_dialer::{BrowserDialerOptions, BrowserDialerServer},
    split_http::connect_split_http_with_browser,
};

struct Profile;
impl zero_traits::SplitHttpTransportProfile for Profile {
    fn host(&self) -> Option<&str> {
        Some("relay.example")
    }
    fn path(&self) -> &str {
        "/xhttp"
    }
    fn mode(&self) -> &str {
        "packet-up"
    }
}

async fn browser(
    server: &BrowserDialerServer,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let page = url::Url::parse(server.page_url()).unwrap();
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
}

#[tokio::test]
async fn browser_xhttp_get_downlink_and_packet_uplink_form_one_stream() {
    let server = BrowserDialerServer::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        BrowserDialerOptions {
            task_timeout: std::time::Duration::from_secs(2),
            ..BrowserDialerOptions::default()
        },
    )
    .await
    .unwrap();
    let mut first = browser(&server).await;
    let mut second = browser(&server).await;
    let agent = tokio::spawn(async move {
        let first_task: serde_json::Value =
            serde_json::from_str(first.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        let (mut get, mut packet) = if first_task["method"] == "GET" {
            first.send(Message::Text("ok".into())).await.unwrap();
            first
                .send(Message::Binary(b"downlink".to_vec()))
                .await
                .unwrap();
            (first, second)
        } else {
            panic!("download task must be assigned before upload")
        };
        let packet_task: serde_json::Value =
            serde_json::from_str(packet.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(packet_task["method"], "POST");
        assert!(packet_task["url"]
            .as_str()
            .unwrap()
            .starts_with("https://relay.example/xhttp/"));
        packet.send(Message::Text("ok".into())).await.unwrap();
        assert_eq!(packet.next().await.unwrap().unwrap().into_data(), b"uplink");
        packet.send(Message::Text("ok".into())).await.unwrap();
        get.close(None).await.unwrap();
    });
    let mut stream =
        connect_split_http_with_browser(&server.dialer(), &Profile, "https", "relay.example")
            .await
            .unwrap();
    stream.write_all(b"uplink").await.unwrap();
    stream.flush().await.unwrap();
    let mut reply = [0; 8];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"downlink");
    agent.await.unwrap();
}

#[tokio::test]
async fn browser_xhttp_rejects_stream_modes_before_waiting_for_browser() {
    struct StreamOne;
    impl zero_traits::SplitHttpTransportProfile for StreamOne {
        fn host(&self) -> Option<&str> {
            None
        }
        fn path(&self) -> &str {
            "/xhttp"
        }
        fn mode(&self) -> &str {
            "stream-one"
        }
    }
    let server = BrowserDialerServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        BrowserDialerOptions::default(),
    )
    .await
    .unwrap();
    let error =
        connect_split_http_with_browser(&server.dialer(), &StreamOne, "https", "relay.example")
            .await
            .err()
            .expect("stream-one must be rejected");
    assert_eq!(
        error.to_string(),
        "Browser Dialer XHTTP supports packet-up only"
    );
}
