#![cfg(all(feature = "ws", feature = "http_server"))]

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zero_transport::browser_dialer::{BrowserDialerOptions, BrowserDialerServer};

/// An external browser opens the page written to ZERO_BROWSER_DIALER_READY_FILE.
/// This explicitly exercises the shipped JavaScript rather than a WS simulator.
#[tokio::test]
#[ignore = "requires a real browser to open the loopback page"]
async fn real_browser_executes_websocket_and_fetch_data_channels() {
    let ready_file = std::env::var("ZERO_BROWSER_DIALER_READY_FILE")
        .expect("set ZERO_BROWSER_DIALER_READY_FILE for the external browser driver");
    tokio::time::timeout(Duration::from_secs(180), async {
        let browser = BrowserDialerServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            BrowserDialerOptions {
                idle_capacity: 3,
                task_timeout: Duration::from_secs(120),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let websocket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_address = websocket.local_addr().unwrap();
        let ws_task = tokio::spawn(async move {
            let (socket, _) = websocket.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let message = socket.next().await.unwrap().unwrap();
            assert_eq!(message.clone().into_data(), b"browser-websocket");
            socket.send(message).await.unwrap();
        });

        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http.local_addr().unwrap();
        let posted = Arc::new(tokio::sync::Notify::new());
        let received = posted.clone();
        let http_task = tokio::spawn(async move {
            loop {
                let (socket, _) = http.accept().await.unwrap();
                let received = received.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        move |request: http::Request<hyper::body::Incoming>| {
                            let received = received.clone();
                            async move {
                                let body = match request.method() {
                                    &http::Method::OPTIONS => Bytes::new(),
                                    &http::Method::GET => Bytes::from_static(b"browser-downstream"),
                                    &http::Method::POST => {
                                        let body =
                                            request.into_body().collect().await.unwrap().to_bytes();
                                        assert_eq!(body.as_ref(), b"browser-upstream");
                                        received.notify_one();
                                        Bytes::new()
                                    }
                                    method => panic!("unexpected browser method {method}"),
                                };
                                Ok::<_, Infallible>(
                                    http::Response::builder()
                                        .header("Access-Control-Allow-Origin", "*")
                                        .header(
                                            "Access-Control-Allow-Methods",
                                            "GET, POST, OPTIONS",
                                        )
                                        .header("Access-Control-Allow-Headers", "*")
                                        .body(Full::new(body))
                                        .unwrap(),
                                )
                            }
                        },
                    );
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                        .await;
                });
            }
        });
        std::fs::write(
            &ready_file,
            serde_json::to_vec(&serde_json::json!({
                "page_url": browser.page_url()
            }))
            .unwrap(),
        )
        .unwrap();
        let mut stream = browser
            .dialer()
            .dial_websocket(&format!("ws://{ws_address}/echo"), None, 0)
            .await
            .unwrap();
        stream.write_all(b"browser-websocket").await.unwrap();
        stream.flush().await.unwrap();
        let mut echoed = [0; 17];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"browser-websocket");
        drop(stream);
        ws_task.await.unwrap();

        let mut down = browser
            .dialer()
            .dial_get(
                &format!("http://{http_address}/download"),
                &http::HeaderMap::new(),
            )
            .await
            .unwrap();
        let mut payload = [0; 18];
        down.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"browser-downstream");
        drop(down);
        browser
            .dialer()
            .send_packet(
                &http::Method::POST,
                &format!("http://{http_address}/upload"),
                &http::HeaderMap::new(),
                b"browser-upstream",
            )
            .await
            .unwrap();
        posted.notified().await;
        http_task.abort();
        browser.dialer().close();
    })
    .await
    .expect("real-browser local acceptance timed out");
}
