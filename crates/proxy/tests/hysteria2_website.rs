#![cfg(feature = "hysteria2")]
mod support;
mod website_support;
use std::time::Duration;
use support::{free_port, wait_for_listener};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use website_support::*;
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[tokio::test]
async fn hysteria2_website_serves_http1_tls_http1_and_http2_with_shared_content() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let site = Site::new();
        let config = site.config(false);
        let running = Proxy::new(parse(&config)).unwrap().spawn();
        wait_for_listener(site.http).await;
        let response = http1(TcpStream::connect(("127.0.0.1", site.http)).await.unwrap()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.ends_with("website"), "{response}");
        assert!(response
            .to_lowercase()
            .contains(&format!("alt-svc: h3=\":{}\"; ma=2592000", site.quic)));
        let response = http1(site.tls(b"http/1.1").await).await;
        assert!(response.ends_with("website"), "{response}");
        let (status, alt, data) = site.http2().await;
        assert_eq!(status, 200, "TCP website must never authenticate HY2");
        assert_eq!(alt, format!("h3=\":{}\"; ma=2592000", site.quic));
        assert_eq!(data, b"website");
        running.shutdown().await.unwrap();
        assert!(TcpListener::bind(("127.0.0.1", site.http)).await.is_ok());
        assert!(TcpListener::bind(("127.0.0.1", site.https)).await.is_ok());
        assert!(UdpSocket::bind(("127.0.0.1", site.quic)).await.is_ok());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn hysteria2_website_redirect_and_listener_reload_rollback_are_atomic() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let site = Site::new();
        let config = site.config(true);
        let proxy = Proxy::new(parse(&config)).unwrap();
        let engine = proxy.engine().clone();
        let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
        let running = proxy.spawn();
        wait_for_listener(site.http).await;
        let response = http1(TcpStream::connect(("127.0.0.1", site.http)).await.unwrap()).await;
        assert!(response.starts_with("HTTP/1.1 301"), "{response}");
        assert!(
            response
                .to_lowercase()
                .contains(&format!("location: https://localhost:{}/auth", site.https)),
            "{response}"
        );
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let temporary = free_port();
        let mut failed = config.clone();
        failed["inbounds"][0]["protocol"]["masquerade"]["http"]["port"] = temporary.into();
        failed["inbounds"][0]["protocol"]["masquerade"]["https"]["port"] =
            occupied.local_addr().unwrap().port().into();
        assert!(handle
            .apply_config_and_wait(parse(&failed), Duration::from_secs(5))
            .await
            .is_err());
        assert_eq!(engine.config_revision(), 1);
        assert!(
            TcpListener::bind(("127.0.0.1", temporary)).await.is_ok(),
            "partial bind leaked HTTP port"
        );
        assert!(http1(site.tls(b"http/1.1").await)
            .await
            .ends_with("website"));
        let mut next = config.clone();
        next["inbounds"][0]["protocol"]["masquerade"]["http"]["port"] = temporary.into();
        next["inbounds"][0]["protocol"]["masquerade"]["force_https"] = false.into();
        next["inbounds"][0]["protocol"]["masquerade"]["content"] = "updated".into();
        handle
            .apply_config_and_wait(parse(&next), Duration::from_secs(5))
            .await
            .unwrap();
        assert!(TcpStream::connect(("127.0.0.1", site.http)).await.is_err());
        assert!(
            http1(TcpStream::connect(("127.0.0.1", temporary)).await.unwrap())
                .await
                .ends_with("updated")
        );
        assert!(http1(site.tls(b"http/1.1").await)
            .await
            .ends_with("updated"));
        running.shutdown().await.unwrap();
        assert!(TcpListener::bind(("127.0.0.1", temporary)).await.is_ok());
        assert!(TcpListener::bind(("127.0.0.1", site.https)).await.is_ok());
        assert!(UdpSocket::bind(("127.0.0.1", site.quic)).await.is_ok());
    })
    .await
    .unwrap();
}
fn parse(value: &serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&value.to_string()).unwrap()
}
