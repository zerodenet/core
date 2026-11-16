//! Ordinary HTTP/Mixed sources -> actual Direct/Node/relay providers -> Message/duplex.
use super::inbound_receipts::{closed, owner};
use crate::Proxy;
use serde_json::json;
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{timeout, Instant},
};
use zero_config::RuntimeConfig;
use zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration;

fn config(value: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&value.to_string()).unwrap()
}
async fn headers(io: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(io.read_u8().await.unwrap());
        assert!(bytes.len() < 8192);
    }
    bytes
}
fn native_owner(
    proxy: &Proxy,
    tag: Option<&str>,
    direct: bool,
) -> (
    std::sync::Arc<crate::component::instance::ComponentSlot>,
    CarrierOwnerRegistration,
) {
    let services = proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let slot = if direct {
        plan.direct(tag).unwrap()
    } else {
        plan.configured(tag.unwrap()).unwrap()
    };
    let original = slot
        .original_carrier_owner(0)
        .or_else(|| {
            slot.dependency_instances()
                .iter()
                .find_map(|id| plan.original_carrier_owner(*id, 0))
        })
        .expect("the actual selected native provider, never a substitute factory");
    (slot, original)
}

#[tokio::test]
async fn ordinary_message_and_upgrade_keep_standard_tcp_owners_across_direct_node_and_relay() {
    timeout(Duration::from_secs(90), async {
        for protocol in ["http", "mixed"] {
            for route in ["direct", "node", "relay"] {
                let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let target = origin.local_addr().unwrap();
                let (pending, suspended) = oneshot::channel();
                // The retained listener itself moves to this real worker, not a freed port.
                let backend = tokio::spawn(async move {
                    let (mut first, _) = origin.accept().await.unwrap();
                    assert!(headers(&mut first).await.starts_with(b"GET /one HTTP/1.1\r\n"));
                    first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none").await.unwrap();
                    first.shutdown().await.unwrap();
                    drop(first);
                    let (mut upgraded, _) = origin.accept().await.unwrap();
                    assert!(headers(&mut upgraded).await.starts_with(b"GET /socket HTTP/1.1\r\n"));
                    upgraded.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\nserver frame").await.unwrap();
                    let mut frame = [0; 12];
                    upgraded.read_exact(&mut frame).await.unwrap();
                    assert_eq!(&frame, b"client frame");
                    let mut tail = Vec::new();
                    {
                        let mut reading = Box::pin(upgraded.read_to_end(&mut tail));
                        std::future::poll_fn(|cx| {
                            assert!(reading.as_mut().poll(cx).is_pending(), "actual native read is suspended before half-close");
                            std::task::Poll::Ready(())
                        }).await;
                        pending.send(()).unwrap();
                        reading.await.unwrap();
                    }
                    assert!(tail.is_empty());
                    upgraded.write_all(b"after client EOF").await.unwrap();
                    upgraded.shutdown().await.unwrap();
                });
                let upstream = Proxy::new(config(json!({
                    "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                    "route":{"final":{"type":"direct"}}
                }))).unwrap();
                let upstream_running = upstream.spawn();
                let upstream_source = owner(&upstream, "socks").await;
                let socks = *upstream_source.original_listener_address.get().unwrap();
                let node = |tag| json!({"tag":tag,"protocol":{"type":"socks5","server":"127.0.0.1","port":socks.port()}});
                let mut value = json!({
                    "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":protocol}}],
                    "outbounds":[{"tag":"native","protocol":{"type":"direct"}}],
                    "route":{"final":{"type":"route","outbound":"native"}}
                });
                if route == "node" { value["outbounds"] = json!([node("native")]); }
                if route == "relay" {
                    value["outbounds"] = json!([node("first"),node("last")]);
                    value["outbound_groups"] = json!([{"tag":"chain","type":"relay","proxies":["first","last"]}]);
                    value["route"]["final"]["outbound"] = json!("chain");
                }
                let proxy = Proxy::new(config(value)).unwrap();
                let running = proxy.spawn();
                let source = owner(&proxy, "entry").await;
                let address = *source.original_listener_address.get().unwrap();
                let mut client = TcpStream::connect(address).await.unwrap();
                client.write_all(format!("GET http://{target}/one HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes()).await.unwrap();
                assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 200 OK\r\n"));
                let mut body = [0; 3];
                client.read_exact(&mut body).await.unwrap();
                assert_eq!(&body, b"one");
                // The same actual HTTP source continues, including its buffered byte.
                client.write_all(format!("GET http://{target}/socket HTTP/1.1\r\nHost: {target}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\nclient frame").as_bytes()).await.unwrap();
                assert!(headers(&mut client).await.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"));
                let mut frame = [0; 12];
                client.read_exact(&mut frame).await.unwrap();
                assert_eq!(&frame, b"server frame");
                suspended.await.unwrap();
                let (selected, provider) = native_owner(&proxy, Some(if route == "relay" {"first"} else {"native"}), route == "direct");
                let observed = provider.observe().unwrap();
                assert!(!observed.slots().is_empty());
                assert!(observed.slots().iter().any(|slot| slot.native().is_some()));
                let originals: Vec<_> = observed.slots().iter().map(|slot| slot.original().clone()).collect();
                let upstream_native = (route != "direct").then(|| native_owner(&upstream, None, true));
                client.shutdown().await.unwrap();
                let mut tail = Vec::new();
                client.read_to_end(&mut tail).await.unwrap();
                assert_eq!(tail, b"after client EOF");
                drop(client);
                backend.await.unwrap();
                timeout(Duration::from_secs(5), async {
                    while !proxy.engine().active_sessions().is_empty() { tokio::task::yield_now().await; }
                }).await.unwrap();
                let sessions = proxy.engine().completed_sessions();
                assert_eq!(sessions.len(), 2, "one real business identity for each message");
                assert_ne!(sessions[0].id, sessions[1].id);
                assert!(sessions.iter().all(|session| session.bytes_up > 0 && session.bytes_down > 0));
                running.shutdown().await.unwrap();
                closed(&source);
                assert!(selected.shutdown(Instant::now()+Duration::from_secs(5)).await.unwrap().is_complete());
                assert!(provider.closed().unwrap().matches(&provider));
                assert!(provider.users_closed().unwrap().matches(&provider));
                for actual in originals { assert!(actual.matches_closed(&actual.closed().unwrap())); }
                upstream_running.shutdown().await.unwrap();
                closed(&upstream_source);
                if let Some((slot, original)) = upstream_native {
                    assert!(slot.shutdown(Instant::now()+Duration::from_secs(5)).await.unwrap().is_complete());
                    assert!(original.closed().unwrap().matches(&original));
                    assert!(original.users_closed().unwrap().matches(&original));
                }
                assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
                assert_eq!(upstream.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
            }
        }
    }).await.unwrap();
}
