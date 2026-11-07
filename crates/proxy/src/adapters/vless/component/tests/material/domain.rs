//! Actual admitted Business retries native addresses before any protocol bytes.
use super::fixture::*;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::{TcpSocket, UdpSocket};
pub(super) struct Node {
    pub(super) address: SocketAddr,
    pub(super) queries: Arc<AtomicUsize>,
    stop: zero_platform_tokio::contract::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Node {
    pub(super) async fn start() -> Self {
        let io = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = io.local_addr().unwrap();
        let stop = zero_platform_tokio::contract::CancellationToken::new();
        let cancelled = stop.clone();
        let queries = Arc::new(AtomicUsize::new(0));
        let observed = queries.clone();
        let task = tokio::spawn(async move {
            let mut bytes = [0; 4096];
            loop {
                let (length, peer) = tokio::select! {
                    _ = cancelled.cancelled() => break,
                    incoming = io.recv_from(&mut bytes) => incoming.unwrap(),
                };
                let query = &bytes[..length];
                assert!(length >= 17);
                assert_eq!(&query[4..6], &[0, 1]);
                let mut offset = 12;
                let mut labels = Vec::new();
                loop {
                    let count = query[offset] as usize;
                    offset += 1;
                    if count == 0 {
                        break;
                    }
                    assert!(count <= 63 && offset + count <= length);
                    labels.push(std::str::from_utf8(&query[offset..offset + count]).unwrap());
                    offset += count;
                }
                assert_eq!(labels.join("."), "node.example");
                assert!(offset + 4 <= length);
                let kind = u16::from_be_bytes([query[offset], query[offset + 1]]);
                assert_eq!(&query[offset + 2..offset + 4], &[0, 1]);
                assert!(kind == 1 || kind == 28);
                let count = if kind == 1 {
                    observed.fetch_add(1, Ordering::SeqCst);
                    2u16
                } else {
                    0
                };
                let mut answer = query[..2].to_vec();
                answer.extend_from_slice(&[0x81, 0x80, 0, 1]);
                answer.extend_from_slice(&count.to_be_bytes());
                answer.extend_from_slice(&[0, 0, 0, 0]);
                answer.extend_from_slice(&query[12..offset + 4]);
                if kind == 1 {
                    for address in [[127, 0, 0, 2], [127, 0, 0, 1]] {
                        answer.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4]);
                        answer.extend_from_slice(&address);
                    }
                }
                assert_eq!(io.send_to(&answer, peer).await.unwrap(), answer.len());
            }
        });
        Self {
            address,
            queries,
            stop,
            task,
        }
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
#[tokio::test]
async fn ordinary_h2_and_upgrade_retry_real_refused_first_address_with_one_live_business_n1() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for kind in ["h2", "http_upgrade"] {
            let directory = tempfile::tempdir().unwrap();
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert_path = directory.path().join("server.pem");
            let key_path = directory.path().join("server.key");
            let ca_path = directory.path().join("client.pem");
            std::fs::write(&cert_path, cert.cert.pem()).unwrap();
            std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
            std::fs::write(&ca_path, cert.cert.pem()).unwrap();
            let remote = server(kind, &cert_path, &key_path).await;
            let bad = TcpSocket::new_v4().unwrap();
            bad.bind(SocketAddr::new("127.0.0.2".parse().unwrap(), remote.address.port())).unwrap();
            let node = Node::start().await;
            let mut outbound = serde_json::json!({"tag":"node","protocol":{"type":"vless","server":"node.example","port":remote.address.port(),"id":"11111111-2222-3333-4444-555555555555","tls":{"server_name":"localhost","ca_cert_path":ca_path,"client_fingerprint":null,"options":{"backend":"rustls"}}}});
            outbound["protocol"][kind] = serde_json::json!({"host":"front.test","path":"/original"});
            let client = start(serde_json::json!({"runtime":{"native_stream_max_originals":1,"dns":{"servers":{"node":{"type":"udp","host":"127.0.0.1","port":node.address.port()}},"default_server":"node"}},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[outbound],"route":{"final":{"type":"route","outbound":"node"}}}), "entry").await;
            let (owner, original) = echo_inspect(&client, |owner, original| {
                let snapshot = owner.observe().unwrap();
                assert_eq!(snapshot.retained(), 1, "failed original already genuinely retired");
                let actual = snapshot.slots().iter().find(|slot| slot.original().same_original(original)).unwrap();
                assert_eq!(actual.ordinal(), 2, "one true refused native attempt, then the good address");
                assert!(actual.driver().is_none());
                let auxiliary = actual.auxiliary().unwrap();
                let joined = auxiliary.try_joined().unwrap();
                assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
            }).await;
            assert_eq!(node.queries.load(Ordering::SeqCst), 1, "whole Business demand resolves Node only once");
            wait_returned(&owner, &original).await;
            client.close().await;
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            remote.close().await; drop(bad); node.close().await;
        }
    }).await.unwrap();
}
