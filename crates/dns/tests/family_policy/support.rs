use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{Notify, Semaphore};
use zero_config::{DnsAddressFamilyPolicy, DnsCacheConfig, DnsConfig, DnsServerConfig};
use zero_traits::IpAddress;

pub(super) const V4: IpAddress = IpAddress::V4([192, 0, 2, 42]);
pub(super) const V6: IpAddress =
    IpAddress::V6([0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42]);

pub(super) struct Fixture {
    port: u16,
    addresses: Arc<RwLock<Vec<IpAddress>>>,
    queries: Arc<Mutex<Vec<u16>>>,
    received: Arc<Notify>,
    permits: Arc<Semaphore>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    pub(super) async fn new(addresses: Vec<IpAddress>, held: bool) -> Self {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = socket.local_addr().unwrap().port();
        let addresses = Arc::new(RwLock::new(addresses));
        let queries = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::new(Notify::new());
        let permits = Arc::new(Semaphore::new(if held { 0 } else { 1000 }));
        let task = {
            let addresses = addresses.clone();
            let queries = queries.clone();
            let received = received.clone();
            let permits = permits.clone();
            tokio::spawn(async move {
                let mut responders = tokio::task::JoinSet::new();
                let mut request = [0_u8; 4096];
                loop {
                    let (size, peer) = socket.recv_from(&mut request).await.unwrap();
                    let query = request[..size].to_vec();
                    let question = zero_dns::udp::parse_dns_question(&query).unwrap();
                    queries.lock().unwrap().push(question.query_type);
                    received.notify_one();
                    let addresses = addresses.read().unwrap().clone();
                    let socket = socket.clone();
                    let permits = permits.clone();
                    responders.spawn(async move {
                        let _permit = permits.acquire().await.unwrap();
                        let response = zero_dns::udp::build_dns_response(&query, &addresses);
                        socket.send_to(&response, peer).await.unwrap();
                    });
                }
            })
        };
        Self { port, addresses, queries, received, permits, task }
    }

    pub(super) fn server(&self) -> DnsServerConfig {
        DnsServerConfig::Udp {
            host: "127.0.0.1".to_owned(),
            port: self.port,
            bootstrap: Vec::new(),
            detour: None,
        }
    }

    pub(super) fn config(&self, family: DnsAddressFamilyPolicy) -> DnsConfig {
        DnsConfig {
            servers: BTreeMap::from([("fixture".to_owned(), self.server())]),
            default_server: "fixture".to_owned(),
            dispatch: Vec::new(),
            reverse_mapping: None,
            answer: zero_config::DnsAnswerConfig::Real,
            cache: Some(DnsCacheConfig { max_entries: 32, max_ttl_seconds: None }),
            policy: zero_config::DnsPolicyConfig {
                address_family: family,
                timeout_ms: 1500,
                ..Default::default()
            },
        }
    }

    pub(super) fn answer(&self, addresses: Vec<IpAddress>) {
        *self.addresses.write().unwrap() = addresses;
    }

    pub(super) fn queries(&self) -> Vec<u16> {
        self.queries.lock().unwrap().clone()
    }

    pub(super) async fn wait_for_queries(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.queries.lock().unwrap().len() < count {
                self.received.notified().await;
            }
        }).await.expect("DNS fixture did not receive the expected independent queries");
    }

    pub(super) fn release(&self) {
        self.permits.add_permits(1000);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) { self.task.abort(); }
}
