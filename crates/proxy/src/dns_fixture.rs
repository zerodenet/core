use std::net::SocketAddr;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{mpsc, oneshot},
};
use zero_platform_tokio::contract::CancellationToken;
pub(crate) struct Request {
    pub tcp: bool,
    pub query: Vec<u8>,
    reply: oneshot::Sender<Vec<u8>>,
}
impl Request {
    pub fn answer(self, truncated: bool, failed: bool) {
        self.answer_ips(
            truncated,
            failed,
            &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
        );
    }
    pub fn answer_ips(self, truncated: bool, failed: bool, ips: &[zero_traits::IpAddress]) {
        let mut answer = zero_dns::udp::build_dns_response(&self.query, ips);
        if truncated {
            answer[2] |= 2;
        }
        if failed {
            answer[3] = (answer[3] & 0xf0) | 2;
        }
        self.reply.send(answer).unwrap();
    }
}
pub(crate) struct Dns {
    pub address: SocketAddr,
    requests: mpsc::UnboundedReceiver<Request>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Dns {
    pub async fn new() -> Self {
        let (tcp, udp) = crate::socket_fixture::tcp_udp();
        tcp.set_nonblocking(true).unwrap();
        udp.set_nonblocking(true).unwrap();
        // Transfer both actual originals into the resolver fixture. Neither
        // endpoint is released and rebound between selecting TCP and UDP.
        let listener = TcpListener::from_std(tcp).unwrap();
        let address = listener.local_addr().unwrap();
        let socket = Arc::new(UdpSocket::from_std(udp).unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            let mut bytes = vec![0; 65536];
            loop {
                tokio::select! { biased;
                    _ = cancel.cancelled() => break,
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let tx = tx.clone(); let cancel = cancel.clone();
                        children.spawn(async move {
                            let work = async {
                                let size = stream.read_u16().await.unwrap() as usize;
                                let mut query = vec![0;size]; stream.read_exact(&mut query).await.unwrap();
                                let (reply, answer) = oneshot::channel();
                                tx.send(Request { tcp: true, query, reply }).unwrap();
                                if let Ok(answer) = answer.await {
                                    stream.write_u16(answer.len() as u16).await.unwrap();
                                    stream.write_all(&answer).await.unwrap();
                                }
                            };
                            tokio::select! {biased; _=cancel.cancelled()=>{},_=work=>{}}
                        });
                    },
                    received = socket.recv_from(&mut bytes) => {
                        let (size, peer) = received.unwrap(); let query = bytes[..size].to_vec();
                        let (reply, answer) = oneshot::channel();
                        tx.send(Request { tcp: false, query, reply }).unwrap();
                        let socket = socket.clone(); let cancel = cancel.clone();
                        children.spawn(async move {
                            tokio::select! {biased;_=cancel.cancelled()=>{},answer=answer=>{
                                if let Ok(answer) = answer { socket.send_to(&answer,peer).await.unwrap(); }
                            }}
                        });
                    }
                }
                while let Some(result) = children.try_join_next() {
                    result.unwrap();
                }
            }
            while let Some(result) = children.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            requests,
            stop,
            task,
        }
    }
    pub async fn request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(4), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub fn no_new_request(&mut self) {
        assert!(
            self.requests.try_recv().is_err(),
            "a new original was opened before old custody settled"
        );
    }
    pub fn config(&self) -> zero_config::DnsConfig {
        zero_config::DnsConfig {
            servers: std::collections::BTreeMap::from([("native".into(), self.server())]),
            default_server: "native".into(),
            dispatch: vec![],
            reverse_mapping: None,
            answer: zero_config::DnsAnswerConfig::Real,
            cache: None,
            policy: zero_config::DnsPolicyConfig {
                address_family: zero_config::DnsAddressFamilyPolicy::Ipv4Only,
                timeout_ms: 10000,
                ..Default::default()
            },
        }
    }
    pub fn server(&self) -> zero_config::DnsServerConfig {
        zero_config::DnsServerConfig::Udp {
            host: "127.0.0.1".into(),
            port: self.address.port(),
            bootstrap: vec![],
            detour: None,
        }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
