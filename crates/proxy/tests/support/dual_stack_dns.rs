//! Deterministic local A/AAAA answers, independent of host DNS and active TUN.
use tokio::{net::UdpSocket, task::JoinHandle};
use zero_config::RuntimeConfig;
use zero_traits::IpAddress;

pub struct DualStackDns {
    port: u16,
    task: JoinHandle<()>,
}

impl DualStackDns {
    pub async fn start() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let addresses = [
                IpAddress::V4([127, 0, 0, 1]),
                IpAddress::V6(std::net::Ipv6Addr::LOCALHOST.octets()),
            ];
            let mut query = [0; 4096];
            loop {
                let (size, peer) = socket.recv_from(&mut query).await.unwrap();
                let response = zero_dns::udp::build_dns_response(&query[..size], &addresses);
                socket.send_to(&response, peer).await.unwrap();
            }
        });
        Self { port, task }
    }

    pub fn configure(&self, config: &mut RuntimeConfig) {
        config.runtime.dns = Some(
            serde_json::from_value(serde_json::json!({
                "servers":{"local":{"type":"udp","host":"127.0.0.1","port":self.port}},
                "default_server":"local"
            }))
            .unwrap(),
        );
    }
}

impl Drop for DualStackDns {
    fn drop(&mut self) {
        self.task.abort();
    }
}
