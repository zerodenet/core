use super::*;
use std::{io, net::SocketAddr};
use zero_config::RuntimeConfig;
use zero_traits::AsyncSocket;

#[derive(Clone, Copy)]
pub(super) struct Case {
    pub(super) method: &'static str,
    user: &'static str,
    identity: Option<&'static str>,
}
pub(super) fn cases() -> [Case; 5] {
    [
        Case {
            method: "aes-128-gcm",
            user: "legacy-secret",
            identity: None,
        },
        Case {
            method: "2022-blake3-aes-128-gcm",
            user: "MDEyMzQ1Njc4OWFiY2RlZg==",
            identity: None,
        },
        Case {
            method: "2022-blake3-aes-128-gcm",
            user: "MDEyMzQ1Njc4OWFiY2RlZg==",
            identity: Some("ZmVkY2JhOTg3NjU0MzIxMA=="),
        },
        Case {
            method: "aes-128-cfb",
            user: "stream-secret",
            identity: None,
        },
        Case {
            method: "none",
            user: "plain-secret",
            identity: None,
        },
    ]
}
impl Case {
    pub(super) fn authenticated(self) -> bool {
        self.method.contains("gcm")
    }
    fn client_password(self) -> String {
        self.identity.map_or_else(
            || self.user.into(),
            |identity| format!("{identity}:{}", self.user),
        )
    }
}
pub(super) struct Fixture {
    pub(super) probe: Proxy,
    pub(super) running: zero_proxy::RunningProxy,
    port: u16,
    config: ShadowsocksTcpConnectConfig,
}
impl Fixture {
    pub(super) fn new(case: Case, blocked: bool) -> Self {
        let port = free_port();
        let config=RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"ss-standard-in","listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"shadowsocks","cipher":case.method,"identity_password":case.identity,"replay_attack":"reject",
                    "users":[{"password":case.user,"principal_key":"configured:ss","up_bps":65536,"down_bps":32768,"device_limit":2,"quota_remaining_bytes":4096,"policy_revision":7}]}}],
            "route":{"rules":[],"final":{"type":if blocked {"reject"} else {"direct"}}}
        }).to_string()).unwrap();
        let engine = Proxy::new(config).unwrap();
        let probe = engine.clone();
        let running = spawn_engine(engine);
        let config =
            ShadowsocksTcpConnectConfig::from_config(case.method, &case.client_password()).unwrap();
        Self {
            probe,
            running,
            port,
            config,
        }
    }
    pub(super) async fn request(
        &self,
        target: SocketAddr,
    ) -> (shadowsocks::ShadowsocksOutboundSession, Vec<u8>) {
        let mut sink = Wire(Vec::new());
        let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), target.port());
        let session = self
            .config
            .establish_tcp_to(&mut sink, &destination)
            .await
            .unwrap();
        (session, sink.0)
    }
    pub(super) async fn open(
        &self,
        target: SocketAddr,
    ) -> (ShadowsocksAeadStream<TokioSocket>, SocketAddr, Vec<u8>) {
        let (session, wire) = self.request(target).await;
        wait_for_listener(self.port).await;
        let mut io = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let source = io.local_addr().unwrap();
        io.write_all(&wire).await.unwrap();
        (
            self.config
                .wrap_outbound_stream(TokioSocket::new(io), session),
            source,
            wire,
        )
    }
    pub(super) async fn reject_wire(&self, wire: &[u8]) {
        wait_for_listener(self.port).await;
        let mut io = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        io.write_all(wire).await.unwrap();
        io.shutdown().await.unwrap();
        let mut response = Vec::new();
        let result = io.read_to_end(&mut response).await;
        assert!(
            response.is_empty(),
            "no unencrypted or encrypted success reply on rejection"
        );
        if let Err(source) = result {
            assert!(matches!(
                source.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ));
        }
    }
}
/// Captures only outbound framing construction; all admission/relay assertions
/// above use actual native TCP and the production proxy listener.
struct Wire(Vec<u8>);
impl AsyncSocket for Wire {
    type Error = io::Error;
    async fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::UnexpectedEof.into())
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}
