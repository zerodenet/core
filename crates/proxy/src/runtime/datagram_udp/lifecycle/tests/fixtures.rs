use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use zero_config::RuntimeConfig;
use zero_core::{Address, InboundUdpDispatch, ProtocolType};

use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_ingress::UdpIngressRuntime;
use crate::runtime::Proxy;

pub(super) const IDLE_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) struct UpstreamFixture {
    pub(super) proxy: Proxy,
    pub(super) runtime: UdpIngressRuntime,
    listener: TcpListener,
    relay: UdpSocket,
    control: TcpStream,
    session_id: u64,
}

impl UpstreamFixture {
    pub(super) async fn establish() -> (Self, UdpDispatch) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_port = listener.local_addr().unwrap().port();
        let relay = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let relay_port = relay.local_addr().unwrap().port();
        let config = RuntimeConfig::parse(&format!(
            r#"{{
                "outbounds": [{{
                    "tag": "upstream",
                    "protocol": {{"type": "socks5", "server": "127.0.0.1", "port": {upstream_port}}}
                }}],
                "route": {{"final": {{"type": "route", "outbound": "upstream"}}}}
            }}"#
        ))
        .unwrap();
        let proxy = Proxy::new(config)
            .unwrap()
            .with_udp_upstream_idle_timeout(Duration::from_secs(60));
        let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch = runtime.new_dispatch("idle-test").await.unwrap();
        let packet = inbound_packet();
        let (sent, control) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                runtime.dispatch_inbound_packet(&mut dispatch, &packet, None, None),
                accept_association(&listener, relay_port),
            )
        })
        .await
        .expect("upstream association setup timed out");
        let session_id = sent.expect("send first upstream packet");
        receive_packet(&relay).await;
        assert_eq!(proxy.stats_snapshot().udp_upstream.active_associations, 1);
        (
            Self {
                proxy,
                runtime,
                listener,
                relay,
                control,
                session_id,
            },
            dispatch,
        )
    }

    pub(super) fn assert_idle_closed(&self) {
        let stats = self.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.idle_timeouts, 1, "expired upstream was not reclaimed");
        assert_eq!(stats.active_associations, 0);
        assert_eq!(stats.closed_associations, 0);
        assert_eq!(self.proxy.engine().active_sessions().len(), 1);
        assert_eq!(self.proxy.engine().active_sessions()[0].id, self.session_id);
    }

    pub(super) async fn assert_reestablishes(mut self, mut dispatch: UdpDispatch) {
        assert!(dispatch.upstream_association_view().is_none());
        assert!(
            dispatch.poll_refs().2.is_none(),
            "expired deadline remains armed"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), self.control.read(&mut [0]))
                .await
                .expect("idle upstream control socket was not released")
                .unwrap(),
            0
        );

        let packet = inbound_packet();
        let relay_port = self.relay.local_addr().unwrap().port();
        let (sent, mut control) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                self.runtime
                    .dispatch_inbound_packet(&mut dispatch, &packet, None, None),
                accept_association(&self.listener, relay_port),
            )
        })
        .await
        .expect("replacement upstream association setup timed out");
        assert_eq!(sent.unwrap(), self.session_id, "tracked UDP flow changed");
        receive_packet(&self.relay).await;

        let stats = self.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.created_associations, 2);
        assert_eq!(stats.active_associations, 1);
        assert_eq!(stats.reused_associations, 0);
        assert_eq!(stats.idle_timeouts, 1);
        assert!(dispatch.poll_refs().2.is_some());

        let completed = dispatch.finish_all();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].record.id, self.session_id);
        assert_eq!(
            completed[0].record.inbound_rx_bytes,
            2 * b"idle-test".len() as u64
        );
        // Outbound accounting includes both SOCKS handshakes and datagram
        // framing; reclaiming the carrier must not reset the logical flow.
        assert!(completed[0].record.outbound_tx_bytes > completed[0].record.inbound_rx_bytes);
        assert_eq!(
            completed[0].record.bytes_up,
            completed[0].record.outbound_tx_bytes
        );
        assert!(self.proxy.engine().active_sessions().is_empty());
        assert_eq!(self.proxy.engine().completed_sessions().len(), 1);
        let stats = self.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.active_associations, 0);
        assert_eq!(stats.idle_timeouts, 1);
        assert_eq!(stats.closed_associations, 1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), control.read(&mut [0]))
                .await
                .expect("replacement control socket was not released")
                .unwrap(),
            0
        );
    }
}

fn inbound_packet() -> InboundUdpDispatch {
    InboundUdpDispatch::new(
        ProtocolType::new("idle-test"),
        Address::Ipv4(Ipv4Addr::LOCALHOST.octets()),
        9000,
        b"idle-test".to_vec(),
        None,
    )
}

async fn receive_packet(relay: &UdpSocket) {
    let mut packet = [0; 128];
    let (read, _) = tokio::time::timeout(Duration::from_secs(2), relay.recv_from(&mut packet))
        .await
        .expect("upstream UDP packet timed out")
        .unwrap();
    assert_eq!(&packet[10..read], b"idle-test");
}

async fn accept_association(listener: &TcpListener, relay_port: u16) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut greeting = [0; 3];
    stream.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
    stream.write_all(&[5, 0]).await.unwrap();
    let mut request = [0; 10];
    stream.read_exact(&mut request).await.unwrap();
    assert_eq!(&request[..4], &[5, 3, 0, 1]);
    stream
        .write_all(&[
            5,
            0,
            0,
            1,
            127,
            0,
            0,
            1,
            (relay_port >> 8) as u8,
            relay_port as u8,
        ])
        .await
        .unwrap();
    stream
}
