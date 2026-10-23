use std::net::Ipv4Addr;
use std::time::Duration;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
};

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
    control: tokio::sync::Mutex<Option<TcpStream>>,
    idle_control_eof: AtomicBool,
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        proxy
            .protocols
            .start_components(proxy.engine().config(), deadline)
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&proxy.engine().config(), || Ok(()))
            .unwrap();
        let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch = runtime.new_dispatch("idle-test").await.unwrap();
        let packet = inbound_packet();
        let (sent, mut control) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                runtime.dispatch_inbound_packet(&mut dispatch, &packet, None, None),
                accept_association(&listener, relay_port),
            )
        })
        .await
        .expect("upstream association setup timed out");
        let session_id = sent.expect("send first upstream packet");
        receive_packet(&relay).await;
        // Establish actual control-read suspension before the idle timer. A
        // delay or a retained socket alone would not prove pending cleanup.
        assert_control_pending(&mut control).await;
        assert_eq!(proxy.stats_snapshot().udp_upstream.active_associations, 1);
        (
            Self {
                proxy,
                runtime,
                listener,
                relay,
                control: tokio::sync::Mutex::new(Some(control)),
                idle_control_eof: AtomicBool::new(false),
                session_id,
            },
            dispatch,
        )
    }

    pub(super) async fn confirm_idle_control<F: Future>(&self, mut relay: Pin<&mut F>) {
        // The timer has fired under virtual time. Native TCP EOF must progress
        // with real I/O; auto-advancing a virtual cleanup deadline is no ACK.
        tokio::time::resume();
        let mut control = self
            .control
            .lock()
            .await
            .take()
            .expect("original server control");
        let closing = async {
            assert_eq!(
                control.read(&mut [0]).await.unwrap(),
                0,
                "idle upstream control socket was not released"
            );
            drop(control); // The actual server now sends EOF back to the client.
            self.idle_control_eof.store(true, Ordering::Release);
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = relay.as_mut() => panic!("idle relay unexpectedly ended before control EOF"),
                _ = closing => {},
            }
            poll_fn(|cx| {
                assert!(relay.as_mut().poll(cx).is_pending());
                if self.proxy.stats_snapshot().udp_upstream.idle_timeouts == 1 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        })
        .await
        .expect("actual idle control/native retirement did not finish");
        tokio::time::pause();
    }

    pub(super) fn assert_idle_closed(&self) {
        let stats = self.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.idle_timeouts, 1, "expired upstream was not reclaimed");
        assert_eq!(stats.active_associations, 0);
        assert_eq!(stats.closed_associations, 0);
        assert_eq!(self.proxy.engine().active_sessions().len(), 1);
        assert_eq!(self.proxy.engine().active_sessions()[0].id, self.session_id);
    }

    pub(super) async fn assert_reestablishes(self, mut dispatch: UdpDispatch) {
        assert!(dispatch.upstream_association_view().is_none());
        assert!(
            dispatch.poll_refs().2.is_none(),
            "expired deadline remains armed"
        );
        assert!(
            self.idle_control_eof.load(Ordering::Acquire),
            "actual original server EOF was not confirmed"
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
        assert_control_pending(&mut control).await;

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
        let retired = self
            .runtime
            .services()
            .packet_path_execution()
            .tcp()
            .retired_datagrams();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let (report, ()) = tokio::join!(retired.progress_retired(deadline), async {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), control.read(&mut [0]))
                    .await
                    .expect("replacement control socket was not released")
                    .unwrap(),
                0
            );
            drop(control);
        },);
        assert!(
            report.unwrap().is_complete(),
            "replacement original native closure remains unconfirmed"
        );
        let stats = self.proxy.stats_snapshot().udp_upstream;
        assert_eq!(stats.active_associations, 0);
        assert_eq!(stats.idle_timeouts, 1);
        assert_eq!(stats.closed_associations, 1);
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

async fn assert_control_pending(control: &mut TcpStream) {
    let mut byte = [0];
    let read = control.read(&mut byte);
    tokio::pin!(read);
    poll_fn(|cx| {
        assert!(
            read.as_mut().poll(cx).is_pending(),
            "actual TCP control read must be suspended before retirement"
        );
        Poll::Ready(())
    })
    .await;
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
