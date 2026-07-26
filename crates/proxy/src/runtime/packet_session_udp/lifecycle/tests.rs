use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use zero_config::RuntimeConfig;
use zero_core::{Address, InboundUdpDispatch, ProtocolType};

use super::relay::{run_packet_session_udp_relay_with_dispatch, PacketSessionUdpLoopExit};
use crate::runtime::packet_session_udp::{
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpReadFailure,
    PacketSessionUdpReadResult, PacketSessionUdpRelayRequest,
};
use crate::runtime::udp_ingress::UdpIngressRuntime;

enum TestInbound {
    Dispatch(InboundUdpDispatch),
    End,
}

struct TestHandler {
    inbound: mpsc::UnboundedReceiver<TestInbound>,
    responses: mpsc::UnboundedSender<Vec<u8>>,
}

impl PacketSessionUdpHandler for TestHandler {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<PacketSessionUdpReadResult, PacketSessionUdpReadFailure> {
        Ok(match self.inbound.recv().await {
            Some(TestInbound::Dispatch(dispatch)) => PacketSessionUdpReadResult::Dispatch(dispatch),
            Some(TestInbound::End) | None => PacketSessionUdpReadResult::End,
        })
    }

    async fn write_response_for_target(
        &mut self,
        _target: &Address,
        _port: u16,
        payload: &[u8],
    ) -> Result<usize, zero_core::Error> {
        let len = payload.len();
        self.responses
            .send(payload.to_vec())
            .map_err(|_| zero_core::Error::Protocol("test response receiver closed"))?;
        Ok(len)
    }
}

fn test_handler() -> (
    mpsc::UnboundedSender<TestInbound>,
    mpsc::UnboundedReceiver<Vec<u8>>,
    TestHandler,
) {
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let (response_tx, response_rx) = mpsc::unbounded_channel();
    (
        inbound_tx,
        response_rx,
        TestHandler {
            inbound: inbound_rx,
            responses: response_tx,
        },
    )
}

fn inbound_packet(port: u16, payload: &[u8]) -> InboundUdpDispatch {
    InboundUdpDispatch::new(
        ProtocolType::new("test-mux"),
        Address::Ipv4(Ipv4Addr::LOCALHOST.octets()),
        port,
        payload.to_vec(),
        None,
    )
}

#[tokio::test]
async fn preserved_dispatch_reuses_the_same_udp_flow_after_transport_reattach() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let dispatch = runtime
        .new_dispatch("mux-in")
        .await
        .expect("create UDP dispatch");

    let echo = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind UDP echo");
    let echo_port = echo.local_addr().expect("echo address").port();
    let echo_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        for _ in 0..2 {
            let (read, peer) = echo.recv_from(&mut buf).await.expect("receive UDP packet");
            echo.send_to(&buf[..read], peer)
                .await
                .expect("echo UDP packet");
        }
    });

    let (first_tx, mut first_responses, first_handler) = test_handler();
    let first_runtime = runtime.clone();
    let first_task = tokio::spawn(async move {
        run_packet_session_udp_relay_with_dispatch(
            first_runtime,
            PacketSessionUdpRelayRequest {
                handler: first_handler,
                inbound_tag: "mux-in",
                protocol: "test-mux",
                auth: None,
                failure_policy: PacketSessionUdpFailurePolicy::ReturnError,
            },
            dispatch,
        )
        .await
    });

    first_tx
        .send(TestInbound::Dispatch(inbound_packet(
            echo_port,
            b"before-detach",
        )))
        .expect("send first inbound packet");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), first_responses.recv())
            .await
            .expect("first response timed out")
            .expect("first response channel closed"),
        b"before-detach"
    );
    let first_session = engine
        .active_sessions()
        .into_iter()
        .next()
        .expect("first UDP flow is active");

    first_tx
        .send(TestInbound::End)
        .expect("detach first transport");
    let first_exit = first_task.await.expect("first relay task");
    assert!(matches!(
        first_exit.outcome,
        Ok(PacketSessionUdpLoopExit::InboundEnded)
    ));
    assert_eq!(engine.active_sessions().len(), 1);

    let (second_tx, mut second_responses, second_handler) = test_handler();
    let second_runtime = runtime.clone();
    let second_task = tokio::spawn(async move {
        run_packet_session_udp_relay_with_dispatch(
            second_runtime,
            PacketSessionUdpRelayRequest {
                handler: second_handler,
                inbound_tag: "mux-in",
                protocol: "test-mux",
                auth: None,
                failure_policy: PacketSessionUdpFailurePolicy::ReturnError,
            },
            first_exit.dispatch,
        )
        .await
    });

    second_tx
        .send(TestInbound::Dispatch(inbound_packet(
            echo_port,
            b"after-reattach",
        )))
        .expect("send second inbound packet");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second_responses.recv())
            .await
            .expect("second response timed out")
            .expect("second response channel closed"),
        b"after-reattach"
    );
    let active = engine.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, first_session.id);

    second_tx
        .send(TestInbound::End)
        .expect("finish second transport");
    let second_exit = second_task.await.expect("second relay task");
    let completed = second_exit.dispatch.finish_all();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].record.id, first_session.id);
    assert_eq!(
        completed[0].record.bytes_up,
        (b"before-detach".len() + b"after-reattach".len()) as u64
    );
    assert_eq!(
        completed[0].record.bytes_down,
        (b"before-detach".len() + b"after-reattach".len()) as u64
    );
    assert!(engine.active_sessions().is_empty());
    assert_eq!(engine.completed_sessions().len(), 1);

    tokio::time::timeout(Duration::from_secs(2), echo_task)
        .await
        .expect("echo task timed out")
        .expect("echo task failed");
}
