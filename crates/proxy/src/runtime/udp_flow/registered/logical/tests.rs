use super::*;
use crate::runtime::udp_flow::{
    outbound::UdpFlowOutbound, sessions::UdpFlowKey, snapshot::UdpFlowSnapshot,
};
use tokio::sync::mpsc;
use zero_core::{Network, ProtocolType};

struct SessionFlow {
    sends: mpsc::Sender<Packet>,
    responses: broadcast::Sender<Packet>,
}
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for SessionFlow {
    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.sends
            .send((target.clone(), port, payload.to_vec()))
            .await
            .map_err(std::io::Error::other)?;
        Ok(payload.len())
    }
    fn subscribe_responses(&self) -> broadcast::Receiver<Packet> {
        self.responses.subscribe()
    }
    fn closed_message(&self) -> &'static str {
        "test session closed"
    }
}
fn connection() -> (
    LogicalConnection,
    mpsc::Receiver<Packet>,
    broadcast::Sender<Packet>,
) {
    let (sends, sent) = mpsc::channel(8);
    let (responses, _) = broadcast::channel(8);
    let connection = LogicalConnection::from_flow(SessionFlow {
        sends,
        responses: responses.clone(),
    });
    (connection, sent, responses)
}

fn state() -> RegisteredUdpState {
    RegisteredUdpState::new()
}
fn flow(id: u64, managed: ManagedUdpFlowRef, target: Address, port: u16) -> UdpFlowSnapshot {
    UdpFlowSnapshot {
        key: UdpFlowKey::new(&target, port, None),
        session: Session::new(id, target, port, Network::Udp, ProtocolType::UNKNOWN),
        outbound: UdpFlowOutbound::LogicalStreamPacket {
            tag: "standard".into(),
            managed,
        },
        passive_relay_selections: vec![],
        rate_limiters: Default::default(),
    }
}
fn proxy() -> crate::Proxy {
    crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn standard_session_sends_multiple_targets_and_empty_packets_without_legacy_resume_lookup() {
    let mut state = state();
    let (connection, mut sends, _) = connection();
    let reference = state.register_logical(7, connection);
    let proxy = proxy();
    let mut tasks = JoinSet::new();
    for (target, port, payload) in [
        (Address::Domain("first.test".into()), 53, b"one".as_slice()),
        (Address::Ipv4([203, 0, 113, 7]), 443, &[]),
    ] {
        let flow = flow(7, reference, target.clone(), port);
        assert_eq!(
            state
                .forward_existing_managed_flow(
                    &mut tasks,
                    proxy.udp_runtime_services(),
                    (&flow, payload),
                )
                .await
                .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error)),
            payload.len()
        );
        assert_eq!(
            sends.recv().await.unwrap(),
            (target, port, payload.to_vec())
        );
    }
    assert!(tasks.is_empty(), "a send does not start a response worker");
    state.release_session(7);
    assert!(sends.recv().await.is_none());
}

#[tokio::test]
async fn replacing_or_releasing_a_session_rejects_old_references_without_legacy_fallback() {
    let mut state = state();
    let (first, mut first_sends, _) = connection();
    let old = state.register_logical(7, first);
    let (second, mut second_sends, _) = connection();
    let current = state.register_logical(7, second);
    assert_ne!(old, current);
    // Logical release drops this fixture's sender. This is not a native close receipt.
    assert!(first_sends.recv().await.is_none());
    let proxy = proxy();
    let mut tasks = JoinSet::new();
    let old = flow(7, old, Address::Domain("old.test".into()), 53);
    let result = state
        .forward_existing_managed_flow(&mut tasks, proxy.udp_runtime_services(), (&old, b"old"))
        .await;
    assert_eq!(result.unwrap_err().stage, "udp_protocol_forward");
    let current = flow(7, current, Address::Domain("new.test".into()), 53);
    state
        .forward_existing_managed_flow(&mut tasks, proxy.udp_runtime_services(), (&current, b"new"))
        .await
        .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
    assert_eq!(second_sends.recv().await.unwrap().2, b"new");
    state.release_session(7);
    assert!(second_sends.recv().await.is_none());
    let result = state
        .forward_existing_managed_flow(
            &mut tasks,
            proxy.udp_runtime_services(),
            (&current, b"late"),
        )
        .await;
    assert_eq!(result.unwrap_err().stage, "udp_protocol_forward");
}

#[tokio::test]
async fn typed_sessions_keep_one_response_reader_and_rearm_only_after_actual_join() {
    let mut state = state();
    let (connection, _, responses) = connection();
    let inspection = connection.clone();
    state.register_logical(7, connection);
    let owner = super::super::super::response_join::ResponseTasks::default();
    let mut tasks = JoinSet::new();
    for payload in [b"unsolicited".as_slice(), &[]] {
        state.poll_logical_responses(&mut tasks, &owner);
        state.poll_logical_responses(&mut tasks, &owner);
        assert_eq!(tasks.len(), 1);
        responses
            .send((
                Address::Domain("return.test".into()),
                5353,
                payload.to_vec(),
            ))
            .unwrap();
        // Observe consumption without taking the JoinSet result. An empty
        // original receiver after acquiring its lock means the read returned.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let ConnectionIo::Tuple { receiver, .. } = &inspection.0.io else {
                    panic!("tuple fixture");
                };
                if receiver.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(inspection.0.reader.joined().is_none());
        state.poll_logical_responses(&mut tasks, &owner);
        assert_eq!(
            tasks.len(),
            1,
            "body readiness cannot start a second reader"
        );
        let joined =
            tokio::time::timeout(std::time::Duration::from_secs(2), tasks.join_next_with_id())
                .await
                .unwrap()
                .unwrap();
        let packet = owner.observe(joined).unwrap().unwrap();
        assert!(inspection.0.reader.joined().is_some());
        assert_eq!(
            packet,
            (
                Address::Domain("return.test".into()),
                5353,
                payload.to_vec().into(),
                Some(7)
            )
        );
    }
    state.release_session(7);
    state.poll_logical_responses(&mut tasks, &owner);
    assert!(tasks.is_empty());
}
