use super::*;
use crate::runtime::udp_flow::{
    managed::{
        cache::ManagedUdpConnectionCache,
        connection::{
            managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
            SharedManagedUdpConnection,
        },
    },
    outbound::UdpFlowOutbound,
    registered::{state::RegisteredUdpConnection, RegisteredUdpState},
    sessions::UdpFlowKey,
    snapshot::UdpFlowSnapshot,
};
use tokio::sync::{broadcast, mpsc};
use zero_core::{Network, Session};
use zero_engine::EngineError;
type Packet = (Address, u16, Vec<u8>);
struct Wire {
    sends: mpsc::Sender<Packet>,
    replies: broadcast::Sender<Packet>,
}
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for Wire {
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
        self.replies.subscribe()
    }
    fn closed_message(&self) -> &'static str {
        "wire ended"
    }
}
fn wire() -> (
    SharedManagedUdpConnection,
    mpsc::Receiver<Packet>,
    broadcast::Sender<Packet>,
) {
    let (sends, rx) = mpsc::channel(4);
    let (replies, _) = broadcast::channel(4);
    (
        managed_tuple_udp_connection_from_flow(Wire {
            sends,
            replies: replies.clone(),
        }),
        rx,
        replies,
    )
}
#[tokio::test]
async fn merged_connection_cache_keeps_distinct_typed_wire_owners_with_equal_private_keys() {
    let (first, mut a, _) = wire();
    let (second, mut b, _) = wire();
    let mut cache = ManagedUdpConnectionCache::default();
    cache.insert::<u8>("same opaque key".into(), first.clone());
    cache.insert::<u16>("same opaque key".into(), second.clone());
    let selected = cache.get::<u8>("same opaque key").unwrap();
    assert!(std::sync::Arc::ptr_eq(&selected, &first));
    selected
        .send(&Address::Ipv4([192, 0, 2, 1]), 53, b"first")
        .await
        .unwrap();
    assert_eq!(a.recv().await.unwrap().2, b"first");
    assert!(b.try_recv().is_err());
    let selected = cache.get::<u16>("same opaque key").unwrap();
    assert!(std::sync::Arc::ptr_eq(&selected, &second));
    selected
        .send(&Address::Ipv4([192, 0, 2, 1]), 53, b"second")
        .await
        .unwrap();
    assert_eq!(b.recv().await.unwrap().2, b"second");
    assert!(cache.get::<u32>("same opaque key").is_none());
}
#[tokio::test]
async fn registered_actual_connection_forwards_without_recipe_and_release_revokes_the_original_reference(
) {
    let (connection, mut sent, replies) = wire();
    let mut state = RegisteredUdpState::new();
    let id = state.register_connection(
        Some(91),
        RegisteredUdpConnection::Tuple {
            connection,
            stage: "wire_send",
        },
    );
    let target = Address::Domain("target.test".into());
    let flow = UdpFlowSnapshot {
        key: UdpFlowKey::new(&target, 53, None),
        session: Session::new(91, target.clone(), 53, Network::Udp, ProtocolType::UNKNOWN),
        outbound: UdpFlowOutbound::StreamPacket {
            tag: "wire".into(),
            server: "upstream.test".into(),
            port: 443,
            managed: id,
            remote: None,
        },
        passive_relay_selections: vec![],
        rate_limiters: Default::default(),
    };
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    assert_eq!(
        state
            .forward_existing_managed_flow(
                &mut tasks,
                proxy.udp_runtime_services(),
                (&flow, b"data")
            )
            .await
            .map_err(|failure| failure.error)
            .unwrap(),
        4
    );
    assert_eq!(
        sent.recv().await.unwrap(),
        (target.clone(), 53, b"data".to_vec())
    );
    replies.send((target, 53, b"reply".to_vec())).unwrap();
    assert_eq!(
        tasks.join_next().await.unwrap().unwrap().unwrap().2.as_ref(),
        b"reply"
    );
    state.release_session(91);
    let failure = state
        .forward_existing_managed_flow(&mut tasks, proxy.udp_runtime_services(), (&flow, b"late"))
        .await
        .unwrap_err();
    assert_eq!(failure.stage, "udp_protocol_forward");
    assert!(sent.try_recv().is_err());
    assert!(tasks.is_empty());
}

#[derive(Debug, Clone)]
struct OccupiedPlan {
    address: std::net::SocketAddr,
    owner: std::sync::Arc<()>,
}
#[async_trait::async_trait]
impl super::super::datagram_manager::ManagedDatagramResumeConnector for OccupiedPlan {
    type Connection = Wire;
    const ESTABLISH_STAGE: &'static str = "typed_bind";
    fn connector_flow(
        &self,
        _: crate::runtime::path::OutboundEndpoint,
        _: u64,
    ) -> super::super::datagram_manager::ManagedDatagramConnectorFlow {
        super::super::datagram_manager::ManagedDatagramConnectorFlow::new("occupied".into())
    }
    async fn open_connection(
        self,
        _: crate::protocol_registry::PacketPathExecutionServices,
        _: crate::runtime::path::OutboundEndpoint,
        _: crate::runtime::udp_flow::packet_path::UdpPacketRef<'_>,
    ) -> Result<Wire, EngineError> {
        let _owner = self.owner;
        // The listener remains bound in the test until every attempted open
        // returns the original OS error; no successful resource is fabricated.
        let error = tokio::net::UdpSocket::bind(self.address).await.unwrap_err();
        Err(EngineError::Io(error))
    }
}
#[tokio::test]
async fn unsuccessful_typed_open_keeps_os_error_and_does_not_accumulate_cached_plan_owners() {
    let occupied = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let owner = std::sync::Arc::new(());
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
        .new_dispatch("incoming")
        .await
        .unwrap();
    let session = Session::new(
        91,
        Address::Ipv4([192, 0, 2, 1]),
        53,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );
    for _ in 0..3 {
        let plan = crate::runtime::udp_dispatch::operation::ManagedDatagramStartPlan::from_parts((
            "node".into(),
            "127.0.0.1".into(),
            address.port(),
            OccupiedPlan {
                address,
                owner: owner.clone(),
            },
        ));
        let failure = dispatch
            .start_transport_managed_datagram(
                Some(proxy.udp_runtime_services()),
                &session,
                b"packet",
                plan,
            )
            .await
            .err()
            .expect("the held OS socket must reject this bind");
        assert_eq!(failure.stage, "typed_bind");
        let EngineError::Io(error) = failure.error else {
            panic!("preserve original bind error")
        };
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(
            std::sync::Arc::strong_count(&owner),
            1,
            "a failed cold open cannot pin another recipe"
        );
    }
    dispatch.finish_all();
    drop(occupied);
}
