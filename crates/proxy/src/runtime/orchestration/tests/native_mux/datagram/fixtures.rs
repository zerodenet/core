use super::*;
pub(super) async fn open(
    proxy: &Proxy,
    id: u64,
    targets: DatagramTargets,
) -> Result<BoxDatagramSession, zero_platform_tokio::contract::ChannelError> {
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        53,
        Network::Udp,
        ProtocolType::new("native-mux-datagram-test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("a")
        .unwrap();
    let input = &proxy.engine().config().outbounds[0];
    let (server, port) = input.protocol.endpoint().unwrap();
    let endpoint = Destination::new(
        {
            assert_eq!(server, "127.0.0.1");
            Address::Ipv4([127, 0, 0, 1])
        },
        port,
    );
    crate::component::stream_bridge::datagram::open(
        &services.execution(),
        &slot,
        "a",
        endpoint,
        DatagramRequest {
            association: AssociationId(id),
            targets,
        },
    )
    .await
    .map(|opened| opened.channel)
}
pub(super) struct UdpEcho {
    address: std::net::SocketAddr,
    stop: zero_platform_tokio::contract::CancellationToken,
    worker: tokio::task::JoinHandle<()>,
}
impl UdpEcho {
    pub(super) async fn start() -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let stop = zero_platform_tokio::contract::CancellationToken::new();
        let cancel = stop.clone();
        let worker = tokio::spawn(async move {
            let mut buffer = vec![0; 65535];
            loop {
                tokio::select! {biased;
                    _=cancel.cancelled()=>break,
                    packet=socket.recv_from(&mut buffer)=>{let (size,source)=packet.unwrap();socket.send_to(&buffer[..size],source).await.unwrap();}
                }
            }
        });
        Self {
            address,
            stop,
            worker,
        }
    }
    pub(super) fn target(&self) -> Destination {
        Destination::new(Address::Ipv4([127, 0, 0, 1]), self.address.port())
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.worker.await.unwrap();
    }
}
pub(super) async fn send_packet(
    channel: &mut BoxDatagramSession,
    id: u64,
    target: Destination,
    payload: &[u8],
) {
    let frame = Datagram {
        association: AssociationId(id),
        address: target,
        payload: payload.to_vec().into(),
    };
    let receipt = send(
        channel.tx.as_mut(),
        channel.control.as_mut(),
        &frame,
        &channel.limits,
    )
    .await
    .unwrap();
    assert_eq!(receipt.accepted_bytes, payload.len());
    assert_eq!(receipt.delivery, DeliveryState::LocallyAccepted);
}
pub(super) async fn receive(
    channel: &mut BoxDatagramSession,
    id: u64,
    target: Destination,
    payload: &[u8],
) {
    let frame = timeout(Duration::from_secs(3), recv(channel.rx.as_mut()))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(frame.association, AssociationId(id));
    assert_eq!(frame.address, target);
    assert_eq!(frame.payload.as_ref(), payload);
}
pub(super) async fn close(channel: BoxDatagramSession) {
    let zero_platform_tokio::contract::datagram::DatagramSession {
        tx,
        rx,
        mut control,
        ..
    } = channel;
    drop(tx);
    drop(rx);
    let report = control.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty() && report.exited_tasks.is_empty(),
        "logical completion must not impersonate original native or driver evidence"
    );
}
