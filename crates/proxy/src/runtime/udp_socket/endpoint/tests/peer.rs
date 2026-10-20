//! Real peer registration + native grant, without fabricated host tickets.
use super::*;
use crate::runtime::udp_socket::endpoint::{dispatch::DirectDispatchEndpoint, peer::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use zero_platform_tokio::contract::lifecycle::erase_lifecycle;

#[tokio::test]
async fn actual_peer_driver_ready_keeps_original_until_real_registered_peer_join() {
    let fixture = Fixture::new().await;
    let tag = "direct-native-peer";
    let services = fixture.proxy.tcp_runtime_services().for_inbound(tag.into());
    let ingress = services.execution().ingress.as_ref().unwrap().role;
    let egress = services.execution().upstream().egress_control();
    let network = crate::protocol_registry::UdpRuntimeServices::new(services).network();
    let policy = super::super::super::tests::policy(&fixture.proxy, Some("first"));
    let mut business = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        1,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );
    fixture
        .proxy
        .engine()
        .prepare_session(&mut business, tag)
        .unwrap();
    let session = SessionId(business.id);
    let retirement = fixture.retirement.clone();
    let (stop, mut stopping) = watch::channel(false);
    let installed = fixture
        .proxy
        .inbound_owners
        .install(
            tag,
            &fixture.proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let host_owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let (bound_tx, bound_rx) = oneshot::channel();
    let (read_tx, read_rx) = oneshot::channel();
    let (mut wait_exit, mut release_exit) = tokio::io::duplex(1);
    let pending_exit = Arc::new(Notify::new());
    host_owner
        .spawn_driver({
            let group = group.clone();
            let pending_exit = pending_exit.clone();
            async move {
                let for_claim = group.clone();
                assert!(group.spawn_bound(&host, move |binding| {
                    assert!(bound_tx.send(binding.clone()).is_ok());
                    let context = for_claim.resource_context(binding, ingress);
                    async move {
                        let binding = context.binding();
                        let (slot, job) = DirectPeerDriverSlot::channel();
                        let (mut bridge, driver) = context
                            .claim_outbound_datagram(deadline(), target, egress, |grant| {
                                let one = NonZeroUsize::new(1).unwrap();
                                let (owner, driver) = DirectEndpointOwner::for_peer(
                                    DirectPeerOptions {
                                        identity: DirectEndpointIdentity {
                                            resource: grant.identity.instance,
                                            ingress: context.ingress(),
                                            dispatcher: grant.identity.association,
                                        },
                                        task: grant.identity.task,
                                        target,
                                        network_generation: grant.network_generation,
                                        preferred_port: None,
                                        queue: UdpReceiveQueueConfig::default(),
                                        limits: DirectEndpointLimits {
                                            logical: one,
                                            native_sockets: one,
                                            native_open_attempts: one,
                                            candidates_per_job: one,
                                            pending_bytes: NonZeroUsize::new(65_600).unwrap(),
                                        },
                                        resources: grant.resources,
                                    },
                                    network,
                                    binding,
                                    None,
                                )?;
                                let bridge =
                                    DirectDispatchEndpoint::new(owner.endpoint(), retirement);
                                Ok((erase_lifecycle(owner), (bridge, driver)))
                            })
                            .await
                            .map_err(std::io::Error::other)?;
                        slot.submit(driver).map_err(std::io::Error::other)?;
                        run_peer_with_driver(
                            async move {
                                bridge
                                    .admit(session, policy, None, CancellationToken::new())
                                    .map_err(std::io::Error::other)?;
                                let sent = bridge
                                    .send_new_packet(
                                        session,
                                        &Address::Ipv4([127, 0, 0, 1]),
                                        &[target],
                                        b"actual grant",
                                    )
                                    .await?;
                                assert_eq!(sent.sent, 12);
                                let mut bytes = [0; 32];
                                let (size, source) = bridge.recv_from_addr(&mut bytes).await?;
                                assert_eq!(&bytes[..size], b"actual reply");
                                assert!(source.is_current());
                                assert!(read_tx.send(source.clone()).is_ok());
                                assert!(bridge
                                    .finish_session(session, deadline())
                                    .await
                                    .map_err(std::io::Error::other)?
                                    .is_complete());
                                assert!(!source.is_current());
                                Ok(())
                            },
                            job,
                        )
                        .await?;
                        // The I/O driver really completed, but this same registered
                        // peer remains pending in native I/O. Ready is no TaskExit.
                        let read = wait_exit.read_u8();
                        tokio::pin!(read);
                        std::future::poll_fn(|cx| {
                            let result = read.as_mut().poll(cx);
                            if result.is_pending() {
                                pending_exit.notify_one();
                            }
                            result
                        })
                        .await?;
                        Ok(())
                    }
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                group.wait_closed().await
            }
        })
        .unwrap();
    let binding = bound_rx.await.unwrap();
    let task = binding.wait_bound().await.unwrap();
    let mut bytes = [0; 32];
    let (size, original) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"actual grant");
    peer.send_to(b"actual reply", original).await.unwrap();
    let source = read_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pending_exit.notified())
        .await
        .unwrap();
    assert!(!source.is_current());
    assert_eq!(
        binding
            .wait_joined(Instant::now() + Duration::from_millis(1))
            .await
            .unwrap_err()
            .reason,
        Reason::Timeout
    );
    group.retire_joined(deadline()).await.unwrap();
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    release_exit.write_u8(1).await.unwrap();
    let observing = host_owner.observe_driver();
    tokio::pin!(observing);
    let joined = tokio::select! {
        joined = binding.wait_joined(deadline()) => joined.unwrap(),
        result = &mut observing => panic!("listener must still run while actual peer joins: {result:?}"),
    };
    assert_eq!(joined.task(), task);
    assert!(joined.failure().is_none());
    // Native close is driven only after the genuine dispatched peer receipt.
    group.retire_joined(deadline()).await.unwrap();
    tokio::net::UdpSocket::bind(original).await.unwrap();
    host_owner.stop();
    tokio::time::timeout(Duration::from_secs(2), &mut observing)
        .await
        .unwrap()
        .unwrap();
    host_owner.shutdown(deadline()).await.unwrap();
}
