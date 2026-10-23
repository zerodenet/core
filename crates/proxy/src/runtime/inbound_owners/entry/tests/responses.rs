//! The real caller exit, original response joins and native receipt stay distinct.
use super::*;
use crate::runtime::{inbound_owners::HostPeerBinding, udp_flow::response_join::ResponseSetOwner};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use zero_platform_tokio::{
    contract::lifecycle::{erase_lifecycle, ResourceLifecycle},
    TokioDatagramSocket,
};
#[path = "responses/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test]
async fn caller_join_and_cancelled_close_retain_the_original_response_set_and_peer_permit() {
    let live = Live::new();
    let (responses, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let (mut io, mut finish) = pair().await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let pending_io = Arc::new(Notify::new());
    let pending_response = Arc::new(Notify::new());
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (lease_tx, lease_rx) = tokio::sync::oneshot::channel();
    let actual = responses.clone();
    let response_entered = pending_response.clone();
    let peer_entered = pending_io.clone();
    assert!(live
        .group
        .spawn_bound(&live.tasks, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            binding
                .install_response_owner(&actual)
                .map_err(std::io::Error::other)?;
            binding
                .install_response_owner(&actual)
                .map_err(std::io::Error::other)?;
            binding_tx.send(binding).unwrap();
            lease.spawn(async move {
                let mut bytes = [0];
                let recv = socket.recv_from(&mut bytes);
                tokio::pin!(recv);
                std::future::poll_fn(|cx| {
                    let result = recv.as_mut().poll(cx);
                    if result.is_pending() {
                        response_entered.notify_one();
                    }
                    result
                })
                .await?;
                Ok((
                    zero_core::Address::Ipv4([127, 0, 0, 1]),
                    0,
                    Vec::new(),
                    None,
                ))
            });
            lease_tx
                .send(lease)
                .ok()
                .expect("retain original dispatcher lease");
            read_pending(&mut io, &peer_entered).await;
            Ok(())
        })
        .unwrap());
    let binding = binding_rx.await.unwrap();
    let held_lease = lease_rx.await.unwrap();
    timeout(Duration::from_secs(2), pending_io.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), pending_response.notified())
        .await
        .unwrap();
    finish.write_u8(1).await.unwrap();
    live.record_peer(&binding).await;
    live.group.begin_shutdown();
    assert!(binding.responses_pending());
    pending(live.group.retire_joined(deadline())).await;
    assert!(!responses.is_closed());
    assert!(!live.group.physically_closed());
    assert!(UdpSocket::bind(address).await.is_err());
    drop(held_lease); // Return the original Set, not a response join.
    assert!(!responses.is_closed());
    live.group.retire_joined(deadline()).await.unwrap();
    assert!(responses.is_closed());
    assert!(!binding.responses_pending());
    assert!(live.group.physically_closed());
    assert_eq!(
        responses.shutdown(deadline()).await.unwrap().joined_tasks(),
        1
    );
    assert_eq!(
        live.group.closed_io_receipts_count(),
        0,
        "response joins are not native receipts"
    );
    assert!(
        binding.install_response_owner(&responses).is_err(),
        "no late attachment after caller join"
    );
    live.owner.shutdown(deadline()).await.unwrap();
    let joined = binding.join_observer();
    let weak_responses = Arc::downgrade(&responses);
    drop(binding);
    drop(responses);
    assert!(
        weak_responses.upgrade().is_none(),
        "join observation cannot retain the response graph"
    );
    assert!(joined.observed_join().is_some());
}

struct Native {
    socket: Option<TokioDatagramSocket>,
}
impl ResourceLifecycle for Native {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if let Some(socket) = self.socket.take() {
            socket.close().map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source)
            })?;
            Ok(CloseReport {
                closed_io: vec!["same original UDP descriptor explicitly closed".into()],
                ..Default::default()
            })
        } else {
            Ok(CloseReport::default())
        }
    }
}
#[tokio::test]
async fn response_panic_keeps_original_source_after_real_response_joins_and_child_native_close() {
    let live = Live::new();
    let ingress = live
        .proxy
        .tcp_runtime_services()
        .for_inbound("response-peer".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (responses, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let (mut io, mut panic_now) = pair().await;
    let pending_response = Arc::new(Notify::new());
    let actual = responses.clone();
    let group = live.group.clone();
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (lease_tx, lease_rx) = tokio::sync::oneshot::channel();
    let entered = pending_response.clone();
    assert!(live
        .group
        .spawn_bound(&live.tasks, move |binding| {
            let context = group.resource_context(binding.clone(), ingress);
            async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                binding
                    .install_response_owner(&actual)
                    .map_err(std::io::Error::other)?;
                binding_tx.send(binding).unwrap();
                context
                    .claim(deadline(), |_| {
                        Ok((
                            erase_lifecycle(Native {
                                socket: Some(socket),
                            }),
                            (),
                        ))
                    })
                    .await
                    .map_err(std::io::Error::other)?;
                lease.spawn(async move {
                    read_pending(&mut io, &entered).await;
                    panic!("original response task panic after actual TCP Pending");
                });
                lease_tx
                    .send(lease)
                    .ok()
                    .expect("retain original Set lease");
                Ok(())
            }
        })
        .unwrap());
    let binding = binding_rx.await.unwrap();
    let mut held_lease = lease_rx.await.unwrap();
    timeout(Duration::from_secs(2), pending_response.notified())
        .await
        .unwrap();
    live.record_peer(&binding).await;
    assert!(UdpSocket::bind(address).await.is_err());
    panic_now.write_u8(1).await.unwrap();
    let original = held_lease.join_next_with_id().await.unwrap().unwrap_err();
    assert!(original.is_panic());
    assert!(Arc::ptr_eq(
        &original,
        &responses.first_fatal_failure().unwrap()
    ));
    drop(held_lease);
    let failure = live.group.retire_joined(deadline()).await.unwrap_err();
    let sticky = live.group.first_fault().unwrap();
    assert!(std::error::Error::source(&failure).is_some());
    let saved = std::error::Error::source(sticky.as_ref())
        .unwrap()
        .downcast_ref::<Arc<tokio::task::JoinError>>()
        .expect("same original response JoinError Arc");
    assert!(Arc::ptr_eq(saved, &original));
    assert!(responses.is_closed());
    assert!(!binding.responses_pending());
    assert_eq!(
        responses.shutdown(deadline()).await.unwrap().joined_tasks(),
        1
    );
    assert_eq!(live.group.closed_io_receipts_count(), 1);
    let _rebound = UdpSocket::bind(address).await.unwrap();
    assert!(
        live.owner.shutdown(deadline()).await.is_err(),
        "physical close never clears original panic"
    );
    assert!(Arc::ptr_eq(
        live.owner.failure.lock().unwrap().as_ref().unwrap(),
        &sticky
    ));
    assert!(Arc::ptr_eq(
        &original,
        &responses.first_fatal_failure().unwrap()
    ));
    assert!(live.owner.shutdown(deadline()).await.is_err());
    assert!(Arc::ptr_eq(
        live.owner.failure.lock().unwrap().as_ref().unwrap(),
        &sticky
    ));
}
