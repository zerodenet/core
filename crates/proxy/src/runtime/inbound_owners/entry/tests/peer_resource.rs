use super::*;
#[path = "peer_resource/failure.rs"]
mod failure;
#[cfg(feature = "raw-ip-runtime")]
mod live;
#[path = "peer_resource/recovery.rs"]
mod recovery;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{
    contract::lifecycle::{erase_lifecycle, ResourceLifecycle},
    TokioDatagramSocket,
};

struct SocketOwner {
    socket: Option<TokioDatagramSocket>,
    address: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
    gate: tokio::net::TcpStream,
    pending: Arc<Notify>,
    gate_done: bool,
    recovery: Option<tokio::net::TcpStream>,
    failed: Option<Arc<Notify>>,
}
impl ResourceLifecycle for SocketOwner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let socket = TokioDatagramSocket::bind("127.0.0.1:0")
            .await
            .map_err(io_error)?;
        let address = socket.local_addr().map_err(io_error)?;
        self.socket = Some(socket);
        self.address.take().unwrap().send(address).unwrap();
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if self.socket.is_none() {
            return Ok(CloseReport::default());
        }
        if !self.gate_done {
            let mut byte = [0];
            let read = self.gate.read_exact(&mut byte);
            tokio::pin!(read);
            let result = std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    self.pending.notify_one();
                }
                result
            })
            .await;
            self.gate_done = true;
            if let Err(error) = result {
                if let Some(failed) = &self.failed {
                    failed.notify_one();
                }
                return Err(io_error(error));
            }
        }
        if let Some(recovery) = self.recovery.as_mut() {
            recovery.read_u8().await.map_err(io_error)?;
            self.recovery.take();
        }
        // The actual platform syscall result supplies the native close receipt.
        self.socket.take().unwrap().close().map_err(io_error)?;
        Ok(CloseReport {
            closed_io: vec!["original fixture UDP socket explicitly closed".into()],
            ..CloseReport::default()
        })
    }
}
fn io_error(source: std::io::Error) -> ChannelError {
    ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source)
}

#[tokio::test]
async fn actual_peer_resource_survives_cancelled_cleanup_after_real_join_until_native_close() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("resource-peer".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "resource-peer",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut release_close = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (gate, _) = listener.accept().await.unwrap();
    let (address_tx, address_rx) = tokio::sync::oneshot::channel();
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (mut peer_io, mut finish_peer) = tokio::io::duplex(1);
    let pending = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let group = group.clone();
            let pending = pending.clone();
            async move {
                let for_claim = group.clone();
                assert!(group.spawn_bound(&host, move |binding| {
                    binding_tx.send(binding.clone()).unwrap();
                    let context = for_claim.resource_context(binding, ingress);
                    async move {
                        let identity = context
                            .claim(Instant::now() + Duration::from_secs(2), |identity| {
                                assert_ne!(identity.task.0, 0);
                                assert_ne!(identity.association.0, 0);
                                assert_eq!(context.ingress(), ingress);
                                Ok((
                                    erase_lifecycle(SocketOwner {
                                        socket: None,
                                        address: Some(address_tx),
                                        gate,
                                        pending,
                                        gate_done: false,
                                        recovery: None,
                                        failed: None,
                                    }),
                                    identity,
                                ))
                            })
                            .await
                            .map_err(std::io::Error::other)?;
                        assert_eq!(
                            identity.task,
                            context
                                .binding()
                                .wait_bound()
                                .await
                                .map_err(std::io::Error::other)?
                        );
                        peer_io.read_u8().await?;
                        Ok(())
                    }
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                group.wait_closed().await
            }
        })
        .unwrap();
    let address = address_rx.await.unwrap();
    let binding = binding_rx.await.unwrap();
    let task = binding.wait_bound().await.unwrap();
    assert!(UdpSocket::bind(address).await.is_err());
    finish_peer.write_u8(1).await.unwrap();
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.id, task);
    owner.record(exit);
    let mut cleanup = Box::pin(group.retire_joined(Instant::now() + Duration::from_secs(2)));
    tokio::select! {
        result = &mut cleanup => panic!("real TCP cleanup gate must remain pending: {result:?}"),
        _ = pending.notified() => {}
    }
    drop(cleanup);
    assert!(UdpSocket::bind(address).await.is_err());
    release_close.write_u8(1).await.unwrap();
    group
        .retire_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let _rebound = UdpSocket::bind(address).await.unwrap();
    owner.stop();
    timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
