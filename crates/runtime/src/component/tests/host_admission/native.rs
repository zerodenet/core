//! Actual standard Direct grants and native peers for catalog admission tests.
use super::*;
use crate::component::instance::ComponentSlot;
use std::{
    io,
    pin::Pin,
    sync::{atomic::AtomicUsize, Mutex, Weak},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, ControlledStreamRequest, UpstreamOpener},
        BoxFuture,
    },
    controlled_carrier::{CarrierBusinessDemand, CarrierIo, CarrierUserView, NativeCarrierOpener},
    EgressInterfaceControl,
};

pub(super) struct Root {
    pub(super) calls: AtomicUsize,
    pub(super) egress: Arc<EgressInterfaceControl>,
    listener: TcpListener,
    slots: Mutex<Vec<Weak<ComponentSlot>>>,
    peers: Mutex<Vec<TcpStream>>,
}
impl Default for Root {
    fn default() -> Self {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        Self {
            calls: AtomicUsize::new(0),
            egress: Arc::default(),
            listener: TcpListener::from_std(socket).unwrap(),
            slots: Mutex::default(),
            peers: Mutex::default(),
        }
    }
}
impl Root {
    pub(super) fn track(&self, slot: &Arc<ComponentSlot>) {
        self.slots.lock().unwrap().push(Arc::downgrade(slot));
    }
    pub(super) fn destination(&self) -> Destination {
        Destination::new(
            Address::Ipv4([127, 0, 0, 1]),
            self.listener.local_addr().unwrap().port(),
        )
    }
    pub(super) async fn confirm_peers_closed(&self) {
        let peers = std::mem::take(&mut *self.peers.lock().unwrap());
        for mut peer in peers {
            assert_eq!(
                tokio::time::timeout_at(deadline(), peer.read_u8())
                    .await
                    .unwrap()
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
    }
}
struct Demand {
    slot: Weak<ComponentSlot>,
    identity: RequestIdentity,
    call: CarrierContext,
}
impl CarrierBusinessDemand for Demand {
    fn check(&self) -> Result<(), ChannelError> {
        self.slot
            .upgrade()
            .ok_or_else(|| {
                crate::component::instance::invalid(
                    Reason::Revoked,
                    "original fixture owner released",
                )
            })?
            .check_business_carrier(&self.identity)
    }
    fn session(&self) -> SessionId {
        self.call.session.unwrap()
    }
    fn cancellation(&self) -> CancellationToken {
        self.call.cancel.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        self.call.deadline
    }
}
struct Io {
    io: CarrierIo,
    _view: CarrierUserView,
    _demand: Option<Arc<Demand>>,
}
impl AsyncRead for Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, b)
    }
}
impl AsyncWrite for Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
impl UpstreamOpener for Root {
    fn supports_controlled_stream(&self) -> bool {
        true
    }
    fn open_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let slot = self
                .slots
                .lock()
                .unwrap()
                .iter()
                .filter_map(Weak::upgrade)
                .find(|slot| slot.instance_id() == request.grants.instance())
                .expect("same actual installed Direct owner");
            assert_eq!(request.destination, self.destination());
            let mut grant = request.grants.reserve()?;
            let (carrier, view, demand) = if request.context.session.is_some() {
                let view = grant.claim_transferred_view()?;
                let demand = Arc::new(Demand {
                    slot: Arc::downgrade(&slot),
                    identity: identity(true),
                    call: request.context.clone(),
                });
                let ticket = slot.issue_business_carrier(
                    &request.destination,
                    &binding,
                    &grant,
                    &demand.identity,
                    demand.clone(),
                    None,
                )?;
                let native = NativeCarrierOpener::business(ticket, self.egress.clone(), None)?;
                let carrier = native
                    .acquire_controlled_stream(
                        &binding,
                        request.destination.clone(),
                        request.context.clone(),
                        grant,
                    )
                    .await?;
                (carrier, view, Some(demand))
            } else {
                slot.check_direct_background_owner(
                    &binding.dial,
                    &request.grants.registration(),
                    &request.tasks.scope_identity(),
                )?;
                let (view, preparation) = grant.claim_preparing_transferred_view()?;
                let original = grant.adoption();
                let (start, started) = tokio::sync::oneshot::channel();
                let (send, receive) = tokio::sync::oneshot::channel();
                let egress = self.egress.clone();
                let call = request.context.clone();
                let destination = request.destination.clone();
                let actual_binding = binding.clone();
                let (_, witness) = original.spawn_auxiliary_weak(&request.tasks, async move {
                    let caller = started
                        .await
                        .expect("actual initializer registration installed");
                    let result = async {
                        let ticket = slot.issue_direct_background_carrier(
                            caller,
                            &destination,
                            &actual_binding,
                            &grant,
                            &call,
                        )?;
                        NativeCarrierOpener::service(ticket, egress, None)?
                            .acquire_controlled_stream(&actual_binding, destination, call, grant)
                            .await
                    }
                    .await;
                    match result {
                        Ok(io) => {
                            let _ = send.send(Ok(io));
                            Ok(())
                        }
                        Err(error) => {
                            let source = Arc::new(error);
                            let _ = send.send(Err(source.clone()));
                            Err(ChannelError::retained(source))
                        }
                    }
                })?;
                assert!(start.send(witness.clone()).is_ok());
                let carrier = receive.await.unwrap().map_err(ChannelError::retained)?;
                let joined = original.accept_auxiliary().await?.unwrap();
                assert!(witness.matches_joined(&joined));
                assert!(joined.result().is_ok());
                let released = witness.wait_registration_released().await;
                assert!(witness.matches_joined(&released) && released.registration_released());
                preparation.publish_transferred()?;
                (carrier, view, None)
            };
            let (peer, _) = self.listener.accept().await.unwrap();
            self.peers.lock().unwrap().push(peer);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(Io {
                io: carrier,
                _view: view,
                _demand: demand,
            }) as BoxStream)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        panic!("native Direct must never fall back to the legacy fixture opener")
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("Stream admission fixture cannot acquire Datagram IO")
    }
}
