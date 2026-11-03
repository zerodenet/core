//! Real originals and original Scope joins survive standard source erasure.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    path::PathBuf,
    pin::Pin,
    sync::Mutex,
    task::Poll,
};
use zero_core::{request::Destination, Address, ProtocolType};
use zero_platform_tokio::contract::{
    component::{AcceptedTcpLimits, GrantedDatagramListener, ResourceAccess, ResourceDeclaration},
    context::InboundConnectionFacts,
    lifecycle::{BuildError, ResourceLifecycle, TaskClass},
    packet::BoxPacketSession,
    scope::{SpawnError, TaskScope},
    BoxFuture, CloseReason, Impact, Reason, Stage,
};
use zero_platform_tokio::{ControlledTcpListener, ControlledTcpListenerOwner, TokioSocket};
#[path = "native/addresses.rs"]
mod addresses;
#[path = "native/datagram.rs"]
mod datagram;
#[path = "native/logical.rs"]
mod logical;
#[path = "native/udp_source.rs"]
mod udp_source;

struct Resources {
    address: Destination,
    listener: Mutex<Option<ControlledTcpListener>>,
    calls: AtomicUsize,
}
impl ResourceAccess for Resources {
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, BuildError>> {
        Box::pin(async move {
            assert_eq!(address, self.address);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .listener
                .lock()
                .unwrap()
                .take()
                .expect("one actual accept view"))
        })
    }
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("undeclared file")
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, BuildError>> {
        panic!("undeclared UDP")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("undeclared device")
    }
}
struct SocketHandshake {
    socket: TokioSocket,
    started: Arc<AtomicUsize>,
    pending: Arc<Notify>,
}
impl InboundHandshake for SocketHandshake {
    async fn complete(
        mut self,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let mut byte = [0];
        let mut read = tokio::io::ReadBuf::new(&mut byte);
        poll_fn(|cx| {
            let result = tokio::io::AsyncRead::poll_read(Pin::new(&mut self.socket), cx, &mut read);
            if result.is_pending() {
                self.pending.notify_one();
            }
            result
        })
        .await
        .map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(source)
        })?;
        let _facts = InboundConnectionFacts::from_stream(&self.socket).request(
            &context,
            ProtocolType::new("source-test"),
            None,
            None,
        )?;
        panic!("cancellation must occur at the original pending read")
    }
}

#[tokio::test]
async fn standard_source_preserves_native_entry_primary_task_and_original_close_receipts() {
    timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let address = Destination::new(Address::Ipv4([127, 0, 0, 1]), 0);
        let declaration = ResourceDeclaration::TcpListener {
            destination: address.clone(),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: one,
                max_resources_per_socket: 0,
            }),
            max_bind_attempts: one,
        };
        let cancel = CancellationToken::new();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), one);
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one,
            cancel.clone(),
            one,
            one,
            &scope.registrar(),
        )
        .unwrap();
        let view = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let endpoint = view.local_addr().unwrap();
        let resources = Arc::new(Resources {
            address: address.clone(),
            listener: Mutex::new(Some(view)),
            calls: AtomicUsize::new(0),
        });
        let started = Arc::new(AtomicUsize::new(0));
        let pending_read = Arc::new(Notify::new());
        let (mut lifecycle, source) = tcp::source(address, resources.clone(), cancel, {
            let started = started.clone();
            let pending = pending_read.clone();
            move |socket| SocketHandshake {
                socket,
                started: started.clone(),
                pending: pending.clone(),
            }
        });
        assert_eq!(
            resources.calls.load(Ordering::SeqCst),
            0,
            "pure construction"
        );
        lifecycle
            .start(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(resources.calls.load(Ordering::SeqCst), 1);
        let mut source = erase_inbound(source);
        // Observe a real pending native accept, then cancel that wait. It must
        // leave the same unique accept view ready for the next operation.
        let wait_cancel = CancellationToken::new();
        let mut waiting = source.next(AcceptContext {
            deadline: None,
            cancel: wait_cancel.clone(),
        });
        poll_fn(|cx| match waiting.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("accept must actually be pending"),
        })
        .await;
        wait_cancel.cancel();
        match waiting.await {
            Err(error) => assert_eq!(error.reason, Reason::Cancelled),
            Ok(_) => panic!("cancelled accept must return its actual cancellation"),
        }
        let client = tokio::net::TcpStream::connect(endpoint).await.unwrap();
        match source
            .next(AcceptContext {
                deadline: Some(Instant::now()),
                cancel: CancellationToken::new(),
            })
            .await
        {
            Err(error) => assert_eq!(error.reason, Reason::Timeout),
            Ok(_) => panic!("expired accept cannot consume an already queued original"),
        }
        assert_eq!(started.load(Ordering::SeqCst), 0);
        let pending = source
            .next(AcceptContext {
                deadline: None,
                cancel: CancellationToken::new(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pending.connection_facts().unwrap().effective_source,
            Some(client.local_addr().unwrap())
        );
        assert_eq!(
            pending.connection_facts().unwrap().local_endpoint,
            Some(endpoint)
        );
        assert_eq!(
            started.load(Ordering::SeqCst),
            0,
            "accept must not read the protocol"
        );
        let (claim, receiver) = pending.accepted_tcp().expect("original survives erasure");
        let handshake_cancel = CancellationToken::new();
        let (finished, exited) = tokio::sync::oneshot::channel();
        let (id, witness) = pending
            .spawn(TaskClass::Auxiliary, {
                let cancel = handshake_cancel.clone();
                move |pending, tasks| async move {
                    tasks.caller().unwrap().check_open().unwrap();
                    let error = match pending
                        .complete(HandshakeContext {
                            accepted: None,
                            connection: None,
                            inbound: support::inbound(),
                            identities: Arc::new(Identity),
                            deadline: Instant::now() + Duration::from_secs(2),
                            cancel,
                        })
                        .await
                    {
                        Err(error) => error,
                        Ok(_) => panic!("expected cancelled pending read"),
                    };
                    assert_eq!(error.reason, Reason::Cancelled);
                    let _ = finished.send(());
                    Ok(())
                }
            })
            .unwrap();
        pending_read.notified().await;
        assert!(!witness.is_joined());
        handshake_cancel.cancel();
        exited.await.unwrap();
        assert!(!witness.is_joined(), "body completion is no Scope join");
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        receiver
            .observe_joined(id)
            .expect("consume actual original Scope receipt");
        assert!(witness.is_joined());
        let receipt = receiver
            .close(&claim, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.matches(&claim));
        lifecycle.revoke(CloseReason::Shutdown);
        lifecycle
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let report = original
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert!(
            !report.closed_io.is_empty(),
            "actual original native close receipt"
        );
        assert!(original.accepted_evidence().unwrap().provider_confirmed());
        drop(client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn taskless_factory_view_cannot_borrow_host_credit_even_through_weak_clones() {
    let one = NonZeroUsize::new(1).unwrap();
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), one);
    let host = scope.registrar();
    let denied = host.without_tasks();
    assert!(denied.scope_identity().matches(&host));
    assert!(matches!(
        denied
            .clone()
            .spawn(TaskClass::Auxiliary, async { panic!("denied") }),
        Err(SpawnError::Capacity)
    ));
    assert!(matches!(
        denied
            .downgrade()
            .spawn_tracked(TaskClass::Auxiliary, async { panic!("denied") }),
        Err(SpawnError::Capacity)
    ));
    let (id, witness) = host
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert_eq!(scope.next_exit().await.unwrap().id, id);
    assert!(
        witness.is_joined(),
        "real host retains its original task credit"
    );
}
