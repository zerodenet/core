//! Bounded commands execute in the original real accepted Source task.
use crate::{
    runtime::{pipe::UdpPipeInput, udp_flow::sessions::CompletedUdpFlow},
    Proxy,
};
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot};
use zero_core::{Address, SessionAuth};
use zero_engine::EngineError;

type Packet = (Address, u16, zero_traits::PacketBuffer, Option<u64>);
struct Input {
    facts: UdpPipeInput<'static>,
    bytes: Vec<u8>,
    auth: Option<SessionAuth>,
}
impl Input {
    fn own(packet: UdpPipeInput<'_>) -> Self {
        let bytes = packet.payload.to_vec();
        let auth = packet.auth.cloned();
        Self {
            facts: UdpPipeInput {
                payload: &[],
                auth: None,
                ..packet
            },
            bytes,
            auth,
        }
    }
}
enum Command {
    Dispatch(Box<Input>, oneshot::Sender<Result<u64, EngineError>>),
    Response(oneshot::Sender<Packet>),
    Record(Packet, oneshot::Sender<()>),
    Finish(oneshot::Sender<Vec<CompletedUdpFlow>>),
}
#[derive(Clone)]
pub(super) struct PendingDispatch(Arc<AtomicBool>);
impl PendingDispatch {
    // The original Source sets this only after polling the actual dispatch
    // future Pending. Tests additionally retain the real peer's withheld wire
    // response; a command channel wait alone is never the suspension proof.
    pub(super) async fn wait(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !self.0.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same actual Source dispatch must be Pending after the peer withheld its reply");
    }
}
struct ClearPending(Arc<AtomicBool>);
impl Drop for ClearPending {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub(super) struct Dispatch {
    pending: PendingDispatch,
    commands: mpsc::Sender<Command>,
    worker: tokio::task::JoinHandle<()>,
}
impl Dispatch {
    pub(super) async fn new(proxy: &Proxy, inbound: &str) -> Result<Self, EngineError> {
        let proxy = proxy.clone();
        let inbound = inbound.to_owned();
        let (commands, mut receive) = mpsc::channel(1);
        let (ready, started) = oneshot::channel();
        let pending = PendingDispatch(Arc::new(AtomicBool::new(false)));
        let actual_pending = pending.clone();
        let worker = tokio::spawn(async move {
            crate::runtime::udp_ingress::test_source::run(&proxy, move |proxy, source| async move {
                let mut dispatch = match source.new_dispatch(&inbound).await {
                    Ok(dispatch) => dispatch,
                    Err(error) => { assert!(ready.send(Err(error)).is_ok()); return; }
                };
                assert!(ready.send(Ok(())).is_ok());
                while let Some(command) = receive.recv().await {
                    match command {
                        Command::Dispatch(input, mut reply) => {
                            let Input { facts, bytes, auth } = *input;
                            let packet = UdpPipeInput { payload: &bytes, auth: auth.as_ref(), ..facts };
                            // Dropping the awaited command cancels this exact
                            // operation in its real Source task. The installed
                            // Source owner retains every original for cleanup.
                            let _clear = ClearPending(actual_pending.0.clone());
                            let mut opening = Box::pin(dispatch.dispatch(packet));
                            let observed = poll_fn(|cx| {
                                let result = opening.as_mut().poll(cx);
                                actual_pending.0.store(result.is_pending(), Ordering::Release);
                                result
                            });
                            let result = tokio::select! { biased;
                                _ = reply.closed() => continue,
                                result = observed => result,
                            };
                            let _ = reply.send(result);
                        }
                        Command::Response(mut reply) => {
                            let packet = tokio::select! { biased;
                                _ = reply.closed() => continue,
                                packet = super::super::fixtures::response(&mut dispatch) => packet,
                            };
                            let _ = reply.send(packet);
                        }
                        Command::Record(packet, reply) => {
                            crate::runtime::udp_delivery::record_chain_udp_response_parts(
                                &proxy.udp_runtime_services(), &dispatch,
                                packet.0, packet.1, packet.2, packet.3,
                            );
                            let _ = reply.send(());
                        }
                        Command::Finish(reply) => {
                            let completed = dispatch.finish_all();
                            let _ = reply.send(completed);
                            return;
                        }
                    }
                }
                // A lost external waiter only ends custody. The same native
                // Source owner still joins/receives its originals below.
                dispatch.finish_all();
            }).await;
        });
        match started.await.expect("actual Source start result") {
            Ok(()) => Ok(Self {
                pending,
                commands,
                worker,
            }),
            Err(error) => {
                worker.await.unwrap();
                Err(error)
            }
        }
    }
    pub(super) fn operation_observation(&self) -> PendingDispatch {
        self.pending.clone()
    }
    pub(super) async fn dispatch(&mut self, packet: UdpPipeInput<'_>) -> Result<u64, EngineError> {
        let input = Input::own(packet);
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Dispatch(Box::new(input), reply))
            .await
            .unwrap_or_else(|_| panic!("actual Source exited before dispatch"));
        receive.await.expect("actual Source dispatch result")
    }
    pub(super) async fn response(&mut self) -> Packet {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Response(reply))
            .await
            .unwrap_or_else(|_| panic!("actual Source exited before response"));
        receive.await.expect("actual Source response result")
    }
    pub(super) async fn record(&mut self, packet: Packet) {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Record(packet, reply))
            .await
            .unwrap_or_else(|_| panic!("actual Source exited before accounting"));
        receive.await.expect("actual Source accounting completed");
    }
    pub(super) async fn finish_all(self) -> Vec<CompletedUdpFlow> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Finish(reply))
            .await
            .unwrap_or_else(|_| panic!("actual Source exited before retirement"));
        let completed = receive
            .await
            .expect("actual Source returned business views");
        // The original accepted controller/Scope and all transferred originals
        // must settle through test_source::run before callers inspect C.
        self.worker.await.unwrap();
        completed
    }
}
