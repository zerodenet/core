//! Commands run in the same real Source task that opened and receives the flow.
use super::*;
use crate::runtime::udp_flow::sessions::CompletedUdpFlow;
use tokio::sync::{mpsc, oneshot};
use zero_engine::EngineError;

enum Command {
    Send {
        identity: u64,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<u64, EngineError>>,
    },
    Retire(oneshot::Sender<Vec<CompletedUdpFlow>>),
    Cancellations(oneshot::Sender<bool>),
    Finish(oneshot::Sender<Vec<CompletedUdpFlow>>),
}
pub(crate) struct Flow {
    session: u64,
    commands: mpsc::Sender<Command>,
    worker: tokio::task::JoinHandle<()>,
}
impl Flow {
    pub(crate) fn session(&self) -> u64 {
        self.session
    }
    /// Return business views while the original Source and its retained RX owner remain live.
    pub(crate) async fn retire(&mut self) -> Vec<CompletedUdpFlow> {
        let (reply, receive) = oneshot::channel();
        self.commands.send(Command::Retire(reply)).await.unwrap();
        receive.await.unwrap()
    }
    pub(crate) async fn finish_pending_cancellations(&mut self) -> bool {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Cancellations(reply))
            .await
            .unwrap();
        receive.await.unwrap()
    }

    pub(crate) async fn send(&mut self, identity: u64, payload: &[u8]) -> Result<u64, EngineError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(Command::Send {
                identity,
                payload: payload.to_vec(),
                reply,
            })
            .await
            .unwrap();
        receive.await.unwrap()
    }
    pub(crate) async fn finish_all(self) -> Vec<CompletedUdpFlow> {
        let (reply, receive) = oneshot::channel();
        self.commands.send(Command::Finish(reply)).await.unwrap();
        let flows = receive.await.unwrap();
        self.worker.await.unwrap();
        flows
    }
}
pub(crate) async fn open(proxy: Proxy, identity: u64, payload: &'static [u8]) -> Flow {
    let (commands, mut receive) = mpsc::channel(1);
    let (ready, started) = oneshot::channel();
    let worker = tokio::spawn(async move {
        crate::runtime::udp_ingress::test_source::run(&proxy, move |proxy, source| async move {
            let mut dispatch = source.new_dispatch("incoming").await.unwrap();
            let mut packet = input(53, payload);
            packet.client_session_id = Some(identity);
            let session = dispatch.dispatch(packet).await.unwrap();
            let packet = response(&mut dispatch).await;
            assert_eq!(packet.2.as_ref(), payload);
            crate::runtime::udp_delivery::record_chain_udp_response_parts(
                &proxy.udp_runtime_services(),
                &dispatch,
                packet.0,
                packet.1,
                packet.2,
                packet.3,
            );
            ready.send(session).unwrap();
            let mut dispatch = Some(dispatch);
            while let Some(command) = receive.recv().await {
                match command {
                    Command::Send {
                        identity,
                        payload,
                        mut reply,
                    } => {
                        let mut packet = input(53, &payload);
                        packet.client_session_id = Some(identity);
                        // Cancel in the same original Source task when this
                        // exact awaited command is abandoned. The installed
                        // owner still retains the original pending operation.
                        let result = tokio::select! { biased;
                            _ = reply.closed() => continue,
                            result = dispatch.as_mut().expect("actual unretired association")
                                .dispatch(packet) => result,
                        };
                        let _ = reply.send(result);
                    }
                    Command::Retire(reply) => {
                        let completed = dispatch
                            .take()
                            .map(|actual| actual.finish_all())
                            .unwrap_or_default();
                        assert!(reply.send(completed).is_ok());
                    }
                    Command::Cancellations(reply) => {
                        let pending = dispatch
                            .as_mut()
                            .is_some_and(|actual| actual.finish_pending_cancellations());
                        reply.send(pending).unwrap();
                    }
                    Command::Finish(reply) => {
                        let completed = dispatch
                            .take()
                            .map(|actual| actual.finish_all())
                            .unwrap_or_default();
                        assert!(reply.send(completed).is_ok());
                        return;
                    }
                }
            }
            // Lost test custody requests the same retained owner to clean up;
            // it is never interpreted as a successful native-close receipt.
            if let Some(dispatch) = dispatch {
                dispatch.finish_all();
            }
        })
        .await;
    });
    let session = started.await.unwrap();
    Flow {
        session,
        commands,
        worker,
    }
}
