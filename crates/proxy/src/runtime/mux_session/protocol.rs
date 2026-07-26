use core::future::Future;

use tokio::task::JoinSet;
use zero_core::{InboundMuxServer, Session};
use zero_engine::EngineError;

use super::lifecycle::{finish_mux_tasks, run_mux_session_loop};
use super::model::{MuxOpenedDispatcher, MuxSessionLoop};
use crate::runtime::route_runtime::MuxSubstreamRuntime;

const MUX_SUBSTREAM_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

pub(crate) async fn run_protocol_mux_session<R, S, FTcp, FTcpFut, FUdp, FUdpFut>(
    runtime: MuxSubstreamRuntime,
    reader: R,
    mux_server: S,
    request: MuxSessionLoop<'_>,
    mut spawn_tcp: FTcp,
    mut spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    S: InboundMuxServer<R>,
    FTcp: FnMut(MuxSubstreamRuntime, Session, S::TcpRelay) -> FTcpFut + Send,
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(MuxSubstreamRuntime, S::UdpRelay) -> FUdpFut + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    let mut reader = Some(reader);
    let mut mux_server = Some(mux_server);
    let device_registration = runtime
        .acquire_principal_device(mux_server.as_ref().expect("MUX server is present").auth())?;
    let (principal_cancel_tx, mut principal_cancel_rx) =
        tokio::sync::mpsc::unbounded_channel::<String>();
    let principal_registration = mux_server
        .as_ref()
        .expect("MUX server is present")
        .auth()
        .and_then(|auth| auth.principal_key.as_deref())
        .map(|principal_key| {
            runtime.register_principal_cancellation(principal_key, move |reason| {
                let _ = principal_cancel_tx.send(reason);
            })
        });

    struct OpenedDispatch<'a, R, S, FTcp, FUdp> {
        runtime: &'a MuxSubstreamRuntime,
        mux_server: &'a mut S,
        reader: &'a mut R,
        spawn_tcp: &'a mut FTcp,
        spawn_udp: &'a mut FUdp,
    }

    impl<R, S, FTcp, FTcpFut, FUdp, FUdpFut> MuxOpenedDispatcher
        for OpenedDispatch<'_, R, S, FTcp, FUdp>
    where
        S: InboundMuxServer<R>,
        FTcp: FnMut(MuxSubstreamRuntime, Session, S::TcpRelay) -> FTcpFut + Send,
        FTcpFut: Future<Output = ()> + Send + 'static,
        FUdp: FnMut(MuxSubstreamRuntime, S::UdpRelay) -> FUdpFut + Send,
        FUdpFut: Future<Output = ()> + Send + 'static,
    {
        type Error = EngineError;

        async fn dispatch_next(&mut self, tasks: &mut JoinSet<()>) -> Result<bool, Self::Error> {
            let tasks = std::sync::Mutex::new(tasks);
            let spawn_tcp = &mut self.spawn_tcp;
            let spawn_udp = &mut self.spawn_udp;
            let runtime = self.runtime.clone();
            self.mux_server
                .dispatch_next_opened_route(
                    self.reader,
                    |session, relay| {
                        let mut tasks = tasks.lock().expect("mux task set poisoned");
                        tasks.spawn(spawn_tcp(runtime.clone(), session, relay));
                        Ok::<(), EngineError>(())
                    },
                    |relay| {
                        let mut tasks = tasks.lock().expect("mux task set poisoned");
                        tasks.spawn(spawn_udp(runtime.clone(), relay));
                        Ok::<(), EngineError>(())
                    },
                )
                .await
        }
    }

    let mut mux_tasks = JoinSet::new();
    let abort_on_end = request.abort_on_end;
    let panic_message = request.panic_message;
    let result = {
        let mut dispatcher = OpenedDispatch {
            runtime: &runtime,
            mux_server: mux_server.as_mut().expect("MUX server is present"),
            reader: reader.as_mut().expect("MUX reader is present"),
            spawn_tcp: &mut spawn_tcp,
            spawn_udp: &mut spawn_udp,
        };
        run_mux_session_loop(
            request,
            &mut mux_tasks,
            &mut dispatcher,
            &mut principal_cancel_rx,
        )
        .await
    };
    let _ = mux_server.take();
    let _ = reader.take();
    let graceful_timeout = if abort_on_end {
        MUX_SUBSTREAM_SHUTDOWN_GRACE
    } else {
        std::time::Duration::ZERO
    };
    finish_mux_tasks(&mut mux_tasks, graceful_timeout, panic_message).await;
    drop(principal_registration);
    drop(device_registration);
    result
}
