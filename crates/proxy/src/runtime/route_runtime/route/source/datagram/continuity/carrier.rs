//! Each physical carrier keeps only its own original protocol control/joins.
use super::*;
use zero_platform_tokio::contract::datagram::DatagramSession;
pub(super) async fn run(
    route: InboundRouteRuntime,
    request: Box<dyn AcceptedDatagram>,
    continuity: DatagramContinuity,
    auth: Option<SessionAuth>,
    cancel: CancellationToken,
    mut reservation: Reservation,
) -> Result<(), EngineError> {
    let tasks = route
        .accepted_tasks()
        .ok_or_else(|| std::io::Error::other("continuity carrier lacks its original task"))?;
    let state = Arc::new(AsyncMutex::new(super::super::owner::State::default()));
    let stop = cancel.child_token();
    let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
    let original = Arc::new(
        tasks
            .caller()
            .map_err(std::io::Error::other)?
            .retain_logical_resource(erase_lifecycle(super::super::owner::Owner {
                state: state.clone(),
                cancel: stop.clone(),
                returns,
                retired: route
                    .tcp_runtime
                    .runtime_services()
                    .execution()
                    .retired_datagrams(),
                peer_retirement: None,
            }))
            .map_err(std::io::Error::other)?,
    );
    let retained = original.clone();
    let (complete, completed) = oneshot::channel();
    tasks
        .spawn(TaskClass::Critical, move |driver| {
            let binding = retained.bind_driver(&driver);
            async move {
                binding?;
                let _cancel_on_exit = stop.clone().drop_guard();
                let result = async {
                    let session = request
                        .respond(ReplyOutcome::LocalChannelReady)
                        .await
                        .map_err(std::io::Error::other)?
                        .ok_or_else(|| {
                            std::io::Error::other("continuity Datagram omitted channel")
                        })?;
                    let zero_platform_tokio::contract::inbound::InboundDatagramSession {
                        io,
                        negotiation,
                    } = session;
                    let DatagramSession {
                        tx,
                        rx,
                        control,
                        limits,
                    } = io;
                    state.lock().await.control = Some(control);
                    let (done, finished) = oneshot::channel();
                    reservation
                        .association
                        .sender
                        .send(Attachment {
                            tx,
                            rx,
                            limits,
                            negotiation,
                            state: state.clone(),
                            continuity,
                            auth,
                            source: route.source_addr(),
                            cancel: stop.clone(),
                            done,
                        })
                        .await
                        .map_err(|_| std::io::Error::other("original continuity owner stopped"))?;
                    reservation.submitted = true;
                    finished.await.map_err(std::io::Error::other)?
                }
                .await;
                let _ = complete.send(result);
                Ok(())
            }
        })
        .map_err(std::io::Error::other)?;
    let result = completed.await.map_err(std::io::Error::other)?;
    if !original
        .close(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .map_err(std::io::Error::other)?
    {
        return Err(
            std::io::Error::other("continuity carrier original cleanup remains pending").into(),
        );
    }
    result
}
