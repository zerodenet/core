//! The actual H2 connection and hunk I/O run in one original accepted Scope task.
use super::*;
use crate::profile::OwnedGrpcProfile;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use zero_platform_tokio::{
    contract::{
        scope::TaskClass, BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    AcceptedCaller,
};

pub(super) struct Stop(pub CancellationToken);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.cancel();
    }
} // Request only, never close proof.
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}

pub async fn accept_grpc_with_profile_scoped<S>(
    stream: S,
    profile: &(impl zero_traits::GrpcTransportProfile + ?Sized),
    caller: &AcceptedCaller,
) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let incoming = accept_grpc_connection_scoped(stream, profile, caller)?;
    let mut stream = incoming
        .accept()
        .await
        .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
    stream.scoped = incoming.scoped.clone();
    stream.incoming = Some(incoming);
    Ok(stream)
}
/// The complete logical-stream source, backed by the same single original H2 task.
pub fn accept_grpc_connection_scoped<S>(
    stream: S,
    profile: &(impl zero_traits::GrpcTransportProfile + ?Sized),
    caller: &AcceptedCaller,
) -> Result<GrpcIncoming, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    require_grpc_service_names(profile.service_names(), "inbound")?;
    let profile = OwnedGrpcProfile::from_profile(profile);
    let cancel = CancellationToken::new();
    let stopping = cancel.clone();
    let (tx, rx) = mpsc::channel(64);
    caller
        .spawn(TaskClass::Auxiliary, move |_| {
            drive(stream, profile, tx, stopping)
        })
        .map_err(|source| RuntimeError::Io(io::Error::other(source)))?;
    Ok(server::GrpcIncoming {
        receiver: tokio::sync::Mutex::new(rx),
        driver: None,
        scoped: Some(Arc::new(Stop(cancel))),
    })
}
async fn drive<S>(
    stream: S,
    profile: OwnedGrpcProfile,
    tx: mpsc::Sender<GrpcStream>,
    cancel: CancellationToken,
) -> Result<(), ChannelError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let activity = keepalive::Activity::new();
    let io = keepalive::ObservedIo {
        io: stream,
        activity: activity.clone(),
    };
    let mut builder = h2::server::Builder::new();
    builder.max_concurrent_streams(1024);
    let mut conn = tokio::select! { biased;
        _=cancel.cancelled()=>return Ok(()),
        result=builder.handshake(io)=>result.map_err(failure)?,
    };
    let active = Arc::new(AtomicUsize::new(0));
    let ping = conn.ping_pong().expect("one original ping handle");
    let alive = keepalive::run(ping, profile.clone(), activity, true, active.clone());
    tokio::pin!(alive);
    let mut io_jobs = FuturesUnordered::<BoxFuture<'static, ()>>::new();
    loop {
        let incoming = tokio::select! { biased;
            _=cancel.cancelled()=>return Ok(()),
            _=tx.closed()=>return Ok(()),
            _=&mut alive=>return Ok(()),
            _=io_jobs.next(),if !io_jobs.is_empty()=>continue,
            incoming=conn.accept()=>incoming,
        };
        let Some(incoming) = incoming else {
            return Ok(());
        };
        let (request, mut respond) = incoming.map_err(failure)?;
        let allowed = options::matches(&profile, &request);
        if !allowed || tx.capacity() == 0 || io_jobs.len() >= 130 {
            let response = Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("grpc-status", if allowed { "8" } else { "12" })
                .body(())
                .unwrap();
            let _ = respond.send_response(response, true);
            continue;
        }
        let multi = profile
            .service_names
            .iter()
            .any(|name| options::service_path(name, true) == request.uri().path());
        let response = Response::builder()
            .header("content-type", "application/grpc")
            .body(())
            .unwrap();
        let sender = match respond.send_response(response, false) {
            Ok(sender) => sender,
            Err(_) => continue,
        };
        let (write_tx, write_rx) = mpsc::channel(16);
        let (read_tx, read_rx) = mpsc::channel(64);
        let progress = Arc::new(progress::Progress::default());
        io_jobs.push(Box::pin(write::write_grpc_hunks(
            sender,
            write_rx,
            progress.clone(),
            true,
            None,
        )));
        io_jobs.push(Box::pin(read_grpc_hunks(
            request.into_body(),
            read_tx,
            multi,
            false,
        )));
        let mut stream = GrpcStream::new(
            read_rx,
            tokio_util::sync::PollSender::new(write_tx),
            progress,
        );
        active.fetch_add(1, Ordering::Relaxed);
        stream.active = Some(active.clone());
        // A pending stream never blocks the original task from driving H2 and existing hunk I/O.
        let _ = tx.try_send(stream);
    }
}
