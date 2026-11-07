use super::*;
use crate::profile::OwnedGrpcProfile;
use std::sync::{atomic::AtomicUsize, Arc};
use zero_traits::GrpcTransportProfile;

pub(super) struct Driver(pub tokio::task::AbortHandle);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub struct GrpcIncoming {
    pub(super) receiver: tokio::sync::Mutex<mpsc::Receiver<GrpcStream>>,
    pub(super) driver: Option<Arc<Driver>>,
    pub(super) scoped: Option<Arc<super::scoped_server::Stop>>,
}
impl GrpcIncoming {
    pub async fn accept(&self) -> Option<GrpcStream> {
        self.receiver.lock().await.recv().await
    }
    pub fn close(&self) {
        if let Some(driver) = &self.driver {
            driver.0.abort();
        }
        if let Some(stop) = &self.scoped {
            stop.0.cancel();
        }
    }
}
pub fn accept_grpc_connection<S>(
    stream: S,
    profile: &(impl GrpcTransportProfile + ?Sized),
) -> Result<GrpcIncoming, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    require_grpc_service_names(profile.service_names(), "inbound")?;
    let profile = OwnedGrpcProfile::from_profile(profile);
    let (tx, rx) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        let flush = Arc::new(crate::native_flush::Flushed::default());
        let _guard = crate::native_flush::DriverGuard(flush.clone());
        let activity = keepalive::Activity::new();
        let io = keepalive::ObservedIo {
            io: stream,
            activity: activity.clone(),
            flush: crate::native_flush::NativeFlush::new(flush.clone()),
        };
        let mut builder = h2::server::Builder::new();
        builder.max_concurrent_streams(1024);
        let mut conn = match builder.handshake(io).await {
            Ok(conn) => conn,
            Err(_) => return,
        };
        let active = Arc::new(AtomicUsize::new(0));
        let ping = conn.ping_pong().expect("first ping handle");
        let alive = keepalive::run(ping, profile.clone(), activity, true, active.clone());
        tokio::pin!(alive);
        loop {
            let request = tokio::select! { _ = tx.closed()=>break, result = &mut alive => {if let Err(source)=result {let _=flush.fail(source);}break;}, request = conn.accept()=>request };
            let (request, mut respond) = match request {
                Some(Ok(incoming)) => incoming,
                Some(Err(error)) => {
                    let _ = flush.finish(Err(io::Error::other(error)));
                    break;
                }
                None => {
                    let _ = flush.finish(Ok(()));
                    break;
                }
            };
            if !options::matches(&profile, &request) {
                let response = Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "12")
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
            let Ok(sender) = respond.send_response(response, false) else {
                continue;
            };
            let Ok(mut stream) =
                build_grpc_stream(sender, request.into_body(), multi, flush.clone())
            else {
                continue;
            };
            active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            stream.active = Some(active.clone());
            // Never await admission while the same task must drive H2.
            if tx.try_send(stream).is_err() {
                tracing::debug!("gRPC pending stream limit reached");
            }
        }
    });
    Ok(GrpcIncoming {
        receiver: tokio::sync::Mutex::new(rx),
        driver: Some(Arc::new(Driver(task.abort_handle()))),
        scoped: None,
    })
}
pub async fn accept_grpc<S>(stream: S, names: &[String]) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    accept_grpc_with_profile(stream, &options::legacy(names)).await
}
pub async fn accept_grpc_with_profile<S>(
    stream: S,
    profile: &(impl GrpcTransportProfile + ?Sized),
) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let incoming = accept_grpc_connection(stream, profile)?;
    let mut stream = incoming
        .accept()
        .await
        .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
    // Legacy callers consume one stream. Keep the driver alive while this
    // stream drains; multiplexed callers retain the connection entrypoint.
    stream.driver = incoming.driver.clone();
    stream.incoming = Some(incoming);
    Ok(stream)
}
pub async fn serve_grpc<S, H, F>(
    stream: S,
    names: &[String],
    mut handler: H,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: FnMut(GrpcStream) -> F + Send + 'static,
    F: std::future::Future<Output = Result<(), RuntimeError>> + Send + 'static,
{
    let incoming = accept_grpc_connection(stream, &options::legacy(names))?;
    while let Some(stream) = incoming.accept().await {
        tokio::spawn(handler(stream));
    }
    Ok(())
}
