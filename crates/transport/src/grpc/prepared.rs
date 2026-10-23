//! The original H2/keepalive future before choosing a lifecycle executor.
//! This seam alone does not own the per-logical-stream read/write relays.
use super::{keepalive, *};
use crate::profile::OwnedGrpcProfile;
use std::{
    future::Future,
    sync::{atomic::AtomicUsize, Arc},
};

pub(super) struct PreparedConnection {
    pub(super) sender: h2::client::SendRequest<Bytes>,
    pub(super) active: Arc<AtomicUsize>,
    pub(super) driver: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
}

pub(super) async fn prepare_connection<S>(
    stream: S,
    profile: OwnedGrpcProfile,
    settings: Option<&[u8]>,
) -> Result<PreparedConnection, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let activity = keepalive::Activity::new();
    let stream = keepalive::ObservedIo {
        io: stream,
        activity: activity.clone(),
    };
    let mut builder = h2::client::Builder::new();
    if let Some(settings) = settings {
        builder.peer_application_settings(settings);
    }
    if profile.initial_window_size > 65535 {
        builder.initial_window_size(profile.initial_window_size.min(0x7fff_ffff));
    }
    let (sender, mut connection) = builder.handshake(stream).await.map_err(io::Error::other)?;
    let ping = connection.ping_pong().expect("first ping handle");
    let active = Arc::new(AtomicUsize::new(0));
    let observed = active.clone();
    let driver = Box::pin(async move {
        tokio::select! {
            result=connection=>result.map_err(io::Error::other),
            result=keepalive::run(ping,profile,activity,false,observed)=>result,
        }
    });
    Ok(PreparedConnection {
        sender,
        active,
        driver,
    })
}
