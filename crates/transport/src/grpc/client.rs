use super::*;
use crate::profile::OwnedGrpcProfile;
use std::sync::{atomic::AtomicUsize, Arc};
use zero_traits::GrpcTransportProfile;

pub(super) struct Client {
    sender: h2::client::SendRequest<Bytes>,
    driver: Arc<server::Driver>,
    active: Arc<AtomicUsize>,
}
impl Client {
    pub async fn new_with_settings<S>(
        stream: S,
        profile: OwnedGrpcProfile,
        settings: Option<&[u8]>,
    ) -> Result<Self, RuntimeError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let prepared::PreparedConnection {
            sender,
            active,
            driver,
        } = prepared::prepare_connection(stream, profile, settings).await?;
        let driver = tokio::spawn(async move {
            if let Err(error) = driver.await {
                tracing::debug!(%error,"gRPC connection failed");
            }
        });
        Ok(Self {
            sender,
            driver: Arc::new(server::Driver(driver.abort_handle())),
            active,
        })
    }
    pub fn closed(&self) -> bool {
        self.driver.0.is_finished()
    }
    pub async fn open(
        &self,
        profile: &OwnedGrpcProfile,
        authority: &str,
    ) -> Result<GrpcStream, RuntimeError> {
        let request = options::request(profile, authority)?;
        let mut sender = self
            .sender
            .clone()
            .ready()
            .await
            .map_err(io::Error::other)?;
        let (response, send) = sender
            .send_request(request, false)
            .map_err(io::Error::other)?;
        let mut stream = build_grpc_client_stream(send, response, profile.multi_mode);
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stream.active = Some(self.active.clone());
        stream.driver = Some(self.driver.clone());
        Ok(stream)
    }
}
pub async fn connect_grpc<S>(stream: S, names: &[String]) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    connect_grpc_with_profile(stream, &options::legacy(names), "localhost").await
}
pub async fn connect_grpc_with_profile<S>(
    stream: S,
    profile: &(impl GrpcTransportProfile + ?Sized),
    authority: &str,
) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    connect_grpc_with_settings(stream, profile, authority, None).await
}

pub async fn connect_grpc_with_settings<S>(
    stream: S,
    profile: &(impl GrpcTransportProfile + ?Sized),
    authority: &str,
    settings: Option<&[u8]>,
) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let profile = OwnedGrpcProfile::from_profile(profile);
    Client::new_with_settings(stream, profile.clone(), settings)
        .await?
        .open(&profile, authority)
        .await
}
