//! One new Client and one logical stream, with the original futures unspawned.
use super::{options, prepared, prepared_stream, GrpcStream};
use crate::{profile::OwnedGrpcProfile, RuntimeError};
use std::{future::Future, io, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite};

pub type SingleClientDriver = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

/// Validate every configured service without selecting the actual request yet.
pub fn validate_single_client_profile(
    profile: &OwnedGrpcProfile,
    authority: &str,
) -> Result<(), RuntimeError> {
    super::require_grpc_service_names(&profile.service_names, "outbound")?;
    for name in &profile.service_names {
        let mut single = profile.clone();
        single.service_names = vec![name.clone()];
        options::request(&single, authority)?;
    }
    Ok(())
}

pub struct PreparedSingleClient {
    stream: GrpcStream,
    driver: SingleClientDriver,
}
impl PreparedSingleClient {
    pub async fn prepare<S>(
        io: S,
        profile: &OwnedGrpcProfile,
        authority: &str,
        peer_settings: Option<&[u8]>,
    ) -> Result<Self, RuntimeError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let request = options::request(profile, authority)?;
        let prepared::PreparedConnection {
            sender,
            mut driver,
            active,
        } = prepared::prepare_connection(io, profile.clone(), peer_settings).await?;
        // The original H2 future must also progress if readiness waits for SETTINGS.
        let ready = sender.ready();
        tokio::pin!(ready);
        let mut sender = tokio::select! {
            result=&mut ready=>result.map_err(io::Error::other)?,
            result=driver.as_mut()=>{
                result?;
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof,"gRPC connection ended before stream readiness").into());
            }
        };
        let (response, send) = sender
            .send_request(request, false)
            .map_err(io::Error::other)?;
        let prepared_stream::PreparedStream {
            mut stream,
            read,
            write,
        } = prepared_stream::prepare_stream(send, response, profile.multi_mode);
        active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stream.active = Some(active);
        let driver = Box::pin(async move {
            // This is one real task body, not three hidden Tokio tasks. A local
            // END_STREAM finishes only write; read and H2 stay alive in reverse.
            let workers = async {
                tokio::join!(read, write);
            };
            tokio::pin!(workers);
            tokio::select! {
                result=driver.as_mut()=>result,
                _=&mut workers=>{
                    drop(sender);
                    driver.await
                }
            }
        });
        Ok(Self { stream, driver })
    }
    pub fn into_parts(self) -> (GrpcStream, SingleClientDriver) {
        (self.stream, self.driver)
    }
}
