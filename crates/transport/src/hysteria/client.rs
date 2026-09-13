use super::*;
use crate::{quic::QuicConnection, RuntimeError};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
pub struct Client {
    connection: QuicConnection,
    _sender: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    driver: tokio::task::AbortHandle,
}
impl Drop for Client {
    fn drop(&mut self) {
        self.connection.close(0x100u32.into(), b"");
        self.driver.abort();
    }
}
impl Client {
    pub async fn authenticate(
        connection: QuicConnection,
        profile: &Profile,
    ) -> Result<Arc<Self>, RuntimeError> {
        let (mut driver, mut sender) =
            h3::client::new(h3_quinn::Connection::new(connection.clone()))
                .await
                .map_err(io::Error::other)?;
        let task = tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        let request = http::Request::builder()
            .method("POST")
            .uri("https://hysteria/auth")
            .header("hysteria-auth", profile.auth.as_ref())
            .header("hysteria-cc-rx", profile.downlink.to_string())
            .header("hysteria-padding", padding())
            .body(())
            .map_err(io::Error::other)?;
        let mut client = Self {
            connection,
            _sender: sender.clone(),
            driver: task.abort_handle(),
        };
        let auth = async {
            let mut request = sender
                .send_request(request)
                .await
                .map_err(io::Error::other)?;
            request.finish().await.map_err(io::Error::other)?;
            let response = request.recv_response().await.map_err(io::Error::other)?;
            if response.status().as_u16() != 233 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Hysteria carrier authentication rejected",
                ));
            }
            let peer_downlink = response
                .headers()
                .get("hysteria-cc-rx")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            profile.negotiate(&client.connection, peer_downlink);
            Ok(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), auth)
            .await
            .map_err(io::Error::other)??;
        client._sender = sender;
        Ok(Arc::new(client))
    }
    pub async fn open(self: &Arc<Self>) -> Result<super::HysteriaStream, RuntimeError> {
        let (mut send, recv) = self.connection.open_bi().await.map_err(io::Error::other)?;
        send.write_all(&[0x44, 0x01])
            .await
            .map_err(io::Error::other)?;
        Ok(super::HysteriaStream::new(
            send,
            recv,
            &self.connection,
            Some(self.clone()),
        ))
    }
}
#[derive(Clone, Default)]
pub struct Pool(Arc<PoolState>);
#[derive(Default)]
struct PoolState {
    cached: Mutex<Option<Arc<Client>>>,
    connecting: tokio::sync::Mutex<()>,
    retired: AtomicBool,
}
impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HysteriaPool").finish_non_exhaustive()
    }
}
impl Pool {
    pub fn retire(&self) {
        self.0.retired.store(true, Ordering::Release);
        self.0.cached.lock().unwrap().take();
    }
    pub async fn open<F, Fut>(
        &self,
        profile: &Profile,
        connect: F,
    ) -> Result<super::HysteriaStream, RuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<QuicConnection, RuntimeError>>,
    {
        let _guard = self.0.connecting.lock().await;
        let cached = self.0.cached.lock().unwrap().clone();
        if let Some(client) = cached.filter(|client| client.connection.close_reason().is_none()) {
            if let Ok(stream) = client.open().await {
                return Ok(stream);
            }
            // Only the carrier marker was attempted. No tunneled request or
            // application payload has been handed to this logical stream.
            self.0.cached.lock().unwrap().take();
        }
        let client = Client::authenticate(connect().await?, profile).await?;
        if !self.0.retired.load(Ordering::Acquire) {
            let mut cache = self.0.cached.lock().unwrap();
            if !self.0.retired.load(Ordering::Acquire) {
                *cache = Some(client.clone());
            }
        }
        client.open().await
    }
}
