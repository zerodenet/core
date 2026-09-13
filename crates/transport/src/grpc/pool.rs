//! A carrier-local pool. Retired plans cannot repopulate a reloaded registry.
use super::{client::Client, GrpcStream};
use crate::{profile::OwnedGrpcProfile, RuntimeError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
#[derive(Clone, Default)]
pub struct GrpcPool(Arc<State>);
#[derive(Default)]
struct State {
    retired: AtomicBool,
    cached: Mutex<Option<Arc<Client>>>,
    connecting: tokio::sync::Mutex<()>,
}
impl std::fmt::Debug for GrpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcPool").finish_non_exhaustive()
    }
}
impl GrpcPool {
    pub fn retire(&self) {
        self.0.retired.store(true, Ordering::Release);
        self.0.cached.lock().unwrap().take();
    }
    pub async fn open<S, F, Fut>(
        &self,
        profile: &OwnedGrpcProfile,
        authority: &str,
        open: F,
    ) -> Result<GrpcStream, RuntimeError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<S, RuntimeError>>,
    {
        super::options::request(profile, authority)?;
        let cached = self.0.cached.lock().unwrap().clone();
        if let Some(client) = cached.filter(|client| !client.closed()) {
            if let Ok(stream) = client.open(profile, authority).await {
                return Ok(stream);
            }
            let mut cached = self.0.cached.lock().unwrap();
            if cached
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &client))
            {
                cached.take();
            }
        }
        let _guard = self.0.connecting.lock().await;
        let cached = self.0.cached.lock().unwrap().clone();
        if let Some(client) = cached.filter(|client| !client.closed()) {
            if let Ok(stream) = client.open(profile, authority).await {
                return Ok(stream);
            }
            let mut cached = self.0.cached.lock().unwrap();
            if cached
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &client))
            {
                cached.take();
            }
        }
        let client = Arc::new(Client::new(open().await?, profile.clone()).await?);
        if !self.0.retired.load(Ordering::Acquire) {
            let mut cached = self.0.cached.lock().unwrap();
            if !self.0.retired.load(Ordering::Acquire) {
                *cached = Some(client.clone());
            }
        }
        client.open(profile, authority).await
    }
}
