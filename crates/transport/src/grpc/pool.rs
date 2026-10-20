//! A carrier-local pool. Retired plans cannot repopulate a reloaded registry.
use super::{client::Client, GrpcStream};
use crate::{profile::OwnedGrpcProfile, RuntimeError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
#[derive(Clone, Default)]
pub struct GrpcPool(pub(super) Arc<State>);
#[derive(Default)]
pub(super) struct State {
    pub(super) retired: AtomicBool,
    pub(super) cached: Mutex<Option<Cached>>,
    pub(super) connecting: tokio::sync::Mutex<()>,
    pub(super) ownership: Mutex<super::owned::Ownership>,
}
#[derive(Clone)]
pub(super) enum Cached {
    Legacy(Arc<Client>),
    Owned(Arc<super::owned::NativeClient>),
}
impl std::fmt::Debug for GrpcPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcPool").finish_non_exhaustive()
    }
}
impl GrpcPool {
    pub fn retire(&self) {
        self.0.retired.store(true, Ordering::Release);
        if let super::owned::Ownership::Managed(owner) = &*self.0.ownership.lock().unwrap() {
            owner.stop();
        }
        self.0.cached.lock().unwrap().take();
    }
    /// Open a TLS-aware carrier, preserving its authenticated application settings.
    pub async fn open_carrier<F, Fut>(
        &self,
        profile: &OwnedGrpcProfile,
        authority: &str,
        open: F,
    ) -> Result<GrpcStream, RuntimeError>
    where
        F: FnOnce() -> Fut,
        Fut:
            std::future::Future<Output = Result<zero_platform_tokio::TcpRelayStream, RuntimeError>>,
    {
        self.open_with_settings(profile, authority, || async move {
            let stream = open().await?;
            let settings = stream
                .application_settings()
                .filter(|s| s.protocol == b"h2")
                .map(|s| s.peer.clone());
            Ok((stream, settings))
        })
        .await
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
        self.open_with_settings(profile, authority, || async move {
            open().await.map(|stream| (stream, None))
        })
        .await
    }

    pub async fn open_with_settings<S, F, Fut>(
        &self,
        profile: &OwnedGrpcProfile,
        authority: &str,
        open: F,
    ) -> Result<GrpcStream, RuntimeError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(S, Option<Vec<u8>>), RuntimeError>>,
    {
        self.enter_legacy()?;
        super::options::request(profile, authority)?;
        let cached = self.legacy_cached()?;
        if let Some(client) = cached.filter(|client| !client.closed()) {
            if let Ok(stream) = client.open(profile, authority).await {
                return Ok(stream);
            }
            let mut cached = self.0.cached.lock().unwrap();
            if cached.as_ref().is_some_and(
                |current| matches!(current,Cached::Legacy(current) if Arc::ptr_eq(current,&client)),
            ) {
                cached.take();
            }
        }
        let _guard = self.0.connecting.lock().await;
        let cached = self.legacy_cached()?;
        if let Some(client) = cached.filter(|client| !client.closed()) {
            if let Ok(stream) = client.open(profile, authority).await {
                return Ok(stream);
            }
            let mut cached = self.0.cached.lock().unwrap();
            if cached.as_ref().is_some_and(
                |current| matches!(current,Cached::Legacy(current) if Arc::ptr_eq(current,&client)),
            ) {
                cached.take();
            }
        }
        let (stream, settings) = open().await?;
        let client = Arc::new(
            Client::new_with_settings(stream, profile.clone(), settings.as_deref()).await?,
        );
        if !self.0.retired.load(Ordering::Acquire) {
            let mut cached = self.0.cached.lock().unwrap();
            if !self.0.retired.load(Ordering::Acquire) {
                *cached = Some(Cached::Legacy(client.clone()));
            }
        }
        client.open(profile, authority).await
    }
    fn legacy_cached(&self) -> Result<Option<Arc<Client>>, RuntimeError> {
        match self.0.cached.lock().unwrap().as_ref() {
            None => Ok(None),
            Some(Cached::Legacy(client)) => Ok(Some(client.clone())),
            Some(Cached::Owned(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "gRPC pool belongs to its controlled native owner",
            )
            .into()),
        }
    }
}
