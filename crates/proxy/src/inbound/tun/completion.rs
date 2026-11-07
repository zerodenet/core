//! Compatibility control for the same actual System TUN Source and its receipts.
use std::{io, sync::Arc};
use tokio::time::Instant;
#[cfg(any(test, feature = "raw-ip-runtime"))]
use tokio::{sync::Mutex, time::timeout_at};

#[cfg(test)]
mod legacy;
#[cfg(feature = "raw-ip-runtime")]
mod source;

type Source = Arc<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub(crate) struct TunCompletion {
    #[cfg(feature = "raw-ip-runtime")]
    source: Option<source::SourceCompletion>,
    #[cfg(feature = "raw-ip-runtime")]
    restart: std::sync::OnceLock<Arc<super::managed::Start>>,
    #[cfg(test)]
    legacy: legacy::LegacyCompletion,
}
impl TunCompletion {
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::inbound::tun) fn install_restart(&self, start: Arc<super::managed::Start>) {
        assert!(
            self.restart.set(start).is_ok(),
            "one original immutable TUN restart intent"
        );
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::inbound::tun) fn restart(&self) -> Option<Arc<super::managed::Start>> {
        self.restart.get().cloned()
    }
    #[cfg(test)]
    pub(super) fn new(
        done: tokio::sync::oneshot::Receiver<()>,
        routes: Option<tokio::sync::oneshot::Receiver<Result<(), String>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            #[cfg(feature = "raw-ip-runtime")]
            source: None,
            #[cfg(feature = "raw-ip-runtime")]
            restart: std::sync::OnceLock::new(),
            legacy: legacy::LegacyCompletion::new(Some(done), routes),
        })
    }
    #[cfg(test)]
    pub(super) fn install_main(
        &self,
        task: tokio::task::JoinHandle<Result<(), zero_engine::EngineError>>,
    ) {
        self.legacy.install_main(task);
    }
    #[cfg(test)]
    pub(super) fn install_routes(&self, task: tokio::task::JoinHandle<io::Result<()>>) {
        self.legacy.install_routes(task);
    }
    pub(super) async fn resources_complete(&self) -> bool {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(source) = &self.source {
            return source.physically_closed();
        }
        #[cfg(test)]
        {
            self.legacy.resources_complete().await
        }
        #[cfg(not(test))]
        {
            false
        }
    }
    pub(super) async fn wait_main(&self, deadline: Instant) -> io::Result<()> {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(source) = &self.source {
            return source.wait_closed(deadline).await;
        }
        #[cfg(test)]
        {
            self.legacy.wait_main(deadline).await
        }
        #[cfg(not(test))]
        {
            let _ = deadline;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN completion requires its actual Source owner",
            ))
        }
    }
    pub(super) async fn wait_routes(&self, deadline: Instant) -> io::Result<()> {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(source) = &self.source {
            return source.wait_resources_closed(deadline).await;
        }
        #[cfg(test)]
        {
            self.legacy.wait_routes(deadline).await
        }
        #[cfg(not(test))]
        {
            let _ = deadline;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN completion requires its actual Source owner",
            ))
        }
    }
    pub(super) async fn wait_native(&self, deadline: Instant) -> io::Result<()> {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(source) = &self.source {
            return source.wait_resources_closed(deadline).await;
        }
        #[cfg(test)]
        {
            self.legacy.wait_native(deadline).await
        }
        #[cfg(not(test))]
        {
            let _ = deadline;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN completion requires its actual Source owner",
            ))
        }
    }
}
pub(super) fn join_results(main: io::Result<()>, routes: io::Result<()>) -> io::Result<()> {
    let main = main.map_err(|source| Arc::new(source) as Source);
    let routes = routes.map_err(|source| Arc::new(source) as Source);
    combine(Some(&main), Some(&routes))
}
fn combine(
    notification: Option<&Result<(), Source>>,
    joined: Option<&Result<(), Source>>,
) -> io::Result<()> {
    let failures = [notification, joined]
        .into_iter()
        .flatten()
        .filter_map(|r| r.as_ref().err())
        .cloned()
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(Failures(failures)))
    }
}
#[derive(Debug)]
struct Failures(Vec<Source>);
impl std::fmt::Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source.as_ref() as _)
    }
}

#[cfg(test)]
mod tests;
