//! Retained event-delivery resources, retired by the accepted host transaction.
use epoch::Epoch;
use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
#[cfg(feature = "event-dispatcher")]
use zero_connector::{EventSinkTcpConnectFuture, EventSinkTcpDialer};
use zero_platform_tokio::EgressInterfaceControl;
mod epoch;
mod stream;
#[cfg(test)]
mod tests;

#[cfg(feature = "event-dispatcher")]
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) struct ApplicationEventSinkTcpDialer {
    egress: EgressInterfaceControl,
    epoch: Mutex<Option<Arc<Epoch>>>,
    transition: tokio::sync::Mutex<()>,
}
impl ApplicationEventSinkTcpDialer {
    pub(crate) fn new(egress: EgressInterfaceControl) -> Self {
        Self {
            egress,
            epoch: Mutex::new(None),
            transition: tokio::sync::Mutex::new(()),
        }
    }
    fn current(&self) -> io::Result<Arc<Epoch>> {
        self.epoch
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| io::Error::other("event network generation not started"))
    }
}
impl ApplicationEventSinkTcpDialer {
    pub(crate) fn begin_generation(&self) -> io::Result<()> {
        let mut state = self.epoch.lock().unwrap();
        if state.as_ref().is_some_and(|old| !old.closed()) {
            return Err(io::Error::other(
                "original event network has not confirmed retirement",
            ));
        }
        let retired = state.replace(Epoch::new(self.egress.generation()));
        drop(state);
        drop(retired);
        Ok(())
    }
    pub(crate) fn retire_generation(
        &self,
        deadline: Instant,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let _transition = self.transition.lock().await;
            let original = self.current()?;
            original.retire(deadline).await
        })
    }
    #[cfg(feature = "event-dispatcher")]
    fn prepare_request_owned(
        &self,
        deadline: Instant,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let _transition = self.transition.lock().await;
            let original = self.current()?;
            if original.generation != self.egress.generation() {
                original.retire(deadline).await?;
                self.begin_generation()?;
            } else {
                original.reap().await?;
            }
            Ok(())
        })
    }
}
#[cfg(feature = "event-dispatcher")]
impl EventSinkTcpDialer for ApplicationEventSinkTcpDialer {
    fn begin_generation(&self) -> io::Result<()> {
        Self::begin_generation(self)
    }
    fn retire_generation(
        &self,
        deadline: Instant,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        Self::retire_generation(self, deadline)
    }
    fn prepare_request(
        &self,
        deadline: Instant,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>> {
        self.prepare_request_owned(deadline)
    }
    fn spawn_event_task(
        &self,
        task: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
    ) -> io::Result<()> {
        self.current()?.spawn(task).map(|_| ())
    }
    fn drive_event_tasks(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Ok(epoch) = self.current() {
                epoch.drive().await;
            }
        })
    }
    fn connect(&self, host: String, port: u16) -> EventSinkTcpConnectFuture {
        let epoch = self.current();
        let egress = self.egress.clone();
        Box::pin(async move {
            let epoch = epoch?;
            let candidates = epoch.resolve(host, port).await?;
            let mut last = None;
            for candidate in candidates {
                // Ordered attempts intentionally settle the previous original
                // before constructing another candidate under the close gate.
                match epoch
                    .connect(candidate, &egress, Instant::now() + CANDIDATE_TIMEOUT)
                    .await
                {
                    Ok(stream) => {
                        return Ok(Box::new(stream) as Box<dyn zero_connector::EventSinkTcpStream>)
                    }
                    Err(error) => {
                        epoch.reap().await?;
                        if epoch.generation != egress.generation() {
                            return Err(error);
                        }
                        last = Some(error);
                    }
                }
            }
            Err(last.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "webhook host resolved to no addresses",
                )
            }))
        })
    }
}
pub(crate) mod download;
fn unique_candidates(values: impl Iterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut unique = HashSet::new();
    values
        .filter(|candidate| unique.insert(*candidate))
        .collect()
}
#[cfg(test)]
fn select_candidate_egress(
    candidate: SocketAddr,
    egress: &EgressInterfaceControl,
) -> io::Result<Option<zero_platform_tokio::EgressInterface>> {
    let selection = egress.select_for_peer(candidate);
    selection.ensure_connectable()?;
    Ok(selection.interface().cloned())
}
