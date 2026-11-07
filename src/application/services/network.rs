//! Retained event-delivery resources, retired by the accepted host transaction.
use epoch::{Epoch, EpochLimits};
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
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::EgressInterfaceControl;
use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;
mod epoch;
mod stream;
#[cfg(test)]
mod tests;

#[cfg(feature = "event-dispatcher")]
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) struct ApplicationEventSinkTcpDialer {
    egress: EgressInterfaceControl,
    epoch: Mutex<Epochs>,
    instance: ComponentInstanceId,
    originals: Arc<CarrierOriginalTotals>,
    limits: Mutex<EpochLimits>,
    transition: tokio::sync::Mutex<()>,
}
struct Epochs {
    current: Option<Arc<Epoch>>,
    next: Option<ResourceGeneration>,
}
impl ApplicationEventSinkTcpDialer {
    pub(crate) fn new(
        egress: EgressInterfaceControl,
        instance: ComponentInstanceId,
        originals: Arc<CarrierOriginalTotals>,
    ) -> Self {
        Self {
            egress,
            epoch: Mutex::new(Epochs {
                current: None,
                next: Some(instance.generation),
            }),
            instance,
            originals,
            limits: Mutex::new(EpochLimits::default()),
            transition: tokio::sync::Mutex::new(()),
        }
    }
    #[cfg(test)]
    pub(crate) fn for_test(egress: EgressInterfaceControl) -> Self {
        let original = zero_proxy::ProxyPreparation::new();
        let (instance, totals) = original.application_network_originals().unwrap();
        Self::new(egress, instance, totals)
    }
    fn current(&self) -> io::Result<Arc<Epoch>> {
        self.epoch
            .lock()
            .unwrap()
            .current
            .clone()
            .ok_or_else(|| io::Error::other("event network generation not started"))
    }
}
impl ApplicationEventSinkTcpDialer {
    /// Actual same Epoch native/controller/Scope closure, independent of the
    /// original failure still returned to its application receiver.
    pub(crate) fn retirement_complete(&self) -> bool {
        self.epoch
            .lock()
            .unwrap()
            .current
            .as_ref()
            .is_none_or(|epoch| epoch.closed())
    }
    /// Cold metadata change only. A live original Epoch must first complete
    /// its actual native/user/Scope retirement even if counters fit new limits.
    pub(crate) fn configure_limits(&self, target: EpochLimits) -> io::Result<()> {
        let state = self.epoch.lock().unwrap();
        let mut current = self.limits.lock().unwrap();
        if *current != target && state.current.as_ref().is_some_and(|epoch| !epoch.closed()) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual event Epoch still owns resources",
            ));
        }
        *current = target;
        Ok(())
    }
    pub(crate) fn begin_generation(&self) -> io::Result<()> {
        let mut state = self.epoch.lock().unwrap();
        if state.current.as_ref().is_some_and(|old| !old.closed()) {
            return Err(io::Error::other(
                "original event network has not confirmed retirement",
            ));
        }
        let generation = state
            .next
            .take()
            .ok_or_else(|| io::Error::other("actual event resource generation exhausted"))?;
        // Consume attempted generations even when cold binding fails. Network
        // generation is egress fencing and never substitutes resource identity.
        state.next = generation.0.checked_add(1).map(ResourceGeneration);
        let instance = ComponentInstanceId {
            component: self.instance.component,
            generation,
        };
        let next = Epoch::new(
            self.egress.generation(),
            *self.limits.lock().unwrap(),
            instance,
            self.originals.clone(),
        )?;
        let retired = state.current.replace(next);
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
            if original.closed() {
                // The previous caller received the retained original fault.
                // Only actual native/users/Scope confirmation permits recovery.
                self.begin_generation()?;
            } else {
                original.reap().await?;
                if original.generation != self.egress.generation() || original.needs_retirement() {
                    // Return the first actual fault to this receiver after all
                    // cleanup. A subsequent ordinary request can use closed().
                    original.retire(deadline).await?;
                    self.begin_generation()?;
                }
            }
            Ok(())
        })
    }
}
#[cfg(feature = "event-dispatcher")]
impl EventSinkTcpDialer for ApplicationEventSinkTcpDialer {
    fn configure_limits(&self, limits: EpochLimits) -> io::Result<()> {
        Self::configure_limits(self, limits)
    }
    fn retirement_complete(&self) -> bool {
        Self::retirement_complete(self)
    }

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
                        return Ok(Box::new(stream) as Box<dyn zero_connector::EventSinkTcpStream>);
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
